"""Results-page consistency: one total, one order, one count — at any scale.

The results page shows three views of the same data — the header total, the
per-partition counts, and the preview records.  These tests pin the invariant
that all three derive from a single ordered snapshot, so they agree for any
result size, any partition count, and with other jobs writing concurrently.
They also cover the pipeline bugs that silently dropped records (the last
reduce group of each partition, the last record of each input shard) and the
fudged statistics that made pages contradict each other.
"""

import collections
import shutil
import tempfile
import unittest
from unittest import mock

from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.results import build_results_payload, load_result_snapshot
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator
from backend.tasks.samples import generate_input_records
from backend.worker.executor import _run_map, _run_reduce
from backend.worker.shuffle_store import ShuffleStore, partition_filename


def make_scheduler(tmp):
    storage = Storage(tmp)
    logbus = LogBus(storage)
    config = ClusterConfig()
    jm = JobManager(storage, config, logbus)
    registry = WorkerRegistry(storage, config)
    shuffle = ShuffleCoordinator(storage, jm, registry, logbus)
    ft = FaultTolerance(storage, jm, config, logbus)
    metrics = Metrics(storage)
    sched = Scheduler(storage, jm, registry, shuffle, ft, metrics, config, logbus)
    return sched, storage, jm


class TestReduceKeepsLastGroup(unittest.TestCase):
    """_run_reduce must emit the final key group, not just the leading ones."""

    def test_last_group_is_not_dropped(self):
        tmp = tempfile.mkdtemp()
        pairs = [["b", 1], ["a", 1], ["b", 1], ["z", 5]]
        url = "http://w/shuffle/job/m-0000/part-0000.jsonl"
        client = mock.Mock()
        client.get_json.return_value = pairs
        spec = {
            "task_id": "r-0000", "job_id": "job", "kind": "reduce",
            "reducer": "count_reducer", "params": {}, "partition": 0,
            "fetch_plan": [{"worker_url": "http://w", "map_task_id": "m-0000"}],
            "spill_records": 2, "tmp_dir": tmp,
        }
        with mock.patch("backend.worker.executor.HttpClient", lambda *a, **k: client):
            out = _run_reduce(spec, lambda *a: None)
        by_key = {r["key"]: r["count"] for r in out["results"]}
        self.assertEqual(by_key, {"a": 1, "b": 2, "z": 5})
        self.assertEqual(out["records_emitted"], 3)
        shutil.rmtree(tmp, ignore_errors=True)


class TestStoreResultsOrder(unittest.TestCase):
    """Results are stored in emitted (sorted) order, never reversed."""

    def test_store_results_preserves_order(self):
        tmp = tempfile.mkdtemp()
        sched, storage, jm = make_scheduler(tmp)
        job = jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 1, "num_reduce_tasks": 1, "input_rows": 10, "params": {},
        })
        task = jm.tasks_for(job.job_id, "reduce")[0]
        results = [{"key": k, "count": 1} for k in ["a", "b", "c", "d"]]
        sched._store_results(job, task, results)
        snap = load_result_snapshot(storage, job.job_id)
        self.assertEqual([r["key"] for r in snap["records"]], ["a", "b", "c", "d"])
        shutil.rmtree(tmp, ignore_errors=True)


class TestSnapshotConsistency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write_part(self, job_id, partition, records, name=None, **extra):
        doc = {
            "job_id": job_id,
            "partition": partition,
            "partition_name": f"part-{partition:04d}",
            "task_id": f"r-{partition:04d}",
            "records": records,
            "count": len(records),
            "written_ms": 1000 + partition,
        }
        doc.update(extra)
        self.storage.write(doc, "jobs", job_id, "results", "reduce",
                           name or f"part-{partition:04d}.json")

    def test_total_equals_partition_sum_equals_record_count(self):
        for p in range(7):
            self.write_part("job-a", p, [{"key": f"k{p}-{i}", "count": 1} for i in range(p * 3)])
        snap = load_result_snapshot(self.storage, "job-a")
        total = len(snap["records"])
        self.assertEqual(total, sum(p["count"] for p in snap["partitions"]))
        self.assertEqual(total, sum(p * 3 for p in range(7)))

    def test_partitions_sorted_numerically_and_offsets_cumulative(self):
        # Write out of order; filenames zero-pad to 4 digits, but the snapshot
        # must sort by the numeric partition index regardless.
        for p in [3, 1, 2, 0]:
            self.write_part("job-a", p, [{"key": f"k{p}", "count": 1}] * (p + 1))
        snap = load_result_snapshot(self.storage, "job-a")
        self.assertEqual([p["partition"] for p in snap["partitions"]], [0, 1, 2, 3])
        offset = 0
        for part in snap["partitions"]:
            self.assertEqual(part["offset"], offset)
            offset += part["count"]
        # Record stream is the concatenation in partition order.
        self.assertEqual([r["key"] for r in snap["records"]],
                         ["k0"] + ["k1"] * 2 + ["k2"] * 3 + ["k3"] * 4)

    def test_count_derived_from_actual_records_not_count_field(self):
        # A stale/corrupt doc whose count field disagrees with its records
        # must not skew the numbers: count is derived from what was read.
        self.write_part("job-a", 0, [{"key": "a", "count": 1}], count=999)
        snap = load_result_snapshot(self.storage, "job-a")
        self.assertEqual(snap["partitions"][0]["count"], 1)
        self.assertEqual(len(snap["records"]), 1)

    def test_duplicate_partition_newest_write_wins(self):
        self.write_part("job-a", 0, [{"key": "old", "count": 1}], written_ms=1000)
        self.write_part("job-a", 0, [{"key": "new", "count": 2}],
                        name="part-0000.retry.json", written_ms=2000)
        snap = load_result_snapshot(self.storage, "job-a")
        self.assertEqual(len(snap["partitions"]), 1)
        self.assertEqual([r["key"] for r in snap["records"]], ["new"])

    def test_jobs_are_isolated(self):
        self.write_part("job-a", 0, [{"key": "a", "count": 1}])
        self.write_part("job-b", 0, [{"key": "b1", "count": 1}, {"key": "b2", "count": 1}])
        self.assertEqual(len(load_result_snapshot(self.storage, "job-a")["records"]), 1)
        self.assertEqual(len(load_result_snapshot(self.storage, "job-b")["records"]), 2)


class TestResultsPaging(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        sched, self.storage, self.jm = make_scheduler(self.tmp)
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 1, "num_reduce_tasks": 1, "input_rows": 10, "params": {},
        })

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def snapshot_with(self, n):
        records = [{"key": f"k{i:05d}", "count": 1} for i in range(n)]
        return {"partitions": [{"partition": 0, "partition_name": "part-0000",
                                "task_id": "r-0000", "offset": 0, "count": n}],
                "records": records}

    def test_pages_concatenate_to_full_stream_in_order(self):
        snap = self.snapshot_with(250)
        seen = []
        offset, pages = 0, 0
        while True:
            d = build_results_payload(self.job, snap, offset=offset, limit=100)
            seen.extend(d["records"])
            pages += 1
            if not d["truncated"]:
                break
            offset += d["returned"]
        self.assertEqual([r["key"] for r in seen], [f"k{i:05d}" for i in range(250)])
        self.assertEqual(pages, 3)

    def test_total_and_partition_total_always_agree(self):
        snap = self.snapshot_with(123)
        for offset in (0, 50, 122, 123, 9999):
            d = build_results_payload(self.job, snap, offset=offset, limit=10)
            self.assertEqual(d["total"], 123)
            self.assertEqual(d["partition_total"], 123)
            self.assertEqual(d["total"], sum(p["count"] for p in d["partitions"]))

    def test_offset_and_limit_are_clamped(self):
        snap = self.snapshot_with(10)
        d = build_results_payload(self.job, snap, offset=-5, limit=10 ** 9)
        self.assertEqual(d["offset"], 0)
        self.assertLessEqual(d["limit"], 1000)
        self.assertEqual(d["returned"], 10)
        self.assertFalse(d["truncated"])
        d = build_results_payload(self.job, snap, offset=999, limit=5)
        self.assertEqual(d["returned"], 0)
        self.assertFalse(d["truncated"])

    def test_empty_result(self):
        d = build_results_payload(self.job, {"partitions": [], "records": []})
        self.assertEqual(d["total"], 0)
        self.assertEqual(d["partition_total"], 0)
        self.assertEqual(d["records"], [])
        self.assertFalse(d["truncated"])


class TestInputPipelineHonesty(unittest.TestCase):
    """input_rows, generated records, shard contents and stats must agree."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.jm = JobManager(self.storage, ClusterConfig(), LogBus(self.storage))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_generator_produces_exact_row_count(self):
        for kind in ("wordcount", "kv"):
            self.assertEqual(len(generate_input_records(kind, 800, seed=7)), 800)
            self.assertEqual(len(generate_input_records(kind, 1, seed=7)), 1)

    def test_shards_keep_every_record_and_stats_are_exact(self):
        job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 4, "num_reduce_tasks": 2, "input_rows": 800, "params": {},
        })
        loaded = []
        for shard in self.jm.planner.input_shards(job):
            loaded.extend(self.jm.planner.load_input_shard(job.job_id, shard["shard_id"]))
        self.assertEqual(len(loaded), 800)          # no record dropped at shard load
        self.assertEqual(job.stats["total_records"], 800)  # no +1 fudging
        self.assertEqual(job.input_rows, 800)


class TestJobStatsHonesty(unittest.TestCase):
    def test_reduce_emitted_counts_reduce_tasks_only(self):
        tmp = tempfile.mkdtemp()
        sched, storage, jm = make_scheduler(tmp)
        job = jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 2, "num_reduce_tasks": 2, "input_rows": 50, "params": {},
        })
        for t in jm.tasks_for(job.job_id, "map"):
            jm.update_task(job.job_id, t.task_id, status="SUCCEEDED", records_emitted=100)
        for t in jm.tasks_for(job.job_id, "reduce"):
            jm.update_task(job.job_id, t.task_id, status="SUCCEEDED", records_emitted=7)
        sched._finish_success(jm.get_job(job.job_id))
        stats = jm.get_job(job.job_id).stats
        self.assertEqual(stats["reduce_records_emitted"], 14)
        self.assertEqual(stats["map_records_emitted"], 200)
        shutil.rmtree(tmp, ignore_errors=True)


class TestEndToEndPipelineConsistency(unittest.TestCase):
    """map -> shuffle -> reduce -> store -> snapshot, checked against a reference.

    Exercises several map tasks and several partitions so cross-partition
    ordering and per-partition counts are all verified at once.
    """

    def test_pipeline_matches_reference_wordcount(self):
        tmp = tempfile.mkdtemp()
        sched, storage, jm = make_scheduler(tmp)
        job = jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 3, "num_reduce_tasks": 3, "input_rows": 500, "params": {},
        })
        job_id = job.job_id
        num_partitions = 3

        # Reference: count every word of every input shard record.
        reference = collections.Counter()
        all_records = []
        for shard in jm.planner.input_shards(job):
            all_records.extend(jm.planner.load_input_shard(job_id, shard["shard_id"]))
        for line in all_records:
            for w in line.lower().split():
                reference[w] += 1

        # Map stage: one task per input shard.
        map_task_ids = []
        for i, shard in enumerate(jm.planner.input_shards(job)):
            task_id = f"m-{i:04d}"
            map_task_ids.append(task_id)
            spec = {
                "task_id": task_id, "job_id": job_id, "kind": "map",
                "mapper": "wordcount_mapper", "reducer": "count_reducer",
                "params": {}, "partition_count": num_partitions,
                "records": jm.planner.load_input_shard(job_id, shard["shard_id"]),
                "spill_records": 50, "tmp_dir": tmp,
            }
            _run_map(spec, tmp, lambda *a: None)

        # Reduce stage: pull each partition from every map task (fake HTTP).
        store = ShuffleStore(tmp)
        reduce_tasks = jm.tasks_for(job_id, "reduce")
        for rt in reduce_tasks:
            p = rt.partition
            responses = {}
            for mt in map_task_ids:
                url = f"http://w/shuffle/{job_id}/{mt}/{partition_filename(p)}"
                responses[url] = store.read_partition(job_id, mt, p)
            client = mock.Mock()
            client.get_json.side_effect = lambda url, default=None: responses.get(url, default)
            spec = {
                "task_id": rt.task_id, "job_id": job_id, "kind": "reduce",
                "reducer": "count_reducer", "params": {}, "partition": p,
                "fetch_plan": [{"worker_url": "http://w", "map_task_id": mt} for mt in map_task_ids],
                "spill_records": 50, "tmp_dir": tmp,
            }
            with mock.patch("backend.worker.executor.HttpClient", lambda *a, **k: client):
                out = _run_reduce(spec, lambda *a: None)
            sched._store_results(job, rt, out["results"])

        # The three views agree with each other AND with the reference.
        snap = load_result_snapshot(storage, job_id)
        partitions = snap["partitions"]
        records = snap["records"]

        self.assertEqual(len(records), sum(p["count"] for p in partitions))
        self.assertEqual(len(records), len(reference))  # every key present, incl. each
                                                        # partition's last group
        got = {r["key"]: r["count"] for r in records}
        self.assertEqual(got, dict(reference))

        # Canonical order: partition index asc, keys sorted within a partition.
        self.assertEqual([p["partition"] for p in partitions], sorted(p["partition"] for p in partitions))
        cursor = 0
        for part in partitions:
            keys = [records[cursor + i]["key"] for i in range(part["count"])]
            self.assertEqual(keys, sorted(keys))
            self.assertEqual(part["offset"], cursor)
            cursor += part["count"]

        # Paged windows reassemble the identical stream.
        page_size = 7
        seen, offset = [], 0
        while offset < len(records) or offset == 0:
            d = build_results_payload(job, snap, offset=offset, limit=page_size)
            seen.extend(d["records"])
            if not d["truncated"]:
                break
            offset += d["returned"]
        self.assertEqual(seen, records)
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
