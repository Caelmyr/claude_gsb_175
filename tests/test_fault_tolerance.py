"""Tests for fault tolerance / retry, and map+reduce task correctness."""

import collections
import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.tasks.registry import get_reducer
from backend.tasks.samples import generate_input_records
from backend.worker import executor
from backend.worker.executor import _run_map, _run_reduce
from backend.worker.shuffle_store import ShuffleStore


class TestFaultTolerance(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(max_attempts=2)
        self.jm = JobManager(self.storage, self.config, LogBus(self.storage))
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": 3, "num_reduce_tasks": 2, "input_rows": 100, "params": {},
        })
        self.ft = FaultTolerance(self.storage, self.jm, self.config, LogBus(self.storage))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_retry_then_permanent_failure(self):
        task = self.jm.tasks_for(self.job.job_id, "map")[0]
        # First failure -> retry.
        self.assertTrue(self.ft.handle_task_failure(self.job, task, "boom"))
        task = self.jm.get_task(self.job.job_id, task.task_id)
        self.assertEqual(task.status, "RETRYING")
        self.assertEqual(task.attempts, 1)
        # Exhaust retries until the job fails.
        retried = True
        while retried:
            retried = self.ft.handle_task_failure(self.job, task, "boom")
            task = self.jm.get_task(self.job.job_id, task.task_id)
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(self.jm.get_job(self.job.job_id).status, "FAILED")

    def test_fault_event_recorded(self):
        task = self.jm.tasks_for(self.job.job_id, "map")[0]
        self.ft.handle_task_failure(self.job, task, "boom")
        faults = self.ft.list_faults(self.job.job_id)
        self.assertEqual(len(faults), 1)
        self.assertEqual(faults[0]["kind"], "task_failed")


class TestMapReduceCorrectness(unittest.TestCase):
    def test_wordcount_matches_reference(self):
        tmp = tempfile.mkdtemp()
        records = generate_input_records("wordcount", 800, seed=7)
        reference = collections.Counter()
        for line in records:
            for w in line.lower().split():
                reference[w] += 1

        spec = {
            "task_id": "m-0000", "job_id": "job", "kind": "map",
            "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "params": {}, "partition_count": 4, "records": records,
            "spill_records": 300, "tmp_dir": tmp,
        }
        _run_map(spec, tmp, lambda p, a, b: None)

        store = ShuffleStore(tmp)
        grouped = collections.defaultdict(list)
        for p in range(4):
            for k, v in store.read_partition("job", "m-0000", p):
                grouped[k].append(v)

        reducer = get_reducer("count_reducer")
        result = {k: reducer(k, vs, {})["count"] for k, vs in grouped.items()}
        self.assertEqual(dict(result), dict(reference))
        shutil.rmtree(tmp, ignore_errors=True)


class TestReduceCorrectness(unittest.TestCase):
    def test_final_group_is_reduced_in_sorted_order(self):
        class FakeHttpClient:
            def __init__(self, pairs):
                self.pairs = pairs

            def get_json(self, url, default=None):
                return self.pairs

        tmp = tempfile.mkdtemp()
        original_client = executor.HttpClient
        executor.HttpClient = lambda *args, **kwargs: FakeHttpClient([
            ["banana", 1], ["apple", 1], ["apple", 2], ["cherry", 1],
        ])
        try:
            result = _run_reduce({
                "task_id": "r-0000",
                "job_id": "job",
                "partition": 0,
                "reducer": "count_reducer",
                "params": {},
                "fetch_plan": [{"worker_url": "http://worker", "map_task_id": "m-0000"}],
                "tmp_dir": tmp,
            }, lambda *args: None)
        finally:
            executor.HttpClient = original_client
            shutil.rmtree(tmp, ignore_errors=True)

        self.assertEqual(result["records_processed"], 4)
        self.assertEqual([r["key"] for r in result["results"]], ["apple", "banana", "cherry"])
        self.assertEqual(result["results"][0]["count"], 3)
        self.assertEqual(result["records_emitted"], 3)


class TestMapRetryOutput(unittest.TestCase):
    def test_map_retry_replaces_append_only_shuffle_partitions(self):
        tmp = tempfile.mkdtemp()
        spec = {
            "task_id": "m-0000",
            "job_id": "job-retry",
            "kind": "map",
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "params": {},
            "partition_count": 1,
            "records": ["map map reduce", "map reduce"],
            "tmp_dir": tmp,
        }
        try:
            _run_map(spec, tmp, lambda *args: None)
            _run_map(spec, tmp, lambda *args: None)  # retry the deterministic task
            pairs = ShuffleStore(tmp).read_partition("job-retry", "m-0000", 0)
            self.assertEqual(sorted(k for k, _ in pairs), sorted(["map"] * 3 + ["reduce"] * 2))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
