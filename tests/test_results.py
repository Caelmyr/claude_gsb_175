"""Tests for the canonical job-results snapshot."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.ids import partition_name
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.results import build_result_snapshot, paginate_snapshot


class TestResultsSnapshot(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.jm = JobManager(self.storage, ClusterConfig(), LogBus(self.storage))
        self.job = self.jm.submit({
            "name": "results",
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "num_map_tasks": 2,
            "num_reduce_tasks": 3,
            "input_rows": 20,
            "params": {},
        })
        self.other_job = self.jm.submit({
            "name": "other",
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "num_map_tasks": 1,
            "num_reduce_tasks": 2,
            "input_rows": 10,
            "params": {},
        })

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_partition(self, job, partition, keys):
        records = [{"key": key, "count": i + 1} for i, key in enumerate(keys)]
        self.storage.write({
            "job_id": job.job_id,
            "partition": partition,
            "partition_name": partition_name(partition),
            "task_id": f"r-{partition:04d}",
            "records": records,
            "count": len(records),
        }, "jobs", job.job_id, "results", C.STAGE_REDUCE, f"{partition_name(partition)}.json")
        return records

    def _succeed_job(self):
        self.jm.set_job_status(self.job, C.JOB_SUCCEEDED)

    def test_total_partition_sum_and_canonical_order_come_from_one_snapshot(self):
        self._write_partition(self.job, 1, ["z", "a", "m"])
        self._write_partition(self.job, 0, ["d", "b"])
        self._write_partition(self.job, 2, [])
        self._write_partition(self.other_job, 0, ["other"])
        self._succeed_job()

        snapshot = build_result_snapshot(self.jm, self.storage, self.job)
        page_one = paginate_snapshot(snapshot, 1, 2)
        page_two = paginate_snapshot(snapshot, 2, 2)
        page_three = paginate_snapshot(snapshot, 3, 2)

        self.assertTrue(snapshot["complete"])
        self.assertEqual(snapshot["total"], 5)
        self.assertEqual(sum(p["count"] for p in snapshot["partitions"]), 5)
        self.assertEqual([p["partition"] for p in snapshot["partitions"]], [0, 1, 2])
        self.assertEqual([(p["start"], p["end"]) for p in snapshot["partitions"]],
                         [(1, 2), (3, 5), (0, 0)])
        self.assertEqual([r["key"] for r in snapshot["records"]], ["b", "d", "a", "m", "z"])
        self.assertEqual(page_two["preview_start"], 3)
        self.assertEqual(page_two["preview_end"], 4)
        self.assertTrue(page_two["truncated"])
        self.assertFalse(page_three["truncated"])
        self.assertEqual([r["key"] for r in page_two["records"]], ["a", "m"])

    def test_incomplete_job_lists_expected_empty_partitions(self):
        self._write_partition(self.job, 0, ["only"])

        snapshot = build_result_snapshot(self.jm, self.storage, self.job)

        self.assertFalse(snapshot["complete"])
        self.assertEqual(snapshot["expected_partition_count"], 3)
        self.assertEqual(snapshot["committed_partition_count"], 1)
        self.assertEqual([p["count"] for p in snapshot["partitions"]], [1, 0, 0])
        self.assertEqual(snapshot["total"], 1)
        self.assertEqual(sum(p["count"] for p in snapshot["partitions"]), snapshot["total"])
        self.assertEqual([r["key"] for r in snapshot["records"]], ["only"])

    def test_empty_committed_partition_is_distinct_from_pending_partition(self):
        self._write_partition(self.job, 0, [])
        self._write_partition(self.job, 1, ["a"])

        snapshot = build_result_snapshot(self.jm, self.storage, self.job)

        self.assertEqual(snapshot["committed_partition_count"], 2)
        self.assertEqual(snapshot["partitions"][0]["start"], 0)
        self.assertEqual(snapshot["partitions"][1]["start"], 1)
        self.assertEqual(snapshot["partitions"][1]["end"], 1)


if __name__ == "__main__":
    unittest.main()
