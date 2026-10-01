"""Canonical result snapshots for the results page and exports.

The UI has three views of the same result set: the aggregate total, the
per-partition list and the record preview.  Computing them separately allowed
metadata counters, independent filesystem reads and partial polls to disagree.
This module builds all three from one in-memory scan of the committed partition
files.

Each result file is atomically replaced.  A snapshot therefore never sees a
half-written partition; reduce tasks finishing concurrently are represented as
all-or-nothing partitions, with uncommitted planned partitions shown explicitly
with zero records.
"""

from __future__ import annotations

import os

from backend.common import constants as C
from backend.common.models import Job
from backend.common.storage import Storage, list_files, read_json
from backend.master.job_manager import JobManager


def _partition_from_path(path: str) -> int:
    stem = os.path.splitext(os.path.basename(path))[0]
    return int(stem.rsplit("-", 1)[-1])


def _partition_view(partition: int, doc: dict | None, task_id: str, start: int) -> tuple[dict, list[dict]]:
    """Build one partition summary and return it with its record list."""
    if doc is None:
        return {
            "partition": partition,
            "partition_name": f"part-{partition:04d}",
            "count": 0,
            "start": 0,
            "end": 0,
            "task_id": task_id,
        }, []

    records = [rec for rec in doc.get("records", []) if isinstance(rec, dict)]
    records.sort(key=lambda rec: str(rec.get("key", "")))
    count = len(records)
    return {
        "partition": partition,
        "partition_name": doc.get("partition_name") or f"part-{partition:04d}",
        "count": count,
        "start": start if count else 0,
        "end": start + count - 1 if count else 0,
        "task_id": doc.get("task_id") or task_id,
    }, records


def build_result_snapshot(job_manager: JobManager, storage: Storage, job: Job) -> dict:
    """Return one internally consistent, ordered view of a job's results."""
    reduce_tasks = job_manager.tasks_for(job.job_id, C.TASK_REDUCE)
    task_by_partition = {t.partition: t for t in reduce_tasks}
    expected = max(int(job.num_reduce_tasks or 0), len(task_by_partition))

    root = storage.path("jobs", job.job_id, "results", C.STAGE_REDUCE)
    docs_by_partition: dict[int, dict] = {}
    for path in list_files(root, suffix=".json"):
        doc = read_json(path)
        if not isinstance(doc, dict):
            continue
        try:
            partition = int(doc.get("partition"))
        except (TypeError, ValueError):
            partition = _partition_from_path(path)
        # Deterministic plan partitions win if duplicate/legacy files exist.
        docs_by_partition[partition] = doc

    partitions: list[dict] = []
    records: list[dict] = []
    committed = 0
    cursor = 1

    def consume(partition: int, doc: dict | None) -> None:
        nonlocal cursor
        task = task_by_partition.get(partition)
        task_id = task.task_id if task else ""
        view, partition_records = _partition_view(partition, doc, task_id, cursor)
        partitions.append(view)
        records.extend(partition_records)
        cursor += view["count"]

    for partition in range(expected):
        doc = docs_by_partition.pop(partition, None)
        if doc is not None:
            committed += 1
        consume(partition, doc)
    for partition in sorted(docs_by_partition):
        committed += 1
        consume(partition, docs_by_partition[partition])

    return {
        "job_id": job.job_id,
        "status": job.status,
        "partitions": partitions,
        "records": records,
        "total": len(records),
        "partition_count": len(partitions),
        "expected_partition_count": expected,
        "committed_partition_count": committed,
        "complete": job.status == C.JOB_SUCCEEDED and expected > 0 and committed == expected,
    }


def paginate_snapshot(snapshot: dict, page: int = 1, page_size: int = 100) -> dict:
    """Add stable page metadata and replace records with the requested page."""
    try:
        page_size = max(1, min(1000, int(page_size)))
    except (TypeError, ValueError):
        page_size = 100
    try:
        page = max(1, int(page))
    except (TypeError, ValueError):
        page = 1

    total = int(snapshot["total"])
    page_count = max(1, (total + page_size - 1) // page_size)
    page = min(page, page_count)
    offset = (page - 1) * page_size
    page_records = snapshot["records"][offset:offset + page_size]

    out = dict(snapshot)
    out.update({
        "records": page_records,
        "page": page,
        "page_size": page_size,
        "page_count": page_count,
        "offset": offset,
        "preview_start": offset + 1 if page_records else 0,
        "preview_end": offset + len(page_records),
        "truncated": offset + len(page_records) < total,
    })
    return out
