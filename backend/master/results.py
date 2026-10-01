"""Consistent, single-scan read views over a job's reduce-output files.

The results page shows three numbers that must always agree — the header
total, the sum of the per-partition counts, and the preview records.  They
used to come from two independent directory scans and two different count
conventions, so they drifted apart whenever a reduce task finished mid-request
(and the reduce output itself silently dropped records).  This module derives
all three from **one snapshot** of the result directory:

* **one scan** — partition rows and the record stream are read from the same
  file set, so a concurrent writer can shift the whole snapshot but never
  split the total from the per-partition counts;
* **one order** — partitions sort by numeric index and the record stream
  concatenates them in that same order, each partition keeping its stored
  (sorted-by-key) order, so preview, partition list and download all agree;
* **one count** — every partition's ``count`` is ``len(records)`` of the
  document actually read, so ``total == sum(counts)`` holds by construction;
* **dedup** — if two files ever claim the same partition (e.g. a retried
  reduce wrote before a stale attempt was cleaned up), the newest write wins.

The module is Flask-free so the consistency rules are unit-testable without
booting the HTTP server.
"""

from __future__ import annotations

from backend.common import constants as C
from backend.common.ids import partition_name
from backend.common.models import Job
from backend.common.storage import Storage, list_files, read_json

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000


def _doc_stamp(doc: dict) -> tuple:
    """Freshness ordering for duplicate partition files: newest write wins."""
    try:
        written_ms = int(doc.get("written_ms", 0) or 0)
    except (TypeError, ValueError):
        written_ms = 0
    try:
        version = int(doc.get("_version", 0) or 0)
    except (TypeError, ValueError):
        version = 0
    return (written_ms, version)


def load_result_snapshot(storage: Storage, job_id: str) -> dict:
    """Scan the reduce-result directory once and return an ordered snapshot.

    Returns ``{"partitions": [...], "records": [...]}`` where ``partitions``
    is sorted by numeric partition index and ``records`` is the concatenation
    of each partition's records in that same order.  Each partition row
    carries ``offset`` (its 0-based start index in the global record stream)
    and ``count == len(records)`` read from its own document.
    """
    root = storage.path("jobs", job_id, "results", C.STAGE_REDUCE)
    by_partition: dict[int, dict] = {}
    for path in list_files(root, suffix=".json"):
        doc = read_json(path)
        if not isinstance(doc, dict):
            continue
        try:
            partition = int(doc.get("partition"))
        except (TypeError, ValueError):
            continue  # not a partition result file; never let it skew counts
        existing = by_partition.get(partition)
        if existing is not None and _doc_stamp(existing) >= _doc_stamp(doc):
            continue
        by_partition[partition] = doc

    partitions: list[dict] = []
    records: list[dict] = []
    for partition in sorted(by_partition):
        doc = by_partition[partition]
        part_records = doc.get("records")
        if not isinstance(part_records, list):
            part_records = []
        partitions.append({
            "partition": partition,
            "partition_name": doc.get("partition_name") or partition_name(partition),
            "task_id": doc.get("task_id"),
            "offset": len(records),
            "count": len(part_records),
        })
        records.extend(part_records)
    return {"partitions": partitions, "records": records}


def build_results_payload(
    job: Job,
    snapshot: dict,
    offset: int = 0,
    limit: int = DEFAULT_PAGE_SIZE,
) -> dict:
    """Build the ``/api/jobs/<id>/results`` response from one snapshot.

    ``total``, ``partition_total`` and the paged ``records`` window all derive
    from the same ordered record stream, so the three views on the results
    page can never disagree.  ``offset``/``limit`` select a deterministic
    window over that stream; paging through every window reassembles the
    complete result exactly once.
    """
    records: list[dict] = snapshot["records"]
    partitions: list[dict] = snapshot["partitions"]
    total = len(records)

    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = DEFAULT_PAGE_SIZE
    limit = max(1, min(MAX_PAGE_SIZE, limit))
    try:
        offset = int(offset)
    except (TypeError, ValueError):
        offset = 0
    offset = max(0, min(offset, total))

    page = records[offset:offset + limit]
    return {
        "job_id": job.job_id,
        "status": job.status,
        "total": total,
        "partition_total": sum(p["count"] for p in partitions),
        "partitions": partitions,
        "records": page,
        "offset": offset,
        "limit": limit,
        "returned": len(page),
        "truncated": offset + len(page) < total,
    }
