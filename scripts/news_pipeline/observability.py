"""Best-effort operational records: job runs, model calls and the 24 h funnel.

Every writer here is advisory. A missing v12 schema, a locked database or any
other failure returns ``False`` and never changes the outcome of the job that
called it. Wall-clock time is measured only by callers (jobs, CLI); pipeline
cores keep their injected, deterministic clocks.
"""
from __future__ import annotations

import dataclasses
import enum
import json
import sqlite3
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .schema_v12 import JOB_RUN_STATES, MODEL_CALL_OUTCOMES, v12_available

_SUMMARY_LIMIT_BYTES = 16384
# A pending claim older than this is reported as expired. Expiry is derived at read
# time and changes no row: a later corroboration (event_store) may still verify it.
CLAIM_EXPIRY_DAYS = 14
_CLAIM_AGE_BUCKETS = (("lt_1d", 1), ("1_3d", 3), ("3_7d", 7), ("7_14d", CLAIM_EXPIRY_DAYS))
_BUSY_TIMEOUT_SECONDS = 5.0


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_ms(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def scalars(value: Any) -> dict[str, Any]:
    """Return the int/float/str/bool/None fields of a dataclass or mapping."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        items: Iterable[tuple[str, Any]] = (
            (field.name, getattr(value, field.name)) for field in dataclasses.fields(value)
        )
    elif isinstance(value, Mapping):
        items = value.items()
    else:
        return {}
    return {
        str(key): item
        for key, item in items
        if item is None or type(item) in (int, float, str, bool)
    }


def plain(value: Any) -> Any:
    """JSON-ready copy of a report: dataclasses become objects, tuples lists, enums values."""
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {field.name: plain(getattr(value, field.name)) for field in dataclasses.fields(value)}
    if isinstance(value, Mapping):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [plain(item) for item in value]
    if isinstance(value, enum.Enum):
        return plain(value.value)
    if isinstance(value, datetime):
        return iso_ms(value)
    if value is None or type(value) in (int, float, str, bool):
        return value
    return str(value)


def summary_json(payload: Mapping[str, Any]) -> str:
    """Compact job summary: top-level scalars plus small nested count maps."""
    summary: dict[str, Any] = {}
    for key, item in payload.items():
        if item is None or type(item) in (int, float, str, bool):
            summary[key] = item
        elif key in {"counts", "event_counts", "funnel_24h", "claims_pending_age", "error"} and isinstance(item, Mapping):
            summary[key] = item
    text = json.dumps(summary, ensure_ascii=False, separators=(",", ":"), default=str)
    if len(text.encode("utf-8")) > _SUMMARY_LIMIT_BYTES:
        text = json.dumps(
            {key: summary[key] for key in summary if key not in {"counts", "event_counts", "funnel_24h"}}
            | {"truncated": True},
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
    return text


def _connect(db_path: str | Path) -> sqlite3.Connection | None:
    path = Path(db_path)
    if not path.is_file():
        return None
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=_BUSY_TIMEOUT_SECONDS)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def record_job_run(
    db_path: str | Path,
    *,
    job: str,
    slot_utc: str | None,
    started_at: datetime,
    finished_at: datetime,
    exit_code: int,
    state: str,
    error_class: str | None,
    payload: Mapping[str, Any] | None,
) -> bool:
    """Insert one ``job_runs`` row; return False (never raise) when it cannot."""
    try:
        connection = _connect(db_path)
        if connection is None:
            return False
        try:
            if not v12_available(connection):
                return False
            if state not in JOB_RUN_STATES:
                state = "failed"
            duration_ms = max(0, round((finished_at - started_at).total_seconds() * 1000))
            connection.execute(
                """INSERT INTO job_runs(job,slot_utc,started_at,finished_at,duration_ms,
                                        exit_code,state,error_class,summary_json)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (
                    job,
                    slot_utc,
                    iso_ms(started_at),
                    iso_ms(finished_at),
                    duration_ms,
                    int(exit_code),
                    state,
                    error_class,
                    summary_json(payload or {}),
                ),
            )
            return True
        finally:
            connection.close()
    except Exception:
        return False


def record_model_calls(
    db_path: str | Path,
    *,
    job: str,
    records: Iterable[Mapping[str, Any]],
) -> int:
    """Insert ``model_calls`` rows; return how many were written (0 on any failure)."""
    rows = []
    for record in records:
        outcome = record.get("outcome")
        if outcome not in MODEL_CALL_OUTCOMES:
            continue
        rows.append(
            (
                job,
                record.get("started_at"),
                record.get("model"),
                record.get("endpoint_host"),
                record.get("request_bytes"),
                record.get("response_bytes"),
                record.get("timeout_s"),
                record.get("latency_ms"),
                outcome,
                record.get("http_status"),
            )
        )
    if not rows:
        return 0
    try:
        connection = _connect(db_path)
        if connection is None:
            return 0
        try:
            if not v12_available(connection):
                return 0
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executemany(
                    """INSERT INTO model_calls(job,started_at,model,endpoint_host,request_bytes,
                                               response_bytes,timeout_s,latency_ms,outcome,http_status)
                       VALUES(?,?,?,?,?,?,?,?,?,?)""",
                    rows,
                )
            except Exception:
                connection.rollback()
                raise
            connection.commit()
            return len(rows)
        finally:
            connection.close()
    except Exception:
        return 0


def model_call_counts(records: Iterable[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        outcome = str(record.get("outcome"))
        counts[outcome] = counts.get(outcome, 0) + 1
    return counts


def category_funnel(db_path: str | Path, as_of: str, *, hours: int = 24) -> dict[str, Any]:
    """Per-category counts for the ``hours`` before ``as_of`` (read-only).

    items: source items retrieved; claims: claims extracted from those items,
    split by status; event_versions: event versions that became valid;
    eligible: latest versions that pass the report's own selection
    (``report_builder._latest_events``); plus stories per subject from the
    latest subject report in the window.
    """
    if not as_of.endswith("Z"):
        raise ValueError("as_of must be a UTC Z timestamp")
    upper_dt = datetime.fromisoformat(as_of[:-1] + "+00:00")
    lower = (upper_dt - timedelta(hours=hours)).isoformat(timespec="seconds").replace("+00:00", "Z")
    upper = as_of
    connection = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True, timeout=_BUSY_TIMEOUT_SECONDS)
    try:
        categories: dict[str, dict[str, int]] = {}

        def bucket(category: str) -> dict[str, int]:
            return categories.setdefault(
                category,
                {
                    "items": 0,
                    "claims": 0,
                    "verified": 0,
                    "pending": 0,
                    "rejected": 0,
                    "event_versions": 0,
                    "eligible": 0,
                },
            )

        for category, count in connection.execute(
            "SELECT category,COUNT(*) FROM source_items WHERE retrieved_at>? AND retrieved_at<=? GROUP BY category",
            (lower, upper),
        ):
            bucket(category)["items"] = count
        for category, status, count in connection.execute(
            """SELECT s.category,c.status,COUNT(*) FROM claims c
               JOIN source_items s ON s.source_item_id=c.source_item_id
               WHERE c.extracted_at>? AND c.extracted_at<=? GROUP BY s.category,c.status""",
            (lower, upper),
        ):
            row = bucket(category)
            row["claims"] += count
            if status in ("verified", "pending", "rejected"):
                row[status] += count
        for category, count in connection.execute(
            """SELECT e.category,COUNT(*) FROM event_versions v JOIN events e ON e.id=v.event_id
               WHERE v.valid_from>? AND v.valid_from<=? GROUP BY e.category""",
            (lower, upper),
        ):
            bucket(category)["event_versions"] = count
        from .report_builder import _latest_events

        for row in _latest_events(connection, lower, upper):
            bucket(str(row[3]))["eligible"] += 1
        stories: dict[str, int] = {}
        try:
            for subject_id, story_count in connection.execute(
                """SELECT r.subject_id,r.story_count FROM subject_reports r
                   JOIN (SELECT subject_id,MAX(created_at) AS created_at FROM subject_reports
                         WHERE created_at>? AND created_at<=? GROUP BY subject_id) latest
                     ON latest.subject_id=r.subject_id AND latest.created_at=r.created_at""",
                (lower, upper),
            ):
                stories[str(subject_id)] = int(story_count)
        except sqlite3.OperationalError:
            stories = {}
        return {
            "window_hours": hours,
            "categories": {key: categories[key] for key in sorted(categories)},
            "stories": {key: stories[key] for key in sorted(stories)},
        }
    finally:
        connection.close()


def claims_pending_age(db_path: str | Path, as_of: str) -> dict[str, Any]:
    """Pending claims by age at ``as_of`` (read-only); older than 14 days counts as expired."""
    if not as_of.endswith("Z"):
        raise ValueError("as_of must be a UTC Z timestamp")
    upper_dt = datetime.fromisoformat(as_of[:-1] + "+00:00")

    def bound(days: int) -> str:
        return (upper_dt - timedelta(days=days)).isoformat(timespec="seconds").replace("+00:00", "Z")

    connection = sqlite3.connect(f"file:{Path(db_path)}?mode=ro", uri=True, timeout=_BUSY_TIMEOUT_SECONDS)
    try:
        columns = ["SUM(extracted_at>=?)"]
        params: list[str] = [bound(1)]
        for (_, newer), (_, older) in zip(_CLAIM_AGE_BUCKETS, _CLAIM_AGE_BUCKETS[1:]):
            columns.append("SUM(extracted_at<? AND extracted_at>=?)")
            params += [bound(newer), bound(older)]
        columns += ["SUM(extracted_at<?)", "MIN(CASE WHEN extracted_at>=? THEN extracted_at END)", "MIN(extracted_at)"]
        params += [bound(CLAIM_EXPIRY_DAYS), bound(CLAIM_EXPIRY_DAYS)]
        row = connection.execute(
            f"SELECT {','.join(columns)} FROM claims WHERE status='pending' AND extracted_at<=?",
            (*params, as_of),
        ).fetchone()
    finally:
        connection.close()
    counts = [int(value or 0) for value in row[: len(_CLAIM_AGE_BUCKETS) + 1]]
    buckets = dict(zip([name for name, _ in _CLAIM_AGE_BUCKETS] + ["expired"], counts))
    return {
        "expiry_days": CLAIM_EXPIRY_DAYS,
        "pending": sum(counts[:-1]),
        "expired": counts[-1],
        "buckets": buckets,
        "oldest_pending_at": row[-2],
        "oldest_at": row[-1],
    }


__all__ = [
    "CLAIM_EXPIRY_DAYS",
    "category_funnel",
    "claims_pending_age",
    "iso_ms",
    "model_call_counts",
    "plain",
    "record_job_run",
    "record_model_calls",
    "scalars",
    "summary_json",
    "utc_now",
]
