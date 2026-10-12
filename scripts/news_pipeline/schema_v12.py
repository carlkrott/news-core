"""Transactional additive schema-v12 migration for operational observability.

v12 adds side tables only; no existing table gains or loses a column, so every
earlier validator still passes on a migrated database.

* ``job_runs``: measured wall-clock start/end/duration per cron job run.
* ``model_calls``: one row per subject-model HTTP call (model, endpoint host,
  sizes, timeout, latency, outcome class). No request content, path or key.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from .schema_v11 import validate_v11

SCHEMA_VERSION = 12
V12_TABLES = (
    "job_runs",
    "model_calls",
)
V12_COLUMNS = {
    "job_runs": (
        "job_run_id",
        "job",
        "slot_utc",
        "started_at",
        "finished_at",
        "duration_ms",
        "exit_code",
        "state",
        "error_class",
        "summary_json",
    ),
    "model_calls": (
        "model_call_id",
        "job",
        "started_at",
        "model",
        "endpoint_host",
        "request_bytes",
        "response_bytes",
        "timeout_s",
        "latency_ms",
        "outcome",
        "http_status",
    ),
}
V12_FOREIGN_KEYS = {
    "job_runs": frozenset(),
    "model_calls": frozenset(),
}
JOB_RUN_STATES = ("completed", "disabled", "failed")
MODEL_CALL_OUTCOMES = (
    "ok",
    "timeout",
    "http_error",
    "connection",
    "invalid_response",
    "oversize",
)

_TABLE_SQL = (
    """
CREATE TABLE job_runs(
    job_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job TEXT NOT NULL CHECK(length(trim(job)) > 0),
    slot_utc TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK(duration_ms >= 0),
    exit_code INTEGER NOT NULL,
    state TEXT NOT NULL CHECK(state IN ('completed','disabled','failed')),
    error_class TEXT,
    summary_json TEXT NOT NULL CHECK(json_valid(summary_json))
)
""",
    """
CREATE TABLE model_calls(
    model_call_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job TEXT NOT NULL CHECK(length(trim(job)) > 0),
    started_at TEXT NOT NULL,
    model TEXT NOT NULL CHECK(length(trim(model)) > 0),
    endpoint_host TEXT NOT NULL CHECK(length(trim(endpoint_host)) > 0),
    request_bytes INTEGER NOT NULL CHECK(request_bytes >= 0),
    response_bytes INTEGER CHECK(response_bytes IS NULL OR response_bytes >= 0),
    timeout_s REAL NOT NULL CHECK(timeout_s > 0),
    latency_ms INTEGER NOT NULL CHECK(latency_ms >= 0),
    outcome TEXT NOT NULL CHECK(outcome IN ('ok','timeout','http_error','connection','invalid_response','oversize')),
    http_status INTEGER CHECK(http_status IS NULL OR http_status BETWEEN 100 AND 599)
)
""",
)
V12_INDEXES = {
    "idx_job_runs_job_started": ("job_runs", ("job", "started_at")),
    "idx_model_calls_started": ("model_calls", ("started_at",)),
    "idx_model_calls_outcome": ("model_calls", ("outcome", "started_at")),
}
_INDEX_SQL = tuple(
    f"CREATE INDEX {name} ON {table}({', '.join(columns)})"
    for name, (table, columns) in V12_INDEXES.items()
)


def _timestamp(value: object) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError("applied_at must be a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("applied_at must be a valid UTC timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError("applied_at must be UTC")
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        raise ValueError("applied_at must use canonical UTC Z form")
    return value


def _tables(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )


def v12_available(connection: sqlite3.Connection) -> bool:
    """Return whether the v12 marker is present (cheap check for best-effort writers)."""
    try:
        return connection.execute(
            "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
        ).fetchone() is not None
    except sqlite3.Error:
        return False


def validate_v12(connection: sqlite3.Connection) -> None:
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for schema v12")
    validate_v11(connection)
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    if marker is None:
        raise ValueError("schema v12 migration marker is missing")
    _timestamp(marker[0])
    missing = set(V12_TABLES) - _tables(connection)
    if missing:
        raise ValueError(f"schema v12 is incomplete; missing tables: {sorted(missing)}")
    for table, expected in V12_COLUMNS.items():
        actual = tuple(
            row[0] for row in connection.execute(f"SELECT name FROM pragma_table_info('{table}')")
        )
        if actual != expected:
            raise ValueError(f"schema v12 {table} has incompatible columns")
        fks = frozenset(
            (row[2], row[3], row[4])
            for row in connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
        )
        if fks != V12_FOREIGN_KEYS[table]:
            raise ValueError(f"schema v12 {table} has incompatible foreign keys")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    for name, (table, columns) in V12_INDEXES.items():
        if indexes.get(name) != table:
            raise ValueError(f"schema v12 index {name} is missing or incompatible")
        actual = tuple(
            row[0] for row in connection.execute(f"SELECT name FROM pragma_index_info('{name}')")
        )
        if actual != columns:
            raise ValueError(f"schema v12 index {name} columns are incompatible")


def migrate_v12(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v12 atomically; return whether it was newly applied."""
    applied_at = _timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v12 requires no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    validate_v11(connection)
    marker = connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V12_TABLES) & _tables(connection)
    if marker is not None:
        validate_v12(connection)
        return False
    if present:
        raise ValueError(f"partial schema v12 state without migration marker: {sorted(present)}")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for statement in _TABLE_SQL:
            connection.execute(statement)
        for statement in _INDEX_SQL:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)",
            (SCHEMA_VERSION, applied_at),
        )
        validate_v12(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    validate_v12(connection)
    return True


__all__ = [
    "JOB_RUN_STATES",
    "MODEL_CALL_OUTCOMES",
    "SCHEMA_VERSION",
    "V12_COLUMNS",
    "V12_TABLES",
    "migrate_v12",
    "v12_available",
    "validate_v12",
]
