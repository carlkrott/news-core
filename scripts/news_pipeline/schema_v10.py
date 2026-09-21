"""Transactional schema-v10 migration for subject generation receipts."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from .schema_v9 import validate_v9


SCHEMA_VERSION = 10
V10_TABLES = ("subject_generation_receipts",)
V10_COLUMNS = {
    "subject_generation_receipts": (
        "subject_report_id",
        "mode",
        "model_call_count",
        "cache_hit_count",
        "fallback_count",
        "malformed_count",
        "transport_error_count",
        "created_at",
    )
}

_TABLE_SQL = """
CREATE TABLE subject_generation_receipts(
    subject_report_id TEXT PRIMARY KEY REFERENCES subject_reports(subject_report_id),
    mode TEXT NOT NULL CHECK(mode IN ('model','cache','fallback','empty')),
    model_call_count INTEGER NOT NULL CHECK(model_call_count BETWEEN 0 AND 1),
    cache_hit_count INTEGER NOT NULL CHECK(cache_hit_count BETWEEN 0 AND 1),
    fallback_count INTEGER NOT NULL CHECK(fallback_count >= 0),
    malformed_count INTEGER NOT NULL CHECK(malformed_count >= 0),
    transport_error_count INTEGER NOT NULL CHECK(transport_error_count >= 0),
    created_at TEXT NOT NULL
)
"""
_INDEX_SQL = (
    "CREATE INDEX idx_subject_generation_receipts_mode "
    "ON subject_generation_receipts(mode, created_at)"
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


def _require_v9(connection: sqlite3.Connection) -> None:
    try:
        validate_v9(connection)
    except (ValueError, sqlite3.Error) as exc:
        raise ValueError(f"schema v10 requires a valid schema v9 database: {exc}") from exc


def validate_v10(connection: sqlite3.Connection) -> None:
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for schema v10")
    _require_v9(connection)
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    if marker is None:
        raise ValueError("schema v10 migration marker is missing")
    _timestamp(marker[0])
    missing = set(V10_TABLES) - _tables(connection)
    if missing:
        raise ValueError(f"schema v10 is incomplete; missing tables: {sorted(missing)}")
    actual_columns = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM pragma_table_info('subject_generation_receipts')"
        )
    )
    if actual_columns != V10_COLUMNS["subject_generation_receipts"]:
        raise ValueError(
            "schema v10 subject_generation_receipts has incompatible columns"
        )
    pk = tuple(
        row[1]
        for row in sorted(
            connection.execute(
                "PRAGMA table_info(subject_generation_receipts)"
            ).fetchall(),
            key=lambda row: row[5],
        )
        if row[5] > 0
    )
    if pk != ("subject_report_id",):
        raise ValueError("schema v10 has an incompatible primary key")
    foreign_keys = connection.execute(
        "PRAGMA foreign_key_list(subject_generation_receipts)"
    ).fetchall()
    mappings = tuple(
        (row[2], row[3], row[4]) for row in sorted(foreign_keys, key=lambda row: row[1])
    )
    if mappings != (("subject_reports", "subject_report_id", "subject_report_id"),):
        raise ValueError("schema v10 has incompatible foreign keys")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master "
            "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if indexes.get("idx_subject_generation_receipts_mode") != "subject_generation_receipts":
        raise ValueError("schema v10 generation receipt index is missing")
    index_columns = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM pragma_index_info('idx_subject_generation_receipts_mode')"
        )
    )
    if index_columns != ("mode", "created_at"):
        raise ValueError("schema v10 generation receipt index is incompatible")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v10(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v10 atomically; return False for a valid replay."""
    _timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v10 requires no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v9(connection)
    marker = connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V10_TABLES) & _tables(connection)
    if marker is not None:
        validate_v10(connection)
        return False
    if present:
        raise ValueError(
            f"partial schema v10 state without migration marker: {sorted(present)}"
        )
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(_TABLE_SQL)
        connection.execute(_INDEX_SQL)
        connection.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)",
            (SCHEMA_VERSION, applied_at),
        )
        validate_v10(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    validate_v10(connection)
    return True


apply_v10 = migrate_v10

__all__ = [
    "SCHEMA_VERSION",
    "V10_COLUMNS",
    "V10_TABLES",
    "apply_v10",
    "migrate_v10",
    "validate_v10",
]
