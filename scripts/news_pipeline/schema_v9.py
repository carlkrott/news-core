"""Transactional additive schema-v9 migration for subject reports, delivery outbox, and delivery attempts."""
from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Sequence

from .schema_v8 import validate_v8

SCHEMA_VERSION = 9
V9_TABLES = ("subject_reports", "subject_delivery_outbox", "subject_delivery_attempts")
V9_COLUMNS = {
    "subject_reports": (
        "subject_report_id",
        "parent_report_id",
        "subject_id",
        "revision",
        "content_sha256",
        "story_count",
        "created_at",
    ),
    "subject_delivery_outbox": (
        "subject_report_id",
        "idempotency_key",
        "channel",
        "recipient_hash",
        "content_sha256",
        "state",
        "current_attempt_id",
        "created_at",
        "updated_at",
    ),
    "subject_delivery_attempts": (
        "attempt_id",
        "subject_report_id",
        "ordinal",
        "state",
        "recipient_hash",
        "content_sha256",
        "prepared_at",
        "completed_at",
        "message_ids_json",
        "error_code",
        "error_detail",
    ),
}

_SUBJECTS = "'ai','world','audio_engineering','professional_av','hardware','fantasy_novel','our_setup'"
_OUTBOX_STATES = "'prepared','sent','failed','ambiguous','skipped'"
_ATTEMPT_STATES = "'prepared','sent','failed','ambiguous'"

_TABLE_SQL = {
    "subject_reports": f"""
        CREATE TABLE subject_reports(
            subject_report_id TEXT PRIMARY KEY,
            parent_report_id TEXT NOT NULL REFERENCES reports(report_id),
            subject_id TEXT NOT NULL CHECK(subject_id IN ({_SUBJECTS})),
            revision INTEGER NOT NULL CHECK(revision >= 1),
            content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
            story_count INTEGER NOT NULL CHECK(story_count >= 0),
            created_at TEXT NOT NULL,
            UNIQUE(parent_report_id, subject_id, revision),
            UNIQUE(parent_report_id, subject_id, content_sha256)
        )
    """,
    "subject_delivery_outbox": f"""
        CREATE TABLE subject_delivery_outbox(
            subject_report_id TEXT PRIMARY KEY REFERENCES subject_reports(subject_report_id),
            idempotency_key TEXT NOT NULL UNIQUE,
            channel TEXT CHECK(channel IS NULL OR (length(trim(channel)) > 0 AND channel = trim(channel))),
            recipient_hash TEXT CHECK(recipient_hash IS NULL OR length(recipient_hash) = 64),
            content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
            state TEXT NOT NULL CHECK(state IN ({_OUTBOX_STATES})),
            current_attempt_id TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """,
    "subject_delivery_attempts": f"""
        CREATE TABLE subject_delivery_attempts(
            attempt_id TEXT PRIMARY KEY,
            subject_report_id TEXT NOT NULL REFERENCES subject_delivery_outbox(subject_report_id),
            ordinal INTEGER NOT NULL CHECK(ordinal >= 1),
            state TEXT NOT NULL CHECK(state IN ({_ATTEMPT_STATES})),
            recipient_hash TEXT NOT NULL CHECK(length(recipient_hash) = 64),
            content_sha256 TEXT NOT NULL CHECK(length(content_sha256) = 64),
            prepared_at TEXT NOT NULL,
            completed_at TEXT,
            message_ids_json TEXT,
            error_code TEXT,
            error_detail TEXT,
            UNIQUE(subject_report_id, ordinal)
        )
    """,
}

_INDEX_SQL = {
    "idx_subject_reports_lookup": "CREATE INDEX idx_subject_reports_lookup ON subject_reports(parent_report_id, subject_id, revision)",
    "idx_subject_delivery_outbox_state": "CREATE INDEX idx_subject_delivery_outbox_state ON subject_delivery_outbox(state, updated_at)",
    "idx_subject_delivery_attempts_ordinal": "CREATE INDEX idx_subject_delivery_attempts_ordinal ON subject_delivery_attempts(subject_report_id, ordinal)",
}

_INDEX_COLUMNS = {
    "idx_subject_reports_lookup": ("parent_report_id", "subject_id", "revision"),
    "idx_subject_delivery_outbox_state": ("state", "updated_at"),
    "idx_subject_delivery_attempts_ordinal": ("subject_report_id", "ordinal"),
}

_EXPECTED_PKS = {
    "subject_reports": ("subject_report_id",),
    "subject_delivery_outbox": ("subject_report_id",),
    "subject_delivery_attempts": ("attempt_id",),
}

_EXPECTED_FKS = {
    "subject_reports": {"reports": (("parent_report_id", "report_id"),)},
    "subject_delivery_outbox": {"subject_reports": (("subject_report_id", "subject_report_id"),)},
    "subject_delivery_attempts": {"subject_delivery_outbox": (("subject_report_id", "subject_report_id"),)},
}


def _validate_timestamp(value: object) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError("applied_at must be a UTC ISO-8601 timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("applied_at must be a valid UTC ISO-8601 timestamp") from exc
    offset = parsed.utcoffset()
    if offset is None or offset.total_seconds() != 0:
        raise ValueError("applied_at must be UTC")
    return value


def _tables(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )


def _require_v8(connection: sqlite3.Connection) -> None:
    try:
        validate_v8(connection)
    except (ValueError, sqlite3.Error) as exc:
        raise ValueError(f"schema v9 requires a valid schema v8 database: {exc}") from exc


def _validate_v9(connection: sqlite3.Connection) -> None:
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for schema v9")
    _require_v8(connection)
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    if marker is None:
        raise ValueError("schema v9 migration marker is missing")
    _validate_timestamp(marker[0])
    missing = set(V9_TABLES) - _tables(connection)
    if missing:
        raise ValueError(f"schema v9 is incomplete; missing tables: {sorted(missing)}")
    for table, expected in V9_COLUMNS.items():
        actual = tuple(
            row[0] for row in connection.execute("SELECT name FROM pragma_table_info(?)", (table,))
        )
        if actual != expected:
            raise ValueError(f"schema v9 table {table} has incompatible columns: {actual}")
    for table, expected_pk in _EXPECTED_PKS.items():
        actual_pk = tuple(
            row[1] for row in sorted(
                connection.execute(f"PRAGMA table_info({table})").fetchall(),
                key=lambda r: r[5]
            ) if row[5] > 0
        )
        if actual_pk != expected_pk:
            raise ValueError(f"schema v9 table {table} has incompatible primary key: {actual_pk}")
    for table, expected_fks in _EXPECTED_FKS.items():
        fk_rows = connection.execute(f"PRAGMA foreign_key_list({table})").fetchall()
        mappings: dict[str, list[tuple[int, str, str]]] = {}
        for _id, seq, ref_table, from_col, to_col, *_rest in fk_rows:
            mappings.setdefault(ref_table, []).append((seq, from_col, to_col))
        normalized = {
            ref_table: tuple((from_col, to_col) for _seq, from_col, to_col in sorted(cols))
            for ref_table, cols in mappings.items()
        }
        if normalized != expected_fks:
            raise ValueError(f"schema v9 table {table} foreign keys are incompatible: {normalized}")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    for index_name, table in ((name, statement.split(" ON ", 1)[1].split("(", 1)[0].strip()) for name, statement in _INDEX_SQL.items()):
        if indexes.get(index_name) != table:
            raise ValueError(f"schema v9 index {index_name} is missing or incompatible")
    for index_name, expected_cols in _INDEX_COLUMNS.items():
        actual_cols = tuple(
            row[0] for row in connection.execute("SELECT name FROM pragma_index_info(?)", (index_name,))
        )
        if actual_cols != expected_cols:
            raise ValueError(f"schema v9 index {index_name} has incompatible columns: {actual_cols}")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v9(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v9 once, atomically; return False for a valid replay."""
    _validate_timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v9 requires a connection with no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v8(connection)
    marker = connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V9_TABLES) & _tables(connection)
    if marker is not None:
        _validate_v9(connection)
        return False
    if present:
        raise ValueError(f"partial schema v9 state without migration marker: {sorted(present)}")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in V9_TABLES:
            connection.execute(_TABLE_SQL[table])
        for statement in _INDEX_SQL.values():
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, applied_at),
        )
        _validate_v9(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    _validate_v9(connection)
    return True


validate_v9 = _validate_v9
apply_v9 = migrate_v9

__all__ = ["SCHEMA_VERSION", "V9_COLUMNS", "V9_TABLES", "apply_v9", "migrate_v9", "validate_v9"]
