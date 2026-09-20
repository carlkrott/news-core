"""Transactional additive schema-v8 migration for feed lanes and investigations."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Sequence

from .schema_v7 import validate_v7

SCHEMA_VERSION = 8
V8_TABLES = ("feed_lane_receipts", "candidate_feed_lanes", "investigations")
V8_COLUMNS = {
    "feed_lane_receipts": (
        "receipt_id", "feed_lane_id", "source_id", "query_plan_id", "attempt_id",
        "category", "result_set_hash", "returned_count", "inserted_count",
        "duplicate_count", "rejected_count", "recorded_at",
    ),
    "candidate_feed_lanes": (
        "source_item_id", "feed_lane_id", "query_plan_id", "attempt_id", "category", "first_seen_at",
    ),
    "investigations": (
        "investigation_id", "candidate_id", "feed_lane_id", "query_plan_id", "category",
        "round_number", "state", "terminal_state", "targeted_query_ids_json", "created_at", "updated_at",
    ),
}

_TABLE_SQL = {
    "feed_lane_receipts": """
        CREATE TABLE feed_lane_receipts(
            receipt_id TEXT PRIMARY KEY,
            feed_lane_id TEXT NOT NULL,
            source_id TEXT NOT NULL REFERENCES source_registry(source_id),
            query_plan_id TEXT NOT NULL REFERENCES query_plans(query_plan_id),
            attempt_id TEXT NOT NULL REFERENCES query_attempts(attempt_id),
            category TEXT NOT NULL CHECK(category IN ('ai','world','audio_engineering','hardware','fantasy_novel','audiovisual','av_corporate','our_setup')),
            result_set_hash TEXT NOT NULL CHECK(length(result_set_hash)=64),
            returned_count INTEGER NOT NULL CHECK(returned_count >= 0),
            inserted_count INTEGER NOT NULL CHECK(inserted_count >= 0),
            duplicate_count INTEGER NOT NULL CHECK(duplicate_count >= 0),
            rejected_count INTEGER NOT NULL CHECK(rejected_count >= 0),
            recorded_at TEXT NOT NULL,
            UNIQUE(feed_lane_id, attempt_id)
        )
    """,
    "candidate_feed_lanes": """
        CREATE TABLE candidate_feed_lanes(
            source_item_id TEXT NOT NULL REFERENCES source_items(source_item_id),
            feed_lane_id TEXT NOT NULL,
            query_plan_id TEXT NOT NULL REFERENCES query_plans(query_plan_id),
            attempt_id TEXT NOT NULL REFERENCES query_attempts(attempt_id),
            category TEXT NOT NULL CHECK(category IN ('ai','world','audio_engineering','hardware','fantasy_novel','audiovisual','av_corporate','our_setup')),
            first_seen_at TEXT NOT NULL,
            PRIMARY KEY(source_item_id, feed_lane_id, query_plan_id)
        )
    """,
    "investigations": """
        CREATE TABLE investigations(
            investigation_id TEXT PRIMARY KEY,
            candidate_id TEXT NOT NULL REFERENCES source_items(source_item_id),
            feed_lane_id TEXT NOT NULL,
            query_plan_id TEXT NOT NULL REFERENCES query_plans(query_plan_id),
            category TEXT NOT NULL CHECK(category IN ('ai','world','audio_engineering','hardware','fantasy_novel','audiovisual','av_corporate','our_setup')),
            round_number INTEGER NOT NULL CHECK(round_number >= 0 AND round_number < 2),
            state TEXT NOT NULL CHECK(state IN ('pending','running','complete','failed','blocked')),
            terminal_state TEXT CHECK(terminal_state IS NULL OR terminal_state IN ('complete','failed','blocked')),
            targeted_query_ids_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(candidate_id),
            CHECK((state IN ('complete','failed','blocked') AND terminal_state = state) OR (state IN ('pending','running') AND terminal_state IS NULL))
        )
    """,
}

_INDEX_SQL = {
    "idx_feed_lane_receipts_lane": "CREATE INDEX idx_feed_lane_receipts_lane ON feed_lane_receipts(feed_lane_id, recorded_at)",
    "idx_candidate_feed_lanes_lane": "CREATE INDEX idx_candidate_feed_lanes_lane ON candidate_feed_lanes(feed_lane_id, category, first_seen_at)",
    "idx_investigations_state": "CREATE INDEX idx_investigations_state ON investigations(state, updated_at)",
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


def _require_v7(connection: sqlite3.Connection) -> None:
    try:
        validate_v7(connection)
    except (ValueError, sqlite3.Error) as exc:
        raise ValueError(f"schema v8 requires a valid schema v7 database: {exc}") from exc


def _validate_v8(connection: sqlite3.Connection) -> None:
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    if marker is None:
        raise ValueError("schema v8 migration marker is missing")
    missing = set(V8_TABLES) - _tables(connection)
    if missing:
        raise ValueError(f"schema v8 is incomplete; missing tables: {sorted(missing)}")
    for table, expected in V8_COLUMNS.items():
        actual = tuple(
            row[0] for row in connection.execute("SELECT name FROM pragma_table_info(?)", (table,))
        )
        if actual != expected:
            raise ValueError(f"schema v8 table {table} has incompatible columns: {actual}")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    for index_name, table in ((name, statement.split(" ON ", 1)[1].split("(", 1)[0]) for name, statement in _INDEX_SQL.items()):
        if indexes.get(index_name) != table:
            raise ValueError(f"schema v8 index {index_name} is missing or incompatible")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for schema v8")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v8(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v8 once, atomically; return False for a valid replay."""
    _validate_timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v8 requires a connection with no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v7(connection)
    marker = connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V8_TABLES) & _tables(connection)
    if marker is not None:
        _validate_v8(connection)
        return False
    if present:
        raise ValueError(f"partial schema v8 state without migration marker: {sorted(present)}")
    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in V8_TABLES:
            connection.execute(_TABLE_SQL[table])
        for statement in _INDEX_SQL.values():
            connection.execute(statement)
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, applied_at),
        )
        _validate_v8(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    _validate_v8(connection)
    return True


validate_v8 = _validate_v8
apply_v8 = migrate_v8

__all__ = ["SCHEMA_VERSION", "V8_COLUMNS", "V8_TABLES", "apply_v8", "migrate_v8", "validate_v8"]
