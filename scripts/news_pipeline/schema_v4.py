"""Explicit additive SQLite schema-v4 migration for fetch-state persistence.

The migration accepts an existing sqlite3 connection and caller-supplied UTC
``applied_at`` timestamp. It never opens a path, reads a clock, or hooks the
legacy ``db.init_db`` function.

Prerequisite: schema v3 must be fully applied before schema v4 is applied.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

SCHEMA_VERSION = 4
V4_TABLES = ("fetch_state",)

_TABLE_SQL: dict[str, str] = {
    "fetch_state": """
        CREATE TABLE fetch_state(
            source_id TEXT PRIMARY KEY REFERENCES source_registry(source_id),
            etag TEXT,
            last_modified TEXT,
            cursor TEXT,
            rate_limit_remaining INTEGER CHECK(rate_limit_remaining IS NULL OR rate_limit_remaining >= 0),
            rate_limit_reset_at TEXT,
            retry_after_seconds INTEGER CHECK(retry_after_seconds IS NULL OR retry_after_seconds >= 0),
            last_http_status INTEGER CHECK(last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599),
            updated_at TEXT NOT NULL
        )
    """,
}

V4_COLUMNS: dict[str, tuple[str, ...]] = {
    "fetch_state": (
        "source_id", "etag", "last_modified", "cursor",
        "rate_limit_remaining", "rate_limit_reset_at", "retry_after_seconds",
        "last_http_status", "updated_at",
    ),
}

_INDEX_SQL: dict[str, str] = {
    "idx_fetch_state_reset": (
        "CREATE INDEX idx_fetch_state_reset ON fetch_state(rate_limit_reset_at)"
        " WHERE rate_limit_reset_at IS NOT NULL"
    ),
}


def _validate_applied_at(value: object) -> str:
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


def _table_names(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )


def _require_v3(connection: sqlite3.Connection) -> None:
    """Verify that schema v3 migration marker exists and all v3 tables are present."""
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=3"
    ).fetchone()
    if marker is None:
        raise ValueError("schema v3 must be applied before schema v4")
    tables = _table_names(connection)
    from news_pipeline.schema_v3 import V3_TABLES as V3
    missing = set(V3) - tables
    if missing:
        raise ValueError(f"incompatible pre-v4 database; missing v3 tables: {sorted(missing)}")


def _validate_v4(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    missing = set(V4_TABLES) - tables
    if missing:
        raise ValueError(f"schema v4 is incomplete; missing tables: {sorted(missing)}")
    for table, expected in V4_COLUMNS.items():
        actual = tuple(
            row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')
        )
        if actual != expected:
            raise ValueError(f"schema v4 table {table} has incompatible columns: {actual}")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master "
            "WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    for index_name, statement in _INDEX_SQL.items():
        expected_table = statement.split(" ON ", 1)[1].split("(", 1)[0]
        if indexes.get(index_name) != expected_table:
            raise ValueError(f"schema v4 index {index_name} is missing or incompatible")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v4(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v4 transactionally; return True if applied, False if already valid."""
    _validate_applied_at(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v4 requires a connection with no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v3(connection)
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V4_TABLES) & _table_names(connection)
    if marker is not None:
        _validate_v4(connection)
        return False
    if present:
        raise ValueError(f"partial schema v4 state without migration marker: {sorted(present)}")

    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in V4_TABLES:
            connection.execute(_TABLE_SQL[table])
        for statement in _INDEX_SQL.values():
            connection.execute(statement)
        _validate_v4(connection)
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, applied_at),
        )
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    _validate_v4(connection)
    return True
