"""Transactional additive schema-v5 migration for event claim linkage."""
from __future__ import annotations

import sqlite3
from datetime import datetime

SCHEMA_VERSION = 5
V5_TABLES = ("event_claims",)
V5_COLUMNS = {"event_claims": ("event_id", "event_version", "claim_id")}
_INDEX_SQL = {
    "idx_event_claims_claim": "CREATE INDEX idx_event_claims_claim ON event_claims(claim_id, event_id, event_version)",
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


def _tables(connection: sqlite3.Connection) -> set[str]:
    return {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}


def _require_v4(connection: sqlite3.Connection) -> None:
    tables = _tables(connection)
    marker = connection.execute("SELECT 1 FROM schema_migrations WHERE version=4").fetchone()
    if marker is None:
        raise ValueError("schema v4 must be applied before schema v5")
    required = {"schema_migrations", "claims", "event_versions", "events"}
    missing = required - tables
    if missing:
        raise ValueError(f"incompatible pre-v5 database; missing tables: {sorted(missing)}")
    from .schema_v4 import _validate_v4
    _validate_v4(connection)


def _validate_v5(connection: sqlite3.Connection) -> None:
    marker = connection.execute("SELECT applied_at FROM schema_migrations WHERE version=5").fetchone()
    if marker is None:
        raise ValueError("schema v5 migration marker is missing")
    if "event_claims" not in _tables(connection):
        raise ValueError("schema v5 is incomplete; missing event_claims")
    table_info = tuple(connection.execute("PRAGMA table_info(event_claims)").fetchall())
    actual = tuple(row[1] for row in table_info)
    if actual != V5_COLUMNS["event_claims"]:
        raise ValueError(f"schema v5 table event_claims has incompatible columns: {actual}")
    primary_key = tuple(row[1] for row in sorted(table_info, key=lambda row: row[5]) if row[5])
    if primary_key != V5_COLUMNS["event_claims"]:
        raise ValueError(f"schema v5 event_claims has incompatible primary key: {primary_key}")
    index_rows = connection.execute("PRAGMA index_list(event_claims)").fetchall()
    required_index = next((row for row in index_rows if row[1] == "idx_event_claims_claim"), None)
    if required_index is None or required_index[2] != 0 or required_index[4] != 0:
        raise ValueError("schema v5 index idx_event_claims_claim is missing or incompatible")
    index_columns = tuple(row[2] for row in connection.execute("PRAGMA index_info(idx_event_claims_claim)").fetchall())
    if index_columns != ("claim_id", "event_id", "event_version"):
        raise ValueError(f"schema v5 index idx_event_claims_claim has incompatible columns: {index_columns}")
    foreign_keys = tuple(connection.execute("PRAGMA foreign_key_list(event_claims)").fetchall())
    mappings: dict[str, list[tuple[int, str, str]]] = {}
    for _id, seq, table, from_column, to_column, *_rest in foreign_keys:
        mappings.setdefault(table, []).append((seq, from_column, to_column))
    normalized = {
        table: tuple((from_column, to_column) for _seq, from_column, to_column in sorted(rows))
        for table, rows in mappings.items()
    }
    expected = {
        "claims": (("claim_id", "claim_id"),),
        "event_versions": (("event_id", "event_id"), ("event_version", "version")),
    }
    if normalized != expected:
        raise ValueError(f"schema v5 event_claims foreign keys are incompatible: {normalized}")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v5(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply v5 once, atomically; return False for an already-valid replay."""
    _validate_timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v5 requires a connection with no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v4(connection)
    marker = connection.execute("SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)).fetchone()
    present = set(V5_TABLES) & _tables(connection)
    if marker is not None:
        _validate_v5(connection)
        return False
    if present:
        raise ValueError(f"partial schema v5 state without migration marker: {sorted(present)}")
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute("""CREATE TABLE event_claims(
            event_id TEXT NOT NULL,
            event_version INTEGER NOT NULL CHECK(event_version > 0),
            claim_id TEXT NOT NULL,
            PRIMARY KEY(event_id, event_version, claim_id),
            FOREIGN KEY(event_id, event_version) REFERENCES event_versions(event_id, version),
            FOREIGN KEY(claim_id) REFERENCES claims(claim_id)
        )""")
        connection.execute(_INDEX_SQL["idx_event_claims_claim"])
        connection.execute("INSERT INTO schema_migrations(version, applied_at) VALUES (?,?)", (SCHEMA_VERSION, applied_at))
        _validate_v5(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    _validate_v5(connection)
    return True

apply_v5 = migrate_v5
