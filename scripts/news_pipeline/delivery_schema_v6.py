"""Schema v6 for durable legacy combined-report delivery receipts.

This migration is intentionally separate from Phase 5.  It is never applied
implicitly by a delivery job; the caller must perform the gated v6 migration
as an explicit production operation after the Phase 5 shadow gate.
Run 8 subject-scoped outbox tables are additive schema-v9 state and do not
retroactively alter this published migration.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

SCHEMA_VERSION = 6
DELIVERY_STATES = frozenset({"prepared", "sent", "failed", "ambiguous", "dry_run", "skipped"})
ATTEMPT_STATES = frozenset({"prepared", "sent", "failed", "ambiguous", "dry_run"})

_DELIVERY_COLUMNS = (
    "report_id", "idempotency_key", "channel", "recipient_hash",
    "content_sha256", "state", "current_attempt_id", "created_at", "updated_at",
)
_ATTEMPT_COLUMNS = (
    "attempt_id", "report_id", "ordinal", "state", "content_sha256",
    "prepared_at", "completed_at", "message_ids_json", "error_code", "error_detail",
)


def _validate_timestamp(value: object, *, name: str) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{name} must be a UTC ISO-8601 timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid UTC ISO-8601 timestamp") from exc
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{name} must be UTC")
    return value


def _table_names(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )


def _require_v5(connection: sqlite3.Connection) -> None:
    versions = {int(row[0]) for row in connection.execute("SELECT version FROM schema_migrations")}
    required = {1, 2, 3, 4, 5}
    if not required.issubset(versions):
        raise ValueError(f"schema v6 requires migration markers 1-5, got {sorted(versions)}")
    missing = {"event_claims", "reports", "report_events"} - _table_names(connection)
    if missing:
        raise ValueError(f"schema v6 requires report-capable v5 tables: {sorted(missing)}")


def validate_v6(connection: sqlite3.Connection) -> None:
    """Validate v6 markers, columns, and foreign-key enforcement."""
    _require_v5(connection)
    versions = {int(row[0]) for row in connection.execute("SELECT version FROM schema_migrations")}
    if SCHEMA_VERSION not in versions:
        raise ValueError("schema v6 migration marker is missing")
    tables = _table_names(connection)
    missing = {"report_deliveries", "report_delivery_attempts"} - tables
    if missing:
        raise ValueError(f"schema v6 is missing tables: {sorted(missing)}")
    actual_delivery = tuple(
        row[1] for row in connection.execute("PRAGMA table_info(report_deliveries)")
    )
    actual_attempt = tuple(
        row[1] for row in connection.execute("PRAGMA table_info(report_delivery_attempts)")
    )
    if actual_delivery != _DELIVERY_COLUMNS:
        raise ValueError(f"report_deliveries columns are incompatible: {actual_delivery}")
    if actual_attempt != _ATTEMPT_COLUMNS:
        raise ValueError(f"report_delivery_attempts columns are incompatible: {actual_attempt}")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for v6 delivery operations")


def migrate_v6(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply v6 exactly once and validate it; return whether it was newly applied."""
    _validate_timestamp(applied_at, name="applied_at")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled before applying schema v6")
    _require_v5(connection)
    versions = {int(row[0]) for row in connection.execute("SELECT version FROM schema_migrations")}
    if SCHEMA_VERSION in versions:
        validate_v6(connection)
        return False
    if any(version > SCHEMA_VERSION for version in versions):
        raise ValueError(f"cannot apply schema v6 after a newer marker: {sorted(versions)}")

    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """CREATE TABLE report_deliveries(
                   report_id TEXT PRIMARY KEY REFERENCES reports(report_id),
                   idempotency_key TEXT NOT NULL UNIQUE,
                   channel TEXT NOT NULL CHECK(channel='telegram'),
                   recipient_hash TEXT NOT NULL CHECK(length(recipient_hash)=64),
                   content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
                   state TEXT NOT NULL CHECK(state IN ('prepared','sent','failed','ambiguous','dry_run','skipped')),
                   current_attempt_id TEXT,
                   created_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL
               )"""
        )
        connection.execute(
            """CREATE TABLE report_delivery_attempts(
                   attempt_id TEXT PRIMARY KEY,
                   report_id TEXT NOT NULL REFERENCES report_deliveries(report_id),
                   ordinal INTEGER NOT NULL CHECK(ordinal >= 1),
                   state TEXT NOT NULL CHECK(state IN ('prepared','sent','failed','ambiguous','dry_run')),
                   content_sha256 TEXT NOT NULL CHECK(length(content_sha256)=64),
                   prepared_at TEXT NOT NULL,
                   completed_at TEXT,
                   message_ids_json TEXT,
                   error_code TEXT,
                   error_detail TEXT,
                   UNIQUE(report_id, ordinal)
               )"""
        )
        connection.execute(
            "CREATE INDEX idx_report_deliveries_state ON report_deliveries(state, updated_at)"
        )
        connection.execute(
            "CREATE INDEX idx_report_delivery_attempts_report ON report_delivery_attempts(report_id, ordinal)"
        )
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES(?, ?)",
            (SCHEMA_VERSION, applied_at),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    validate_v6(connection)
    return True


__all__ = [
    "ATTEMPT_STATES",
    "DELIVERY_STATES",
    "SCHEMA_VERSION",
    "migrate_v6",
    "validate_v6",
]
