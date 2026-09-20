"""Transactional additive schema-v7 migration for publisher provenance."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Iterable, Sequence

from .delivery_schema_v6 import validate_v6
from .live_contracts import SourceRole
from .provenance import PublisherRegistry, PublisherRule

SCHEMA_VERSION = 7
V7_TABLES = ("publisher_registry", "source_item_provenance")
V7_COLUMNS = {
    "publisher_registry": (
        "rule_id", "normalized_host", "effective_source_role", "independence_group",
        "category_scope_json", "authority_entities_json", "enabled", "audit_note", "created_at",
    ),
    "source_item_provenance": (
        "source_item_id", "normalized_publisher_host", "effective_source_role",
        "independence_group", "matched_rule_id", "authority_match",
        "classification_timestamp", "classification_reason",
    ),
}

_TABLE_SQL = {
    "publisher_registry": """
        CREATE TABLE publisher_registry(
            rule_id TEXT PRIMARY KEY,
            normalized_host TEXT NOT NULL,
            effective_source_role TEXT NOT NULL CHECK(effective_source_role IN ('discovery','primary','neutral','specialist')),
            independence_group TEXT NOT NULL,
            category_scope_json TEXT NOT NULL,
            authority_entities_json TEXT NOT NULL,
            enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
            audit_note TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """,
    "source_item_provenance": """
        CREATE TABLE source_item_provenance(
            source_item_id TEXT PRIMARY KEY REFERENCES source_items(source_item_id),
            normalized_publisher_host TEXT NOT NULL,
            effective_source_role TEXT NOT NULL CHECK(effective_source_role IN ('discovery','primary','neutral','specialist')),
            independence_group TEXT NOT NULL,
            matched_rule_id TEXT REFERENCES publisher_registry(rule_id),
            authority_match INTEGER NOT NULL CHECK(authority_match IN (0,1)),
            classification_timestamp TEXT NOT NULL,
            classification_reason TEXT NOT NULL
        )
    """,
}

_INDEX_SQL = {
    "idx_publisher_registry_host": "CREATE INDEX idx_publisher_registry_host ON publisher_registry(normalized_host, enabled)",
    "idx_source_item_provenance_group": "CREATE INDEX idx_source_item_provenance_group ON source_item_provenance(independence_group, effective_source_role)",
    "idx_source_item_provenance_rule": "CREATE INDEX idx_source_item_provenance_rule ON source_item_provenance(matched_rule_id)",
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


def _table_names(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )


def _require_v6(connection: sqlite3.Connection) -> None:
    try:
        validate_v6(connection)
    except (ValueError, sqlite3.Error) as exc:
        raise ValueError(f"schema v7 requires a valid schema v6 database: {exc}") from exc


def _validate_v7(connection: sqlite3.Connection) -> None:
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    if marker is None:
        raise ValueError("schema v7 migration marker is missing")
    missing = set(V7_TABLES) - _table_names(connection)
    if missing:
        raise ValueError(f"schema v7 is incomplete; missing tables: {sorted(missing)}")
    for table, expected in V7_COLUMNS.items():
        actual = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM pragma_table_info(?)", (table,)
            )
        )
        if actual != expected:
            raise ValueError(f"schema v7 table {table} has incompatible columns: {actual}")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    for index_name in _INDEX_SQL:
        if indexes.get(index_name) not in V7_TABLES:
            raise ValueError(f"schema v7 index {index_name} is missing or incompatible")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for schema v7")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def _insert_rules(connection: sqlite3.Connection, rules: Sequence[PublisherRule], applied_at: str) -> None:
    for rule in rules:
        connection.execute(
            """INSERT INTO publisher_registry(
                   rule_id,normalized_host,effective_source_role,independence_group,
                   category_scope_json,authority_entities_json,enabled,audit_note,created_at)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (
                rule.rule_id,
                rule.host,
                rule.source_role.value,
                rule.independence_group,
                json.dumps(rule.categories, ensure_ascii=False, separators=(",", ":")),
                json.dumps(rule.authority_entities, ensure_ascii=False, separators=(",", ":")),
                int(rule.enabled),
                rule.audit_note,
                applied_at,
            ),
        )


def backfill_source_item_provenance(
    connection: sqlite3.Connection,
    registry: PublisherRegistry,
    classified_at: str,
    *,
    source_item_ids: Iterable[str] | None = None,
) -> int:
    """Classify source items using reviewed rules; unknown items stay discovery."""
    _validate_timestamp(classified_at)
    _validate_v7(connection)
    requested = None if source_item_ids is None else tuple(source_item_ids)
    if requested is not None and len(set(requested)) != len(requested):
        raise ValueError("source_item_ids must not contain duplicates")
    if requested is None:
        rows = connection.execute(
            "SELECT source_item_id,canonical_url,category,publisher FROM source_items ORDER BY source_item_id"
        ).fetchall()
    elif not requested:
        return 0
    else:
        placeholders = ",".join("?" for _ in requested)
        rows = connection.execute(
            f"SELECT source_item_id,canonical_url,category,publisher FROM source_items WHERE source_item_id IN ({placeholders}) ORDER BY source_item_id",
            requested,
        ).fetchall()
        found = {row[0] for row in rows}
        missing = sorted(set(requested) - found)
        if missing:
            raise ValueError(f"source items not found: {missing}")
    classified = 0
    for source_item_id, canonical_url, category, publisher in rows:
        result = registry.classify(
            canonical_url,
            category=category,
            classified_at=classified_at,
            claim_subject=publisher,
        )
        connection.execute(
            """INSERT INTO source_item_provenance(
                   source_item_id,normalized_publisher_host,effective_source_role,
                   independence_group,matched_rule_id,authority_match,
                   classification_timestamp,classification_reason)
               VALUES(?,?,?,?,?,?,?,?)
               ON CONFLICT(source_item_id) DO UPDATE SET
                   normalized_publisher_host=excluded.normalized_publisher_host,
                   effective_source_role=excluded.effective_source_role,
                   independence_group=excluded.independence_group,
                   matched_rule_id=excluded.matched_rule_id,
                   authority_match=excluded.authority_match,
                   classification_timestamp=excluded.classification_timestamp,
                   classification_reason=excluded.classification_reason""",
            (
                source_item_id,
                result.normalized_publisher_host,
                result.effective_source_role.value,
                result.independence_group,
                result.matched_rule_id,
                int(result.authority_match),
                result.classification_timestamp,
                result.classification_reason,
            ),
        )
        classified += 1
    return classified


def registry_from_connection(connection: sqlite3.Connection) -> PublisherRegistry:
    """Rebuild the reviewed registry stored in schema-v7 tables."""
    _validate_v7(connection)
    rules: list[PublisherRule] = []
    for row in connection.execute(
        """SELECT rule_id,normalized_host,effective_source_role,independence_group,
                  category_scope_json,authority_entities_json,enabled,audit_note
             FROM publisher_registry ORDER BY rule_id"""
    ):
        try:
            categories = json.loads(row[4])
            authority_entities = json.loads(row[5])
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("publisher registry JSON is invalid") from exc
        if not isinstance(categories, list) or not isinstance(authority_entities, list):
            raise ValueError("publisher registry JSON values must be lists")
        rules.append(
            PublisherRule(
                rule_id=row[0],
                host=row[1],
                source_role=SourceRole(row[2]),
                independence_group=row[3],
                categories=tuple(categories),
                authority_entities=tuple(authority_entities),
                enabled=bool(row[6]),
                audit_note=row[7],
            )
        )
    return PublisherRegistry(tuple(rules))


def migrate_v7(
    connection: sqlite3.Connection,
    applied_at: str,
    *,
    rules: Iterable[PublisherRule] = (),
) -> bool:
    """Apply schema v7 once, seed reviewed rules, and backfill provenance."""
    _validate_timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v7 requires a connection with no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v6(connection)
    rule_values = tuple(rules)
    if any(type(rule) is not PublisherRule for rule in rule_values):
        raise ValueError("rules must contain only PublisherRule values")
    marker = connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V7_TABLES) & _table_names(connection)
    if marker is not None:
        _validate_v7(connection)
        return False
    if present:
        raise ValueError(f"partial schema v7 state without migration marker: {sorted(present)}")

    registry = PublisherRegistry(rule_values)
    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in V7_TABLES:
            connection.execute(_TABLE_SQL[table])
        for statement in _INDEX_SQL.values():
            connection.execute(statement)
        _insert_rules(connection, rule_values, applied_at)
        connection.execute(
            "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
            (SCHEMA_VERSION, applied_at),
        )
        backfill_source_item_provenance(connection, registry, applied_at)
        _validate_v7(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    _validate_v7(connection)
    return True


validate_v7 = _validate_v7
apply_v7 = migrate_v7

__all__ = [
    "SCHEMA_VERSION",
    "V7_COLUMNS",
    "V7_TABLES",
    "apply_v7",
    "backfill_source_item_provenance",
    "migrate_v7",
    "registry_from_connection",
    "validate_v7",
]
