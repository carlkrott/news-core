"""Transactional additive schema-v11 migration for claim-level provenance snapshots."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta

from .schema_v10 import validate_v10
from .verification import authority_entity_matches, lead_retrieval_reason

SCHEMA_VERSION = 11
V11_TABLES = ("claim_evidence_provenance",)
V11_COLUMNS = {
    "claim_evidence_provenance": (
        "evidence_id",
        "source_item_id",
        "normalized_publisher_host",
        "effective_source_role",
        "independence_group",
        "matched_rule_id",
        "authority_scope_json",
        "authority_entities_json",
        "authority_match",
        "classification_timestamp",
        "classification_reason",
    )
}

_TABLE_SQL = """
CREATE TABLE claim_evidence_provenance(
    evidence_id TEXT PRIMARY KEY REFERENCES claim_evidence(evidence_id),
    source_item_id TEXT NOT NULL REFERENCES source_items(source_item_id),
    normalized_publisher_host TEXT NOT NULL CHECK(length(trim(normalized_publisher_host)) > 0),
    effective_source_role TEXT NOT NULL CHECK(effective_source_role IN ('discovery','primary','neutral','specialist')),
    independence_group TEXT NOT NULL CHECK(length(trim(independence_group)) > 0),
    matched_rule_id TEXT REFERENCES publisher_registry(rule_id),
    authority_scope_json TEXT NOT NULL CHECK(json_valid(authority_scope_json)),
    authority_entities_json TEXT NOT NULL CHECK(json_valid(authority_entities_json)),
    authority_match INTEGER NOT NULL CHECK(authority_match IN (0,1)),
    classification_timestamp TEXT NOT NULL,
    classification_reason TEXT NOT NULL CHECK(length(trim(classification_reason)) > 0)
)
"""
_INDEX_SQL = (
    "CREATE INDEX idx_claim_evidence_provenance_group "
    "ON claim_evidence_provenance(independence_group, effective_source_role)",
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


def _valid_timestamp(value: object) -> bool:
    try:
        _timestamp(value)
        return True
    except ValueError:
        return False


def _tables(connection: sqlite3.Connection) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    )


def _authority_match(role: str, scope_json: str, entities_json: str, category: str, subject: str) -> bool:
    if role != "primary":
        return False
    try:
        scopes = json.loads(scope_json)
        entities = json.loads(entities_json)
    except (TypeError, json.JSONDecodeError):
        return False
    if type(scopes) is not list or type(entities) is not list:
        return False
    return (
        category in scopes
        and authority_entity_matches(subject, entities)
    )


def _backfill(connection: sqlite3.Connection, applied_at: str) -> None:
    rows = connection.execute(
        """SELECT ce.evidence_id,ce.source_item_id,c.subject,si.category,si.retrieval_method,
                  sp.normalized_publisher_host,sp.effective_source_role,
                  sp.independence_group,sp.matched_rule_id,
                  sp.classification_timestamp,sp.classification_reason,
                  pr.category_scope_json,pr.authority_entities_json
             FROM claim_evidence ce
             JOIN claims c ON c.claim_id=ce.claim_id
             JOIN source_items si ON si.source_item_id=ce.source_item_id
             LEFT JOIN source_item_provenance sp ON sp.source_item_id=ce.source_item_id
             LEFT JOIN publisher_registry pr ON pr.rule_id=sp.matched_rule_id
            ORDER BY ce.evidence_id"""
    ).fetchall()
    for row in rows:
        (evidence_id, source_item_id, subject, category, retrieval_method,
         host, role, group, rule_id, classified_at, reason, scope_json,
         entities_json) = row
        transport_reason = lead_retrieval_reason(retrieval_method)
        if transport_reason is not None:
            role = "discovery"
            group = "unknown"
            rule_id = None
            scope_json, entities_json = "[]", "[]"
            reason = transport_reason
        if role not in {"discovery", "primary", "neutral", "specialist"}:
            role = "discovery"
        if type(host) is not str or not host.strip():
            host = "unknown"
            role = "discovery"
        if type(group) is not str or not group.strip():
            group = "unknown"
            role = "discovery"
        if type(rule_id) is not str or not rule_id.strip():
            rule_id = None
            role = "discovery"
        if type(scope_json) is not str or type(entities_json) is not str:
            scope_json, entities_json = "[]", "[]"
        try:
            scope = json.loads(scope_json)
            entities = json.loads(entities_json)
            if type(scope) is not list or any(type(value) is not str for value in scope):
                raise ValueError
            if type(entities) is not list or any(type(value) is not str for value in entities):
                raise ValueError
        except (TypeError, json.JSONDecodeError, ValueError):
            scope_json, entities_json = "[]", "[]"
            role = "discovery"
            rule_id = None
        authority_match = _authority_match(role, scope_json, entities_json, category, subject)
        if not _valid_timestamp(classified_at):
            classified_at = applied_at
        connection.execute(
            """INSERT INTO claim_evidence_provenance(
                   evidence_id,source_item_id,normalized_publisher_host,
                   effective_source_role,independence_group,matched_rule_id,
                   authority_scope_json,authority_entities_json,authority_match,
                   classification_timestamp,classification_reason)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                evidence_id,
                source_item_id,
                host,
                role,
                group,
                rule_id,
                scope_json,
                entities_json,
                int(authority_match),
                classified_at if type(classified_at) is str and classified_at else applied_at,
                reason if type(reason) is str and reason.strip() else "missing_source_provenance",
            ),
        )


def validate_v11(connection: sqlite3.Connection) -> None:
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement must be enabled for schema v11")
    validate_v10(connection)
    marker = connection.execute(
        "SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    if marker is None:
        raise ValueError("schema v11 migration marker is missing")
    _timestamp(marker[0])
    missing = set(V11_TABLES) - _tables(connection)
    if missing:
        raise ValueError(f"schema v11 is incomplete; missing tables: {sorted(missing)}")
    actual_columns = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM pragma_table_info('claim_evidence_provenance')"
        )
    )
    if actual_columns != V11_COLUMNS["claim_evidence_provenance"]:
        raise ValueError("schema v11 claim_evidence_provenance has incompatible columns")
    primary_key = tuple(
        row[1]
        for row in sorted(
            connection.execute("PRAGMA table_info(claim_evidence_provenance)").fetchall(),
            key=lambda row: row[5],
        )
        if row[5] > 0
    )
    if primary_key != ("evidence_id",):
        raise ValueError("schema v11 has an incompatible primary key")
    foreign_keys = connection.execute(
        "PRAGMA foreign_key_list(claim_evidence_provenance)"
    ).fetchall()
    actual_fks = {(row[2], row[3], row[4]) for row in foreign_keys}
    expected_fks = {
        ("claim_evidence", "evidence_id", "evidence_id"),
        ("publisher_registry", "matched_rule_id", "rule_id"),
        ("source_items", "source_item_id", "source_item_id"),
    }
    if actual_fks != expected_fks:
        raise ValueError("schema v11 has incompatible foreign keys")
    indexes = {
        row[0]: row[1]
        for row in connection.execute(
            "SELECT name,tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
        )
    }
    if indexes.get("idx_claim_evidence_provenance_group") != "claim_evidence_provenance":
        raise ValueError("schema v11 provenance index is missing or incompatible")
    columns = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM pragma_index_info('idx_claim_evidence_provenance_group')"
        )
    )
    if columns != ("independence_group", "effective_source_role"):
        raise ValueError("schema v11 provenance index columns are incompatible")
    counts = connection.execute(
        "SELECT (SELECT COUNT(*) FROM claim_evidence),"
        "(SELECT COUNT(*) FROM claim_evidence_provenance)"
    ).fetchone()
    if counts[0] != counts[1]:
        raise ValueError("schema v11 claim evidence provenance is incomplete")
    rows = connection.execute(
        """SELECT p.effective_source_role,p.independence_group,p.matched_rule_id,
                  p.normalized_publisher_host,p.authority_scope_json,
                  p.authority_entities_json,p.authority_match,
                  p.classification_timestamp,p.classification_reason,
                  c.subject,si.category
             FROM claim_evidence_provenance p
             JOIN claim_evidence ce ON ce.evidence_id=p.evidence_id
             JOIN claims c ON c.claim_id=ce.claim_id
             JOIN source_items si ON si.source_item_id=ce.source_item_id"""
    ).fetchall()
    for row in rows:
        role, group, rule_id, host, scope_json, entities_json, matched, classified_at, reason, subject, category = row
        try:
            scopes = json.loads(scope_json)
            entities = json.loads(entities_json)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("schema v11 provenance JSON is malformed") from exc
        if (
            role not in {"discovery", "primary", "neutral", "specialist"}
            or type(group) is not str or not group.strip()
            or type(host) is not str or not host.strip()
            or type(reason) is not str or not reason.strip()
            or type(scopes) is not list
            or any(type(value) is not str or not value.strip() for value in scopes)
            or len(scopes) != len(set(scopes))
            or type(entities) is not list
            or any(type(value) is not str or not value.strip() for value in entities)
            or len(entities) != len(set(entities))
            or not _valid_timestamp(classified_at)
        ):
            raise ValueError("schema v11 claim provenance snapshot has invalid fields")
        if role != "discovery" and (
            type(rule_id) is not str or not rule_id.strip()
            or host == "unknown" or group == "unknown"
        ):
            raise ValueError("schema v11 effective publisher identity is incomplete")
        expected_match = _authority_match(
            role, scope_json, entities_json, category, subject
        )
        if type(matched) is not int or matched != int(expected_match):
            raise ValueError("schema v11 claim authority match is inconsistent")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v11(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v11 atomically; persist claim-specific provenance for all evidence."""
    applied_at = _timestamp(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v11 requires no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    validate_v10(connection)
    marker = connection.execute(
        "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
    ).fetchone()
    present = set(V11_TABLES) & _tables(connection)
    if marker is not None:
        validate_v11(connection)
        return False
    if present:
        raise ValueError(f"partial schema v11 state without migration marker: {sorted(present)}")
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(_TABLE_SQL)
        for statement in _INDEX_SQL:
            connection.execute(statement)
        _backfill(connection, applied_at)
        connection.execute(
            "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)",
            (SCHEMA_VERSION, applied_at),
        )
        validate_v11(connection)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    validate_v11(connection)
    return True


__all__ = ["SCHEMA_VERSION", "V11_COLUMNS", "V11_TABLES", "migrate_v11", "validate_v11"]
