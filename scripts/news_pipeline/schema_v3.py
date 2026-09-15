"""Explicit additive SQLite schema-v3 migration.

The migration accepts an existing sqlite3 connection and caller-supplied UTC
``applied_at`` timestamp. It never opens a path, reads a clock, or hooks the
legacy ``db.init_db`` function.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

SCHEMA_VERSION = 3
V3_TABLES = (
    "source_registry",
    "source_items",
    "query_plans",
    "query_attempts",
    "claims",
    "claim_evidence",
    "event_versions",
    "event_dates",
    "reports",
    "report_events",
)
_REQUIRED_V2_TABLES = frozenset({
    "schema_migrations", "runs", "articles", "observations", "events",
    "event_articles", "fact_fingerprints", "decisions", "delivery_attempts",
    "manual_review", "query_telemetry",
})

_TABLE_SQL: dict[str, str] = {
    "source_registry": """
        CREATE TABLE source_registry(
            source_id TEXT PRIMARY KEY,
            adapter_type TEXT NOT NULL CHECK(adapter_type IN ('searxng','rss','hacker_news','github')),
            source_role TEXT NOT NULL CHECK(source_role IN ('discovery','primary','neutral','specialist')),
            host TEXT NOT NULL,
            category_scope_json TEXT NOT NULL,
            enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
            queries_json TEXT NOT NULL,
            title_blocklist_json TEXT NOT NULL,
            content_blocklist_json TEXT NOT NULL,
            url_blocklist_json TEXT NOT NULL,
            allowlist_domains_json TEXT NOT NULL,
            cadence_minutes INTEGER CHECK(cadence_minutes IS NULL OR cadence_minutes > 0),
            terms_notes TEXT,
            rate_limit_notes TEXT,
            next_due_at TEXT,
            config_hash TEXT NOT NULL CHECK(length(config_hash)=64),
            created_at TEXT NOT NULL
        )
    """,
    "source_items": """
        CREATE TABLE source_items(
            source_item_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL REFERENCES source_registry(source_id),
            external_id TEXT,
            category TEXT NOT NULL CHECK(category IN ('ai','world','audio_engineering','hardware','fantasy_novel','audiovisual','av_corporate','our_setup')),
            original_url TEXT NOT NULL,
            canonical_url TEXT NOT NULL,
            publisher TEXT NOT NULL,
            source_role TEXT NOT NULL CHECK(source_role IN ('discovery','primary','neutral','specialist')),
            author_handle TEXT,
            retrieval_method TEXT NOT NULL,
            raw_content_hash TEXT NOT NULL CHECK(length(raw_content_hash)=64),
            title TEXT,
            body TEXT,
            raw TEXT,
            retrieved_at TEXT NOT NULL,
            published_at TEXT,
            updated_at TEXT,
            publication_evidence TEXT,
            UNIQUE(source_id, canonical_url, raw_content_hash)
        )
    """,
    "query_plans": """
        CREATE TABLE query_plans(
            query_plan_id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL REFERENCES source_registry(source_id),
            query_text TEXT NOT NULL,
            category TEXT NOT NULL CHECK(category IN ('ai','world','audio_engineering','hardware','fantasy_novel','audiovisual','av_corporate','our_setup')),
            topic TEXT,
            entity TEXT,
            reason_selected TEXT NOT NULL,
            cooldown_seconds INTEGER NOT NULL CHECK(cooldown_seconds >= 0),
            max_rounds INTEGER NOT NULL CHECK(max_rounds > 0),
            created_at TEXT NOT NULL,
            UNIQUE(source_id, query_text, category, topic, entity)
        )
    """,
    "query_attempts": """
        CREATE TABLE query_attempts(
            attempt_id TEXT PRIMARY KEY,
            query_plan_id TEXT NOT NULL REFERENCES query_plans(query_plan_id),
            status TEXT NOT NULL CHECK(status IN ('pending','running','success','partial','failed','rate_limited')),
            started_at TEXT NOT NULL,
            finished_at TEXT,
            returned_count INTEGER NOT NULL DEFAULT 0 CHECK(returned_count >= 0),
            novel_count INTEGER NOT NULL DEFAULT 0 CHECK(novel_count >= 0),
            verified_count INTEGER NOT NULL DEFAULT 0 CHECK(verified_count >= 0),
            duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_count >= 0),
            stale_count INTEGER NOT NULL DEFAULT 0 CHECK(stale_count >= 0),
            error_count INTEGER NOT NULL DEFAULT 0 CHECK(error_count >= 0),
            error TEXT,
            rate_limit_reset_at TEXT
        )
    """,
    "claims": """
        CREATE TABLE claims(
            claim_id TEXT PRIMARY KEY,
            source_item_id TEXT NOT NULL REFERENCES source_items(source_item_id),
            subject TEXT NOT NULL,
            predicate TEXT NOT NULL,
            object_value TEXT NOT NULL,
            statement_type TEXT NOT NULL,
            extraction_confidence TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending','verified','rejected','superseded')),
            extracted_at TEXT NOT NULL
        )
    """,
    "claim_evidence": """
        CREATE TABLE claim_evidence(
            evidence_id TEXT PRIMARY KEY,
            claim_id TEXT NOT NULL REFERENCES claims(claim_id),
            source_item_id TEXT NOT NULL REFERENCES source_items(source_item_id),
            evidence_role TEXT NOT NULL CHECK(evidence_role IN ('supports','contradicts')),
            exact_excerpt TEXT NOT NULL,
            excerpt_hash TEXT NOT NULL CHECK(length(excerpt_hash)=64),
            independence_group TEXT NOT NULL,
            observed_at TEXT NOT NULL,
            UNIQUE(claim_id, source_item_id, evidence_role, excerpt_hash)
        )
    """,
    "event_versions": """
        CREATE TABLE event_versions(
            event_id TEXT NOT NULL REFERENCES events(id),
            version INTEGER NOT NULL CHECK(version > 0),
            material_change_reason TEXT NOT NULL,
            summary TEXT NOT NULL,
            verification_state TEXT NOT NULL CHECK(verification_state IN ('unverified','watchlist','verified','rejected')),
            valid_from TEXT NOT NULL,
            superseded_at TEXT,
            verified_at TEXT,
            PRIMARY KEY(event_id, version),
            CHECK(verification_state != 'verified' OR verified_at IS NOT NULL)
        )
    """,
    "event_dates": """
        CREATE TABLE event_dates(
            event_date_id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            event_version INTEGER NOT NULL,
            date_type TEXT NOT NULL CHECK(date_type IN ('published_at','observed_at','updated_at','announced_at','occurred_at','scheduled_for','first_seen_at','last_seen_at','verified_at','valid_from','superseded_at')),
            date_value TEXT,
            date_precision TEXT NOT NULL CHECK(date_precision IN ('instant','day','month','year','range','unknown')),
            evidence_id TEXT REFERENCES claim_evidence(evidence_id),
            unknown_reason TEXT,
            FOREIGN KEY(event_id, event_version) REFERENCES event_versions(event_id, version),
            CHECK((date_value IS NOT NULL AND unknown_reason IS NULL) OR (date_value IS NULL AND unknown_reason IS NOT NULL AND date_precision='unknown')),
            UNIQUE(event_id, event_version, date_type, date_value, unknown_reason)
        )
    """,
    "reports": """
        CREATE TABLE reports(
            report_id TEXT PRIMARY KEY,
            window_start TEXT NOT NULL,
            window_end TEXT NOT NULL,
            generation_status TEXT NOT NULL CHECK(generation_status IN ('pending','generating','complete','failed')),
            json_sha256 TEXT CHECK(json_sha256 IS NULL OR length(json_sha256)=64),
            jsonl_sha256 TEXT CHECK(jsonl_sha256 IS NULL OR length(jsonl_sha256)=64),
            markdown_sha256 TEXT CHECK(markdown_sha256 IS NULL OR length(markdown_sha256)=64),
            manifest_sha256 TEXT CHECK(manifest_sha256 IS NULL OR length(manifest_sha256)=64),
            delivery_state TEXT NOT NULL CHECK(delivery_state IN ('not_attempted','dry_run','sent','failed','skipped')),
            delivery_id TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(window_start, window_end)
        )
    """,
    "report_events": """
        CREATE TABLE report_events(
            report_id TEXT NOT NULL REFERENCES reports(report_id),
            event_id TEXT NOT NULL,
            event_version INTEGER NOT NULL,
            section TEXT NOT NULL,
            sort_order INTEGER NOT NULL CHECK(sort_order >= 0),
            inclusion_reason TEXT NOT NULL,
            PRIMARY KEY(report_id, event_id, event_version),
            FOREIGN KEY(event_id, event_version) REFERENCES event_versions(event_id, version),
            UNIQUE(report_id, section, sort_order)
        )
    """,
}

V3_COLUMNS: dict[str, tuple[str, ...]] = {
    "source_registry": ("source_id", "adapter_type", "source_role", "host", "category_scope_json", "enabled", "queries_json", "title_blocklist_json", "content_blocklist_json", "url_blocklist_json", "allowlist_domains_json", "cadence_minutes", "terms_notes", "rate_limit_notes", "next_due_at", "config_hash", "created_at"),
    "source_items": ("source_item_id", "source_id", "external_id", "category", "original_url", "canonical_url", "publisher", "source_role", "author_handle", "retrieval_method", "raw_content_hash", "title", "body", "raw", "retrieved_at", "published_at", "updated_at", "publication_evidence"),
    "query_plans": ("query_plan_id", "source_id", "query_text", "category", "topic", "entity", "reason_selected", "cooldown_seconds", "max_rounds", "created_at"),
    "query_attempts": ("attempt_id", "query_plan_id", "status", "started_at", "finished_at", "returned_count", "novel_count", "verified_count", "duplicate_count", "stale_count", "error_count", "error", "rate_limit_reset_at"),
    "claims": ("claim_id", "source_item_id", "subject", "predicate", "object_value", "statement_type", "extraction_confidence", "status", "extracted_at"),
    "claim_evidence": ("evidence_id", "claim_id", "source_item_id", "evidence_role", "exact_excerpt", "excerpt_hash", "independence_group", "observed_at"),
    "event_versions": ("event_id", "version", "material_change_reason", "summary", "verification_state", "valid_from", "superseded_at", "verified_at"),
    "event_dates": ("event_date_id", "event_id", "event_version", "date_type", "date_value", "date_precision", "evidence_id", "unknown_reason"),
    "reports": ("report_id", "window_start", "window_end", "generation_status", "json_sha256", "jsonl_sha256", "markdown_sha256", "manifest_sha256", "delivery_state", "delivery_id", "created_at"),
    "report_events": ("report_id", "event_id", "event_version", "section", "sort_order", "inclusion_reason"),
}

_INDEX_SQL: dict[str, str] = {
    "idx_source_registry_due": "CREATE INDEX idx_source_registry_due ON source_registry(enabled, next_due_at, source_id)",
    "idx_source_items_source_retrieved": "CREATE INDEX idx_source_items_source_retrieved ON source_items(source_id, retrieved_at)",
    "idx_source_items_canonical": "CREATE INDEX idx_source_items_canonical ON source_items(canonical_url)",
    "idx_query_plans_source_category": "CREATE INDEX idx_query_plans_source_category ON query_plans(source_id, category)",
    "idx_query_attempts_plan_started": "CREATE INDEX idx_query_attempts_plan_started ON query_attempts(query_plan_id, started_at)",
    "idx_claims_source_item_status": "CREATE INDEX idx_claims_source_item_status ON claims(source_item_id, status)",
    "idx_claim_evidence_claim_role": "CREATE INDEX idx_claim_evidence_claim_role ON claim_evidence(claim_id, evidence_role)",
    "idx_event_versions_state_valid": "CREATE INDEX idx_event_versions_state_valid ON event_versions(verification_state, valid_from)",
    "idx_event_dates_type_value": "CREATE INDEX idx_event_dates_type_value ON event_dates(date_type, date_value)",
    "idx_reports_window_status": "CREATE INDEX idx_reports_window_status ON reports(window_start, generation_status)",
    "idx_report_events_report_order": "CREATE INDEX idx_report_events_report_order ON report_events(report_id, section, sort_order)",
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
    return frozenset(row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))


def _require_v2(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    missing = _REQUIRED_V2_TABLES - tables
    if missing:
        raise ValueError(f"incompatible pre-v3 database; missing tables: {sorted(missing)}")
    columns = tuple(row[1] for row in connection.execute("PRAGMA table_info(schema_migrations)"))
    if columns != ("version", "applied_at"):
        raise ValueError("incompatible schema_migrations table")
    versions = tuple(row[0] for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version"))
    if 1 not in versions or 2 not in versions:
        raise ValueError("database must have schema migrations 1 and 2")
    if any(type(version) is not int or version < 1 or version > SCHEMA_VERSION for version in versions):
        raise ValueError(f"unsupported schema migration versions: {versions}")


def _validate_v3(connection: sqlite3.Connection) -> None:
    tables = _table_names(connection)
    missing = set(V3_TABLES) - tables
    if missing:
        raise ValueError(f"schema v3 is incomplete; missing tables: {sorted(missing)}")
    for table, expected in V3_COLUMNS.items():
        actual = tuple(row[1] for row in connection.execute(f'PRAGMA table_info("{table}")'))
        if actual != expected:
            raise ValueError(f"schema v3 table {table} has incompatible columns: {actual}")
    indexes = {row[0]: row[1] for row in connection.execute("SELECT name,tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'")}
    for index_name, statement in _INDEX_SQL.items():
        expected_table = statement.split(" ON ", 1)[1].split("(", 1)[0]
        if indexes.get(index_name) != expected_table:
            raise ValueError(f"schema v3 index {index_name} is missing or incompatible")
    if connection.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("database integrity check failed")
    violations = connection.execute("PRAGMA foreign_key_check").fetchall()
    if violations:
        raise ValueError(f"database has foreign-key violations: {violations[:3]}")


def migrate_v3(connection: sqlite3.Connection, applied_at: str) -> bool:
    """Apply schema v3 transactionally; return True if applied, False if already valid."""
    _validate_applied_at(applied_at)
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if connection.in_transaction:
        raise ValueError("migrate_v3 requires a connection with no active transaction")
    connection.execute("PRAGMA foreign_keys=ON")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        raise ValueError("foreign-key enforcement could not be enabled")
    _require_v2(connection)
    marker = connection.execute("SELECT applied_at FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)).fetchone()
    present = set(V3_TABLES) & _table_names(connection)
    if marker is not None:
        _validate_v3(connection)
        return False
    if present:
        raise ValueError(f"partial schema v3 state without migration marker: {sorted(present)}")

    connection.execute("BEGIN IMMEDIATE")
    try:
        for table in V3_TABLES:
            connection.execute(_TABLE_SQL[table])
        for statement in _INDEX_SQL.values():
            connection.execute(statement)
        _validate_v3(connection)
        connection.execute("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)", (SCHEMA_VERSION, applied_at))
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
    _validate_v3(connection)
    return True
