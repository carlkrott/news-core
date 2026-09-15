"""Slice 6.1 — V1 schema, checksum, and verification.

V1 is the only schema slice 6.1 defines. The DDL, its sha256 checksum,
and the migration gate are immutable. Any deviation raises
:class:`Phase6SchemaError`.

The contract is Revision 4: five application tables, three named
application indexes, no views/triggers/virtual tables/extra objects,
unconditional CREATE (no IF NOT EXISTS), one explicit transaction, exact
schema verification before commit, no V2.

application_id is the four-byte ASCII tag ``P6R4`` (0x50365234).
user_version is 1.

Schema checksum is
``SHA256(UTF8("phase6-schema-v1\\n") + canonical V1 DDL bytes in exact
statement order)``. The marker is the literal 14 bytes
``b"phase6-schema-v1\\n"``. The DDL bytes are the ``utf-8`` encoded
CREATE statements in their tuple order, concatenated with no separator.
"""
from __future__ import annotations

import hashlib
import sqlite3

from .types import Phase6SchemaError


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

SCHEMA_V1_VERSION: int = 1

# Decimal encoding of the four-byte ASCII tag "P6R4".
SCHEMA_V1_APPLICATION_ID: int = 0x50365234

# Bytes prefixed to the DDL bytes before SHA-256. The trailing newline is
# part of the marker, not a separator between statements.
SCHEMA_V1_CHECKSUM_MARKER: bytes = b"phase6-schema-v1\n"

# Migration identifier persisted in schema_migrations.migration_name.
SCHEMA_V1_MIGRATION_NAME: str = "phase6_v1"

# Recoverable / dry-run template id pinned in dry_run_outbox.template_id.
SCHEMA_V1_TEMPLATE_ID: str = "recovery-observation.v1"

# Single accepted value of dry_run_outbox.state.
SCHEMA_V1_DRY_RUN_STATE: str = "PREVIEW_ONLY"

# Allowed values for result / event-kind columns.
SCHEMA_V1_RESULT_VALUES: frozenset[str] = frozenset({"EXPIRED", "STALE"})
SCHEMA_V1_EVENT_KINDS: frozenset[str] = frozenset({
    "EXPIRED_OBSERVED",
    "STALE_OBSERVED",
})

# The exact set of application tables that V1 ships.
SCHEMA_V1_EXPECTED_TABLES: frozenset[str] = frozenset({
    "schema_migrations",
    "evaluation_inputs",
    "audit_events",
    "dry_run_outbox",
    "artifact_manifest",
})

# The exact set of application indexes that V1 ships.
SCHEMA_V1_EXPECTED_INDEXES: frozenset[str] = frozenset({
    "idx_evaluation_inputs_age",
    "idx_audit_events_job",
    "idx_dry_run_outbox_job",
})


# ---------------------------------------------------------------------------
# DDL — exact statement order is part of the checksum, do not reorder.
# ---------------------------------------------------------------------------

_DDL_SCHEMA_MIGRATIONS = """
CREATE TABLE schema_migrations (
 version INTEGER PRIMARY KEY,
 migration_name TEXT NOT NULL,
 checksum_sha256 TEXT NOT NULL,
 applied_session_id TEXT NOT NULL,
 applied_session_utc TEXT NOT NULL,
 CHECK (version = 1),
 CHECK (migration_name = 'phase6_v1'),
 CHECK (length(checksum_sha256) = 64 AND checksum_sha256 NOT GLOB '*[^0-9a-f]*')
);
""".strip()


_DDL_EVALUATION_INPUTS = """
CREATE TABLE evaluation_inputs (
 job_id TEXT PRIMARY KEY,
 reference_utc TEXT NOT NULL,
 age_seconds INTEGER NOT NULL,
 source_state TEXT NOT NULL,
 canonical_input_json TEXT NOT NULL,
 input_sha256 TEXT NOT NULL,
 CHECK (age_seconds >= 0),
 CHECK (length(source_state) BETWEEN 1 AND 64 AND source_state NOT GLOB '*[^A-Z0-9_:-]*'),
 CHECK (length(input_sha256) = 64 AND input_sha256 NOT GLOB '*[^0-9a-f]*')
);
""".strip()


_DDL_AUDIT_EVENTS = """
CREATE TABLE audit_events (
 event_sha256 TEXT,
 job_id TEXT,
 event_kind TEXT NOT NULL,
 result_value TEXT NOT NULL,
 evaluation_utc TEXT NOT NULL,
 age_seconds INTEGER NOT NULL,
 input_sha256 TEXT NOT NULL,
 canonical_event_json TEXT NOT NULL,
 PRIMARY KEY (event_sha256, job_id),
 FOREIGN KEY (job_id) REFERENCES evaluation_inputs(job_id),
 CHECK (event_kind IN ('EXPIRED_OBSERVED', 'STALE_OBSERVED')),
 CHECK (result_value IN ('EXPIRED', 'STALE')),
 CHECK (age_seconds >= 0),
 CHECK (
   (event_kind = 'EXPIRED_OBSERVED' AND result_value = 'EXPIRED')
   OR
   (event_kind = 'STALE_OBSERVED' AND result_value = 'STALE')
 ),
 CHECK (length(event_sha256) = 64 AND event_sha256 NOT GLOB '*[^0-9a-f]*'),
 CHECK (length(input_sha256) = 64 AND input_sha256 NOT GLOB '*[^0-9a-f]*')
);
""".strip()


_DDL_DRY_RUN_OUTBOX = """
CREATE TABLE dry_run_outbox (
 payload_sha256 TEXT PRIMARY KEY,
 event_sha256 TEXT NOT NULL,
 job_id TEXT NOT NULL,
 result_value TEXT NOT NULL,
 age_seconds INTEGER NOT NULL,
 state TEXT NOT NULL,
 template_id TEXT NOT NULL,
 canonical_payload_json TEXT NOT NULL,
 session_id TEXT NOT NULL,
 evaluation_utc TEXT NOT NULL,
 credential_free INTEGER NOT NULL,
 delivery_evidence INTEGER NOT NULL,
 FOREIGN KEY (event_sha256, job_id) REFERENCES audit_events(event_sha256, job_id),
 CHECK (length(payload_sha256) = 64 AND payload_sha256 NOT GLOB '*[^0-9a-f]*'),
 CHECK (result_value IN ('EXPIRED', 'STALE')),
 CHECK (age_seconds >= 0),
 CHECK (state = 'PREVIEW_ONLY'),
 CHECK (template_id = 'recovery-observation.v1'),
 CHECK (credential_free = 1),
 CHECK (delivery_evidence = 0)
);
""".strip()


_DDL_ARTIFACT_MANIFEST = """
CREATE TABLE artifact_manifest (
 artifact_name TEXT PRIMARY KEY,
 artifact_class TEXT NOT NULL,
 sha256 TEXT NOT NULL,
 byte_count INTEGER NOT NULL,
 delivery_evidence INTEGER NOT NULL,
 CHECK (artifact_name IN ('input.json', 'audit.jsonl', 'preview.json', 'replay.json')),
 CHECK (artifact_class IN ('INPUT_SNAPSHOT', 'NON_DELIVERY_AUDIT', 'NON_EVIDENT_PREVIEW', 'NON_EVIDENT_REPLAY')),
 CHECK (
   (artifact_name = 'input.json'    AND artifact_class = 'INPUT_SNAPSHOT')
   OR (artifact_name = 'audit.jsonl'  AND artifact_class = 'NON_DELIVERY_AUDIT')
   OR (artifact_name = 'preview.json' AND artifact_class = 'NON_EVIDENT_PREVIEW')
   OR (artifact_name = 'replay.json'  AND artifact_class = 'NON_EVIDENT_REPLAY')
 ),
 CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'),
 CHECK (byte_count >= 0),
 CHECK (delivery_evidence = 0)
);
""".strip()


_DDL_IDX_EVALUATION_INPUTS_AGE = """
CREATE INDEX idx_evaluation_inputs_age ON evaluation_inputs(age_seconds, job_id);
""".strip()


_DDL_IDX_AUDIT_EVENTS_JOB = """
CREATE INDEX idx_audit_events_job ON audit_events(job_id, evaluation_utc, event_kind);
""".strip()


_DDL_IDX_DRY_RUN_OUTBOX_JOB = """
CREATE INDEX idx_dry_run_outbox_job ON dry_run_outbox(job_id, evaluation_utc, result_value);
""".strip()


# Tuple, in exact execution / checksum order. Order is part of the
# checksum: do NOT reorder.
SCHEMA_V1_DDL: tuple[str, ...] = (
    _DDL_SCHEMA_MIGRATIONS,
    _DDL_EVALUATION_INPUTS,
    _DDL_AUDIT_EVENTS,
    _DDL_DRY_RUN_OUTBOX,
    _DDL_ARTIFACT_MANIFEST,
    _DDL_IDX_EVALUATION_INPUTS_AGE,
    _DDL_IDX_AUDIT_EVENTS_JOB,
    _DDL_IDX_DRY_RUN_OUTBOX_JOB,
)


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

def _compute_checksum(ddl: tuple[str, ...]) -> str:
    """Deterministic lowercase hex SHA-256 over marker + DDL bytes."""
    h = hashlib.sha256()
    h.update(SCHEMA_V1_CHECKSUM_MARKER)
    for stmt in ddl:
        h.update(stmt.encode("utf-8"))
    return h.hexdigest()


def expected_v1_checksum() -> str:
    """Return the immutable V1 schema checksum (no DB access)."""
    return _compute_checksum(SCHEMA_V1_DDL)


# Pre-computed at import. Tests pin the exact 64-hex string.
SCHEMA_V1_CHECKSUM: str = expected_v1_checksum()


# ---------------------------------------------------------------------------
# Object set inspection helpers
# ---------------------------------------------------------------------------

def _existing_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {row[0] for row in rows}


def _existing_indexes(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()
    return {row[0] for row in rows}


def _application_indexes(conn: sqlite3.Connection) -> set[str]:
    """User-named application indexes only; excludes ``sqlite_autoindex_*``.

    SQLite creates implicit B-tree indexes named ``sqlite_autoindex_*``
    to back PRIMARY KEY / UNIQUE constraints. These are implementation
    artefacts of the schema, not additional application objects; the V1
    contract requires the three named indexes and forbids any *other*
    user-named indexes.
    """
    return {
        name for name in _existing_indexes(conn)
        if not name.startswith("sqlite_autoindex_")
    }


def _existing_other_objects(conn: sqlite3.Connection) -> set[str]:
    """All non-table, non-index application objects (views/triggers/virtual)."""
    rows = conn.execute(
        "SELECT name, type FROM sqlite_master "
        "WHERE type NOT IN ('table', 'index')"
    ).fetchall()
    return {f"{row[1]}:{row[0]}" for row in rows}


# ---------------------------------------------------------------------------
# Apply V1 schema
# ---------------------------------------------------------------------------

def apply_v1_schema(
    conn: sqlite3.Connection,
    applied_session_id: str,
    applied_session_utc: str,
) -> str:
    """Apply the V1 schema to ``conn`` and return the checksum used.

    The function is idempotent: re-running on an already-migrated DB
    validates the stored checksum and returns it as a no-op. Re-running
    against a tampered DB raises :class:`Phase6SchemaError` before any
    destructive action.

    All DDL, the schema_migrations INSERT, the application_id / user_version
    PRAGMAs, and the post-create verification run inside one explicit
    transaction. On any verification failure the transaction is rolled
    back, leaving the DB unchanged.
    """
    if not isinstance(applied_session_id, str) or not applied_session_id:
        raise Phase6SchemaError("applied_session_id must be a non-empty str")
    if not isinstance(applied_session_utc, str) or not applied_session_utc:
        raise Phase6SchemaError("applied_session_utc must be a non-empty str")

    checksum = _compute_checksum(SCHEMA_V1_DDL)

    # Idempotent re-run: if the migration row already exists, validate and
    # short-circuit. We never re-issue DDL on a migrated DB; unconditional
    # CREATE without IF NOT EXISTS guarantees that.
    existing = _existing_tables(conn)
    if "schema_migrations" in existing:
        row = conn.execute(
            "SELECT version, migration_name, checksum_sha256 "
            "FROM schema_migrations"
        ).fetchone()
        if row is None:
            raise Phase6SchemaError(
                "schema_migrations is present but empty: "
                "V1 was partially applied; refusing to proceed"
            )
        version, migration_name, stored_checksum = row
        if version != SCHEMA_V1_VERSION:
            raise Phase6SchemaError(
                f"schema_migrations.version must be {SCHEMA_V1_VERSION}, "
                f"got {version}"
            )
        if migration_name != SCHEMA_V1_MIGRATION_NAME:
            raise Phase6SchemaError(
                f"schema_migrations.migration_name must be "
                f"{SCHEMA_V1_MIGRATION_NAME!r}, got {migration_name!r}"
            )
        if stored_checksum != checksum:
            raise Phase6SchemaError(
                f"schema checksum mismatch on rerun: "
                f"stored={stored_checksum!r}, expected={checksum!r}"
            )
        # Validate the rest of the schema and return without touching it.
        verify_schema_v1(conn)
        return checksum

    # Fresh DB. Single explicit transaction; rollback on any failure.
    conn.execute("BEGIN IMMEDIATE")
    try:
        # PRAGMA application_id is technically non-transactional in SQLite
        # but is required by the contract; set it before the DDL so the
        # post-create verification observes it.
        conn.execute(f"PRAGMA application_id = {SCHEMA_V1_APPLICATION_ID}")
        for stmt in SCHEMA_V1_DDL:
            conn.execute(stmt)
        conn.execute(
            "INSERT INTO schema_migrations"
            "(version, migration_name, checksum_sha256, "
            " applied_session_id, applied_session_utc) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                SCHEMA_V1_VERSION,
                SCHEMA_V1_MIGRATION_NAME,
                checksum,
                applied_session_id,
                applied_session_utc,
            ),
        )
        conn.execute(f"PRAGMA user_version = {SCHEMA_V1_VERSION}")

        # Exact schema verification before commit.
        _verify_post_create(conn)
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    conn.execute("COMMIT")
    return checksum


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def _verify_post_create(conn: sqlite3.Connection) -> None:
    """Fail-closed verification of exact object set and integrity."""
    # Object set: exactly the named application tables + indexes.
    tables = _existing_tables(conn)
    extra_tables = tables - SCHEMA_V1_EXPECTED_TABLES
    if extra_tables:
        raise Phase6SchemaError(
            f"unexpected application tables present: "
            f"{sorted(extra_tables)}"
        )
    missing_tables = SCHEMA_V1_EXPECTED_TABLES - tables
    if missing_tables:
        raise Phase6SchemaError(
            f"missing required V1 tables: {sorted(missing_tables)}"
        )

    indexes = _application_indexes(conn)
    extra_indexes = indexes - SCHEMA_V1_EXPECTED_INDEXES
    if extra_indexes:
        raise Phase6SchemaError(
            f"unexpected application indexes present: "
            f"{sorted(extra_indexes)}"
        )
    missing_indexes = SCHEMA_V1_EXPECTED_INDEXES - indexes
    if missing_indexes:
        raise Phase6SchemaError(
            f"missing required V1 indexes: {sorted(missing_indexes)}"
        )

    other = _existing_other_objects(conn)
    if other:
        raise Phase6SchemaError(
            f"V1 must not contain views/triggers/virtual tables: "
            f"{sorted(other)}"
        )

    # PRAGMA invariants.
    row = conn.execute("PRAGMA user_version").fetchone()
    user_version = int(row[0]) if row else 0
    if user_version != SCHEMA_V1_VERSION:
        raise Phase6SchemaError(
            f"user_version mismatch: expected {SCHEMA_V1_VERSION}, "
            f"got {user_version}"
        )

    row = conn.execute("PRAGMA application_id").fetchone()
    application_id = int(row[0]) if row else 0
    if application_id != SCHEMA_V1_APPLICATION_ID:
        raise Phase6SchemaError(
            f"application_id mismatch: expected "
            f"{SCHEMA_V1_APPLICATION_ID} (0x50365234), "
            f"got {application_id}"
        )

    # schema_migrations row invariants.
    row = conn.execute(
        "SELECT version, migration_name, checksum_sha256, "
        "       applied_session_id, applied_session_utc "
        "FROM schema_migrations"
    ).fetchone()
    if row is None:
        raise Phase6SchemaError(
            "schema_migrations row missing after CREATE"
        )
    (version, migration_name, stored_checksum,
     applied_session_id, applied_session_utc) = row
    if version != SCHEMA_V1_VERSION:
        raise Phase6SchemaError(
            f"schema_migrations.version must be {SCHEMA_V1_VERSION}, "
            f"got {version}"
        )
    if migration_name != SCHEMA_V1_MIGRATION_NAME:
        raise Phase6SchemaError(
            f"schema_migrations.migration_name must be "
            f"{SCHEMA_V1_MIGRATION_NAME!r}, got {migration_name!r}"
        )
    if stored_checksum != SCHEMA_V1_CHECKSUM:
        raise Phase6SchemaError(
            f"schema_migrations.checksum_sha256 mismatch: "
            f"expected {SCHEMA_V1_CHECKSUM}, got {stored_checksum}"
        )
    if not applied_session_id:
        raise Phase6SchemaError(
            "schema_migrations.applied_session_id is empty"
        )
    if not applied_session_utc:
        raise Phase6SchemaError(
            "schema_migrations.applied_session_utc is empty"
        )

    # Foreign key + integrity checks.
    fk_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    if fk_rows:
        raise Phase6SchemaError(
            f"foreign_key_check reported violations: {fk_rows}"
        )

    integ_rows = conn.execute("PRAGMA integrity_check").fetchall()
    # integrity_check returns one row per check; we accept only 'ok'.
    for r in integ_rows:
        if r[0] != "ok":
            raise Phase6SchemaError(
                f"integrity_check failed: {r[0]!r}"
            )


def verify_schema_v1(conn: sqlite3.Connection) -> None:
    """Raise :class:`Phase6SchemaError` if ``conn`` does not match V1 exactly."""
    row = conn.execute("PRAGMA user_version").fetchone()
    user_version = int(row[0]) if row else 0
    if user_version != SCHEMA_V1_VERSION:
        raise Phase6SchemaError(
            f"user_version mismatch: expected {SCHEMA_V1_VERSION}, "
            f"got {user_version}"
        )

    tables = _existing_tables(conn)
    extra_tables = tables - SCHEMA_V1_EXPECTED_TABLES
    if extra_tables:
        raise Phase6SchemaError(
            f"unexpected application tables present: "
            f"{sorted(extra_tables)}"
        )
    missing_tables = SCHEMA_V1_EXPECTED_TABLES - tables
    if missing_tables:
        raise Phase6SchemaError(
            f"missing required V1 tables: {sorted(missing_tables)}"
        )

    indexes = _application_indexes(conn)
    extra_indexes = indexes - SCHEMA_V1_EXPECTED_INDEXES
    if extra_indexes:
        raise Phase6SchemaError(
            f"unexpected application indexes present: "
            f"{sorted(extra_indexes)}"
        )
    missing_indexes = SCHEMA_V1_EXPECTED_INDEXES - indexes
    if missing_indexes:
        raise Phase6SchemaError(
            f"missing required V1 indexes: {sorted(missing_indexes)}"
        )

    other = _existing_other_objects(conn)
    if other:
        raise Phase6SchemaError(
            f"V1 must not contain views/triggers/virtual tables: "
            f"{sorted(other)}"
        )

    checksum = _compute_checksum(SCHEMA_V1_DDL)
    if checksum != SCHEMA_V1_CHECKSUM:
        raise Phase6SchemaError(
            f"DDL checksum mismatch: expected {SCHEMA_V1_CHECKSUM}, "
            f"got {checksum}"
        )


__all__ = [
    "SCHEMA_V1_VERSION",
    "SCHEMA_V1_APPLICATION_ID",
    "SCHEMA_V1_CHECKSUM_MARKER",
    "SCHEMA_V1_MIGRATION_NAME",
    "SCHEMA_V1_TEMPLATE_ID",
    "SCHEMA_V1_DRY_RUN_STATE",
    "SCHEMA_V1_RESULT_VALUES",
    "SCHEMA_V1_EVENT_KINDS",
    "SCHEMA_V1_EXPECTED_TABLES",
    "SCHEMA_V1_EXPECTED_INDEXES",
    "SCHEMA_V1_DDL",
    "SCHEMA_V1_CHECKSUM",
    "apply_v1_schema",
    "verify_schema_v1",
    "expected_v1_checksum",
]