"""Slice 6.4 — inert dry-run outbox.

The Slice 6.4 contract (inert dry-run outbox):

* consume only the already-open validated V1 in-memory session DB
  (``audit_events`` rows + ``evaluation_inputs`` rows; never create,
  reopen, migrate, or write a different schema),
* consume only ``audit_events`` rows whose event/result mapping is
  ``EXPIRED_OBSERVED -> EXPIRED`` or ``STALE_OBSERVED -> STALE``,
* require a matching ``evaluation_inputs`` row with exact byte-match
  on ``input_sha256`` (the Slice 6.2 canonical-input primary key),
  ``job_id``, ``age_seconds`` and ``source_state``,
* insert ONLY into ``dry_run_outbox`` with rows that satisfy the V1
  CHECK constraints (``state = 'PREVIEW_ONLY'``,
  ``template_id = 'recovery-observation.v1'``, ``credential_free = 1``,
  ``delivery_evidence = 0``) and whose ``payload_sha256`` is the
  SHA-256 of a deterministic, credential-free canonical payload JSON
  built from the audited event plus the contract fields,
* treat an exact-duplicate (same ``payload_sha256`` primary key) as
  a contract-safe validated no-op (no row mutated),
* reject a same-``job_id`` row whose existing ``payload_sha256``
  differs from the new one (append-only conflict),
* be append-only: never ``UPDATE`` or ``DELETE`` rows in
  ``audit_events``, ``evaluation_inputs``, or ``dry_run_outbox``;
  never touch ``artifact_manifest``,
* have NO fields or concepts for consumer / destination / lease /
  attempt / retry / sent / delivered / failed / provider / transport /
  recipient / chat / token / credential / queue / delivery receipt,
* use parameterized SQL, one transaction with rollback on any batch
  failure,
* never call clock / time / random / process / UUID / entropy /
  network APIs, never consult any environment variable, never read or
  write operational / live job state, retry, enqueue, page, or hit
  the network.

The only writes permitted are ``INSERT`` into ``dry_run_outbox``.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import sqlite3
from typing import Final

from .audit import (
    EVENT_EXPIRED_OBSERVED,
    EVENT_STALE_OBSERVED,
    AuditEvent,
)
from .recovery import (
    MAX_BATCH_SIZE,
    RESULT_EXPIRED,
    RESULT_NO_OBSERVATION,
    RESULT_STALE,
    canonical_input_json,
    input_sha256,
)
from .schema import (
    SCHEMA_V1_DRY_RUN_STATE,
    SCHEMA_V1_TEMPLATE_ID,
)
from .time_inputs import (
    ExternalContext,
    validate_external_context,
)
from .types import (
    Phase6ConfigurationError,
    Phase6IdentityError,
    Phase6SandboxError,
    Phase6SchemaError,
)


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: Bounded canonical_payload_json length. 4 KiB is generous for the 11-field
#: canonical payload and still rejects pathological session_id inflation.
MAX_OUTBOX_PAYLOAD_JSON_LEN: Final[int] = 4096

#: Session id length bound (32 bytes).
_MAX_SESSION_ID_LEN: Final[int] = 32

#: Dry-run outbox row contract literals (re-exported for tests / callers).
OUTBOX_STATE_PREVIEW_ONLY: Final[str] = SCHEMA_V1_DRY_RUN_STATE
OUTBOX_TEMPLATE_ID: Final[str] = SCHEMA_V1_TEMPLATE_ID

#: String set for session_id — printable ASCII, no whitespace.
_SESSION_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._:-]{1,32}$"
_SESSION_ID_RE: Final[re.Pattern[str]] = re.compile(_SESSION_ID_PATTERN)

#: 64 lowercase-hex SHA-256.
_HEX64_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")

#: Job_id and source_state patterns mirror Slice 6.2 / 6.3.
_JOB_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._-]{1,128}$"
_JOB_ID_RE: Final[re.Pattern[str]] = re.compile(_JOB_ID_PATTERN)

_SOURCE_STATE_PATTERN: Final[str] = r"^[A-Z0-9_:-]{1,64}$"
_SOURCE_STATE_RE: Final[re.Pattern[str]] = re.compile(_SOURCE_STATE_PATTERN)

_MAX_JOB_ID_LEN: Final[int] = 128
_MAX_SOURCE_STATE_LEN: Final[int] = 64
_MAX_TIMESTAMP_LEN: Final[int] = 32

_INT64_MAX: Final[int] = (2 ** 63) - 1

#: Mapping from event_kind -> expected result_value, enforced on every
#: AuditEvent that queue_previews accepts.
EXPECTED_RESULT_FOR_EVENT_KIND: Final[dict[str, str]] = {
    EVENT_EXPIRED_OBSERVED: RESULT_EXPIRED,
    EVENT_STALE_OBSERVED: RESULT_STALE,
}


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class OutboxError(Phase6SandboxError):
    """Base class for every Slice 6.4 failure mode."""


class OutboxIdentityError(OutboxError, Phase6IdentityError):
    """A caller-supplied AuditEvent or session_id failed identity validation."""


class OutboxConsistencyError(OutboxError):
    """Caller-supplied AuditEvent is inconsistent with evaluation_inputs."""


class OutboxMissingInputError(OutboxConsistencyError):
    """The required evaluation_inputs row does not exist for the event."""


class OutboxBatchError(OutboxError):
    """The batch itself is malformed: empty, oversized, or has duplicate job_ids."""


class OutboxConflictError(OutboxError):
    """A dry_run_outbox row already exists for the job_id with a different
    payload_sha256."""


class OutboxSchemaError(OutboxError, Phase6SchemaError):
    """The open session DB does not match the V1 contract Slice 6.4 expects."""


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class PreviewRow:
    """A single inert dry-run outbox row (record-only, never delivered).

    The first twelve fields are the contract columns of
    ``dry_run_outbox`` (mirroring the V1 schema exactly). The
    remaining two are metadata that the implementation fills in when
    the row is persisted: the duplicate / persisted flags.
    """

    payload_sha256: str
    event_sha256: str
    job_id: str
    result_value: str
    age_seconds: int
    state: str
    template_id: str
    session_id: str
    evaluation_utc: str
    credential_free: int
    delivery_evidence: int
    canonical_payload_json: str
    persisted: bool = False
    duplicate: bool = False


@dataclasses.dataclass(frozen=True)
class BatchPreviewOutcome:
    """The full outcome of a :func:`queue_previews` call.

    ``rows`` contains exactly one :class:`PreviewRow` per audited
    event in input order. Each PreviewRow has ``persisted=True`` if
    the row was newly appended, ``duplicate=True`` if the row was
    already present with the same ``payload_sha256``, and
    ``persisted=False, duplicate=False`` if the call failed before
    persistence (the caller will have received an exception).
    """

    rows: tuple[PreviewRow, ...]


# ---------------------------------------------------------------------------
# Field-level validation
# ---------------------------------------------------------------------------

def _check_session_id(value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: session_id must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise OutboxIdentityError("outbox: session_id must not be empty")
    if len(value) > _MAX_SESSION_ID_LEN:
        raise OutboxIdentityError(
            f"outbox: session_id length must be <= {_MAX_SESSION_ID_LEN}"
        )
    if not _SESSION_ID_RE.match(value):
        raise OutboxIdentityError(
            "outbox: session_id must match [A-Za-z0-9._:-]{1,32}"
        )
    return value


def _check_job_id(value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: job_id must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise OutboxIdentityError("outbox: job_id must not be empty")
    if len(value) > _MAX_JOB_ID_LEN:
        raise OutboxIdentityError(
            f"outbox: job_id length must be <= {_MAX_JOB_ID_LEN}"
        )
    if not _JOB_ID_RE.match(value):
        raise OutboxIdentityError(
            "outbox: job_id must match [A-Za-z0-9._-]{1,128}"
        )
    return value


def _check_source_state(value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: source_state must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise OutboxIdentityError("outbox: source_state must not be empty")
    if len(value) > _MAX_SOURCE_STATE_LEN:
        raise OutboxIdentityError(
            f"outbox: source_state length must be <= {_MAX_SOURCE_STATE_LEN}"
        )
    if not _SOURCE_STATE_RE.match(value):
        raise OutboxIdentityError(
            "outbox: source_state must match [A-Z0-9_:-]{1,64}"
        )
    return value


def _check_timestamp(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: {name} must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise OutboxIdentityError(f"outbox: {name} must not be empty")
    if len(value) > _MAX_TIMESTAMP_LEN:
        raise OutboxIdentityError(
            f"outbox: {name} length must be <= {_MAX_TIMESTAMP_LEN}"
        )
    return value


def _check_age(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise OutboxIdentityError(
            f"outbox: age_seconds must be int, got {type(value).__name__}"
        )
    if value < 0:
        raise OutboxConsistencyError(
            "outbox: age_seconds must be >= 0 (negative not allowed)"
        )
    if value > _INT64_MAX:
        raise OutboxIdentityError(
            f"outbox: age_seconds must be <= {_INT64_MAX}"
        )
    return value


def _check_hex64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: {name} must be str, got {type(value).__name__}"
        )
    if not _HEX64_RE.match(value):
        raise OutboxIdentityError(
            f"outbox: {name} must be 64 lowercase hex chars"
        )
    return value


def _check_event_kind(value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: event_kind must be str, got {type(value).__name__}"
        )
    if value not in (EVENT_EXPIRED_OBSERVED, EVENT_STALE_OBSERVED):
        raise OutboxIdentityError(
            f"outbox: event_kind must be {EVENT_EXPIRED_OBSERVED!r} "
            f"or {EVENT_STALE_OBSERVED!r}, got {value!r}"
        )
    return value


def _check_result_value(value: object) -> str:
    if not isinstance(value, str):
        raise OutboxIdentityError(
            f"outbox: result_value must be str, got {type(value).__name__}"
        )
    if value not in (RESULT_EXPIRED, RESULT_STALE):
        raise OutboxIdentityError(
            f"outbox: result_value must be {RESULT_EXPIRED!r} "
            f"or {RESULT_STALE!r}, got {value!r}"
        )
    return value


# ---------------------------------------------------------------------------
# Canonical payload / JSON / SHA-256
# ---------------------------------------------------------------------------

def canonical_payload_payload(
    audit_event: AuditEvent, session_id: str
) -> dict[str, object]:
    """Return the canonical payload dict (the JSON shape minus encoding)."""
    return {
        "age_seconds": audit_event.age_seconds,
        "credential_free": 1,
        "delivery_evidence": 0,
        "evaluation_utc": audit_event.evaluation_utc,
        "event_sha256": audit_event.event_sha256,
        "input_sha256": audit_event.input_sha256,
        "job_id": audit_event.job_id,
        "result_value": audit_event.result_value,
        "session_id": session_id,
        "state": OUTBOX_STATE_PREVIEW_ONLY,
        "template_id": OUTBOX_TEMPLATE_ID,
    }


def canonical_payload_json(audit_event: AuditEvent, session_id: str) -> str:
    """Return the deterministic UTF-8 JSON for a preview row payload.

    The payload is intentionally narrow: the eleven contract fields
    that map 1-to-1 onto the V1 ``dry_run_outbox`` columns. No
    destination / provider / delivery / retry / queue metadata may be
    present, and the only mutable-key field is ``session_id`` (the
    test-suite session id pinned by the caller).
    """
    return json.dumps(
        canonical_payload_payload(audit_event, session_id),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def payload_sha256(canonical_json: str) -> str:
    """Return the lowercase-hex SHA-256 of the canonical payload JSON."""
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Session DB contract verification
# ---------------------------------------------------------------------------

def _verify_session_db(conn: sqlite3.Connection) -> None:
    """Fail-closed verification of the V1 contract slice 6.4 depends on.

    Slice 6.4 reads from ``evaluation_inputs`` and ``audit_events`` and
    inserts into ``dry_run_outbox``. All three must exist with the
    expected columns. The function does NOT touch ``artifact_manifest``
    or any operational table.
    """
    try:
        row = conn.execute("PRAGMA user_version").fetchone()
    except sqlite3.Error as exc:
        raise OutboxSchemaError("outbox: cannot read user_version") from exc
    user_version = int(row[0]) if row else 0
    if user_version != 1:
        raise OutboxSchemaError(
            f"outbox: open session DB user_version must be 1, got {user_version}"
        )
    try:
        eval_cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(evaluation_inputs)"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        raise OutboxSchemaError(
            "outbox: evaluation_inputs table is missing"
        ) from exc
    eval_required = {
        "job_id", "reference_utc", "age_seconds", "source_state",
        "canonical_input_json", "input_sha256",
    }
    eval_missing = eval_required - eval_cols
    if eval_missing:
        raise OutboxSchemaError(
            f"outbox: evaluation_inputs is missing required columns: "
            f"{sorted(eval_missing)}"
        )
    try:
        audit_cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(audit_events)"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        raise OutboxSchemaError(
            "outbox: audit_events table is missing"
        ) from exc
    audit_required = {
        "event_sha256", "job_id", "event_kind", "result_value",
        "evaluation_utc", "age_seconds", "input_sha256",
    }
    audit_missing = audit_required - audit_cols
    if audit_missing:
        raise OutboxSchemaError(
            f"outbox: audit_events is missing required columns: "
            f"{sorted(audit_missing)}"
        )
    try:
        outbox_cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(dry_run_outbox)"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        raise OutboxSchemaError(
            "outbox: dry_run_outbox table is missing"
        ) from exc
    outbox_required = {
        "payload_sha256", "event_sha256", "job_id", "result_value",
        "age_seconds", "state", "template_id", "canonical_payload_json",
        "session_id", "evaluation_utc", "credential_free",
        "delivery_evidence",
    }
    outbox_missing = outbox_required - outbox_cols
    if outbox_missing:
        raise OutboxSchemaError(
            f"outbox: dry_run_outbox is missing required columns: "
            f"{sorted(outbox_missing)}"
        )


# ---------------------------------------------------------------------------
# Build a single PreviewRow from a validated AuditEvent + session_id
# ---------------------------------------------------------------------------

def _build_preview_row(
    audit_event: AuditEvent,
    session_id: str,
) -> PreviewRow:
    """Compute the deterministic payload JSON + SHA-256 for the row."""
    canonical = canonical_payload_json(audit_event, session_id)
    if len(canonical) > MAX_OUTBOX_PAYLOAD_JSON_LEN:
        raise OutboxIdentityError(
            "outbox: canonical_payload_json length exceeded"
        )
    sha = payload_sha256(canonical)
    return PreviewRow(
        payload_sha256=sha,
        event_sha256=audit_event.event_sha256,
        job_id=audit_event.job_id,
        result_value=audit_event.result_value,
        age_seconds=audit_event.age_seconds,
        state=OUTBOX_STATE_PREVIEW_ONLY,
        template_id=OUTBOX_TEMPLATE_ID,
        session_id=session_id,
        evaluation_utc=audit_event.evaluation_utc,
        credential_free=1,
        delivery_evidence=0,
        canonical_payload_json=canonical,
    )


def _preview_row_values(preview: PreviewRow) -> tuple[object, ...]:
    return (
        preview.payload_sha256,
        preview.event_sha256,
        preview.job_id,
        preview.result_value,
        preview.age_seconds,
        preview.state,
        preview.template_id,
        preview.canonical_payload_json,
        preview.session_id,
        preview.evaluation_utc,
        preview.credential_free,
        preview.delivery_evidence,
    )


def _audit_row_to_event(row: tuple[object, ...]) -> AuditEvent:
    event_sha256, job_id, event_kind, result_value, evaluation_utc, age_seconds, input_sha256, canonical_event_json = row
    return AuditEvent(
        job_id=job_id,
        event_kind=event_kind,
        result_value=result_value,
        evaluation_utc=evaluation_utc,
        age_seconds=age_seconds,
        input_sha256=input_sha256,
        canonical_event_json=canonical_event_json,
        event_sha256=event_sha256,
        persisted=True,
        duplicate=False,
    )


def _load_persisted_audit_event(
    conn: sqlite3.Connection,
    caller_event: AuditEvent,
) -> AuditEvent:
    row = conn.execute(
        "SELECT event_sha256, job_id, event_kind, result_value, "
        "       evaluation_utc, age_seconds, input_sha256, "
        "       canonical_event_json "
        "FROM audit_events WHERE event_sha256 = ? AND job_id = ?",
        (caller_event.event_sha256, caller_event.job_id),
    ).fetchone()
    if row is None:
        raise OutboxConsistencyError(
            "outbox: persisted audit_events row missing for "
            f"(event_sha256, job_id)=({caller_event.event_sha256!r}, "
            f"{caller_event.job_id!r})"
        )
    persisted = _audit_row_to_event(row)
    for field in (
        "event_sha256",
        "job_id",
        "event_kind",
        "result_value",
        "evaluation_utc",
        "age_seconds",
        "input_sha256",
        "canonical_event_json",
    ):
        if getattr(caller_event, field) != getattr(persisted, field):
            raise OutboxConsistencyError(
                "outbox: caller AuditEvent does not match persisted audit_events row "
                f"for job_id {caller_event.job_id!r} field {field!r}"
            )
    return persisted


@dataclasses.dataclass(frozen=True)
class _JobInputShim:
    job_id: str
    reference_utc: str
    evaluation_utc: str
    age_seconds: int
    source_state: str


def _load_persisted_evaluation_input(
    conn: sqlite3.Connection,
    audit_event: AuditEvent,
) -> tuple[str, str, int, str, str, str]:
    row = conn.execute(
        "SELECT job_id, reference_utc, age_seconds, source_state, "
        "       canonical_input_json, input_sha256 "
        "FROM evaluation_inputs WHERE job_id = ?",
        (audit_event.job_id,),
    ).fetchone()
    if row is None:
        raise OutboxMissingInputError(
            f"outbox: evaluation_inputs row missing for job_id {audit_event.job_id!r}"
        )
    db_job_id, db_reference_utc, db_age_seconds, db_source_state, db_canonical_input_json, db_input_sha256 = row
    if db_job_id != audit_event.job_id:
        raise OutboxConsistencyError(
            f"outbox: evaluation_inputs.job_id mismatch for job_id {audit_event.job_id!r}"
        )
    if db_age_seconds != audit_event.age_seconds:
        raise OutboxConsistencyError(
            f"outbox: evaluation_inputs.age_seconds mismatch for job_id {audit_event.job_id!r}: "
            f"db={db_age_seconds}, audit={audit_event.age_seconds}"
        )
    if db_input_sha256 != audit_event.input_sha256:
        raise OutboxConsistencyError(
            f"outbox: evaluation_inputs.input_sha256 mismatch for job_id {audit_event.job_id!r}: "
            f"db={db_input_sha256!r}, audit={audit_event.input_sha256!r}"
        )
    persisted_input_sha256 = input_sha256(db_canonical_input_json)
    if persisted_input_sha256 != db_input_sha256:
        raise OutboxConsistencyError(
            f"outbox: evaluation_inputs.canonical_input_json SHA-256 mismatch for job_id "
            f"{audit_event.job_id!r}: computed={persisted_input_sha256!r}, "
            f"db={db_input_sha256!r}"
        )
    expected_canonical = canonical_input_json(
        _JobInputShim(
            job_id=db_job_id,
            reference_utc=db_reference_utc,
            evaluation_utc=audit_event.evaluation_utc,
            age_seconds=db_age_seconds,
            source_state=db_source_state,
        )
    )
    if db_canonical_input_json != expected_canonical:
        raise OutboxConsistencyError(
            f"outbox: evaluation_inputs.canonical_input_json mismatch for job_id {audit_event.job_id!r}"
        )
    return row


def _validate_audit_event(event: object) -> AuditEvent:
    """Apply field-level identity validation; return the AuditEvent unchanged."""
    if not isinstance(event, AuditEvent):
        raise OutboxIdentityError(
            "outbox: events element is not an AuditEvent"
        )
    _check_job_id(event.job_id)
    _check_timestamp("evaluation_utc", event.evaluation_utc)
    _check_age(event.age_seconds)
    _check_hex64("input_sha256", event.input_sha256)
    _check_hex64("event_sha256", event.event_sha256)
    event_kind = _check_event_kind(event.event_kind)
    result_value = _check_result_value(event.result_value)

    if result_value == RESULT_NO_OBSERVATION:
        raise OutboxIdentityError(
            "outbox: NO_OBSERVATION result cannot produce a preview"
        )

    expected_result = EXPECTED_RESULT_FOR_EVENT_KIND[event_kind]
    if expected_result != result_value:
        raise OutboxIdentityError(
            "outbox: event_kind / result_value pairing is invalid: "
            f"event_kind={event_kind!r} requires "
            f"result_value={expected_result!r}, got {result_value!r}"
        )

    return event


# ---------------------------------------------------------------------------
# Public API: queue_previews
# ---------------------------------------------------------------------------

def queue_previews(
    conn: sqlite3.Connection,
    context: ExternalContext,
    session_id: str,
    events: list[AuditEvent] | tuple[AuditEvent, ...],
) -> BatchPreviewOutcome:
    """Append inert dry_run_outbox rows for the given immutable AuditEvents.

    Only ``AuditEvent`` objects whose ``result_value`` is in
    ``{EXPIRED, STALE}`` produce a row; the V1 ``audit_events`` pipeline
    never produces a row for ``NO_OBSERVATION`` so the input is
    normally already pre-filtered.

    A same-``payload_sha256`` re-submission is an idempotent no-op:
    the existing row is unchanged and the returned ``PreviewRow`` has
    ``duplicate=True``. A same-``job_id`` submission with a different
    ``payload_sha256`` raises :class:`OutboxConflictError`
    (append-only contract).

    All writes run inside a single transaction; any error rolls the
    whole batch back, leaving ``audit_events`` / ``evaluation_inputs``
    / ``dry_run_outbox`` unchanged. The function never touches
    ``artifact_manifest`` or any operational / live job state, and
    never reads from the network, the clock, the RNG, the process
    table, or the environment.

    The function performs NO enqueue / consumer / delivery work. It
    only writes inert preview rows; no caller hook, no scheduler, no
    notifier, no retry.
    """
    if not isinstance(conn, sqlite3.Connection):
        raise Phase6ConfigurationError(
            "outbox: conn must be a sqlite3.Connection"
        )
    if not isinstance(context, ExternalContext):
        raise Phase6ConfigurationError(
            "outbox: context must be an ExternalContext"
        )
    if isinstance(events, (str, bytes)) or not isinstance(events, (list, tuple)):
        raise Phase6ConfigurationError(
            "outbox: events must be a list or tuple of AuditEvent"
        )
    if len(events) == 0:
        raise OutboxBatchError("outbox: events batch must be non-empty")
    if len(events) > MAX_BATCH_SIZE:
        raise OutboxBatchError(
            f"outbox: events batch must be <= {MAX_BATCH_SIZE}"
        )

    try:
        validate_external_context(context)
    except Phase6IdentityError as exc:
        raise OutboxIdentityError(
            "outbox: ExternalContext time identity is invalid"
        ) from exc

    session_id = _check_session_id(session_id)

    _verify_session_db(conn)

    seen: set[str] = set()
    planned: list[tuple[AuditEvent, PreviewRow]] = []
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as exc:
        raise OutboxSchemaError(
            "outbox: cannot BEGIN IMMEDIATE on session DB"
        ) from exc

    try:
        for raw in events:
            caller_event = _validate_audit_event(raw)
            if caller_event.job_id in seen:
                raise OutboxBatchError(
                    "outbox: duplicate job_id within batch"
                )
            seen.add(caller_event.job_id)
            persisted_event = _load_persisted_audit_event(conn, caller_event)
            _load_persisted_evaluation_input(conn, persisted_event)
            preview = _build_preview_row(persisted_event, session_id)
            planned.append((persisted_event, preview))

        written_by_job: dict[str, PreviewRow] = {}
        for persisted_event, preview in planned:
            expected_row = _preview_row_values(preview)
            existing_row = conn.execute(
                "SELECT payload_sha256, event_sha256, job_id, result_value, "
                "       age_seconds, state, template_id, canonical_payload_json, "
                "       session_id, evaluation_utc, credential_free, "
                "       delivery_evidence "
                "FROM dry_run_outbox WHERE payload_sha256 = ?",
                (preview.payload_sha256,),
            ).fetchone()
            if existing_row is not None:
                if tuple(existing_row) != expected_row:
                    raise OutboxSchemaError(
                        "outbox: existing dry_run_outbox row for payload_sha256 "
                        f"{preview.payload_sha256!r} does not exactly match the canonical preview row"
                    )
                written_by_job[persisted_event.job_id] = dataclasses.replace(
                    preview, persisted=False, duplicate=True,
                )
                continue

            existing_job = conn.execute(
                "SELECT payload_sha256 FROM dry_run_outbox WHERE job_id = ?",
                (persisted_event.job_id,),
            ).fetchone()
            if existing_job is not None and existing_job[0] != preview.payload_sha256:
                raise OutboxConflictError(
                    f"outbox: append-only conflict for job_id {persisted_event.job_id!r}: "
                    f"existing payload_sha256 {existing_job[0]!r} != new {preview.payload_sha256!r}"
                )

            try:
                conn.execute(
                    "INSERT INTO dry_run_outbox"
                    "(payload_sha256, event_sha256, job_id, result_value, "
                    " age_seconds, state, template_id, "
                    " canonical_payload_json, session_id, evaluation_utc, "
                    " credential_free, delivery_evidence) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    expected_row,
                )
            except sqlite3.IntegrityError as exc:
                message = str(exc)
                if "UNIQUE constraint failed: dry_run_outbox.payload_sha256" in message:
                    existing_after = conn.execute(
                        "SELECT payload_sha256, event_sha256, job_id, result_value, "
                        "       age_seconds, state, template_id, canonical_payload_json, "
                        "       session_id, evaluation_utc, credential_free, "
                        "       delivery_evidence "
                        "FROM dry_run_outbox WHERE payload_sha256 = ?",
                        (preview.payload_sha256,),
                    ).fetchone()
                    if existing_after is not None and tuple(existing_after) == expected_row:
                        written_by_job[persisted_event.job_id] = dataclasses.replace(
                            preview, persisted=False, duplicate=True,
                        )
                        continue
                    if existing_after is not None:
                        raise OutboxSchemaError(
                            "outbox: payload_sha256 primary-key collision does not match the canonical preview row"
                        ) from exc
                    raise OutboxConflictError(
                        f"outbox: dry_run_outbox payload_sha256 collision for job_id {persisted_event.job_id!r}"
                    ) from exc
                if "FOREIGN KEY constraint failed" in message or "CHECK constraint failed" in message:
                    raise OutboxSchemaError(
                        f"outbox: dry_run_outbox insert violated schema constraints for job_id {persisted_event.job_id!r}: {message}"
                    ) from exc
                raise OutboxSchemaError(
                    f"outbox: dry_run_outbox insert failed for job_id {persisted_event.job_id!r}: {message}"
                ) from exc
            written_by_job[persisted_event.job_id] = dataclasses.replace(
                preview, persisted=True,
            )
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    out: list[PreviewRow] = []
    for raw in events:
        if isinstance(raw, AuditEvent) and raw.result_value in (RESULT_EXPIRED, RESULT_STALE):
            row = written_by_job.get(raw.job_id)
            if row is not None:
                out.append(row)
    return BatchPreviewOutcome(rows=tuple(out))

__all__ = [
    "MAX_OUTBOX_PAYLOAD_JSON_LEN",
    "OUTBOX_STATE_PREVIEW_ONLY",
    "OUTBOX_TEMPLATE_ID",
    "EXPECTED_RESULT_FOR_EVENT_KIND",
    "OutboxError",
    "OutboxIdentityError",
    "OutboxConsistencyError",
    "OutboxMissingInputError",
    "OutboxBatchError",
    "OutboxConflictError",
    "OutboxSchemaError",
    "PreviewRow",
    "BatchPreviewOutcome",
    "canonical_payload_json",
    "canonical_payload_payload",
    "payload_sha256",
    "queue_previews",
]