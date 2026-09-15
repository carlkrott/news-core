"""Slice 6.3 — append-only observation audit.

The Slice 6.3 contract:

* consume only immutable :class:`EvaluationResult` objects (already-
  evaluated batch outcomes from Slice 6.2) and read only the already-open
  validated V1 in-memory session DB,
* append ONLY ``audit_events`` rows for evaluated results:

    - ``age < 300``           -> NO audit event, NO audit row
    - ``300 <= age < 600``    -> ``event_kind=EXPIRED_OBSERVED``,
                                ``result_value=EXPIRED``
    - ``age >= 600``          -> ``event_kind=STALE_OBSERVED``,
                                ``result_value=STALE``

* require the corresponding ``evaluation_inputs`` row to exist with exact
  ``input_sha256``, ``job_id``, ``reference_utc``, ``age_seconds``,
  ``source_state`` and ``canonical_input_json`` byte-match (which
  transitively enforces ``evaluation_utc`` consistency),
* fail closed on missing input or any mismatch,
* be append-only: never ``UPDATE`` or ``DELETE`` rows in
  ``evaluation_inputs`` or ``audit_events``; never touch
  ``dry_run_outbox`` or ``artifact_manifest``,
* treat an exact-duplicate (same ``event_sha256`` + ``job_id``) as a
  contract-safe validated no-op,
* reject a same-``job_id`` row whose existing ``event_sha256`` differs
  from the new one (append-only conflict),
* use parameterized SQL and a single transaction with rollback on any
  batch failure,
* emit deterministic, credential-free canonical event JSON / SHA-256,
  with no destination/provider/delivery fields,
* bound the batch and the string fields, fail closed on any
  over-limit or non-typed input,
* never call clock/time/random/process/UUID/entropy/network APIs,
  never consult any environment variable,
* never read or write operational / live job state, retry, enqueue,
  page, or hit the network.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import sqlite3
from typing import Final

from .recovery import (
    EvaluationResult,
    MAX_BATCH_SIZE,
    RESULT_EXPIRED,
    RESULT_NO_OBSERVATION,
    RESULT_STALE,
    canonical_input_json,
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

EVENT_EXPIRED_OBSERVED: Final[str] = "EXPIRED_OBSERVED"
EVENT_STALE_OBSERVED: Final[str] = "STALE_OBSERVED"

EVENT_KIND_FOR_RESULT: Final[dict[str, str]] = {
    RESULT_EXPIRED: EVENT_EXPIRED_OBSERVED,
    RESULT_STALE: EVENT_STALE_OBSERVED,
}

# Job_id pattern is identical to the Slice 6.2 contract.
_JOB_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._-]{1,128}$"
_JOB_ID_RE: Final[re.Pattern[str]] = re.compile(_JOB_ID_PATTERN)

# Source-state pattern is identical to the Slice 6.2 contract.
_SOURCE_STATE_PATTERN: Final[str] = r"^[A-Z0-9_:-]{1,64}$"
_SOURCE_STATE_RE: Final[re.Pattern[str]] = re.compile(_SOURCE_STATE_PATTERN)

_INT64_MAX: Final[int] = (2 ** 63) - 1
_MAX_JOB_ID_LEN: Final[int] = 128
_MAX_SOURCE_STATE_LEN: Final[int] = 64
_MAX_TIMESTAMP_LEN: Final[int] = 32
_MAX_CANONICAL_EVENT_JSON_LEN: Final[int] = 2048

# Bounded batch (mirrors Slice 6.2).
_BAND_NO_OBSERVATION_UPPER_EXCLUSIVE: Final[int] = 300
_BAND_EXPIRED_UPPER_EXCLUSIVE: Final[int] = 600

_HEX64_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class AuditError(Phase6SandboxError):
    """Base class for every Slice 6.3 failure mode."""


class AuditIdentityError(AuditError, Phase6IdentityError):
    """A caller-supplied EvaluationResult failed field-level identity validation."""


class AuditConsistencyError(AuditError):
    """Caller-supplied EvaluationResult is inconsistent with evaluation_inputs."""


class AuditMissingInputError(AuditConsistencyError):
    """The required evaluation_inputs row does not exist."""


class AuditBatchError(AuditError):
    """The batch itself is malformed: empty, oversized, or has duplicate job_ids."""


class AuditConflictError(AuditError):
    """An audit_events row already exists for the job_id with a different event_sha256."""


class AuditSchemaError(AuditError, Phase6SchemaError):
    """The open session DB does not match the V1 contract Slice 6.3 expects."""


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class AuditEvent:
    """A single audited observation event (append-only record).

    The first six fields are the contract fields used to derive the
    deterministic canonical event JSON. The remaining three are
    metadata that the implementation fills in when the row is
    persisted: the exact canonical JSON bytes written to the row,
    the SHA-256 of those bytes, and the duplicate / persisted flags.
    """

    job_id: str
    event_kind: str
    result_value: str
    evaluation_utc: str
    age_seconds: int
    input_sha256: str
    canonical_event_json: str = ""
    event_sha256: str = ""
    persisted: bool = False
    duplicate: bool = False


@dataclasses.dataclass(frozen=True)
class BatchAuditOutcome:
    """The full outcome of an :func:`audit_observations` call.

    ``events`` contains exactly one :class:`AuditEvent` per audited
    result (NO_OBSERVATION inputs are silently skipped, since they
    produce no audit row). Each AuditEvent has ``persisted=True`` if
    the row was newly appended, ``duplicate=True`` if the row was
    already present with the same ``event_sha256``, and
    ``persisted=False, duplicate=False`` if the audit call failed
    before persistence (the caller will have received an exception
    in that case).
    """

    events: tuple[AuditEvent, ...]


# ---------------------------------------------------------------------------
# Field-level validation
# ---------------------------------------------------------------------------

def _check_job_id(value: object) -> str:
    if not isinstance(value, str):
        raise AuditIdentityError(
            f"audit: job_id must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise AuditIdentityError("audit: job_id must not be empty")
    if len(value) > _MAX_JOB_ID_LEN:
        raise AuditIdentityError(
            f"audit: job_id length must be <= {_MAX_JOB_ID_LEN}"
        )
    if not _JOB_ID_RE.match(value):
        raise AuditIdentityError(
            "audit: job_id must match [A-Za-z0-9._-]{1,128}"
        )
    return value


def _check_source_state(value: object) -> str:
    if not isinstance(value, str):
        raise AuditIdentityError(
            f"audit: source_state must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise AuditIdentityError("audit: source_state must not be empty")
    if len(value) > _MAX_SOURCE_STATE_LEN:
        raise AuditIdentityError(
            f"audit: source_state length must be <= {_MAX_SOURCE_STATE_LEN}"
        )
    if not _SOURCE_STATE_RE.match(value):
        raise AuditIdentityError(
            "audit: source_state must match [A-Z0-9_:-]{1,64}"
        )
    return value


def _check_timestamp(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise AuditIdentityError(
            f"audit: {name} must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise AuditIdentityError(f"audit: {name} must not be empty")
    if len(value) > _MAX_TIMESTAMP_LEN:
        raise AuditIdentityError(
            f"audit: {name} length must be <= {_MAX_TIMESTAMP_LEN}"
        )
    return value


def _check_age(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AuditIdentityError(
            f"audit: age_seconds must be int, got {type(value).__name__}"
        )
    if value < 0:
        raise AuditConsistencyError(
            "audit: age_seconds must be >= 0 (negative not allowed)"
        )
    if value > _INT64_MAX:
        raise AuditIdentityError(
            f"audit: age_seconds must be <= {_INT64_MAX}"
        )
    return value


def _check_hex64(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise AuditIdentityError(
            f"audit: {name} must be str, got {type(value).__name__}"
        )
    if not _HEX64_RE.match(value):
        raise AuditIdentityError(
            f"audit: {name} must be 64 lowercase hex chars"
        )
    return value


def _check_event_kind(value: object) -> str:
    if not isinstance(value, str):
        raise AuditIdentityError(
            f"audit: event_kind must be str, got {type(value).__name__}"
        )
    if value not in (EVENT_EXPIRED_OBSERVED, EVENT_STALE_OBSERVED):
        raise AuditIdentityError(
            f"audit: event_kind must be {EVENT_EXPIRED_OBSERVED!r} "
            f"or {EVENT_STALE_OBSERVED!r}, got {value!r}"
        )
    return value


def _check_result_value(value: object) -> str:
    if not isinstance(value, str):
        raise AuditIdentityError(
            f"audit: result_value must be str, got {type(value).__name__}"
        )
    if value not in (RESULT_EXPIRED, RESULT_STALE):
        raise AuditIdentityError(
            f"audit: result_value must be {RESULT_EXPIRED!r} "
            f"or {RESULT_STALE!r}, got {value!r}"
        )
    return value


# ---------------------------------------------------------------------------
# Canonical event payload / JSON / SHA-256
# ---------------------------------------------------------------------------

def canonical_event_payload(event: AuditEvent) -> dict[str, object]:
    """Return the immutable, sorted-key event payload.

    The payload is intentionally narrow: the six contract fields and
    nothing else. No destination / provider / delivery / retry / queue
    metadata may be present.
    """
    return {
        "age_seconds": event.age_seconds,
        "event_kind": event.event_kind,
        "evaluation_utc": event.evaluation_utc,
        "input_sha256": event.input_sha256,
        "job_id": event.job_id,
        "result_value": event.result_value,
    }


def canonical_event_json(event: AuditEvent) -> str:
    """Return the deterministic UTF-8 JSON encoding of the event payload."""
    return json.dumps(
        canonical_event_payload(event),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def event_sha256(canonical_json: str) -> str:
    """Return the lowercase-hex SHA-256 of the canonical event JSON."""
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Session DB contract verification
# ---------------------------------------------------------------------------

def _verify_session_db(conn: sqlite3.Connection) -> None:
    """Fail-closed verification of the V1 contract slice 6.3 depends on."""
    try:
        row = conn.execute("PRAGMA user_version").fetchone()
    except sqlite3.Error as exc:
        raise AuditSchemaError("audit: cannot read user_version") from exc
    user_version = int(row[0]) if row else 0
    if user_version != 1:
        raise AuditSchemaError(
            f"audit: open session DB user_version must be 1, got {user_version}"
        )
    try:
        eval_cols = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(evaluation_inputs)"
            ).fetchall()
        }
    except sqlite3.Error as exc:
        raise AuditSchemaError(
            "audit: evaluation_inputs table is missing"
        ) from exc
    eval_required = {
        "job_id", "reference_utc", "age_seconds", "source_state",
        "canonical_input_json", "input_sha256",
    }
    eval_missing = eval_required - eval_cols
    if eval_missing:
        raise AuditSchemaError(
            f"audit: evaluation_inputs is missing required columns: "
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
        raise AuditSchemaError(
            "audit: audit_events table is missing"
        ) from exc
    audit_required = {
        "event_sha256", "job_id", "event_kind", "result_value",
        "evaluation_utc", "age_seconds", "input_sha256",
        "canonical_event_json",
    }
    audit_missing = audit_required - audit_cols
    if audit_missing:
        raise AuditSchemaError(
            f"audit: audit_events is missing required columns: "
            f"{sorted(audit_missing)}"
        )


# ---------------------------------------------------------------------------
# Core: build audit events from evaluated results
# ---------------------------------------------------------------------------

def _expected_event_kind_and_value(age_seconds: int) -> tuple[str, str]:
    """Map age to (event_kind, result_value) per the immutable decision table."""
    if age_seconds >= _BAND_EXPIRED_UPPER_EXCLUSIVE:
        return EVENT_STALE_OBSERVED, RESULT_STALE
    if age_seconds >= _BAND_NO_OBSERVATION_UPPER_EXCLUSIVE:
        return EVENT_EXPIRED_OBSERVED, RESULT_EXPIRED
    # NO_OBSERVATION is not auditable; callers must filter before this
    # helper is invoked.
    raise AuditIdentityError(
        "audit: age < 300 cannot produce an audit event"
    )


def _build_audit_event(
    result: EvaluationResult,
) -> AuditEvent:
    """Validate an EvaluationResult and build the (unfinalised) AuditEvent."""
    if not isinstance(result, EvaluationResult):
        raise Phase6ConfigurationError(
            "audit: inputs element is not an EvaluationResult"
        )
    if result.result == RESULT_NO_OBSERVATION:
        # NO_OBSERVATION never produces an audit row.
        raise AuditIdentityError(
            "audit: NO_OBSERVATION result cannot be audited"
        )
    job_id = _check_job_id(result.job_id)
    reference_utc = _check_timestamp("reference_utc", result.reference_utc)
    evaluation_utc = _check_timestamp("evaluation_utc", result.evaluation_utc)
    age_seconds = _check_age(result.age_seconds)
    source_state = _check_source_state(result.source_state)
    input_sha256 = _check_hex64("input_sha256", result.input_sha256)

    if reference_utc > evaluation_utc:
        raise AuditConsistencyError(
            "audit: reference_utc must be <= evaluation_utc"
        )

    expected_kind, expected_value = _expected_event_kind_and_value(age_seconds)
    _check_event_kind(expected_kind)  # belt-and-braces
    _check_result_value(expected_value)

    return AuditEvent(
        job_id=job_id,
        event_kind=expected_kind,
        result_value=expected_value,
        evaluation_utc=evaluation_utc,
        age_seconds=age_seconds,
        input_sha256=input_sha256,
    )


def _finalise_event(event: AuditEvent) -> AuditEvent:
    """Compute and pin the canonical JSON + SHA-256 onto the AuditEvent."""
    canonical = canonical_event_json(event)
    if len(canonical) > _MAX_CANONICAL_EVENT_JSON_LEN:
        raise AuditIdentityError(
            "audit: canonical_event_json length exceeded"
        )
    sha = event_sha256(canonical)
    return dataclasses.replace(
        event, canonical_event_json=canonical, event_sha256=sha,
    )


# ---------------------------------------------------------------------------
# Public API: audit_observations
# ---------------------------------------------------------------------------

def audit_observations(
    conn: sqlite3.Connection,
    context: ExternalContext,
    results: list[EvaluationResult] | tuple[EvaluationResult, ...],
) -> BatchAuditOutcome:
    """Append audit_events rows for the given immutable EvaluationResults.

    Only results with ``result_value in (EXPIRED, STALE)`` produce a row;
    ``NO_OBSERVATION`` results are silently skipped (no audit row, no
    AuditEvent in the returned BatchAuditOutcome).

    A same-``(event_sha256, job_id)`` re-submission is an idempotent
    no-op: the existing audit row is unchanged and the returned
    AuditEvent has ``duplicate=True``. A same-``job_id`` submission
    with a different ``event_sha256`` raises
    :class:`AuditConflictError` (append-only contract).

    All writes run inside a single transaction; any error rolls the
    whole batch back, leaving ``audit_events`` and ``evaluation_inputs``
    unchanged. The function never touches ``dry_run_outbox``,
    ``artifact_manifest``, or any operational / live job state.
    """
    if not isinstance(conn, sqlite3.Connection):
        raise Phase6ConfigurationError(
            "audit: conn must be a sqlite3.Connection"
        )
    if not isinstance(context, ExternalContext):
        raise Phase6ConfigurationError(
            "audit: context must be an ExternalContext"
        )
    if isinstance(results, (str, bytes)) or not isinstance(results, (list, tuple)):
        raise Phase6ConfigurationError(
            "audit: results must be a list or tuple of EvaluationResult"
        )
    if len(results) == 0:
        raise AuditBatchError("audit: results batch must be non-empty")
    if len(results) > MAX_BATCH_SIZE:
        raise AuditBatchError(
            f"audit: results batch must be <= {MAX_BATCH_SIZE}"
        )

    try:
        validate_external_context(context)
    except Phase6IdentityError as exc:
        raise AuditIdentityError(
            "audit: ExternalContext time identity is invalid"
        ) from exc

    _verify_session_db(conn)

    # Build the per-event state. NO_OBSERVATION results are filtered
    # out before any work that touches the DB.
    seen: set[str] = set()
    planned: list[tuple[EvaluationResult, AuditEvent]] = []
    for r in results:
        if not isinstance(r, EvaluationResult):
            raise Phase6ConfigurationError(
                "audit: results element is not an EvaluationResult"
            )
        if r.result == RESULT_NO_OBSERVATION:
            # NO_OBSERVATION -> no audit row, no AuditEvent.
            continue
        if r.job_id in seen:
            raise AuditBatchError(
                "audit: duplicate job_id within batch"
            )
        seen.add(r.job_id)
        event = _build_audit_event(r)
        event = _finalise_event(event)
        planned.append((r, event))

    # Read all matching evaluation_inputs rows in one query.
    if planned:
        placeholders = ",".join("?" for _ in planned)
        eval_rows = conn.execute(
            f"SELECT job_id, reference_utc, age_seconds, source_state, "
            f"       canonical_input_json, input_sha256 "
            f"FROM evaluation_inputs WHERE job_id IN ({placeholders})",
            tuple(e.job_id for _r, e in planned),
        ).fetchall()
        eval_by_job: dict[str, tuple[str, str, int, str, str, str]] = {
            row[0]: row for row in eval_rows
        }
    else:
        eval_by_job = {}

    # Consistency-check every planned event BEFORE writing anything.
    # We use the result (EvaluationResult) and the DB row to enforce
    # canonical_input_json byte-match, which transitively verifies
    # reference_utc, evaluation_utc, age_seconds and source_state.
    for result, event in planned:
        row = eval_by_job.get(event.job_id)
        if row is None:
            raise AuditMissingInputError(
                f"audit: evaluation_inputs row missing for job_id "
                f"{event.job_id!r}"
            )
        _db_job_id, db_ref, db_age, db_src, db_canon, db_sha = row
        # Recompute canonical_input_json from the EvaluationResult
        # fields (which are the contract-validated inputs) and
        # compare to what is stored.
        expected_canon = canonical_input_json_for_result(result)
        if db_canon != expected_canon:
            raise AuditConsistencyError(
                f"audit: canonical_input_json mismatch for job_id "
                f"{event.job_id!r}"
            )
        if db_sha != event.input_sha256:
            raise AuditConsistencyError(
                f"audit: input_sha256 mismatch for job_id "
                f"{event.job_id!r}"
            )
        if db_age != event.age_seconds:
            raise AuditConsistencyError(
                f"audit: age_seconds mismatch for job_id "
                f"{event.job_id!r}"
            )
        if db_ref != result.reference_utc:
            raise AuditConsistencyError(
                f"audit: reference_utc mismatch for job_id "
                f"{event.job_id!r}"
            )
        if db_src != result.source_state:
            raise AuditConsistencyError(
                f"audit: source_state mismatch for job_id "
                f"{event.job_id!r}"
            )

    # Detect duplicate-vs-conflict against pre-existing audit_events rows.
    if planned:
        placeholders = ",".join("?" for _ in planned)
        existing_rows = conn.execute(
            f"SELECT job_id, event_sha256 FROM audit_events "
            f"WHERE job_id IN ({placeholders})",
            tuple(e.job_id for _r, e in planned),
        ).fetchall()
        existing_by_job: dict[str, str] = {row[0]: row[1] for row in existing_rows}
    else:
        existing_by_job = {}

    # Insert in a single transaction with rollback on any failure.
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as exc:
        raise AuditSchemaError(
            "audit: cannot BEGIN IMMEDIATE on session DB"
        ) from exc

    written_by_job: dict[str, AuditEvent] = {}
    try:
        for _result, event in planned:
            existing = existing_by_job.get(event.job_id)
            if existing is not None:
                if existing != event.event_sha256:
                    raise AuditConflictError(
                        f"audit: append-only conflict for job_id "
                        f"{event.job_id!r}: existing event_sha256 "
                        f"{existing!r} != new {event.event_sha256!r}"
                    )
                # Idempotent no-op: same (job_id, event_sha256).
                written_by_job[event.job_id] = dataclasses.replace(
                    event, persisted=False, duplicate=True,
                )
                continue
            try:
                conn.execute(
                    "INSERT INTO audit_events"
                    "(event_sha256, job_id, event_kind, result_value, "
                    " evaluation_utc, age_seconds, input_sha256, "
                    " canonical_event_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.event_sha256,
                        event.job_id,
                        event.event_kind,
                        event.result_value,
                        event.evaluation_utc,
                        event.age_seconds,
                        event.input_sha256,
                        event.canonical_event_json,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise AuditConflictError(
                    f"audit: audit_events already has a row for job_id "
                    f"{event.job_id!r}"
                ) from exc
            written_by_job[event.job_id] = dataclasses.replace(
                event, persisted=True,
            )
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    # Emit AuditEvent markers in input order, skipping NO_OBSERVATION.
    out: list[AuditEvent] = []
    for r in results:
        if r.result == RESULT_NO_OBSERVATION:
            continue
        e = written_by_job.get(r.job_id)
        if e is not None:
            out.append(e)

    return BatchAuditOutcome(events=tuple(out))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def canonical_input_json_for_result(result: EvaluationResult) -> str:
    """Recompute the Slice 6.2 canonical_input_json from an EvaluationResult.

    The EvaluationResult carries job_id / reference_utc / evaluation_utc
    / age_seconds / source_state. We rebuild the Slice 6.2 canonical
    JSON via the exact same payload shape so byte-match is the contract
    test.
    """
    return canonical_input_json(
        # The canonical_input_json helper takes a JobInput; we build a
        # transient one with the contract fields carried by the result.
        # This is a value-copy: we never mutate the EvaluationResult.
        _JobInputShim(
            job_id=result.job_id,
            reference_utc=result.reference_utc,
            evaluation_utc=result.evaluation_utc,
            age_seconds=result.age_seconds,
            source_state=result.source_state,
        )
    )


@dataclasses.dataclass(frozen=True)
class _JobInputShim:
    """Minimal duck-typed stand-in for :class:`JobInput`.

    :func:`recovery.canonical_input_json` only reads five attributes
    from its argument, so a frozen dataclass with the same field names
    is sufficient. We avoid importing :class:`JobInput` directly here
    because it would create a circular-import surface; the Slice 6.2
    helper does not depend on Slice 6.3.
    """

    job_id: str
    reference_utc: str
    evaluation_utc: str
    age_seconds: int
    source_state: str


__all__ = [
    "EVENT_EXPIRED_OBSERVED",
    "EVENT_STALE_OBSERVED",
    "EVENT_KIND_FOR_RESULT",
    "AuditError",
    "AuditIdentityError",
    "AuditConsistencyError",
    "AuditMissingInputError",
    "AuditBatchError",
    "AuditConflictError",
    "AuditSchemaError",
    "AuditEvent",
    "BatchAuditOutcome",
    "canonical_event_payload",
    "canonical_event_json",
    "event_sha256",
    "audit_observations",
]