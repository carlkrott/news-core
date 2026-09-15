"""Phase 4 — Slice 2 shadow ledger.

Owns the three shadow briefing tables documented in the Phase 4 supervisor
addendum §3. The class receives one open ``sqlite3.Connection`` configured
with ``isolation_level=None`` (autocommit) and is responsible for:

  * ``SELECT 1`` to verify the connection is open;
  * setting and verifying ``PRAGMA foreign_keys=ON`` and
    ``PRAGMA busy_timeout=5000``;
  * the exact three-table / one-index schema; creation runs inside a single
    ``BEGIN IMMEDIATE`` DDL transaction and rolls back on any failure;
  * canonical JSON payload normalization (oversize, nonfinite,
    duplicate-key, sensitive-key rejection at every depth);
  * every mutation wrapped in ``BEGIN IMMEDIATE`` with commit/rollback;
  * mapping only ``OperationalError`` lock/busy states to
    ``LockTimeoutError``; integrity/state failures to typed ledger
    errors; programmer errors propagate;
  * validating every persisted timestamp through ``validate_utc_iso``
    (UTC, normalized Z form) before mutation;
  * validating ``run_id`` (1..256) and ``error_code`` (a conservative
    ``[A-Z0-9_.-]{1,128}`` token);
  * ``seen_candidate_ids`` takes an exact tuple and rejects duplicates
    rather than silently deduping.

The ledger never opens or closes a filesystem path. It never imports
or calls network / wall-clock / random / uuid / open / Path APIs.
"""
from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

from .contracts import validate_utc_iso
from .event_contracts import SemanticDecision
from .models import Category


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class LedgerError(Exception):
    """Base class for all Phase 4 ledger errors."""


class LedgerConnectionError(LedgerError):
    """Connection is closed, deferred, or unusable for autocommit SQL."""


class LedgerContractError(LedgerError):
    """Caller-supplied input is structurally invalid for the ledger."""


class LedgerStateError(LedgerError):
    """Public operation invoked before ``initialize_schema`` completed."""


class PayloadValidationError(LedgerError):
    """Canonical payload normalization rejected the supplied value."""


class LockTimeoutError(LedgerError):
    """SQLite lock/busy OperationalError mapped from ``BEGIN IMMEDIATE``."""


class RunConflictError(LedgerError):
    """A persisted row exists with conflicting fields, or a retry differs."""


class RunStateError(LedgerError):
    """A state machine transition was attempted on the wrong state."""


# ---------------------------------------------------------------------------
# Frozen / slotted value objects
# ---------------------------------------------------------------------------


_RUN_ID_MAX = 256
_ERROR_CODE_RE = re.compile(r"^[A-Z0-9_.-]{1,128}\Z")
_C0_C1_BIDI_RE = re.compile(r"[\x00-\x1f\x7f\u0080-\u009f\u202A-\u202E\u2066-\u2069]")


def _check_candidate_id(value: object) -> str:
    """Validate a candidate_id: 1..256 code points and no C0/C1/bidi controls."""
    if not isinstance(value, str):
        raise LedgerContractError(
            f"candidate_id must be a str, got {type(value).__name__}"
        )
    if not value:
        raise LedgerContractError("candidate_id must be non-empty")
    if len(value) > _RUN_ID_MAX:
        raise LedgerContractError(
            f"candidate_id must be at most {_RUN_ID_MAX} code points, "
            f"got {len(value)}"
        )
    if _C0_C1_BIDI_RE.search(value):
        raise LedgerContractError(
            "candidate_id contains disallowed C0/C1 control or bidi character"
        )
    return value


def _validate_run_id(value: object, field: str = "run_id") -> str:
    """Validate ``run_id``: non-empty str, 1..256 code points."""
    if not isinstance(value, str):
        raise LedgerContractError(
            f"{field} must be a str, got {type(value).__name__}"
        )
    if not value:
        raise LedgerContractError(f"{field} must be non-empty")
    if len(value) > _RUN_ID_MAX:
        raise LedgerContractError(
            f"{field} must be at most {_RUN_ID_MAX} code points, got "
            f"{len(value)}"
        )
    return value


def _validate_error_code(value: object) -> str:
    """Validate ``error_code``: conservative ``[A-Z0-9_.-]{1,128}``."""
    if not isinstance(value, str):
        raise LedgerContractError(
            f"error_code must be a str, got {type(value).__name__}"
        )
    if not value or len(value) > 128:
        raise LedgerContractError(
            f"error_code must be 1..128 chars, got {len(value)}"
        )
    if not _ERROR_CODE_RE.match(value):
        raise LedgerContractError(
            "error_code must match [A-Z0-9_.-]{1,128}"
        )
    return value


def _validate_timestamp(value: object, field: str) -> str:
    """Normalize a UTC ISO string to ``Z`` form via ``validate_utc_iso``.

    Rejects naive datetimes, non-zero-offset aware datetimes, and any
    value that ``validate_utc_iso`` cannot normalize. Returns the
    exact ``Z`` form so lexical comparison matches semantically-aware
    ordering.
    """
    if not isinstance(value, str) or not value:
        raise LedgerContractError(
            f"{field} must be a non-empty str, got {value!r}"
        )
    try:
        normalized = validate_utc_iso(value)
    except (TypeError, ValueError) as exc:
        raise LedgerContractError(
            f"{field} is not a valid UTC ISO-8601 string: {value!r}"
        ) from exc
    # ``validate_utc_iso`` always emits Z form per its contract;
    # defensively verify so SQL comparison stays lexical.
    if not normalized.endswith("Z"):
        raise LedgerContractError(
            f"{field} must normalize to UTC Z form, got {normalized!r}"
        )
    return normalized


@dataclass(frozen=True, slots=True)
class ShadowEvent:
    """One ``(candidate_id, payload_json)`` ledger event for ``complete_run``.

    The constructor enforces exact ``Category`` / ``SemanticDecision`` enum
    types, immutable UTF-8 bytes payload with no control characters in
    the candidate_id, and a UTC ISO-8601 ``recorded_at`` normalized to
    ``Z`` form via ``validate_utc_iso``. ``decision`` must be one of
    ``distinct_event`` / ``material_update`` per the addendum.
    """

    candidate_id: str
    category: Category
    decision: SemanticDecision
    payload_json: bytes
    recorded_at_utc: str

    def __post_init__(self) -> None:
        _check_candidate_id(self.candidate_id)
        if not isinstance(self.category, Category):
            raise LedgerContractError(
                f"ShadowEvent.category must be Category, got "
                f"{type(self.category).__name__}"
            )
        if not isinstance(self.decision, SemanticDecision):
            raise LedgerContractError(
                f"ShadowEvent.decision must be SemanticDecision, got "
                f"{type(self.decision).__name__}"
            )
        # Per addendum only ``distinct_event`` and ``material_update`` are
        # eligible for shadow storage.
        if self.decision not in (
            SemanticDecision.distinct_event,
            SemanticDecision.material_update,
        ):
            raise LedgerContractError(
                f"ShadowEvent.decision must be distinct_event or "
                f"material_update, got {self.decision.value!r}"
            )
        if not isinstance(self.payload_json, (bytes, bytearray)):
            raise LedgerContractError(
                f"ShadowEvent.payload_json must be bytes, got "
                f"{type(self.payload_json).__name__}"
            )
        # bytearray is rejected as non-immutable; bytes is the only
        # accepted container.
        if isinstance(self.payload_json, bytearray):
            raise LedgerContractError(
                "ShadowEvent.payload_json must be immutable bytes"
            )
        if not self.payload_json:
            raise LedgerContractError(
                "ShadowEvent.payload_json must be non-empty"
            )
        try:
            self.payload_json.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise LedgerContractError(
                "ShadowEvent.payload_json must be valid UTF-8"
            ) from exc
        # recorded_at_utc is normalized to Z form via validate_utc_iso.
        object.__setattr__(
            self,
            "recorded_at_utc",
            _validate_timestamp(self.recorded_at_utc, "recorded_at_utc"),
        )


def _validate_complete_run_result(
    new_ids: Tuple[str, ...],
    already_seen_ids: Tuple[str, ...],
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Validate exact tuple types, unique nonempty IDs, disjoint partitions."""
    if not isinstance(new_ids, tuple) or not isinstance(already_seen_ids, tuple):
        raise LedgerContractError(
            "CompleteRunResult.new_ids and already_seen_ids must be tuples"
        )
    new_set = set()
    for cid in new_ids:
        _check_candidate_id(cid)
        if cid in new_set:
            raise LedgerContractError(
                f"CompleteRunResult.new_ids contains duplicate entry: {cid!r}"
            )
        new_set.add(cid)
    as_set = set()
    for cid in already_seen_ids:
        _check_candidate_id(cid)
        if cid in as_set:
            raise LedgerContractError(
                f"CompleteRunResult.already_seen_ids contains duplicate entry: "
                f"{cid!r}"
            )
        as_set.add(cid)
    overlap = new_set & as_set
    if overlap:
        raise LedgerContractError(
            f"CompleteRunResult.new_ids and already_seen_ids must be "
            f"disjoint, but both contain: {sorted(overlap)!r}"
        )
    return new_ids, already_seen_ids


@dataclass(frozen=True, slots=True)
class CompleteRunResult:
    """Returned by ``BriefingLedger.complete_run``.

    Both fields are ordered tuples preserving caller input order. The
    partition is exactly the split observed during ``complete_run``:
    ``new_ids`` were inserted into ``shadow_briefing_seen`` with
    ``event_status='RECORDED'`` and ``already_seen_ids`` were left as-is
    with ``event_status='ALREADY_SEEN'``. ``ShadowLedgerProtocol`` /
    engine consumers may rely on:

      * exact tuple types;
      * every entry is a unique non-empty ``candidate_id``;
      * the two tuples are disjoint (no entry appears in both).
    """

    new_ids: Tuple[str, ...]
    already_seen_ids: Tuple[str, ...]

    def __post_init__(self) -> None:
        new_ids, already_seen_ids = _validate_complete_run_result(
            self.new_ids, self.already_seen_ids
        )
        # ``__post_init__`` cannot reassign frozen fields directly; use
        # ``object.__setattr__`` to preserve the validated tuples on the
        # underlying instance.
        object.__setattr__(self, "new_ids", new_ids)
        object.__setattr__(self, "already_seen_ids", already_seen_ids)


# ---------------------------------------------------------------------------
# Canonical payload normalization
# ---------------------------------------------------------------------------


_PAYLOAD_BYTE_LIMIT = 16_384

_SENSITIVE_KEY_TOKENS: Tuple[str, ...] = (
    "token",
    "secret",
    "password",
    "authorization",
)


def _reject_sensitive_keys(value: Any, path: str = "") -> None:
    """Recursively reject any string key whose case-folded value contains a sensitive token.

    ``path`` is the dotted JSON-pointer-style key path used to surface the
    offending key *without* its associated value, length, or hash. Values
    are never logged.
    """
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise PayloadValidationError(
                    f"payload contains non-string key at {path!r}: "
                    f"key type={type(k).__name__}"
                )
            lowered = k.casefold()
            for token in _SENSITIVE_KEY_TOKENS:
                if token in lowered:
                    raise PayloadValidationError(
                        f"payload contains sensitive key at {path!r}: key redacted"
                    )
            next_path = f"{path}.{k}" if path else k
            _reject_sensitive_keys(v, next_path)
    elif isinstance(value, list):
        for idx, item in enumerate(value):
            _reject_sensitive_keys(item, f"{path}[{idx}]")
    elif isinstance(value, tuple):
        raise PayloadValidationError(
            f"payload contains unsupported tuple at {path!r}"
        )


def _parse_strict_json_object(text: str) -> Dict[str, Any]:
    """Parse ``text`` as a single JSON object; reject duplicates/nonfinite.

    Uses ``object_pairs_hook`` to detect duplicate keys at any depth,
    rejecting before they can collide on insertion.
    """
    sentinel = object()

    def pairs_hook(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
        seen: Dict[str, None] = {}
        out: Dict[str, Any] = {}
        for k, v in pairs:
            if not isinstance(k, str):
                raise PayloadValidationError(
                    f"payload contains non-string key at top level: "
                    f"key type={type(k).__name__}"
                )
            if k in seen:
                raise PayloadValidationError(
                    "payload contains duplicate key at top level: key redacted"
                )
            seen[k] = None
            out[k] = v
        return out

    try:
        parsed = json.loads(text, object_pairs_hook=pairs_hook)
    except json.JSONDecodeError as exc:
        raise PayloadValidationError(
            f"payload is not valid JSON: {exc.msg} at line {exc.lineno}"
        ) from exc

    if not isinstance(parsed, dict):
        raise PayloadValidationError(
            "payload must be a JSON object, "
            f"got {type(parsed).__name__}"
        )

    def _check_nonfinite(value: Any) -> None:
        if isinstance(value, float):
            if value != value or value in (float("inf"), float("-inf")):
                raise PayloadValidationError(
                    "payload contains non-finite number"
                )
        elif isinstance(value, dict):
            for v in value.values():
                _check_nonfinite(v)
        elif isinstance(value, list):
            for v in value:
                _check_nonfinite(v)

    _check_nonfinite(parsed)
    return parsed


def normalize_canonical_payload(
    payload: Union[Mapping[str, Any], str, bytes, bytearray],
) -> bytes:
    """Return canonical UTF-8 bytes for a shadow ledger payload.

    Accepts a Python ``dict`` (or any ``Mapping``) or raw JSON text. The
    canonical byte serialization uses ``json.dumps`` with
    ``ensure_ascii=False``, ``separators=(",", ":")``,
    ``allow_nan=False``. Non-canonical floats (NaN/Infinity), duplicate
    keys, oversize payloads (>16,384 UTF-8 bytes), and any sensitive
    key (case-folded tokens ``token``/``secret``/``password``/
    ``authorization``) anywhere in nested dicts / lists are rejected
    with ``PayloadValidationError``; the error message never carries
    the rejected value, length, or hash.
    """
    raw_source: Optional[bytes] = None
    if isinstance(payload, (bytes, bytearray)):
        raw_source = bytes(payload)
        try:
            text = raw_source.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PayloadValidationError(
                "payload bytes are not valid UTF-8"
            ) from exc
        parsed = _parse_strict_json_object(text)
    elif isinstance(payload, str):
        raw_source = payload.encode("utf-8")
        parsed = _parse_strict_json_object(payload)
    elif isinstance(payload, Mapping):
        try:
            text = json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as exc:
            raise PayloadValidationError(
                f"payload is not JSON-serializable: {type(exc).__name__}"
            ) from exc
        parsed = _parse_strict_json_object(text)
    else:
        raise PayloadValidationError(
            f"payload must be a mapping, str, or bytes, got "
            f"{type(payload).__name__}"
        )

    _reject_sensitive_keys(parsed)

    canonical = json.dumps(
        parsed,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
        sort_keys=True,
    ).encode("utf-8")

    if raw_source is not None and raw_source != canonical:
        raise PayloadValidationError("payload is not canonical JSON")

    if len(canonical) > _PAYLOAD_BYTE_LIMIT:
        raise PayloadValidationError(
            f"payload exceeds {_PAYLOAD_BYTE_LIMIT} UTF-8 bytes"
        )

    return canonical


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_busy_or_locked(error: sqlite3.OperationalError) -> bool:
    """Return True for ``OperationalError`` lock/busy states only."""
    msg = str(error).lower()
    return (
        "database is locked" in msg
        or "database table is locked" in msg
        or ("lock" in msg.split(":", 1)[0] and "locked" in msg)
    )


def _aware_utc(dt: datetime) -> datetime:
    """Return ``dt`` as aware datetime in UTC. Reject naive / non-zero tz."""
    if not isinstance(dt, datetime):
        raise LedgerContractError(
            f"cutoff must be a datetime, got {type(dt).__name__}"
        )
    if dt.tzinfo is None:
        raise LedgerContractError(
            "cutoff must be timezone-aware"
        )
    if dt.utcoffset() != timedelta(0):
        raise LedgerContractError(
            "cutoff must be UTC (zero offset)"
        )
    return dt


def _aware_utc_to_z(dt: datetime) -> str:
    """Render an aware UTC ``datetime`` as a normalized ``Z`` ISO string.

    Truncates microseconds for stable lexical equality across writers.
    """
    dt = _aware_utc(dt)
    # Drop microseconds for stable string equality across writers.
    if dt.microsecond:
        dt = dt.replace(microsecond=0)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# timedelta imported lazily to keep the top of the file pristine.
from datetime import timedelta  # noqa: E402  (localized import)


# ---------------------------------------------------------------------------
# BriefingLedger
# ---------------------------------------------------------------------------


_SCHEMA_DDL: Tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS shadow_briefing_runs (
      run_id TEXT PRIMARY KEY,
      window_lower_utc TEXT NOT NULL,
      window_upper_utc TEXT NOT NULL,
      started_at_utc TEXT NOT NULL,
      updated_at_utc TEXT NOT NULL,
      status TEXT NOT NULL
        CHECK(status IN ('RUNNING','COMPLETED','FAILED','STALE')),
      error_code TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_briefing_seen (
      candidate_id TEXT PRIMARY KEY,
      first_run_id TEXT NOT NULL,
      first_seen_at_utc TEXT NOT NULL,
      category TEXT NOT NULL,
      decision TEXT NOT NULL
        CHECK(decision IN ('distinct_event','material_update')),
      payload_json TEXT NOT NULL,
      FOREIGN KEY(first_run_id) REFERENCES shadow_briefing_runs(run_id)
        ON DELETE RESTRICT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS shadow_briefing_run_events (
      run_id TEXT NOT NULL,
      candidate_id TEXT NOT NULL,
      event_status TEXT NOT NULL
        CHECK(event_status IN ('RECORDED','ALREADY_SEEN')),
      payload_json TEXT NOT NULL,
      event_ordinal INTEGER NOT NULL CHECK(event_ordinal >= 0),
      PRIMARY KEY(run_id, candidate_id),
      UNIQUE(run_id, event_ordinal),
      FOREIGN KEY(run_id) REFERENCES shadow_briefing_runs(run_id)
        ON DELETE CASCADE,
      FOREIGN KEY(candidate_id) REFERENCES shadow_briefing_seen(candidate_id)
        ON DELETE RESTRICT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_shadow_briefing_runs_status_updated
      ON shadow_briefing_runs(status, updated_at_utc)
    """,
)


class BriefingLedger:
    """Shadow briefing ledger bound to a caller-supplied autocommit connection."""

    # ----- construction -------------------------------------------------

    def __init__(self, connection: sqlite3.Connection) -> None:
        if not isinstance(connection, sqlite3.Connection):
            raise LedgerConnectionError(
                f"BriefingLedger requires a sqlite3.Connection, got "
                f"{type(connection).__name__}"
            )
        try:
            probe = connection.execute("SELECT 1").fetchone()
        except sqlite3.ProgrammingError as exc:
            raise LedgerConnectionError(
                f"caller-supplied connection is closed or unusable: {exc!s}"
            ) from exc
        except sqlite3.OperationalError as exc:
            raise LedgerConnectionError(
                f"caller-supplied connection failed probe query: {exc!s}"
            ) from exc
        if probe != (1,):
            raise LedgerConnectionError(
                f"caller-supplied connection probe returned unexpected value: "
                f"{probe!r}"
            )
        if getattr(connection, "isolation_level", "<unset>") is not None:
            raise LedgerConnectionError(
                "BriefingLedger requires an autocommit connection "
                "(sqlite3.connect(..., isolation_level=None)); got isolation_level="
                f"{getattr(connection, 'isolation_level', '<unset>')!r}"
            )

        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=5000")

        fk = connection.execute("PRAGMA foreign_keys").fetchone()[0]
        bt = connection.execute("PRAGMA busy_timeout").fetchone()[0]
        if fk != 1:
            raise LedgerConnectionError(
                "PRAGMA foreign_keys could not be enabled (got "
                f"{fk!r})"
            )
        if int(bt) != 5000:
            raise LedgerConnectionError(
                "PRAGMA busy_timeout could not be set to 5000 (got "
                f"{int(bt)!r})"
            )

        self._connection = connection
        self._initialized = False

    # ----- guarded access -----------------------------------------------

    def _require_initialized(self) -> None:
        """Raise ``LedgerStateError`` if ``initialize_schema`` hasn't run.

        ``initialize_schema`` itself is exempt: it must run before any
        other public method. All other public operations go through
        this guard so the ledger never silently runs against an empty
        schema (which would surface as a raw ``OperationalError`` from
        SQLite).
        """
        if not self._initialized:
            raise LedgerStateError(
                "BriefingLedger.initialize_schema() must succeed before "
                "any other public ledger operation is invoked"
            )

    # ----- schema -------------------------------------------------------

    def initialize_schema(self) -> None:
        """Create the three tables and one index; idempotent.

        All DDL runs inside a single ``BEGIN IMMEDIATE`` transaction
        so a partial schema can never be left behind. Any DDL failure
        rolls back; ``OperationalError`` lock/busy states map to
        ``LockTimeoutError``; other ``sqlite3`` exceptions propagate.
        """
        try:
            try:
                self._connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                if _is_busy_or_locked(exc):
                    raise LockTimeoutError(
                        f"initialize_schema BEGIN IMMEDIATE failed: {exc!s}"
                    ) from exc
                raise
            try:
                for stmt in _SCHEMA_DDL:
                    self._connection.execute(stmt)
            except sqlite3.OperationalError as exc:
                # Best-effort rollback; never mask the original.
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                if _is_busy_or_locked(exc):
                    raise LockTimeoutError(
                        f"initialize_schema DDL hit a lock conflict: {exc!s}"
                    ) from exc
                raise
            try:
                self._connection.execute("COMMIT")
            except sqlite3.OperationalError as exc:
                try:
                    self._connection.execute("ROLLBACK")
                except sqlite3.OperationalError:
                    pass
                if _is_busy_or_locked(exc):
                    raise LockTimeoutError(
                        f"initialize_schema COMMIT hit a lock conflict: {exc!s}"
                    ) from exc
                # Per blocker #6: only busy/locked OperationalError
                # is mapped to LockTimeoutError; other OperationalError
                # propagates unchanged.
                raise
        except LedgerError:
            raise
        self._initialized = True

    # ----- transaction helpers -----------------------------------------

    def _begin_immediate(self) -> None:
        try:
            self._connection.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            if _is_busy_or_locked(exc):
                raise LockTimeoutError(
                    f"BEGIN IMMEDIATE failed: {exc!s}"
                ) from exc
            raise

    def _commit(self) -> None:
        try:
            self._connection.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            # Rollback best-effort; never mask the original exception.
            try:
                self._connection.execute("ROLLBACK")
            except sqlite3.OperationalError:
                pass
            if _is_busy_or_locked(exc):
                raise LockTimeoutError(
                    f"COMMIT hit a lock conflict: {exc!s}"
                ) from exc
            raise

    def _rollback_silent(self) -> None:
        try:
            self._connection.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass

    # ----- begin_run ----------------------------------------------------

    def begin_run(
        self,
        run_id: str,
        lower: str,
        upper: str,
        started: str,
    ) -> None:
        """Begin a new run or no-op if the persisted row matches exactly.

        ``lower``, ``upper``, and ``started`` are UTC ISO-8601 strings.
        All three are normalized to the ``Z`` form via
        ``validate_utc_iso``; ``lower`` must be strictly less than
        ``upper``. A second call with the same ``run_id`` and identical
        ``lower``, ``upper``, and ``started`` is a no-op; any mismatch
        raises ``RunConflictError``.
        """
        run_id = _validate_run_id(run_id, "begin_run.run_id")
        lower_z = _validate_timestamp(lower, "begin_run.lower")
        upper_z = _validate_timestamp(upper, "begin_run.upper")
        if lower_z >= upper_z:
            raise LedgerContractError(
                f"begin_run requires lower < upper, got {lower!r} >= {upper!r}"
            )
        started_z = _validate_timestamp(started, "begin_run.started")

        self._require_initialized()
        self._begin_immediate()
        try:
            existing = self._connection.execute(
                "SELECT window_lower_utc, window_upper_utc, started_at_utc, "
                "updated_at_utc, status, error_code "
                "FROM shadow_briefing_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if existing is not None:
                persisted = tuple(existing)
                requested = (
                    lower_z, upper_z, started_z, started_z, "RUNNING", None
                )
                if persisted != requested:
                    self._rollback_silent()
                    raise RunConflictError(
                        f"begin_run {run_id!r} conflicts with persisted fields: "
                        f"persisted={persisted!r} requested={requested!r}"
                    )
                # Idempotent no-op: leave updated_at_utc untouched.
                self._commit()
                return

            try:
                self._connection.execute(
                    "INSERT INTO shadow_briefing_runs("
                    "  run_id, window_lower_utc, window_upper_utc, "
                    "  started_at_utc, updated_at_utc, status"
                    ") VALUES (?, ?, ?, ?, ?, 'RUNNING')",
                    (run_id, lower_z, upper_z, started_z, started_z),
                )
            except sqlite3.IntegrityError as exc:
                self._rollback_silent()
                raise RunConflictError(
                    f"begin_run {run_id!r} raced: {exc!s}"
                ) from exc
            self._commit()
        except LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            if _is_busy_or_locked(exc):
                self._rollback_silent()
                raise LockTimeoutError(
                    f"begin_run {run_id!r} lock conflict: {exc!s}"
                ) from exc
            self._rollback_silent()
            raise

    # ----- seen_candidate_ids ------------------------------------------

    def seen_candidate_ids(self, ids: Sequence[str]) -> Tuple[str, ...]:
        """Return the subset of ``ids`` that exist in ``shadow_briefing_seen``.

        Output order matches input order. ``ids`` must be an exact
        ``tuple``; list / generator inputs are rejected. Duplicate
        candidate_ids inside ``ids`` raise ``LedgerContractError``
        (the addendum removes the silent-dedup ambiguity).
        """
        self._require_initialized()
        if not isinstance(ids, tuple):
            raise LedgerContractError(
                f"seen_candidate_ids requires an exact tuple input, got "
                f"{type(ids).__name__}"
            )
        seen_local: set = set()
        for cid in ids:
            _check_candidate_id(cid)
            if cid in seen_local:
                raise LedgerContractError(
                    f"seen_candidate_ids received duplicate input: {cid!r}"
                )
            seen_local.add(cid)
        if not ids:
            return ()
        placeholders = ", ".join("?" for _ in ids)
        rows = self._connection.execute(
            f"SELECT candidate_id FROM shadow_briefing_seen "
            f"WHERE candidate_id IN ({placeholders})",
            list(ids),
        ).fetchall()
        present = {r[0] for r in rows}
        # Return in the exact input order; duplicates were rejected up front.
        return tuple(cid for cid in ids if cid in present)

    # ----- complete_run -------------------------------------------------

    def complete_run(
        self,
        run_id: str,
        updated: str,
        events: Sequence[ShadowEvent],
    ) -> CompleteRunResult:
        """Atomically insert seen + run_events + COMPLETED status.

        ``run_id`` is validated 1..256. ``updated`` must be an aware
        UTC ISO string normalized to ``Z`` form. ``events`` is a
        list/tuple of ``ShadowEvent`` instances; the ledger assigns
        ``event_ordinal`` from the event-list input position (no
        payload-ordinal parsing). Each supplied ``ShadowEvent`` must
        carry a unique candidate_id; duplicate inputs raise
        ``LedgerContractError`` before the writer transaction opens.

        Cross-run ``ALREADY_SEEN`` events persist THIS run's canonical
        payload in ``shadow_briefing_run_events.payload_json``; the
        first-seen ``shadow_briefing_seen.payload_json`` is never
        overwritten. A retry of a COMPLETED run must supply the exact
        ordered candidate IDs and the exact canonical payload bytes;
        any difference raises ``RunConflictError``.
        """
        run_id = _validate_run_id(run_id, "complete_run.run_id")
        updated_z = _validate_timestamp(updated, "complete_run.updated")
        if not isinstance(events, (list, tuple)):
            raise LedgerContractError(
                f"complete_run.events must be a list/tuple, got "
                f"{type(events).__name__}"
            )

        self._require_initialized()

        # --- pre-flight: validate ShadowEvent types & unique IDs ----
        ordered_ids: List[str] = []
        ids_seen: set = set()
        canonical_payloads: List[bytes] = []
        recorded_at_z_by_idx: List[str] = []
        for index, ev in enumerate(events):
            if not isinstance(ev, ShadowEvent):
                raise LedgerContractError(
                    f"complete_run.events[{index}] must be ShadowEvent, "
                    f"got {type(ev).__name__}"
                )
            cid = ev.candidate_id
            if cid in ids_seen:
                raise LedgerContractError(
                    f"complete_run.events contains duplicate candidate_id: "
                    f"{cid!r} at index {index}"
                )
            ids_seen.add(cid)
            ordered_ids.append(cid)
            try:
                payload_canonical = normalize_canonical_payload(ev.payload_json)
            except PayloadValidationError as exc:
                raise LedgerContractError(
                    f"complete_run.events[{index}].payload_json rejected: "
                    f"{exc!s}"
                ) from exc
            canonical_payloads.append(payload_canonical)
            recorded_at_z_by_idx.append(ev.recorded_at_utc)

        self._begin_immediate()
        try:
            run_row = self._connection.execute(
                "SELECT status FROM shadow_briefing_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run_row is None:
                self._rollback_silent()
                raise RunConflictError(
                    f"complete_run {run_id!r} called before begin_run"
                )

            if run_row[0] == "COMPLETED":
                # Idempotent identical retry: compare the supplied event
                # sequence against this run's persisted run_events rows
                # ordered by event_ordinal ascending. Any difference in
                # candidate IDs, order, or canonical payload bytes
                # raises RunConflictError.
                persisted_rows = self._connection.execute(
                    "SELECT event_ordinal, candidate_id, payload_json "
                    "FROM shadow_briefing_run_events WHERE run_id = ? "
                    "ORDER BY event_ordinal",
                    (run_id,),
                ).fetchall()
                persisted_by_ord: Dict[int, Tuple[str, bytes]] = {}
                for ord_p, cid_p, pl_p in persisted_rows:
                    if isinstance(pl_p, str):
                        pl_bytes = pl_p.encode("utf-8")
                    elif isinstance(pl_p, (bytes, bytearray)):
                        pl_bytes = bytes(pl_p)
                    else:
                        raise LedgerContractError(
                            f"persisted payload_json has unexpected type: "
                            f"{type(pl_p).__name__}"
                        )
                    persisted_by_ord[int(ord_p)] = (cid_p, pl_bytes)

                if len(persisted_by_ord) != len(canonical_payloads):
                    self._rollback_silent()
                    raise RunConflictError(
                        f"complete_run {run_id!r} retry event count "
                        f"differs from persisted: requested="
                        f"{len(canonical_payloads)} persisted="
                        f"{len(persisted_by_ord)}"
                    )
                for ord_, cid_r, pl_r in zip(
                    range(len(canonical_payloads)),
                    ordered_ids,
                    canonical_payloads,
                ):
                    persisted = persisted_by_ord.get(ord_)
                    if persisted is None:
                        self._rollback_silent()
                        raise RunConflictError(
                            f"complete_run {run_id!r} retry is missing a "
                            f"persisted event_ordinal {ord_}"
                        )
                    cid_p, pl_p = persisted
                    if cid_r != cid_p or pl_r != pl_p:
                        self._rollback_silent()
                        raise RunConflictError(
                            f"complete_run {run_id!r} retry differs from "
                            f"persisted run_events at event_ordinal {ord_}"
                        )

                ev_by_cid = {
                    cid: status
                    for cid, status in self._connection.execute(
                        "SELECT candidate_id, event_status FROM "
                        "shadow_briefing_run_events WHERE run_id = ?",
                        (run_id,),
                    ).fetchall()
                }
                new_ids = tuple(
                    cid for cid in ordered_ids
                    if ev_by_cid.get(cid) == "RECORDED"
                )
                already_seen_ids = tuple(
                    cid for cid in ordered_ids
                    if ev_by_cid.get(cid) == "ALREADY_SEEN"
                )
                self._commit()
                return CompleteRunResult(
                    new_ids=new_ids,
                    already_seen_ids=already_seen_ids,
                )

            if run_row[0] != "RUNNING":
                self._rollback_silent()
                raise RunStateError(
                    f"complete_run {run_id!r} requires RUNNING status, "
                    f"got {run_row[0]!r}"
                )

            # Normal path: insert seen (OR IGNORE), record this run's
            # canonical payload in run_events, flip status to COMPLETED
            # atomically.
            new_ids_list: List[str] = []
            already_seen_ids_list: List[str] = []
            recorded_at_z = next(iter(recorded_at_z_by_idx), None) if recorded_at_z_by_idx else None
            if recorded_at_z is None:
                recorded_at_z = updated_z
            for idx, ev in enumerate(events):
                cid = ev.candidate_id
                payload_str = canonical_payloads[idx].decode("utf-8")
                ev_recorded_at_z = recorded_at_z_by_idx[idx]
                try:
                    self._connection.execute(
                        "INSERT OR IGNORE INTO shadow_briefing_seen("
                        "  candidate_id, first_run_id, first_seen_at_utc, "
                        "  category, decision, payload_json"
                        ") VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            cid,
                            run_id,
                            ev_recorded_at_z,
                            ev.category.value,
                            ev.decision.value,
                            payload_str,
                        ),
                    )
                    changes = self._connection.execute(
                        "SELECT changes()"
                    ).fetchone()[0]
                except sqlite3.IntegrityError as exc:
                    self._rollback_silent()
                    raise LedgerContractError(
                        f"shadow_briefing_seen insert failed for "
                        f"{cid!r}: {exc!s}"
                    ) from exc

                if changes > 0:
                    new_ids_list.append(cid)
                    event_status = "RECORDED"
                else:
                    # ALREADY_SEEN: do NOT compare ALREADY_SEEN's
                    # payload against shadow_briefing_seen; the addendum
                    # requires THIS run's canonical payload to be
                    # persisted in shadow_briefing_run_events. The
                    # first-seen row in shadow_briefing_seen stays
                    # untouched (a later cross-run may persist a
                    # different payload for the same candidate_id).
                    already_seen_ids_list.append(cid)
                    event_status = "ALREADY_SEEN"

                # Persist this run's canonical payload in run_events.
                try:
                    self._connection.execute(
                        "INSERT INTO shadow_briefing_run_events("
                        "  run_id, candidate_id, event_status, "
                        "  payload_json, event_ordinal"
                        ") VALUES (?, ?, ?, ?, ?)",
                        (run_id, cid, event_status, payload_str, idx),
                    )
                except sqlite3.IntegrityError as exc:
                    self._rollback_silent()
                    raise LedgerContractError(
                        f"shadow_briefing_run_events insert failed for "
                        f"{cid!r} ord={idx}: {exc!s}"
                    ) from exc

            try:
                self._connection.execute(
                    "UPDATE shadow_briefing_runs "
                    "SET status='COMPLETED', updated_at_utc=?, error_code=NULL "
                    "WHERE run_id=? AND status='RUNNING'",
                    (updated_z, run_id),
                )
            except sqlite3.IntegrityError as exc:
                self._rollback_silent()
                raise LedgerContractError(
                    f"complete_run status flip failed for {run_id!r}: {exc!s}"
                ) from exc

            self._commit()
            return CompleteRunResult(
                new_ids=tuple(new_ids_list),
                already_seen_ids=tuple(already_seen_ids_list),
            )
        except LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            if _is_busy_or_locked(exc):
                self._rollback_silent()
                raise LockTimeoutError(
                    f"complete_run {run_id!r} lock conflict: {exc!s}"
                ) from exc
            self._rollback_silent()
            raise

    # ----- fail_run -----------------------------------------------------

    def fail_run(self, run_id: str, updated: str, error_code: str) -> None:
        """Atomically mark a RUNNING run FAILED; no seen/event rows added.

        ``error_code`` is validated against the conservative pattern
        ``[A-Z0-9_.-]{1,128}``; ``updated`` is normalized to ``Z``.
        """
        run_id = _validate_run_id(run_id, "fail_run.run_id")
        updated_z = _validate_timestamp(updated, "fail_run.updated")
        error_code = _validate_error_code(error_code)
        self._require_initialized()
        self._begin_immediate()
        try:
            run_row = self._connection.execute(
                "SELECT status FROM shadow_briefing_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if run_row is None:
                self._rollback_silent()
                raise RunConflictError(
                    f"fail_run {run_id!r} called before begin_run"
                )
            if run_row[0] != "RUNNING":
                self._rollback_silent()
                raise RunStateError(
                    f"fail_run {run_id!r} requires RUNNING status, got "
                    f"{run_row[0]!r}"
                )
            try:
                self._connection.execute(
                    "UPDATE shadow_briefing_runs "
                    "SET status='FAILED', error_code=?, updated_at_utc=? "
                    "WHERE run_id=? AND status='RUNNING'",
                    (error_code, updated_z, run_id),
                )
            except sqlite3.IntegrityError as exc:
                self._rollback_silent()
                raise LedgerContractError(
                    f"fail_run update failed for {run_id!r}: {exc!s}"
                ) from exc
            self._commit()
        except LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            if _is_busy_or_locked(exc):
                self._rollback_silent()
                raise LockTimeoutError(
                    f"fail_run {run_id!r} lock conflict: {exc!s}"
                ) from exc
            self._rollback_silent()
            raise

    # ----- recover_stale_runs ------------------------------------------

    def recover_stale_runs(
        self,
        cutoff: datetime,
        updated: str,
    ) -> Tuple[str, ...]:
        """Flip stale RUNNING rows to STALE; return run_ids in run_id order.

        ``cutoff`` must be an aware zero-offset UTC ``datetime``; the
        ledger normalizes it to a ``Z`` ISO string before any SQL
        comparison so lexical string ordering matches semantically-
        aware ordering. ``updated`` is normalized to ``Z``.
        """
        cutoff_aware = _aware_utc(cutoff)
        cutoff_iso = _aware_utc_to_z(cutoff_aware)
        updated_z = _validate_timestamp(updated, "recover_stale_runs.updated")
        self._require_initialized()
        self._begin_immediate()
        try:
            updated_rows = self._connection.execute(
                "UPDATE shadow_briefing_runs "
                "SET status='STALE', updated_at_utc=? "
                "WHERE status='RUNNING' AND updated_at_utc < ? "
                "RETURNING run_id",
                (updated_z, cutoff_iso),
            ).fetchall()
            run_ids = sorted(r[0] for r in updated_rows)
            self._commit()
            return tuple(run_ids)
        except LedgerError:
            raise
        except sqlite3.OperationalError as exc:
            if _is_busy_or_locked(exc):
                self._rollback_silent()
                raise LockTimeoutError(
                    f"recover_stale_runs lock conflict: {exc!s}"
                ) from exc
            self._rollback_silent()
            raise

    # ----- last_completed_upper_utc ------------------------------------

    def last_completed_upper_utc(self) -> Optional[datetime]:
        """Latest ``window_upper_utc`` among COMPLETED runs, parsed aware UTC.

        Returns ``None`` when no COMPLETED rows exist. Output is parsed
        via ``validate_utc_iso`` and then ``datetime.fromisoformat`` so
        callers receive an aware ``datetime`` with tzinfo=UTC.
        """
        self._require_initialized()
        row = self._connection.execute(
            "SELECT window_upper_utc FROM shadow_briefing_runs "
            "WHERE status='COMPLETED' "
            "ORDER BY window_upper_utc DESC, run_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        normalized = validate_utc_iso(row[0])
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        return datetime.fromisoformat(normalized)


# ---------------------------------------------------------------------------
# Re-export the public surface that tests import.
# ---------------------------------------------------------------------------

__all__ = (
    # errors
    "LedgerError",
    "LedgerConnectionError",
    "LedgerContractError",
    "LedgerStateError",
    "PayloadValidationError",
    "LockTimeoutError",
    "RunConflictError",
    "RunStateError",
    # frozen/slotted
    "ShadowEvent",
    "CompleteRunResult",
    # ledger
    "BriefingLedger",
    # payload utility
    "normalize_canonical_payload",
)
