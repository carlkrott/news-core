"""Slice 6.2 — immutable recovery evaluation."""
from __future__ import annotations

import dataclasses
import hashlib
import json
import re
import sqlite3
from typing import Final

from .time_inputs import (
    ExternalContext,
    validate_external_context,
    validate_iso8601_utc,
)
from .types import (
    Phase6ConfigurationError,
    Phase6IdentityError,
    Phase6SandboxError,
    Phase6SchemaError,
)


class RecoveryEvaluationError(Phase6SandboxError):
    """Base class for every Slice 6.2 failure mode."""


class RecoveryIdentityError(RecoveryEvaluationError, Phase6IdentityError):
    """A caller-supplied job input failed field-level identity validation."""


class RecoveryConsistencyError(RecoveryEvaluationError):
    """Caller-supplied age is inconsistent with reference/evaluation times."""


class RecoveryBatchError(RecoveryEvaluationError):
    """The batch itself is malformed: empty, oversized, or has duplicate job_ids."""


class RecoveryDuplicateError(RecoveryEvaluationError):
    """A job_id in the batch already exists in evaluation_inputs."""


class RecoverySchemaError(RecoveryEvaluationError, Phase6SchemaError):
    """The open session DB does not match the V1 contract Slice 6.2 expects."""


JOB_ID_PATTERN: Final[str] = r"^[A-Za-z0-9._-]{1,128}$"
_JOB_ID_RE: Final[re.Pattern[str]] = re.compile(JOB_ID_PATTERN)

SOURCE_STATE_PATTERN: Final[str] = r"^[A-Z0-9_:-]{1,64}$"
_SOURCE_STATE_RE: Final[re.Pattern[str]] = re.compile(SOURCE_STATE_PATTERN)

_INT64_MAX: Final[int] = (2 ** 63) - 1
MAX_BATCH_SIZE: Final[int] = 1024
_MAX_JOB_ID_LEN: Final[int] = 128
_MAX_SOURCE_STATE_LEN: Final[int] = 64
_MAX_TIMESTAMP_LEN: Final[int] = 32
_MAX_CANONICAL_INPUT_JSON_LEN: Final[int] = 1024
_BAND_NO_OBSERVATION_UPPER_EXCLUSIVE: Final[int] = 300
_BAND_EXPIRED_UPPER_EXCLUSIVE: Final[int] = 600

RESULT_NO_OBSERVATION: Final[str] = "NO_OBSERVATION"
RESULT_EXPIRED: Final[str] = "EXPIRED"
RESULT_STALE: Final[str] = "STALE"


@dataclasses.dataclass(frozen=True)
class JobInput:
    job_id: str
    reference_utc: str
    evaluation_utc: str
    age_seconds: int
    source_state: str


@dataclasses.dataclass(frozen=True)
class EvaluationResult:
    job_id: str
    result: str
    age_band: str
    age_seconds: int
    reference_utc: str
    evaluation_utc: str
    source_state: str
    input_sha256: str
    persisted: bool


@dataclasses.dataclass(frozen=True)
class BatchEvaluationOutcome:
    results: tuple[EvaluationResult, ...]


def _is_leap_year(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _days_in_month(year: int, month: int) -> int:
    if month in (1, 3, 5, 7, 8, 10, 12):
        return 31
    if month in (4, 6, 9, 11):
        return 30
    return 29 if _is_leap_year(year) else 28


def _seconds_from_utc_calendar(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> int:
    if not (1 <= month <= 12):
        raise RecoveryIdentityError("recovery: month out of range")
    if not (1 <= day <= _days_in_month(year, month)):
        raise RecoveryIdentityError("recovery: day out of range")
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 60):
        raise RecoveryIdentityError("recovery: time component out of range")
    y = year - (1 if month <= 2 else 0)
    era = y // 400 if y >= 0 else (y - 399) // 400
    yoe = y - era * 400
    m = month + (9 if month <= 2 else -3)
    doy = (153 * m + 2) // 5 + day - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    days = era * 146097 + doe - 719468
    return ((days * 24 + hour) * 60 + minute) * 60 + second


def _parse_canonical_utc_to_seconds(value: str) -> int:
    try:
        validate_iso8601_utc(value)
    except Phase6IdentityError as exc:
        raise RecoveryIdentityError("recovery: invalid utc timestamp") from exc
    head = value[:-1]
    if "." in head:
        main, _frac = head.split(".", 1)
    else:
        main = head
    y = int(main[0:4])
    mo = int(main[5:7])
    d = int(main[8:10])
    h = int(main[11:13])
    mi = int(main[14:16])
    s = int(main[17:19])
    return _seconds_from_utc_calendar(y, mo, d, h, mi, s)


def _check_job_id(value: object) -> str:
    if not isinstance(value, str):
        raise RecoveryIdentityError(
            f"recovery: job_id must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise RecoveryIdentityError("recovery: job_id must not be empty")
    if len(value) > _MAX_JOB_ID_LEN:
        raise RecoveryIdentityError(
            f"recovery: job_id length must be <= {_MAX_JOB_ID_LEN}"
        )
    if not _JOB_ID_RE.match(value):
        raise RecoveryIdentityError(
            "recovery: job_id must match [A-Za-z0-9._-]{1,128}"
        )
    return value


def _check_source_state(value: object) -> str:
    if not isinstance(value, str):
        raise RecoveryIdentityError(
            f"recovery: source_state must be str, got {type(value).__name__}"
        )
    if len(value) == 0:
        raise RecoveryIdentityError("recovery: source_state must not be empty")
    if len(value) > _MAX_SOURCE_STATE_LEN:
        raise RecoveryIdentityError(
            f"recovery: source_state length must be <= {_MAX_SOURCE_STATE_LEN}"
        )
    if not _SOURCE_STATE_RE.match(value):
        raise RecoveryIdentityError(
            "recovery: source_state must match [A-Z0-9_:-]{1,64}"
        )
    return value


def _check_age(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise RecoveryIdentityError(
            f"recovery: age_seconds must be int, got {type(value).__name__}"
        )
    if value < 0:
        raise RecoveryConsistencyError(
            "recovery: age_seconds must be >= 0 (negative not allowed)"
        )
    if value > _INT64_MAX:
        raise RecoveryIdentityError(
            f"recovery: age_seconds must be <= {_INT64_MAX}"
        )
    return value


def _check_timestamp_field(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise RecoveryIdentityError(
            f"recovery: {name} must be str, got {type(value).__name__}"
        )
    if len(value) > _MAX_TIMESTAMP_LEN:
        raise RecoveryIdentityError(
            f"recovery: {name} length must be <= {_MAX_TIMESTAMP_LEN}"
        )
    return value


def validate_job_input(job: JobInput) -> JobInput:
    job_id = _check_job_id(job.job_id)
    reference_utc = _check_timestamp_field("reference_utc", job.reference_utc)
    evaluation_utc = _check_timestamp_field("evaluation_utc", job.evaluation_utc)
    age_seconds = _check_age(job.age_seconds)
    source_state = _check_source_state(job.source_state)

    reference_seconds = _parse_canonical_utc_to_seconds(reference_utc)
    evaluation_seconds = _parse_canonical_utc_to_seconds(evaluation_utc)

    if reference_seconds > evaluation_seconds:
        raise RecoveryConsistencyError(
            "recovery: reference_utc must be <= evaluation_utc"
        )

    derived_age = evaluation_seconds - reference_seconds
    if derived_age != age_seconds:
        raise RecoveryConsistencyError(
            "recovery: age_seconds must equal evaluation_utc - reference_utc"
        )

    return JobInput(
        job_id=job_id,
        reference_utc=reference_utc,
        evaluation_utc=evaluation_utc,
        age_seconds=age_seconds,
        source_state=source_state,
    )


def canonical_input_payload(job: JobInput) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "reference_utc": job.reference_utc,
        "evaluation_utc": job.evaluation_utc,
        "age_seconds": job.age_seconds,
        "source_state": job.source_state,
    }


def canonical_input_json(job: JobInput) -> str:
    return json.dumps(
        canonical_input_payload(job),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def input_sha256(canonical_json: str) -> str:
    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()


def _band_for_age(age_seconds: int) -> tuple[str, str]:
    if age_seconds < _BAND_NO_OBSERVATION_UPPER_EXCLUSIVE:
        return "BAND_NO_OBSERVATION", RESULT_NO_OBSERVATION
    if age_seconds < _BAND_EXPIRED_UPPER_EXCLUSIVE:
        return "BAND_EXPIRED", RESULT_EXPIRED
    return "BAND_STALE", RESULT_STALE


def _verify_session_db(conn: sqlite3.Connection) -> None:
    try:
        row = conn.execute("PRAGMA user_version").fetchone()
    except sqlite3.Error as exc:
        raise RecoverySchemaError("recovery: cannot read user_version") from exc
    user_version = int(row[0]) if row else 0
    if user_version != 1:
        raise RecoverySchemaError(
            f"recovery: open session DB user_version must be 1, got {user_version}"
        )
    try:
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(evaluation_inputs)").fetchall()
        }
    except sqlite3.Error as exc:
        raise RecoverySchemaError(
            "recovery: evaluation_inputs table is missing"
        ) from exc
    required = {
        "job_id", "reference_utc", "age_seconds", "source_state",
        "canonical_input_json", "input_sha256",
    }
    missing = required - cols
    if missing:
        raise RecoverySchemaError(
            f"recovery: evaluation_inputs is missing required columns: {sorted(missing)}"
        )


def evaluate_batch(
    conn: sqlite3.Connection,
    context: ExternalContext,
    inputs: list[JobInput] | tuple[JobInput, ...],
) -> BatchEvaluationOutcome:
    if not isinstance(conn, sqlite3.Connection):
        raise Phase6ConfigurationError(
            "recovery: conn must be a sqlite3.Connection"
        )
    if not isinstance(context, ExternalContext):
        raise Phase6ConfigurationError(
            "recovery: context must be an ExternalContext"
        )
    if isinstance(inputs, (str, bytes)) or not isinstance(inputs, (list, tuple)):
        raise Phase6ConfigurationError(
            "recovery: inputs must be a list or tuple of JobInput"
        )
    if len(inputs) == 0:
        raise RecoveryBatchError("recovery: inputs batch must be non-empty")
    if len(inputs) > MAX_BATCH_SIZE:
        raise RecoveryBatchError(
            f"recovery: inputs batch must be <= {MAX_BATCH_SIZE}"
        )

    seen: set[str] = set()
    for job in inputs:
        if not isinstance(job, JobInput):
            raise Phase6ConfigurationError(
                "recovery: inputs element is not a JobInput"
            )
        if job.job_id in seen:
            raise RecoveryBatchError("recovery: duplicate job_id within batch")
        seen.add(job.job_id)

    validated: list[JobInput] = []
    for job in inputs:
        validated.append(validate_job_input(job))

    try:
        validate_external_context(context)
    except Phase6IdentityError as exc:
        raise RecoveryIdentityError(
            "recovery: ExternalContext time identity is invalid"
        ) from exc

    _verify_session_db(conn)

    placeholders = ",".join("?" for _ in validated)
    existing_rows = conn.execute(
        f"SELECT job_id FROM evaluation_inputs WHERE job_id IN ({placeholders})",
        tuple(j.job_id for j in validated),
    ).fetchall()
    if existing_rows:
        raise RecoveryDuplicateError(
            "recovery: job_id already exists in evaluation_inputs"
        )

    decisions: list[tuple[JobInput, str, str, str, bool]] = []
    for job in validated:
        band_label, result = _band_for_age(job.age_seconds)
        canonical = canonical_input_json(job)
        if len(canonical) > _MAX_CANONICAL_INPUT_JSON_LEN:
            raise RecoveryIdentityError(
                "recovery: canonical_input_json length exceeded"
            )
        sha = input_sha256(canonical)
        will_persist = result != RESULT_NO_OBSERVATION
        decisions.append((job, band_label, result, sha, will_persist))

    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.Error as exc:
        raise RecoverySchemaError(
            "recovery: cannot BEGIN IMMEDIATE on session DB"
        ) from exc

    try:
        insert_sql = (
            "INSERT INTO evaluation_inputs"
            "(job_id, reference_utc, age_seconds, source_state, "
            " canonical_input_json, input_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        for job, _band, _result, sha, will_persist in decisions:
            if not will_persist:
                continue
            try:
                conn.execute(
                    insert_sql,
                    (
                        job.job_id,
                        job.reference_utc,
                        job.age_seconds,
                        job.source_state,
                        canonical_input_json(job),
                        sha,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise RecoveryDuplicateError(
                    "recovery: job_id already exists in evaluation_inputs"
                ) from exc
        conn.execute("COMMIT")
    except BaseException:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass
        raise

    results: list[EvaluationResult] = []
    for job, band_label, result, sha, will_persist in decisions:
        results.append(
            EvaluationResult(
                job_id=job.job_id,
                result=result,
                age_band=band_label,
                age_seconds=job.age_seconds,
                reference_utc=job.reference_utc,
                evaluation_utc=job.evaluation_utc,
                source_state=job.source_state,
                input_sha256=sha if will_persist else "",
                persisted=will_persist,
            )
        )

    return BatchEvaluationOutcome(results=tuple(results))


__all__ = [
    "RecoveryEvaluationError",
    "RecoveryIdentityError",
    "RecoveryConsistencyError",
    "RecoveryBatchError",
    "RecoveryDuplicateError",
    "RecoverySchemaError",
    "JOB_ID_PATTERN",
    "SOURCE_STATE_PATTERN",
    "MAX_BATCH_SIZE",
    "RESULT_NO_OBSERVATION",
    "RESULT_EXPIRED",
    "RESULT_STALE",
    "JobInput",
    "EvaluationResult",
    "BatchEvaluationOutcome",
    "canonical_input_payload",
    "canonical_input_json",
    "input_sha256",
    "validate_job_input",
    "evaluate_batch",
]
