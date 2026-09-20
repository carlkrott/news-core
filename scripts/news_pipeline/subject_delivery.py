"""Run 8 subject-scoped delivery outbox and receipt state machine.

This module is deliberately DB-only. Report-generation jobs may prepare an
outbox row, while a private operator adapter owns any external send and calls
``start_subject_delivery`` / ``complete_subject_delivery`` around that send.
No network or credential-loading code belongs here.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .delivery import DeliveryAmbiguous, DeliveryConflict, DeliveryRejected
from .models import Subject
from .schema_v9 import validate_v9

_HASH_RE = re.compile(r"[0-9a-f]{64}\Z")
_CHANNEL_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,31}\Z")
_ERROR_CODE_RE = re.compile(r"[a-z0-9][a-z0-9_]{0,63}\Z")
_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
_OUTBOX_STATES = frozenset({"prepared", "sent", "failed", "ambiguous", "skipped"})
_ATTEMPT_STATES = frozenset({"prepared", "sent", "failed", "ambiguous"})


@dataclass(frozen=True, slots=True)
class SubjectReportRecord:
    subject_report_id: str
    parent_report_id: str
    subject_id: str
    revision: int
    content_sha256: str
    story_count: int
    created_at: str


@dataclass(frozen=True, slots=True)
class SubjectOutboxRecord:
    subject_report_id: str
    idempotency_key: str
    channel: str | None
    recipient_hash: str | None
    content_sha256: str
    state: str
    current_attempt_id: str | None
    created_at: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class SubjectAttemptRecord:
    attempt_id: str
    subject_report_id: str
    ordinal: int
    state: str
    recipient_hash: str
    content_sha256: str
    prepared_at: str
    completed_at: str | None
    message_ids: tuple[str, ...]
    error_code: str | None
    error_detail: str | None


@dataclass(frozen=True, slots=True)
class SubjectDeliverySnapshot:
    report: SubjectReportRecord
    outbox: SubjectOutboxRecord
    attempt: SubjectAttemptRecord | None


def _subject(value: Subject | str) -> Subject:
    if isinstance(value, Subject):
        return value
    try:
        return Subject(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unknown subject: {value!r}") from exc


def _nonempty(value: object, name: str, *, maximum: int = 256) -> str:
    if (
        type(value) is not str
        or not value.strip()
        or len(value) > maximum
        or _CONTROL_RE.search(value)
    ):
        raise ValueError(f"{name} must be a non-empty clean string")
    return value


def _hash(value: object, name: str) -> str:
    if type(value) is not str or _HASH_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return value


def _timestamp(value: object, name: str) -> str:
    if type(value) is not str or not value.endswith("Z"):
        raise ValueError(f"{name} must be a canonical UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{name} must be a valid UTC timestamp") from exc
    if parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be UTC")
    canonical = parsed.isoformat(
        timespec="microseconds" if parsed.microsecond else "seconds"
    ).replace("+00:00", "Z")
    if canonical != value:
        raise ValueError(f"{name} must use canonical UTC Z form")
    return value


def _connection(connection: sqlite3.Connection, *, write: bool = False) -> None:
    if not isinstance(connection, sqlite3.Connection):
        raise TypeError("connection must be sqlite3.Connection")
    if write and connection.in_transaction:
        raise ValueError("subject delivery writes require no active transaction")
    validate_v9(connection)


def subject_report_id(
    parent_report_id: str,
    subject: Subject | str,
    content_sha256: str,
) -> str:
    parent = _nonempty(parent_report_id, "parent_report_id")
    normalized = _subject(subject)
    content_hash = _hash(content_sha256, "content_sha256")
    digest = hashlib.sha256(
        (
            "subject-report-v1\0"
            + parent
            + "\0"
            + normalized.value
            + "\0"
            + content_hash
        ).encode("utf-8")
    ).hexdigest()
    return "subject-report-" + digest


def subject_idempotency_key(
    subject: Subject | str,
    report_id: str,
    content_sha256: str,
) -> str:
    normalized = _subject(subject)
    report = _nonempty(report_id, "subject_report_id")
    content_hash = _hash(content_sha256, "content_sha256")
    return hashlib.sha256(
        (
            "subject-delivery-v1\0"
            + normalized.value
            + "\0"
            + report
            + "\0"
            + content_hash
        ).encode("utf-8")
    ).hexdigest()


def _message_ids(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if type(value) is not str:
        raise DeliveryConflict("message_ids_json is not text")
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise DeliveryConflict("message_ids_json is invalid") from exc
    if type(decoded) is not list:
        raise DeliveryConflict("message_ids_json must decode to a list")
    result = tuple(decoded)
    if any(
        type(item) is not str
        or not item
        or len(item) > 256
        or _CONTROL_RE.search(item)
        for item in result
    ):
        raise DeliveryConflict("message identifiers are malformed")
    return result


def _report_record(row: tuple[Any, ...]) -> SubjectReportRecord:
    if len(row) != 7:
        raise DeliveryConflict("subject report row shape is incompatible")
    report_id, parent_id, subject_id, revision, content_hash, story_count, created_at = row
    parent = _nonempty(parent_id, "parent_report_id")
    subject = _subject(subject_id)
    digest = _hash(content_hash, "content_sha256")
    if type(revision) is not int or isinstance(revision, bool) or revision < 1:
        raise DeliveryConflict("subject report revision is invalid")
    if type(story_count) is not int or isinstance(story_count, bool) or story_count < 0:
        raise DeliveryConflict("subject report story_count is invalid")
    created = _timestamp(created_at, "created_at")
    expected = subject_report_id(parent, subject, digest)
    if report_id != expected:
        raise DeliveryConflict("subject report identity is inconsistent")
    return SubjectReportRecord(
        report_id, parent, subject.value, revision, digest, story_count, created
    )


def _outbox_record(
    row: tuple[Any, ...], report: SubjectReportRecord
) -> SubjectOutboxRecord:
    if len(row) != 9:
        raise DeliveryConflict("subject outbox row shape is incompatible")
    (
        report_id,
        key,
        channel,
        recipient_hash,
        content_hash,
        state,
        attempt_id,
        created_at,
        updated_at,
    ) = row
    if report_id != report.subject_report_id:
        raise DeliveryConflict("subject outbox report identity is inconsistent")
    expected_key = subject_idempotency_key(
        report.subject_id, report.subject_report_id, report.content_sha256
    )
    if key != expected_key:
        raise DeliveryConflict("subject outbox idempotency key is inconsistent")
    if content_hash != report.content_sha256:
        raise DeliveryConflict("subject outbox content hash is inconsistent")
    if state not in _OUTBOX_STATES:
        raise DeliveryConflict("subject outbox state is invalid")
    if channel is not None and (
        type(channel) is not str or _CHANNEL_RE.fullmatch(channel) is None
    ):
        raise DeliveryConflict("subject outbox channel is invalid")
    if recipient_hash is not None:
        _hash(recipient_hash, "recipient_hash")
    if attempt_id is not None:
        _nonempty(attempt_id, "current_attempt_id")
    return SubjectOutboxRecord(
        report_id,
        key,
        channel,
        recipient_hash,
        report.content_sha256,
        state,
        attempt_id,
        _timestamp(created_at, "created_at"),
        _timestamp(updated_at, "updated_at"),
    )


def _attempt_record(
    row: tuple[Any, ...], report: SubjectReportRecord
) -> SubjectAttemptRecord:
    if len(row) != 11:
        raise DeliveryConflict("subject delivery attempt row shape is incompatible")
    (
        attempt_id,
        report_id,
        ordinal,
        state,
        recipient_hash,
        content_hash,
        prepared_at,
        completed_at,
        message_ids_json,
        error_code,
        error_detail,
    ) = row
    _nonempty(attempt_id, "attempt_id")
    if report_id != report.subject_report_id:
        raise DeliveryConflict("subject attempt report identity is inconsistent")
    if type(ordinal) is not int or isinstance(ordinal, bool) or ordinal < 1:
        raise DeliveryConflict("subject attempt ordinal is invalid")
    if state not in _ATTEMPT_STATES:
        raise DeliveryConflict("subject attempt state is invalid")
    recipient = _hash(recipient_hash, "recipient_hash")
    if content_hash != report.content_sha256:
        raise DeliveryConflict("subject attempt content hash is inconsistent")
    completed = None if completed_at is None else _timestamp(completed_at, "completed_at")
    if error_code is not None and (
        type(error_code) is not str or _ERROR_CODE_RE.fullmatch(error_code) is None
    ):
        raise DeliveryConflict("subject attempt error_code is invalid")
    if error_detail is not None:
        _nonempty(error_detail, "error_detail")
    return SubjectAttemptRecord(
        attempt_id,
        report_id,
        ordinal,
        state,
        recipient,
        report.content_sha256,
        _timestamp(prepared_at, "prepared_at"),
        completed,
        _message_ids(message_ids_json),
        error_code,
        error_detail,
    )


def _load_snapshot(
    connection: sqlite3.Connection, report_id: str
) -> SubjectDeliverySnapshot:
    report_row = connection.execute(
        """SELECT subject_report_id,parent_report_id,subject_id,revision,
                  content_sha256,story_count,created_at
             FROM subject_reports WHERE subject_report_id=?""",
        (report_id,),
    ).fetchone()
    if report_row is None:
        raise DeliveryRejected("subject report does not exist")
    report = _report_record(tuple(report_row))
    outbox_row = connection.execute(
        """SELECT subject_report_id,idempotency_key,channel,recipient_hash,
                  content_sha256,state,current_attempt_id,created_at,updated_at
             FROM subject_delivery_outbox WHERE subject_report_id=?""",
        (report_id,),
    ).fetchone()
    if outbox_row is None:
        raise DeliveryConflict("subject report has no outbox row")
    outbox = _outbox_record(tuple(outbox_row), report)
    attempt: SubjectAttemptRecord | None = None
    if outbox.current_attempt_id is not None:
        attempt_row = connection.execute(
            """SELECT attempt_id,subject_report_id,ordinal,state,recipient_hash,
                      content_sha256,prepared_at,completed_at,message_ids_json,
                      error_code,error_detail
                 FROM subject_delivery_attempts WHERE attempt_id=?""",
            (outbox.current_attempt_id,),
        ).fetchone()
        if attempt_row is None:
            raise DeliveryConflict("subject outbox current attempt is missing")
        attempt = _attempt_record(tuple(attempt_row), report)
        if attempt.attempt_id != outbox.current_attempt_id:
            raise DeliveryConflict("subject outbox current attempt is inconsistent")
        if attempt.state != outbox.state:
            raise DeliveryConflict("subject outbox and attempt states differ")
        if attempt.recipient_hash != outbox.recipient_hash:
            raise DeliveryConflict("subject outbox and attempt recipient hashes differ")
    if report.story_count == 0:
        if outbox.state != "skipped" or attempt is not None:
            raise DeliveryConflict("zero-story subject report must be skipped")
    elif outbox.state == "skipped":
        raise DeliveryConflict("non-empty subject report cannot be skipped")
    if outbox.state in {"sent", "failed", "ambiguous"} and attempt is None:
        raise DeliveryConflict("terminal subject outbox state requires an attempt")
    if outbox.state == "prepared" and attempt is None:
        if outbox.channel is not None or outbox.recipient_hash is not None:
            raise DeliveryConflict("fresh prepared outbox cannot bind a recipient")
    if attempt is not None and (
        outbox.channel is None or outbox.recipient_hash is None
    ):
        raise DeliveryConflict("attempted subject delivery requires channel binding")
    return SubjectDeliverySnapshot(report, outbox, attempt)


def load_subject_delivery(
    connection: sqlite3.Connection, subject_report_id: str
) -> SubjectDeliverySnapshot:
    _connection(connection)
    report_id = _nonempty(subject_report_id, "subject_report_id")
    return _load_snapshot(connection, report_id)


def prepare_subject_report(
    connection: sqlite3.Connection,
    *,
    parent_report_id: str,
    subject: Subject | str,
    rendered_text: str,
    story_count: int,
    created_at: str,
) -> SubjectDeliverySnapshot:
    _connection(connection, write=True)
    parent = _nonempty(parent_report_id, "parent_report_id")
    normalized_subject = _subject(subject)
    if type(rendered_text) is not str:
        raise TypeError("rendered_text must be a string")
    if type(story_count) is not int or isinstance(story_count, bool) or story_count < 0:
        raise ValueError("story_count must be an integer >= 0")
    if story_count == 0 and rendered_text:
        raise ValueError("zero-story subject report must have empty rendered_text")
    if story_count > 0 and not rendered_text:
        raise ValueError("non-empty subject report requires rendered_text")
    created = _timestamp(created_at, "created_at")
    content_hash = hashlib.sha256(rendered_text.encode("utf-8")).hexdigest()
    report_id = subject_report_id(parent, normalized_subject, content_hash)
    key = subject_idempotency_key(normalized_subject, report_id, content_hash)
    parent_row = connection.execute(
        "SELECT generation_status FROM reports WHERE report_id=?", (parent,)
    ).fetchone()
    if parent_row is None or parent_row[0] != "complete":
        raise DeliveryRejected("parent report does not exist or is not complete")

    existing = connection.execute(
        """SELECT subject_report_id,story_count FROM subject_reports
             WHERE parent_report_id=? AND subject_id=? AND content_sha256=?""",
        (parent, normalized_subject.value, content_hash),
    ).fetchone()
    if existing is not None:
        if existing[0] != report_id or existing[1] != story_count:
            raise DeliveryConflict("existing subject report content identity conflicts")
        return _load_snapshot(connection, report_id)

    connection.execute("BEGIN IMMEDIATE")
    try:
        existing = connection.execute(
            """SELECT subject_report_id,story_count FROM subject_reports
                 WHERE parent_report_id=? AND subject_id=? AND content_sha256=?""",
            (parent, normalized_subject.value, content_hash),
        ).fetchone()
        if existing is not None:
            if existing[0] != report_id or existing[1] != story_count:
                raise DeliveryConflict("existing subject report content identity conflicts")
            connection.commit()
            return _load_snapshot(connection, report_id)
        revision = int(
            connection.execute(
                """SELECT COALESCE(MAX(revision),0)+1 FROM subject_reports
                     WHERE parent_report_id=? AND subject_id=?""",
                (parent, normalized_subject.value),
            ).fetchone()[0]
        )
        connection.execute(
            """INSERT INTO subject_reports(
                   subject_report_id,parent_report_id,subject_id,revision,
                   content_sha256,story_count,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (
                report_id,
                parent,
                normalized_subject.value,
                revision,
                content_hash,
                story_count,
                created,
            ),
        )
        state = "skipped" if story_count == 0 else "prepared"
        connection.execute(
            """INSERT INTO subject_delivery_outbox(
                   subject_report_id,idempotency_key,channel,recipient_hash,
                   content_sha256,state,current_attempt_id,created_at,updated_at)
               VALUES(?,?,NULL,NULL,?,?,NULL,?,?)""",
            (report_id, key, content_hash, state, created, created),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _load_snapshot(connection, report_id)


def _channel(value: object) -> str:
    if type(value) is not str or _CHANNEL_RE.fullmatch(value) is None:
        raise ValueError("channel must be a lowercase delivery identifier")
    return value


def _attempt_id(idempotency_key: str, ordinal: int) -> str:
    return hashlib.sha256(
        ("subject-attempt-v1\0" + idempotency_key + "\0" + str(ordinal)).encode(
            "utf-8"
        )
    ).hexdigest()


def start_subject_delivery(
    connection: sqlite3.Connection,
    *,
    subject_report_id: str,
    recipient_hash: str,
    channel: str,
    prepared_at: str,
    retry_failed: bool = False,
) -> SubjectDeliverySnapshot:
    _connection(connection, write=True)
    report_id = _nonempty(subject_report_id, "subject_report_id")
    recipient = _hash(recipient_hash, "recipient_hash")
    delivery_channel = _channel(channel)
    prepared = _timestamp(prepared_at, "prepared_at")
    if type(retry_failed) is not bool:
        raise TypeError("retry_failed must be a bool")

    initial = _load_snapshot(connection, report_id)
    if initial.outbox.state in {"sent", "skipped"}:
        return initial
    if initial.outbox.state == "ambiguous":
        raise DeliveryAmbiguous("ambiguous subject delivery blocks automatic retry")
    if initial.outbox.state == "prepared" and initial.attempt is not None:
        raise DeliveryAmbiguous("unresolved prepared subject attempt blocks replay")
    if initial.outbox.state == "failed" and not retry_failed:
        raise DeliveryRejected("failed subject delivery requires explicit retry_failed")
    if initial.outbox.channel is not None and initial.outbox.channel != delivery_channel:
        raise DeliveryConflict("channel does not match durable subject outbox")
    if (
        initial.outbox.recipient_hash is not None
        and initial.outbox.recipient_hash != recipient
    ):
        raise DeliveryConflict("recipient hash does not match durable subject outbox")

    connection.execute("BEGIN IMMEDIATE")
    try:
        current = _load_snapshot(connection, report_id)
        if current != initial:
            raise DeliveryConflict("subject outbox changed before attempt preparation")
        ordinal = int(
            connection.execute(
                """SELECT COALESCE(MAX(ordinal),0)+1
                     FROM subject_delivery_attempts WHERE subject_report_id=?""",
                (report_id,),
            ).fetchone()[0]
        )
        attempt_id = _attempt_id(current.outbox.idempotency_key, ordinal)
        connection.execute(
            """INSERT INTO subject_delivery_attempts(
                   attempt_id,subject_report_id,ordinal,state,recipient_hash,
                   content_sha256,prepared_at,completed_at,message_ids_json,
                   error_code,error_detail)
               VALUES(?,?,?,'prepared',?,?,?,NULL,'[]',NULL,NULL)""",
            (
                attempt_id,
                report_id,
                ordinal,
                recipient,
                current.report.content_sha256,
                prepared,
            ),
        )
        changed = connection.execute(
            """UPDATE subject_delivery_outbox
                  SET channel=?,recipient_hash=?,state='prepared',
                      current_attempt_id=?,updated_at=?
                WHERE subject_report_id=? AND state=?
                  AND current_attempt_id IS ?""",
            (
                delivery_channel,
                recipient,
                attempt_id,
                prepared,
                report_id,
                current.outbox.state,
                current.outbox.current_attempt_id,
            ),
        ).rowcount
        if changed != 1:
            raise DeliveryConflict("subject outbox changed during attempt preparation")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _load_snapshot(connection, report_id)


def _completion_fields(
    state: str,
    message_ids: tuple[str, ...],
    error_code: str | None,
    error_detail: str | None,
) -> tuple[str, str | None, str | None]:
    if state not in {"sent", "failed", "ambiguous"}:
        raise ValueError("state must be sent, failed, or ambiguous")
    if type(message_ids) is not tuple:
        raise TypeError("message_ids must be a tuple")
    encoded_ids = _message_ids(json.dumps(list(message_ids), separators=(",", ":")))
    if state == "sent":
        if not encoded_ids:
            raise ValueError("sent subject delivery requires message_ids")
        if error_code is not None or error_detail is not None:
            raise ValueError("sent subject delivery cannot include error fields")
        return json.dumps(list(encoded_ids), separators=(",", ":")), None, None
    if type(error_code) is not str or _ERROR_CODE_RE.fullmatch(error_code) is None:
        raise ValueError("failed or ambiguous delivery requires a typed error_code")
    detail = _nonempty(error_detail, "error_detail")
    return json.dumps(list(encoded_ids), separators=(",", ":")), error_code, detail


def complete_subject_delivery(
    connection: sqlite3.Connection,
    *,
    subject_report_id: str,
    attempt_id: str,
    state: str,
    completed_at: str,
    message_ids: tuple[str, ...] = (),
    error_code: str | None = None,
    error_detail: str | None = None,
) -> SubjectDeliverySnapshot:
    _connection(connection, write=True)
    report_id = _nonempty(subject_report_id, "subject_report_id")
    attempt = _nonempty(attempt_id, "attempt_id")
    completed = _timestamp(completed_at, "completed_at")
    encoded_ids, normalized_code, normalized_detail = _completion_fields(
        state, message_ids, error_code, error_detail
    )

    connection.execute("BEGIN IMMEDIATE")
    try:
        current = _load_snapshot(connection, report_id)
        if (
            current.outbox.state != "prepared"
            or current.attempt is None
            or current.attempt.attempt_id != attempt
            or current.attempt.state != "prepared"
        ):
            raise DeliveryConflict("subject delivery has no matching prepared attempt")
        if current.attempt.content_sha256 != current.report.content_sha256:
            raise DeliveryConflict("prepared attempt content hash changed")
        changed = connection.execute(
            """UPDATE subject_delivery_attempts
                  SET state=?,completed_at=?,message_ids_json=?,error_code=?,error_detail=?
                WHERE attempt_id=? AND subject_report_id=? AND state='prepared'
                  AND content_sha256=?""",
            (
                state,
                completed,
                encoded_ids,
                normalized_code,
                normalized_detail,
                attempt,
                report_id,
                current.report.content_sha256,
            ),
        ).rowcount
        if changed != 1:
            raise DeliveryConflict("prepared subject attempt changed before completion")
        changed = connection.execute(
            """UPDATE subject_delivery_outbox SET state=?,updated_at=?
                WHERE subject_report_id=? AND state='prepared'
                  AND current_attempt_id=? AND content_sha256=?""",
            (
                state,
                completed,
                report_id,
                attempt,
                current.report.content_sha256,
            ),
        ).rowcount
        if changed != 1:
            raise DeliveryConflict("subject outbox changed before completion")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _load_snapshot(connection, report_id)


__all__ = [
    "SubjectAttemptRecord",
    "SubjectDeliverySnapshot",
    "SubjectOutboxRecord",
    "SubjectReportRecord",
    "complete_subject_delivery",
    "load_subject_delivery",
    "prepare_subject_report",
    "start_subject_delivery",
    "subject_idempotency_key",
    "subject_report_id",
]
