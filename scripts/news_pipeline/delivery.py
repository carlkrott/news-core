"""Fail-closed Phase 6 Telegram delivery with durable idempotency receipts.

The adapter is deliberately test-injectable.  Network delivery is impossible
unless the caller supplies ``enable_live=True``; the normal job wrappers never
do that.  A prepared or ambiguous attempt is a hard stop on replay because
Telegram has no portable idempotency key for ``sendMessage``.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .delivery_schema_v6 import validate_v6
from .report_artifacts import ArtifactRoot, _paths, sha256_hex

_TOKEN_RE = re.compile(r"[0-9]+:[A-Za-z0-9_-]+\Z")
_CHAT_RE = re.compile(r"[0-9]+\Z")
_MAX_TELEGRAM_CHARS = 4096
_MAX_RESPONSE_BYTES = 1_048_576


class DeliveryError(RuntimeError):
    """Base class for delivery failures safe to report without credentials."""


class DeliveryRejected(DeliveryError):
    """The provider returned a definitive non-send response."""


class DeliveryAmbiguous(DeliveryError):
    """The provider outcome cannot be proven; automatic retry is forbidden."""


class DeliveryConflict(DeliveryError):
    """A durable receipt changed unexpectedly during finalization."""


class DeliveryTransport(Protocol):
    @property
    def recipient_hash(self) -> str:
        """Stable SHA-256 identity for the configured delivery recipient."""
        ...

    def send(self, text: str) -> str:
        """Send one message and return the provider message identifier."""


@dataclass(frozen=True, slots=True)
class TelegramConfig:
    token: str = field(repr=False)
    chat_id: str = field(repr=False)

    def __repr__(self) -> str:
        return "TelegramConfig(token=<redacted>, chat_id=<redacted>)"

    @property
    def recipient_hash(self) -> str:
        return hashlib.sha256(self.chat_id.encode("ascii")).hexdigest()


@dataclass(frozen=True, slots=True)
class ReportMaterial:
    report_id: str
    markdown: str
    markdown_sha256: str
    manifest_sha256: str
    window_start: str
    window_end: str
    delivery_state: str


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    report_id: str
    attempt_id: str
    idempotency_key: str
    state: str
    message_ids: tuple[str, ...]
    network_used: bool
    replayed: bool


def load_telegram_config(path: str | Path) -> TelegramConfig:
    """Load only the legacy Telegram fields from an owner-only config file."""
    config_path = Path(path)
    try:
        info = config_path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise DeliveryRejected("Telegram configuration file is not an owner-only regular file")
        text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise DeliveryRejected("Telegram configuration could not be read") from exc
    token_match = re.search(r'bot_token\s*=\s*"([^"]+)"', text)
    chat_match = re.search(r'allowed_users\s*=\s*\[\s*"([^"]+)"', text)
    token = token_match.group(1) if token_match else ""
    chat_id = chat_match.group(1) if chat_match else ""
    if not _TOKEN_RE.fullmatch(token) or not _CHAT_RE.fullmatch(chat_id):
        raise DeliveryRejected("Telegram configuration is missing or invalid")
    return TelegramConfig(token=token, chat_id=chat_id)


class TelegramTransport:
    """Minimal Telegram ``sendMessage`` client; no credential-bearing logging."""

    def __init__(
        self,
        config: TelegramConfig,
        *,
        api_base: str = "https://api.telegram.org",
        timeout: float = 30.0,
        opener: Callable[..., Any] = urlopen,
    ) -> None:
        if not isinstance(config, TelegramConfig):
            raise TypeError("config must be TelegramConfig")
        base = api_base.rstrip("/")
        if not (base.startswith("https://") or base.startswith("http://")):
            raise ValueError("api_base must use http or https")
        if type(timeout) not in (int, float) or timeout <= 0:
            raise ValueError("timeout must be positive")
        self._config = config
        self._api_base = base
        self._timeout = float(timeout)
        self._opener = opener

    @property
    def recipient_hash(self) -> str:
        return self._config.recipient_hash

    def send(self, text: str) -> str:
        if type(text) is not str or not text or len(text) > _MAX_TELEGRAM_CHARS:
            raise DeliveryRejected("Telegram message length is invalid")
        payload = urlencode(
            {"chat_id": self._config.chat_id, "text": text, "parse_mode": "Markdown"}
        ).encode("utf-8")
        request = Request(
            f"{self._api_base}/bot{self._config.token}/sendMessage",
            data=payload,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with self._opener(request, timeout=self._timeout) as response:
                status = int(response.status)
                raw = response.read(_MAX_RESPONSE_BYTES + 1)
        except HTTPError as exc:
            raise DeliveryRejected(f"Telegram rejected the message with HTTP {exc.code}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise DeliveryAmbiguous("Telegram transport outcome is unknown") from exc
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise DeliveryAmbiguous("Telegram response exceeded the safe limit")
        if not 200 <= status < 300:
            raise DeliveryRejected(f"Telegram rejected the message with HTTP {status}")
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeliveryAmbiguous("Telegram response was not valid JSON") from exc
        if body.get("ok") is not True:
            raise DeliveryRejected("Telegram returned a definitive failure")
        message_id = body.get("result", {}).get("message_id")
        if type(message_id) not in (int, str) or not str(message_id):
            raise DeliveryAmbiguous("Telegram response did not contain a message identifier")
        return str(message_id)


def split_telegram_text(text: str, *, max_chars: int = _MAX_TELEGRAM_CHARS) -> tuple[str, ...]:
    """Split deterministically without dropping or reordering characters."""
    if type(text) is not str or not text:
        raise ValueError("text must be a non-empty string")
    if type(max_chars) is not int or max_chars < 1:
        raise ValueError("max_chars must be a positive integer")
    parts: list[str] = []
    remaining = text
    while len(remaining) > max_chars:
        boundary = remaining.rfind("\n", 0, max_chars + 1)
        if boundary <= 0:
            boundary = max_chars
        # Keep the delimiter in the preceding or following part; joining the
        # returned parts must reconstruct the exact input text.
        parts.append(remaining[:boundary])
        remaining = remaining[boundary:]
    if remaining:
        parts.append(remaining)
    return tuple(parts)


def idempotency_key(report_id: str, manifest_sha256: str) -> str:
    if type(report_id) is not str or not report_id:
        raise ValueError("report_id must be non-empty")
    if not re.fullmatch(r"[0-9a-f]{64}", manifest_sha256):
        raise ValueError("manifest_sha256 must be a lowercase SHA-256")
    return hashlib.sha256(
        ("phase6-telegram\0" + report_id + "\0" + manifest_sha256).encode("utf-8")
    ).hexdigest()


def _open_db(path: str | Path) -> sqlite3.Connection:
    connection = sqlite3.connect(Path(path), isolation_level=None, timeout=30.0)
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    if connection.execute("PRAGMA foreign_keys").fetchone()[0] != 1:
        connection.close()
        raise DeliveryError("foreign-key enforcement could not be enabled")
    return connection


def load_report_material(
    db_path: str | Path,
    artifact_root: str | Path,
    report_id: str,
) -> ReportMaterial:
    """Read and hash-check a complete report before any receipt mutation."""
    root = ArtifactRoot(Path(artifact_root))
    connection = _open_db(db_path)
    try:
        validate_v6(connection)
        row = connection.execute(
            """SELECT report_id,window_start,window_end,generation_status,
                      json_sha256,jsonl_sha256,markdown_sha256,manifest_sha256,
                      delivery_state,delivery_id
                 FROM reports WHERE report_id=?""",
            (report_id,),
        ).fetchone()
        if row is None:
            raise DeliveryRejected("report does not exist")
        (
            stored_id, window_start, window_end, generation_status,
            json_hash, jsonl_hash, markdown_hash, manifest_hash,
            delivery_state, delivery_id,
        ) = row
        if generation_status != "complete":
            raise DeliveryRejected("report is not complete")
        if delivery_state not in {"dry_run", "not_attempted", "failed", "sent"}:
            raise DeliveryRejected("report is not in a deliverable state")
        if delivery_state == "sent" and delivery_id is None:
            raise DeliveryConflict("sent report has no delivery receipt identifier")
        if not all(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) for value in (json_hash, jsonl_hash, markdown_hash, manifest_hash)):
            raise DeliveryRejected("report artifact hashes are incomplete")
        paths = _paths(root, window_start, window_end)
        data = tuple(path.read_bytes() for path in paths)
        actual = tuple(sha256_hex(value) for value in data)
        expected = (json_hash, jsonl_hash, markdown_hash, manifest_hash)
        if actual != expected:
            raise DeliveryRejected("report artifact bytes do not match persisted hashes")
        try:
            manifest = json.loads(data[3].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DeliveryRejected("report manifest is invalid") from exc
        manifest_markdown = manifest.get("artifacts", {}).get("markdown", {}).get("sha256")
        if manifest.get("report_id") != stored_id or manifest_markdown != markdown_hash:
            raise DeliveryRejected("report manifest identity does not match the database")
        return ReportMaterial(
            report_id=stored_id,
            markdown=data[2].decode("utf-8"),
            markdown_sha256=markdown_hash,
            manifest_sha256=manifest_hash,
            window_start=window_start,
            window_end=window_end,
            delivery_state=delivery_state,
        )
    finally:
        connection.close()


def _message_ids_json(message_ids: list[str] | tuple[str, ...]) -> str:
    return json.dumps(list(message_ids), ensure_ascii=True, separators=(",", ":"))


def _existing(connection: sqlite3.Connection, report_id: str) -> tuple[Any, ...] | None:
    return connection.execute(
        """SELECT report_id,idempotency_key,channel,recipient_hash,content_sha256,
                  state,current_attempt_id
             FROM report_deliveries WHERE report_id=?""",
        (report_id,),
    ).fetchone()


def _attempt_ids(connection: sqlite3.Connection, attempt_id: str) -> tuple[str, ...]:
    row = connection.execute(
        "SELECT message_ids_json FROM report_delivery_attempts WHERE attempt_id=?",
        (attempt_id,),
    ).fetchone()
    if row is None or row[0] is None:
        return ()
    values = json.loads(row[0])
    if type(values) is not list or any(type(value) is not str for value in values):
        raise DeliveryConflict("persisted message identifiers are malformed")
    return tuple(values)


def _result_from_row(
    row: tuple[Any, ...], *, network_used: bool = False, replayed: bool = True,
    connection: sqlite3.Connection,
) -> DeliveryResult:
    report_id, key, _channel, _recipient_hash, _content_hash, state, attempt_id = row
    if not attempt_id:
        raise DeliveryConflict("delivery receipt has no current attempt")
    return DeliveryResult(
        report_id=report_id,
        attempt_id=attempt_id,
        idempotency_key=key,
        state=state,
        message_ids=_attempt_ids(connection, attempt_id),
        network_used=network_used,
        replayed=replayed,
    )


def _mark_attempt(
    connection: sqlite3.Connection,
    attempt_id: str,
    report_id: str,
    state: str,
    *,
    completed_at: str | None,
    message_ids: list[str] | tuple[str, ...] | None = None,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """UPDATE report_delivery_attempts
                  SET state=?,completed_at=?,message_ids_json=?,error_code=?,error_detail=?
                WHERE attempt_id=? AND report_id=?""",
            (
                state, completed_at,
                None if message_ids is None else _message_ids_json(message_ids),
                error_code, error_detail, attempt_id, report_id,
            ),
        )
        connection.execute(
            """UPDATE report_deliveries
                  SET state=?,updated_at=? WHERE report_id=? AND current_attempt_id=?""",
            (state, completed_at or _utc_now(), report_id, attempt_id),
        )
        connection.execute(
            """UPDATE reports SET delivery_state='failed',delivery_id=?
                WHERE report_id=? AND generation_status='complete'""",
            (attempt_id, report_id),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _utc_now() -> str:
    from datetime import UTC, datetime
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def deliver_report(
    db_path: str | Path,
    artifact_root: str | Path,
    report_id: str,
    *,
    enable_live: bool = False,
    config_path: str | Path | None = None,
    api_base: str = "https://api.telegram.org",
    transport: DeliveryTransport | None = None,
    now: str | None = None,
    retry_failed: bool = False,
) -> DeliveryResult:
    """Deliver or dry-run one complete report using the durable v6 receipt."""
    material = load_report_material(db_path, artifact_root, report_id)
    parts = split_telegram_text(material.markdown)
    key = idempotency_key(material.report_id, material.manifest_sha256)
    content_hash = sha256_hex(material.markdown.encode("utf-8"))
    recipient_hash = sha256_hex(b"phase6-dry-run")
    client = transport
    if enable_live:
        if client is None:
            if config_path is None:
                raise DeliveryRejected("live delivery requires an explicit Telegram config path")
            config = load_telegram_config(config_path)
            recipient_hash = config.recipient_hash
            client = TelegramTransport(config, api_base=api_base)
        else:
            candidate_hash = getattr(client, "recipient_hash", None)
            if type(candidate_hash) is not str or not re.fullmatch(r"[0-9a-f]{64}", candidate_hash):
                raise DeliveryRejected("live transport must provide a lowercase SHA-256 recipient_hash")
            recipient_hash = candidate_hash
    elif transport is not None:
        raise DeliveryRejected("a transport cannot be supplied while live delivery is disabled")

    timestamp = now or _utc_now()
    connection = _open_db(db_path)
    try:
        validate_v6(connection)
        row = _existing(connection, material.report_id)
        if row is not None:
            if row[1] != key or row[4] != content_hash:
                raise DeliveryConflict("durable receipt identity does not match the report")
            if enable_live and row[5] != "dry_run" and row[3] != recipient_hash:
                raise DeliveryConflict("recipient hash does not match the durable receipt")
            if row[5] == "sent":
                return _result_from_row(row, connection=connection)
            if row[5] in {"prepared", "ambiguous"}:
                raise DeliveryAmbiguous("an unresolved delivery attempt blocks automatic replay")
            if row[5] == "skipped":
                raise DeliveryRejected("delivery was explicitly skipped")
            if row[5] == "failed" and (not retry_failed or not enable_live):
                raise DeliveryRejected("a failed delivery requires explicit live retry_failed")
            if row[5] == "dry_run" and not enable_live:
                return _result_from_row(row, connection=connection)
        elif material.delivery_state == "sent":
            raise DeliveryConflict("sent report lacks its durable v6 delivery row")

        if not enable_live:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if row is None:
                    attempt_id = hashlib.sha256((key + "\0dry-run").encode()).hexdigest()
                    connection.execute(
                        """INSERT INTO report_deliveries(
                               report_id,idempotency_key,channel,recipient_hash,content_sha256,
                               state,current_attempt_id,created_at,updated_at)
                           VALUES(?,?,?,?,?,?,?,?,?)""",
                        (material.report_id, key, "telegram", recipient_hash, content_hash,
                         "dry_run", attempt_id, timestamp, timestamp),
                    )
                    connection.execute(
                        """INSERT INTO report_delivery_attempts(
                               attempt_id,report_id,ordinal,state,content_sha256,prepared_at,
                               completed_at,message_ids_json,error_code,error_detail)
                           VALUES(?,?,?,?,?,?,?,?,?,?)""",
                        (attempt_id, material.report_id, 1, "dry_run", content_hash,
                         timestamp, timestamp, "[]", None, None),
                    )
                else:
                    attempt_id = row[6]
                    if not attempt_id:
                        raise DeliveryConflict("dry-run receipt has no current attempt")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
            return DeliveryResult(material.report_id, attempt_id, key, "dry_run", (), False, row is not None)

        assert client is not None
        connection.execute("BEGIN IMMEDIATE")
        try:
            row = _existing(connection, material.report_id)
            if row is not None and row[5] in {"prepared", "ambiguous"}:
                raise DeliveryAmbiguous("an unresolved delivery attempt blocks automatic replay")
            ordinal = int(
                connection.execute(
                    "SELECT COALESCE(MAX(ordinal),0)+1 FROM report_delivery_attempts WHERE report_id=?",
                    (material.report_id,),
                ).fetchone()[0]
            )
            attempt_id = hashlib.sha256((key + "\0" + str(ordinal)).encode()).hexdigest()
            if row is None:
                connection.execute(
                    """INSERT INTO report_deliveries(
                           report_id,idempotency_key,channel,recipient_hash,content_sha256,
                           state,current_attempt_id,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (material.report_id, key, "telegram", recipient_hash, content_hash,
                     "prepared", attempt_id, timestamp, timestamp),
                )
            else:
                connection.execute(
                    """UPDATE report_deliveries SET state='prepared',current_attempt_id=?,
                           recipient_hash=?,updated_at=? WHERE report_id=?""",
                    (attempt_id, recipient_hash, timestamp, material.report_id),
                )
            connection.execute(
                """INSERT INTO report_delivery_attempts(
                       attempt_id,report_id,ordinal,state,content_sha256,prepared_at,
                       completed_at,message_ids_json,error_code,error_detail)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (attempt_id, material.report_id, ordinal, "prepared", content_hash,
                 timestamp, None, "[]", None, None),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        sent_ids: list[str] = []
        try:
            for part in parts:
                sent_ids.append(str(client.send(part)))
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "UPDATE report_delivery_attempts SET message_ids_json=? WHERE attempt_id=?",
                        (_message_ids_json(sent_ids), attempt_id),
                    )
                    connection.commit()
                except Exception:
                    connection.rollback()
                    raise DeliveryAmbiguous("message sent but its receipt could not be persisted")
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """UPDATE report_delivery_attempts
                          SET state='sent',completed_at=?,message_ids_json=?
                        WHERE attempt_id=? AND report_id=? AND state='prepared'""",
                    (timestamp, _message_ids_json(sent_ids), attempt_id, material.report_id),
                )
                changed = connection.execute(
                    """UPDATE report_deliveries
                          SET state='sent',updated_at=? WHERE report_id=? AND current_attempt_id=?
                            AND state='prepared'""",
                    (timestamp, material.report_id, attempt_id),
                ).rowcount
                if changed != 1:
                    raise DeliveryConflict("delivery receipt changed before finalization")
                changed = connection.execute(
                    """UPDATE reports SET delivery_state='sent',delivery_id=?
                        WHERE report_id=? AND generation_status='complete'
                          AND delivery_state IN ('dry_run','not_attempted','failed')""",
                    (attempt_id, material.report_id),
                ).rowcount
                if changed != 1:
                    raise DeliveryConflict("report delivery projection could not be finalized")
                connection.commit()
            except Exception:
                connection.rollback()
                raise
        except DeliveryRejected as exc:
            _mark_attempt(connection, attempt_id, material.report_id, "failed", completed_at=timestamp, message_ids=sent_ids, error_code="rejected", error_detail=str(exc))
            raise
        except DeliveryAmbiguous as exc:
            _mark_attempt(connection, attempt_id, material.report_id, "ambiguous", completed_at=timestamp, message_ids=sent_ids, error_code="ambiguous", error_detail=str(exc))
            raise
        except DeliveryConflict:
            _mark_attempt(connection, attempt_id, material.report_id, "ambiguous", completed_at=timestamp, message_ids=sent_ids, error_code="conflict", error_detail="receipt finalization conflict")
            raise
        return DeliveryResult(material.report_id, attempt_id, key, "sent", tuple(sent_ids), True, False)
    finally:
        connection.close()


__all__ = [
    "DeliveryAmbiguous", "DeliveryConflict", "DeliveryError", "DeliveryRejected",
    "DeliveryResult", "ReportMaterial", "TelegramConfig", "TelegramTransport",
    "deliver_report", "idempotency_key", "load_report_material", "load_telegram_config",
    "split_telegram_text",
]
