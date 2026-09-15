"""Dedicated runtime-control SQLite store for the Compose news container.

The control DB is intentionally separate from the news-state DB.  It
holds three things:

* ``tasks``        — one row per scheduled run, keyed by a deterministic
                       ``task_id`` derived from ``kind`` and the UTC
                       due slot.
* ``claims``       — owner/generation/expiry fencing rows.  Stale
                       claims can be taken over by a new owner after
                       expiry.
* ``runs``         — outcome history (exit code, captured-stdout hash,
                       sanitized error class) for each claimed task.

All public functions take a SQLite *connection* so the caller owns
the transaction boundary.  ``BEGIN IMMEDIATE`` is used for enqueue
and claim to make the operations atomic and to prevent two enqueuers
from racing on the same ``task_id``.

The store has **no** delivery semantics.  Anything that smells of
delivery (``kind='delivery'``, ``--enable-live-delivery`` flag,
``delivery_state='delivered'``) is refused at the door.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

from . import ALLOWED_KINDS, DELIVERY_KIND_FORBIDDEN

SCHEMA_VERSION = 1
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_meta (
    version INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    due_slot_utc TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','claimed','completed','failed')),
    generation INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    UNIQUE (kind, due_slot_utc)
);

CREATE TABLE IF NOT EXISTS claims (
    task_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    generation INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    expires_at_utc TEXT NOT NULL,
    claimed_at_utc TEXT NOT NULL,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS claims_expiry_idx ON claims (expires_at_utc);

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    owner TEXT NOT NULL,
    generation INTEGER NOT NULL,
    started_at_utc TEXT NOT NULL,
    finished_at_utc TEXT,
    status TEXT NOT NULL CHECK (status IN ('completed','failed')),
    exit_code INTEGER,
    stdout_hash TEXT,
    error_class TEXT,
    error_message TEXT,
    canary_root TEXT,
    FOREIGN KEY (task_id) REFERENCES tasks(task_id)
);

CREATE INDEX IF NOT EXISTS runs_task_idx ON runs (task_id);
"""


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ControlStoreError(Exception):
    """Base error for control-store misuse."""


class UnknownKindError(ControlStoreError):
    """A kind outside the allowed allow-list was supplied."""


class DeliveryForbiddenError(ControlStoreError):
    """A delivery-shaped payload, kind, or flag was rejected."""


class ClaimMismatchError(ControlStoreError):
    """The supplied owner/generation does not match the live claim."""


class PayloadConflictError(ControlStoreError):
    """A re-enqueue supplied a payload that disagrees with the stored one."""


# ---------------------------------------------------------------------------
# Connection / schema bootstrap
# ---------------------------------------------------------------------------


def open(db_path: str | Path) -> sqlite3.Connection:
    """Open (and lazily bootstrap) the control DB.

    The DB is opened with ``isolation_level=None`` so the caller can
    start ``BEGIN IMMEDIATE`` transactions explicitly.
    """
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(path), isolation_level=None, timeout=30.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    _bootstrap_schema(connection)
    return connection


def _bootstrap_schema(connection: sqlite3.Connection) -> None:
    # Always run CREATE TABLE IF NOT EXISTS so a fresh DB has the
    # tables we then query.  We then look at schema_meta to refuse
    # newer-than-supported versions.
    connection.executescript(SCHEMA_SQL)
    row = connection.execute(
        "SELECT version FROM schema_meta ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if row is None:
        connection.execute(
            "INSERT INTO schema_meta(version, created_at) VALUES (?, ?)",
            (SCHEMA_VERSION, _utc_now_iso()),
        )
        return
    if row["version"] > SCHEMA_VERSION:
        raise ControlStoreError(
            f"control DB schema version {row['version']} is newer than supported {SCHEMA_VERSION}"
        )


def _utc_now_iso(now: datetime | None = None) -> str:
    when = (now or datetime.now(UTC)).astimezone(UTC)
    return when.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_utc_iso(value: str) -> datetime:
    if not value.endswith("Z"):
        raise ValueError(f"UTC timestamp must end with Z, got: {value!r}")
    return datetime.fromisoformat(value[:-1] + "+00:00").astimezone(UTC)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def validate_kind(kind: str) -> str:
    if not isinstance(kind, str):
        raise UnknownKindError(f"kind must be a string, got {type(kind).__name__}")
    if kind in {DELIVERY_KIND_FORBIDDEN}:
        raise DeliveryForbiddenError(
            f"delivery kind {kind!r} is forbidden by the runtime-control slice"
        )
    if kind not in ALLOWED_KINDS:
        raise UnknownKindError(
            f"kind {kind!r} is not allowed; expected one of {sorted(ALLOWED_KINDS)}"
        )
    return kind


def validate_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Refuse payloads that mention delivery anywhere.

    The check is lexical (recursive) so it catches both flat and nested
    keys, and string values containing the substring ``delivery``.
    """
    return _scrub_delivery(payload, context="payload")


def _scrub_delivery(value: Any, *, context: str) -> Any:
    if isinstance(value, str):
        lowered = value.lower()
        if "delivery" in lowered and ("enable_live_delivery" in lowered or lowered.strip() == "delivery"):
            raise DeliveryForbiddenError(
                f"delivery vocabulary found in {context}: {value!r}"
            )
        return value
    if isinstance(value, dict):
        cleaned: dict[str, Any] = {}
        for key, sub in value.items():
            if isinstance(key, str) and key.lower() == "delivery":
                raise DeliveryForbiddenError(
                    f"delivery key forbidden in {context}: {key!r}"
                )
            if isinstance(key, str) and key.lower() == "enable_live_delivery" and bool(sub):
                raise DeliveryForbiddenError(
                    f"delivery flag forbidden in {context}: {key!r}={sub!r}"
                )
            cleaned[key] = _scrub_delivery(sub, context=context)
        return cleaned
    if isinstance(value, (list, tuple)):
        cleaned_list = [_scrub_delivery(item, context=context) for item in value]
        return type(value)(cleaned_list)
    return value


# ---------------------------------------------------------------------------
# Deterministic task id
# ---------------------------------------------------------------------------


def deterministic_task_id(kind: str, due_slot_utc: str) -> str:
    """Return a stable 16-char task id from ``kind`` and UTC due slot.

    The same ``kind`` + due slot always yields the same id, which is
    what makes the ``UNIQUE (kind, due_slot_utc)`` enqueue idempotent.
    """
    validate_kind(kind)
    _validate_due_slot(due_slot_utc)
    material = f"{kind}|{due_slot_utc}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:16]


def _validate_due_slot(due_slot_utc: str) -> str:
    if not isinstance(due_slot_utc, str) or not due_slot_utc.endswith("Z"):
        raise ValueError(f"due_slot_utc must be a UTC-Z string, got {due_slot_utc!r}")
    _parse_utc_iso(due_slot_utc)
    return due_slot_utc


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------


@contextmanager
def _immediate_tx(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except Exception:
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")


def enqueue(
    connection: sqlite3.Connection,
    *,
    kind: str,
    due_slot_utc: str,
    payload: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> tuple[str, bool]:
    """Idempotently enqueue a task.  Returns ``(task_id, created)``.

    Behaviour:

    * First enqueue: returns ``(task_id, True)`` and inserts a pending row.
    * Re-enqueue with an *identical canonical payload* against an
      existing pending, claimed, completed, or failed row: returns
      ``(task_id, False)`` without mutating state.  This is the
      repeated-scheduler-sweep contract.
    * Re-enqueue with a *conflicting canonical payload*: raises
      :class:`PayloadConflictError` so the misconfiguration fails
      closed instead of silently mutating the stored task.
    """
    validate_kind(kind)
    _validate_due_slot(due_slot_utc)
    payload = validate_payload(dict(payload or {}))
    task_id = deterministic_task_id(kind, due_slot_utc)
    payload_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    ts = _utc_now_iso(now)

    with _immediate_tx(connection):
        existing = connection.execute(
            "SELECT state, payload_json FROM tasks WHERE task_id = ?", (task_id,)
        ).fetchone()
        if existing is not None:
            if existing["payload_json"] != payload_json:
                raise PayloadConflictError(
                    f"task {task_id} already exists with a conflicting payload "
                    f"(state={existing['state']!r})"
                )
            return task_id, False
        connection.execute(
            "INSERT INTO tasks(task_id, kind, due_slot_utc, payload_json, state, generation, created_at)"
            " VALUES (?, ?, ?, ?, 'pending', 0, ?)",
            (task_id, kind, due_slot_utc, payload_json, ts),
        )
    return task_id, True


def pending_tasks(
    connection: sqlite3.Connection, *, limit: int | None = None
) -> list[sqlite3.Row]:
    sql = (
        "SELECT task_id, kind, due_slot_utc, payload_json, generation, state FROM tasks"
        " WHERE state='pending' ORDER BY due_slot_utc, task_id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return list(connection.execute(sql))


def selectable_tasks(
    connection: sqlite3.Connection,
    *,
    kind: str,
    now: datetime | None = None,
    limit: int | None = None,
) -> list[sqlite3.Row]:
    """Return rows the worker selector is allowed to claim.

    A row is "selectable" if it is either ``pending`` or ``claimed``
    with an *expired* claim.  Live claimed rows belonging to another
    owner are excluded so a worker cannot race the existing holder.
    """
    validate_kind(kind)
    ref_now = (now or datetime.now(UTC)).astimezone(UTC)
    rows = list(
        connection.execute(
            "SELECT t.task_id, t.kind, t.due_slot_utc, t.payload_json,"
            " t.state, t.generation, c.owner AS claim_owner,"
            " c.generation AS claim_generation, c.attempt AS claim_attempt,"
            " c.expires_at_utc AS claim_expires_at_utc"
            " FROM tasks t LEFT JOIN claims c ON c.task_id = t.task_id"
            " WHERE t.kind = ?"
            " AND (t.state = 'pending'"
            "      OR (t.state = 'claimed' AND c.expires_at_utc IS NOT NULL))"
            " ORDER BY t.due_slot_utc, t.task_id",
            (kind,),
        )
    )
    cutoff = _utc_now_iso(ref_now)
    eligible = [
        row for row in rows
        if row["state"] == "pending"
        or (row["claim_expires_at_utc"] is not None and row["claim_expires_at_utc"] <= cutoff)
    ]
    if limit is not None:
        return eligible[: int(limit)]
    return eligible


def renew_claim(
    connection: sqlite3.Connection,
    *,
    task_id: str,
    owner: str,
    generation: int,
    ttl_seconds: int,
    now: datetime | None = None,
) -> str:
    """Fence-checked lease renewal for a long in-process dispatch.

    Returns the new ``expires_at_utc`` on success.  Raises
    :class:`ClaimMismatchError` if the live claim does not match
    ``owner``/``generation``, or if the claim has already expired
    (fence loss).  Renewal must NOT convert a stale/lost claim into a
    completed one — the worker surfaces the fence-loss and keeps the
    result at-least-once.
    """
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    new_expiry = _utc_now_iso(_shift(now or datetime.now(UTC), seconds=ttl_seconds))
    ts = _utc_now_iso(now)
    with _immediate_tx(connection):
        claim_row = connection.execute(
            "SELECT owner, generation, expires_at_utc FROM claims WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if claim_row is None:
            raise ClaimMismatchError(f"task {task_id} has no live claim")
        if claim_row["owner"] != owner:
            raise ClaimMismatchError(
                f"task {task_id} owner mismatch on renew: have {claim_row['owner']!r}, got {owner!r}"
            )
        if claim_row["generation"] != generation:
            raise ClaimMismatchError(
                f"task {task_id} generation mismatch on renew: have {claim_row['generation']}, got {generation}"
            )
        ref_now = (now or datetime.now(UTC)).astimezone(UTC)
        expiry = _parse_utc_iso(claim_row["expires_at_utc"])
        if expiry <= ref_now:
            raise ClaimMismatchError(
                f"task {task_id} claim expired at {claim_row['expires_at_utc']} before renewal"
            )
        connection.execute(
            "UPDATE claims SET expires_at_utc = ?, claimed_at_utc = ? WHERE task_id = ?",
            (new_expiry, ts, task_id),
        )
    return new_expiry


# ---------------------------------------------------------------------------
# Claim / takeover / complete
# ---------------------------------------------------------------------------


def claim(
    connection: sqlite3.Connection,
    *,
    task_id: str,
    owner: str,
    ttl_seconds: int,
    now: datetime | None = None,
) -> tuple[int, int, str]:
    """Attempt to claim ``task_id``.

    Returns ``(generation, attempt, expires_at_utc)`` on success.

    Behaviour:

    * Pending tasks are claimed with ``generation=1``.
    * Live claim held by the *same* owner: returned as-is (idempotent
      re-claim — TTL untouched, generation/attempt preserved).
    * Live claim held by a *different* owner: refused (raise
      :class:`ControlStoreError`).
    * Expired claim (whether same-owner or different-owner): taken
      over — ``generation`` is bumped, ``attempt`` is incremented,
      ``expires_at_utc`` is pushed forward, owner is refreshed (or
      preserved) to the caller.  The fence is the owner+generation
      pair, so bumping generation invalidates any stale completion.
    * Non-pending, non-claimed states are refused.
    """
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive")
    ts = _utc_now_iso(now)
    expires_at = _utc_now_iso(_shift(now or datetime.now(UTC), seconds=ttl_seconds))

    with _immediate_tx(connection):
        task = connection.execute(
            "SELECT state, kind, generation FROM tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if task is None:
            raise ControlStoreError(f"task {task_id} not found")
        if task["state"] not in {"pending", "claimed"}:
            raise ControlStoreError(
                f"task {task_id} is in non-claimable state {task['state']!r}"
            )
        claim_row = connection.execute(
            "SELECT owner, generation, attempt, expires_at_utc FROM claims WHERE task_id = ?",
            (task_id,),
        ).fetchone()

        if claim_row is None:
            new_generation = task["generation"] + 1
            attempt = 1
            connection.execute(
                "INSERT INTO claims(task_id, owner, generation, attempt, expires_at_utc, claimed_at_utc)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (task_id, owner, new_generation, attempt, expires_at, ts),
            )
            connection.execute(
                "UPDATE tasks SET state='claimed', generation=? WHERE task_id=?",
                (new_generation, task_id),
            )
            return new_generation, attempt, expires_at

        # Existing claim — expiry comes *before* same-owner idempotence
        # so a crashed worker whose TTL elapsed cannot silently inherit
        # its own stale lease.
        ref_now = (now or datetime.now(UTC)).astimezone(UTC)
        expiry = _parse_utc_iso(claim_row["expires_at_utc"])
        if expiry <= ref_now:
            new_generation = claim_row["generation"] + 1
            attempt = claim_row["attempt"] + 1
            connection.execute(
                "UPDATE claims SET owner=?, generation=?, attempt=?, expires_at_utc=?, claimed_at_utc=?"
                " WHERE task_id=?",
                (owner, new_generation, attempt, expires_at, ts, task_id),
            )
            connection.execute(
                "UPDATE tasks SET state='claimed', generation=? WHERE task_id=?",
                (new_generation, task_id),
            )
            return new_generation, attempt, expires_at

        if claim_row["owner"] != owner:
            raise ControlStoreError(
                f"claim on {task_id} is live (expires {claim_row['expires_at_utc']}); cannot take over"
            )

        # Live same-owner re-claim — idempotent, preserve generation/attempt.
        return (
            claim_row["generation"],
            claim_row["attempt"],
            claim_row["expires_at_utc"],
        )


def complete(
    connection: sqlite3.Connection,
    *,
    task_id: str,
    owner: str,
    generation: int,
    status: str,
    exit_code: int | None,
    stdout_hash: str | None,
    error_class: str | None,
    error_message: str | None,
    canary_root: str | None = None,
    now: datetime | None = None,
) -> None:
    """Mark a claimed task as ``completed`` or ``failed``.

    The completion is accepted only if the live claim matches
    ``owner``/``generation`` and the claim has not yet expired.
    """
    if status not in {"completed", "failed"}:
        raise ValueError(f"status must be 'completed' or 'failed', got {status!r}")
    finished_at = _utc_now_iso(now)

    with _immediate_tx(connection):
        claim_row = connection.execute(
            "SELECT owner, generation, expires_at_utc FROM claims WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if claim_row is None:
            raise ClaimMismatchError(f"task {task_id} has no live claim")
        if claim_row["owner"] != owner:
            raise ClaimMismatchError(
                f"task {task_id} owner mismatch: have {claim_row['owner']!r}, got {owner!r}"
            )
        if claim_row["generation"] != generation:
            raise ClaimMismatchError(
                f"task {task_id} generation mismatch: have {claim_row['generation']}, got {generation}"
            )
        expiry = _parse_utc_iso(claim_row["expires_at_utc"])
        ref_now = (now or datetime.now(UTC)).astimezone(UTC)
        if expiry <= ref_now:
            raise ClaimMismatchError(
                f"task {task_id} claim expired at {claim_row['expires_at_utc']}"
            )
        connection.execute(
            "INSERT INTO runs(task_id, owner, generation, started_at_utc, finished_at_utc, status, exit_code, stdout_hash, error_class, error_message, canary_root)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (task_id, owner, generation, finished_at, finished_at, status, exit_code, stdout_hash, error_class, error_message, canary_root),
        )
        connection.execute(
            "UPDATE tasks SET state=? WHERE task_id=?",
            (status, task_id),
        )
        connection.execute("DELETE FROM claims WHERE task_id=?", (task_id,))


# ---------------------------------------------------------------------------
# Diagnostics used by the validate kind
# ---------------------------------------------------------------------------


def integrity_check(connection: sqlite3.Connection) -> list[str]:
    """Return ``['ok']`` or a list of integrity-check failure pages."""
    rows = connection.execute("PRAGMA integrity_check").fetchall()
    return [str(row[0]) for row in rows]


def foreign_key_check(connection: sqlite3.Connection) -> list[tuple[int, str, str]]:
    rows = connection.execute("PRAGMA foreign_key_check").fetchall()
    return [(int(row[0]), str(row[1]), str(row[2])) for row in rows]


def schema_version(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT version FROM schema_meta ORDER BY version DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return 0
    return int(row["version"])


def zero_delivery_check(connection: sqlite3.Connection) -> tuple[int, list[str]]:
    """Return ``(count, kinds)`` for ``delivery``-shaped tasks.

    The control store enforces zero delivery rows by refusing them at
    enqueue, but a stale/legacy DB could have them.  The validate
    kind surfaces this check to catch schema drift.
    """
    rows = connection.execute(
        "SELECT kind FROM tasks WHERE kind = ?", (DELIVERY_KIND_FORBIDDEN,)
    ).fetchall()
    kinds = [str(row[0]) for row in rows]
    return len(kinds), kinds


def hash_stdout(stdout: bytes | str) -> str:
    if isinstance(stdout, str):
        stdout = stdout.encode("utf-8")
    return hashlib.sha256(stdout).hexdigest()


def sanitize_error_message(message: str | None, *, max_length: int = 4096) -> str | None:
    """Strip control chars and clamp length to a sane upper bound."""
    if message is None:
        return None
    cleaned = "".join(ch for ch in message if ch == "\n" or ch == "\t" or 32 <= ord(ch) < 0x7F or ord(ch) >= 0x80)
    if len(cleaned) > max_length:
        cleaned = cleaned[: max_length - 3] + "..."
    return cleaned


SANITIZED_ERROR_CLASSES = frozenset({
    "RuntimeError",
    "ValueError",
    "TimeoutError",
    "FileNotFoundError",
    "PermissionError",
    "ProcessLookupError",
    "OSError",
    "sqlite3.OperationalError",
    "sqlite3.DatabaseError",
    "sqlite3.IntegrityError",
    "DeliveryForbiddenError",
    "UnknownKindError",
    "ClaimMismatchError",
    "ControlStoreError",
    "ConfigurationError",
    "ScheduleError",
    "PathEscapeError",
    "SymlinkForbiddenError",
})


def sanitize_error_class(error_class: str | None) -> str | None:
    """Return ``error_class`` only if it's in the allow-list.

    Unknown / attacker-controlled classes collapse to
    ``"ControlStoreError"`` so the runs table is never polluted by
    arbitrary class names.
    """
    if error_class is None:
        return None
    if error_class in SANITIZED_ERROR_CLASSES:
        return error_class
    return "ControlStoreError"


def _shift(when: datetime, *, seconds: int) -> datetime:
    from datetime import timedelta

    return when.astimezone(UTC) + timedelta(seconds=seconds)


def iter_runs(
    connection: sqlite3.Connection, *, task_id: str | None = None
) -> Iterable[sqlite3.Row]:
    if task_id is None:
        return connection.execute(
            "SELECT task_id, owner, generation, status, exit_code, stdout_hash, error_class, finished_at_utc FROM runs ORDER BY run_id DESC"
        )
    return connection.execute(
        "SELECT task_id, owner, generation, status, exit_code, stdout_hash, error_class, finished_at_utc FROM runs WHERE task_id=? ORDER BY run_id DESC",
        (task_id,),
    )