"""Role worker runtime for the news container scheduler.

A worker claims only its fixed kind, takes a shared ``fcntl`` flock
on a per-kind lockfile before doing any main-DB work, then invokes
``news_pipeline.jobs.main`` with bounded stdout capture.  Outcomes
(including sanitized error class and a hash of captured stdout) are
written back to the control store.

Canary / production safety
- All paths (state root, artifact root, control DB, lockfile) must
  pass a ``lexical + resolved`` containment check beneath
  ``NEWS_CANARY_ROOT`` when canary mode is enabled.
- Symlink escapes are refused at the lexical layer (realpath must
  match lexpath-or-strictly-descendant).
- Delivery is forbidden: the worker refuses to enqueue, claim, or
  pass through ``--enable-live-delivery``.

The worker also exposes a ``validate`` kind (SQLite integrity + FK +
schema + zero-delivery checks) and a ``report`` kind that invokes the
news pipeline ``daily-report`` job without live delivery.
"""
from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import io
import json
import os
import sqlite3
import sys
import threading
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterator

from .control_store import (
    ALLOWED_KINDS,
    ClaimMismatchError,
    ControlStoreError,
    DeliveryForbiddenError,
    SANITIZED_ERROR_CLASSES,
    claim,
    complete,
    enqueue,
    foreign_key_check,
    hash_stdout,
    integrity_check,
    open as open_db,
    renew_claim,
    sanitize_error_class,
    sanitize_error_message,
    schema_version,
    selectable_tasks,
    zero_delivery_check,
)
from .scheduler import ScheduleError, parse_schedule_toml

# A runnable news-pipeline main.  We import it lazily so the worker's
# CLI still loads on hosts that haven't installed the pipeline yet
# (e.g. CI lint).
_NEWS_PIPELINE_MAIN = "news_pipeline.jobs:main"

# Per-kind argv builder for the news pipeline ``jobs.main`` CLI.
# These are the *only* argv shapes the worker is allowed to build.
# The validate kind never reaches into jobs.main.
_KIND_ARGV_BUILDERS: dict[str, Callable[["DispatchContext"], list[str]]] = {}


@dataclass(frozen=True)
class DispatchContext:
    """Inputs to a per-kind argv builder.

    ``task_id`` is propagated so the captured stdout can mention the
    exact id it was processing — useful when grepping run history.
    """

    db_path: Path
    artifact_root: Path
    task_id: str
    due_slot_utc: str
    payload: dict[str, Any]
    control_db: Path | None = None
    owner: str | None = None
    generation: int | None = None


def _ingest_argv(ctx: DispatchContext) -> list[str]:
    """Build argv for an ingest run.

    Uses the network gate so the dispatch is faithful to the existing
    news_pipeline.jobs contract; live delivery is *not* enabled.
    """
    argv = [
        "tick",
        "--db", str(ctx.db_path),
        "--sources", _require_payload(ctx, "sources"),
        "--topics", _require_payload(ctx, "topics"),
        "--policy", _require_payload(ctx, "policy"),
        "--run-started-at", ctx.due_slot_utc,
        "--enable-network",
    ]
    provenance = ctx.payload.get("provenance")
    if provenance is not None:
        if not isinstance(provenance, str) or not provenance:
            raise ValueError("payload field 'provenance' must be a non-empty string")
        argv[argv.index("--run-started-at"):argv.index("--run-started-at")] = ["--provenance", provenance]
    if ctx.control_db is not None:
        argv += ["--control-db", str(ctx.control_db)]
    return argv


def _process_argv(ctx: DispatchContext) -> list[str]:
    argv = [
        "process",
        "--db", str(ctx.db_path),
        "--evaluated-at", ctx.due_slot_utc,
        "--max-items", str(int(ctx.payload.get("max_items", 500))),
        "--enable-network",
    ]
    history_db = ctx.payload.get("history_db")
    if history_db is not None:
        if not isinstance(history_db, str) or not history_db:
            raise ValueError("payload field 'history_db' must be a non-empty string")
        argv += ["--history-db", history_db]
    config_values = tuple(ctx.payload.get(key) for key in ("sources", "topics", "policy"))
    if any(value is not None for value in config_values):
        if not all(isinstance(value, str) and value for value in config_values):
            raise ValueError("payload fields 'sources', 'topics', and 'policy' must be non-empty strings together")
        for key, value in zip(("--sources", "--topics", "--policy"), config_values):
            argv += [key, value]
    return argv


def _investigate_argv(ctx: DispatchContext) -> list[str]:
    from news_pipeline.investigation import validate_investigation_payload

    validate_investigation_payload(ctx.payload)
    round_number = ctx.payload.get("round_number", 0)
    if type(round_number) is not int or type(round_number) is bool or not 0 <= round_number < 2:
        raise ValueError("payload field 'round_number' must be 0 or 1")
    argv = [
        "investigate",
        "--db", str(ctx.db_path),
        "--candidate-id", _require_payload(ctx, "candidate_id"),
        "--feed-lane-id", _require_payload(ctx, "feed_lane_id"),
        "--query-plan-id", _require_payload(ctx, "query_plan_id"),
        "--investigation-id", _require_payload(ctx, "investigation_id"),
        "--category", _require_payload(ctx, "category"),
        "--round-number", str(round_number),
        "--evaluated-at", ctx.due_slot_utc,
        "--enable-network",
    ]
    fence_values = (ctx.control_db, ctx.owner, ctx.generation)
    if any(value is not None for value in fence_values) and not all(value is not None for value in fence_values):
        raise ValueError(
            "investigate dispatch fence requires control_db, owner, and generation"
        )
    if all(value is not None for value in fence_values):
        argv += [
            "--control-db", str(ctx.control_db),
            "--control-task-id", ctx.task_id,
            "--control-owner", str(ctx.owner),
            "--control-generation", str(ctx.generation),
        ]
    return argv


def _report_argv(ctx: DispatchContext) -> list[str]:
    """Build argv for the daily-report run.

    Live delivery is *deliberately omitted* — the worker hard-rejects
    any delivery enablement.  Only dry-run daily-report is allowed.
    """
    if ctx.payload.get("enable_live_delivery"):
        raise DeliveryForbiddenError(
            "report kind refuses live delivery; use a non-canary operator flow"
        )
    argv = [
        "daily-report",
        "--db", str(ctx.db_path),
        "--artifact-root", str(ctx.artifact_root),
        "--as-of-utc", ctx.due_slot_utc,
    ]
    prior = ctx.payload.get("prior_upper_utc")
    if isinstance(prior, str) and prior:
        argv += ["--prior-upper-utc", prior]
    return argv


def _validate_argv(_ctx: DispatchContext) -> list[str]:
    raise AssertionError("validate kind is in-process; never invokes jobs.main")


def _require_payload(ctx: DispatchContext, key: str) -> str:
    value = ctx.payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"payload missing required string field {key!r}")
    return value


_KIND_ARGV_BUILDERS.update({
    "ingest": _ingest_argv,
    "investigate": _investigate_argv,
    "process": _process_argv,
    "report": _report_argv,
    "validate": _validate_argv,
})


# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------


class PathEscapeError(Exception):
    """A path failed the canary containment check."""


class SymlinkForbiddenError(Exception):
    """A path contained a symlink component we don't trust."""


def assert_path_within_canary(path: Path, *, canary_root: Path | None) -> Path:
    """Refuse ``path`` if it leaves ``canary_root`` after lexical+resolved checks.

    The lexical check rejects:
      * paths with ``..`` segments
      * paths with a symlink anywhere in the chain

    The resolved check (via :func:`os.path.realpath`) then ensures the
    target lives beneath the canary root.
    """
    if not isinstance(path, Path):
        path = Path(path)
    lexical = str(path)
    if lexical != os.path.normpath(lexical):
        raise PathEscapeError(f"path {lexical!r} contains lexical traversal")
    # Reject any symlink in the chain.
    for parent in path.parents:
        if parent == path:
            continue
        if parent.is_symlink():
            raise SymlinkForbiddenError(f"path {lexical!r} traverses symlink {parent!r}")
    if path.is_symlink():
        raise SymlinkForbiddenError(f"path {lexical!r} is a symlink")

    if canary_root is None:
        return path

    if not isinstance(canary_root, Path):
        canary_root = Path(canary_root)
    if str(canary_root) != os.path.normpath(str(canary_root)):
        raise PathEscapeError(f"canary root {canary_root!r} contains lexical traversal")
    if canary_root.is_symlink():
        raise SymlinkForbiddenError(f"canary root {canary_root!r} is a symlink")

    real_path = os.path.realpath(path)
    real_root = os.path.realpath(canary_root)
    # Use os.path.commonpath for the real containment check; raise if
    # the resolved path is identical to the root (which is allowed,
    # the worker writes *into* the canary tree).
    try:
        common = os.path.commonpath([real_path, real_root])
    except ValueError as exc:  # different drives on Windows, etc.
        raise PathEscapeError(f"path {real_path!r} and root {real_root!r} have no common path") from exc
    if common != real_root:
        raise PathEscapeError(
            f"path {real_path!r} escapes canary root {real_root!r} (common={common!r})"
        )
    return path


def assert_canary_mode(enabled: bool, canary_root: Path | None) -> Path | None:
    """If canary mode is enabled, ensure ``canary_root`` is set and valid."""
    if not enabled:
        return canary_root
    if canary_root is None:
        raise PathEscapeError("canary mode requires NEWS_CANARY_ROOT to be set")
    return assert_path_within_canary(canary_root, canary_root=None)


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------


@contextmanager
def kind_flock(lock_path: Path, *, canary_root: Path | None) -> Iterator[Path]:
    """Acquire an exclusive ``fcntl`` flock on ``lock_path``.

    Non-blocking by default — ``LockContendedError`` is raised if
    another worker holds the lock.  ``NEWS_CANARY_ROOT`` containment
    is checked before the flock is taken.
    """
    assert_path_within_canary(lock_path, canary_root=canary_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if not lock_path.exists():
        # Create the file (O_CREAT) but not via symlink — we already
        # rejected symlinks above.
        lock_path.touch(mode=0o600)
    fd = os.open(str(lock_path), os.O_RDWR)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EWOULDBLOCK, errno.EAGAIN):
                raise LockContendedError(f"flock on {lock_path} is held by another worker") from exc
            raise
        yield lock_path
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


class LockContendedError(Exception):
    """The per-kind flock is held by another worker."""


# ---------------------------------------------------------------------------
# Job dispatch
# ---------------------------------------------------------------------------


@dataclass
class DispatchResult:
    exit_code: int
    stdout_bytes: int
    stdout_hash: str
    status: str
    error_class: str | None
    error_message: str | None


MAX_STDOUT_BYTES = 64 * 1024  # bounded capture to keep the runs table compact


class _BoundedTextCapture(io.TextIOBase):
    def __init__(self, limit: int = MAX_STDOUT_BYTES) -> None:
        self._limit = limit
        self._buffer = bytearray()

    def write(self, value: str) -> int:
        encoded = value.encode("utf-8", "replace")
        remaining = self._limit - len(self._buffer)
        if remaining > 0:
            self._buffer.extend(encoded[:remaining])
        return len(value)

    def flush(self) -> None:
        return None

    def bytes(self) -> bytes:
        return bytes(self._buffer)


def _dispatch(argv: list[str], *, env: dict[str, str] | None = None) -> DispatchResult:
    """Invoke ``news_pipeline.jobs.main`` with bounded stdout capture."""
    if any(flag in argv for flag in ("--enable-live-delivery",)):
        raise DeliveryForbiddenError(
            "dispatch refused: --enable-live-delivery is forbidden in this runtime"
        )
    if env is not None and env != dict(os.environ):
        raise ValueError("in-process dispatch does not accept an alternate environment")
    from news_pipeline import jobs

    stdout_capture = _BoundedTextCapture()
    stderr_capture = _BoundedTextCapture()
    try:
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exit_code = jobs.main(argv)
    except SystemExit as exc:
        exit_code = int(exc.code) if isinstance(exc.code, int) else 1
    except Exception as exc:
        return DispatchResult(
            exit_code=1,
            stdout_bytes=len(stdout_capture.bytes()),
            stdout_hash=hash_stdout(stdout_capture.bytes()),
            status="failed",
            error_class=sanitize_error_class(type(exc).__name__),
            error_message=sanitize_error_message(str(exc)),
        )
    stdout = stdout_capture.bytes()
    if exit_code == 0:
        return DispatchResult(
            exit_code=0,
            stdout_bytes=len(stdout),
            stdout_hash=hash_stdout(stdout),
            status="completed",
            error_class=None,
            error_message=None,
        )
    stderr = stderr_capture.bytes().decode("utf-8", "replace")
    err_class = _classify_runtime_error(stderr)
    return DispatchResult(
        exit_code=exit_code,
        stdout_bytes=len(stdout),
        stdout_hash=hash_stdout(stdout),
        status="failed",
        error_class=err_class,
        error_message=sanitize_error_message(stderr or f"jobs.main exit {exit_code}"),
    )


_RUNTIME_ERROR_MAP = {
    "RuntimeError": "RuntimeError",
    "ValueError": "ValueError",
    "TimeoutError": "TimeoutError",
    "FileNotFoundError": "FileNotFoundError",
    "PermissionError": "PermissionError",
    "OSError": "OSError",
    "sqlite3.OperationalError": "sqlite3.OperationalError",
    "sqlite3.DatabaseError": "sqlite3.DatabaseError",
    "sqlite3.IntegrityError": "sqlite3.IntegrityError",
}


def _classify_runtime_error(stderr: str) -> str:
    for needle, klass in _RUNTIME_ERROR_MAP.items():
        if needle in stderr:
            return klass
    return "RuntimeError"


# ---------------------------------------------------------------------------
# Validate kind (in-process)
# ---------------------------------------------------------------------------


def _read_state_health(state_db: Path | None) -> dict[str, Any]:
    """Read the news state DB health snapshot without ever writing it.

    Returns a bounded JSON-safe dict.  Never raises; the caller inspects
    ``schema_v6`` and ``ready`` to decide pass/fail.
    """
    if state_db is None:
        return {
            "schema_v6": False,
            "integrity": "unavailable",
            "ready": False,
            "error": "state_db_not_provided",
            "report_delivery_attempts": 0,
            "unresolved_delivery_attempts": 0,
        }
    try:
        from news_pipeline.news_health import health_snapshot
    except Exception as exc:  # pragma: no cover - import error path
        return {
            "schema_v6": False,
            "integrity": "unavailable",
            "ready": False,
            "error": f"news_health_import_failed:{type(exc).__name__}",
            "report_delivery_attempts": 0,
            "unresolved_delivery_attempts": 0,
        }
    try:
        snapshot = health_snapshot(state_db)
    except Exception as exc:
        return {
            "schema_v6": False,
            "integrity": "unavailable",
            "ready": False,
            "error": f"{type(exc).__name__}:{exc}",
            "report_delivery_attempts": 0,
            "unresolved_delivery_attempts": 0,
        }
    counts = snapshot.get("counts") or {}
    return {
        "schema_v6": bool(snapshot.get("schema_v6")),
        "integrity": snapshot.get("integrity", "unavailable"),
        "foreign_key_errors": snapshot.get("foreign_key_errors", []),
        "ready": bool(snapshot.get("ready")),
        "schema_versions": snapshot.get("schema_versions", []),
        "report_delivery_attempts": int(counts.get("report_delivery_attempts", 0)),
        "unresolved_delivery_attempts": int(
            snapshot.get("unresolved_delivery_attempts", 0)
        ),
        "schema_error": snapshot.get("schema_error"),
        "error": snapshot.get("error"),
    }


def run_validate(
    connection: sqlite3.Connection,
    *,
    state_db: Path | None = None,
) -> DispatchResult:
    """Run integrity / FK / schema / zero-delivery checks.

    The ``validate`` kind does *not* dispatch ``jobs.main``; it
    inspects the control store directly and, when ``state_db`` is
    provided, the news state DB read-only via
    ``news_pipeline.news_health.health_snapshot``.  Missing, corrupt,
    or non-v6 state DBs fail the validate kind; a valid synthetic v6
    state DB passes.
    """
    try:
        integrity = integrity_check(connection)
        fk_rows = foreign_key_check(connection)
        ver = schema_version(connection)
        delivery_count, delivery_kinds = zero_delivery_check(connection)
    except Exception as exc:
        return DispatchResult(
            exit_code=1,
            stdout_bytes=0,
            stdout_hash=hash_stdout(b""),
            status="failed",
            error_class=type(exc).__name__,
            error_message=sanitize_error_message(str(exc)),
        )
    state_health = _read_state_health(state_db)
    payload = json.dumps(
        {
            "integrity": integrity,
            "foreign_key_violations": [list(row) for row in fk_rows],
            "schema_version": ver,
            "delivery_count": delivery_count,
            "delivery_kinds": delivery_kinds,
            "state_db": str(state_db) if state_db is not None else None,
            "state_health": state_health,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if integrity != ["ok"]:
        return DispatchResult(
            exit_code=1,
            stdout_bytes=len(payload),
            stdout_hash=hash_stdout(payload),
            status="failed",
            error_class="sqlite3.DatabaseError",
            error_message=f"integrity_check reported {integrity!r}",
        )
    if fk_rows:
        return DispatchResult(
            exit_code=1,
            stdout_bytes=len(payload),
            stdout_hash=hash_stdout(payload),
            status="failed",
            error_class="sqlite3.IntegrityError",
            error_message=f"foreign_key_check reported {fk_rows!r}",
        )
    if delivery_count:
        return DispatchResult(
            exit_code=1,
            stdout_bytes=len(payload),
            stdout_hash=hash_stdout(payload),
            status="failed",
            error_class="DeliveryForbiddenError",
            error_message=f"zero-delivery check failed: found {delivery_kinds!r}",
        )
    # State DB must be v6-clean and healthy.  Missing, corrupt, non-v6,
    # integrity/FK failures, and unresolved delivery attempts all fail
    # validation; the bounded health details remain in the result hash
    # for operator diagnostics.
    if not state_health["schema_v6"] or not state_health["ready"]:
        if not state_health["schema_v6"]:
            reason = (
                state_health.get("schema_error")
                or state_health.get("error")
                or "state DB is not schema v6"
            )
        else:
            reasons: list[str] = []
            if state_health.get("integrity") != "ok":
                reasons.append("integrity check failed")
            if state_health.get("foreign_key_errors"):
                reasons.append("foreign-key check failed")
            if state_health.get("unresolved_delivery_attempts", 0):
                reasons.append("unresolved delivery attempts present")
            reason = "; ".join(reasons) or "state DB health check failed"
        return DispatchResult(
            exit_code=1,
            stdout_bytes=len(payload),
            stdout_hash=hash_stdout(payload),
            status="failed",
            error_class="sqlite3.DatabaseError",
            error_message=f"state DB failed v6 check: {reason}",
        )
    return DispatchResult(
        exit_code=0,
        stdout_bytes=len(payload),
        stdout_hash=hash_stdout(payload),
        status="completed",
        error_class=None,
        error_message=None,
    )


# ---------------------------------------------------------------------------
# Worker loop
# ---------------------------------------------------------------------------


@dataclass
class WorkerConfig:
    kind: str
    control_db: Path
    state_db: Path
    artifact_root: Path
    lock_path: Path
    owner: str
    claim_ttl_seconds: int
    canary_root: Path | None
    max_iterations: int = 0
    # When > 0, the worker spawns a renewal thread that calls
    # ``renew_claim`` at ``lease_renew_seconds`` intervals for any
    # in-flight dispatch.  Renewal uses a *separate* SQLite connection
    # opened from ``lease_renew_db_path`` (or, if unset, the
    # ``control_db``) because SQLite connections are not safe across
    # threads by default.  When 0 (default), no renewal is performed —
    # the dispatch is bounded by ``claim_ttl_seconds`` and the result
    # is at-least-once.
    lease_renew_seconds: int = 0
    lease_renew_db_path: Path | None = None
    # Test-only deterministic owner suffix counter.  When unset, a
    # monotonic process-local counter is used so two simultaneous
    # ``run_worker`` invocations in the same process do not share an
    # owner and accidentally inherit each other's claims.
    owner_counter: "Iterator[int] | None" = field(default=None, repr=False)


def _unique_owner(role: str, counter: "Iterator[int] | None" = None) -> str:
    """Compose ``role`` with a process/run identity.

    A bare static role causes ambiguity when a Compose restart lands on
    a live claim that another instance of the same role is holding.
    The suffix is ``pid + monotonic counter`` (or a caller-supplied
    iterator for tests).
    """
    if counter is None:
        return f"{role}@{os.getpid()}#{_OWNER_COUNTER.next()}"
    return f"{role}@{os.getpid()}#{next(counter)}"


class _MonotonicCounter:
    """Thread-safe monotonic counter used by ``_unique_owner``."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._n = 0

    def next(self) -> int:
        with self._lock:
            self._n += 1
            return self._n


_OWNER_COUNTER = _MonotonicCounter()


@dataclass(frozen=True)
class WorkerOutcome:
    """Counters returned by :func:`run_worker`.

    ``processed`` counts tasks that completed with a written run row.
    ``failed`` counts tasks whose dispatch returned ``status='failed'``
    or whose ``complete()`` could not persist the row (the truthful
    outcome is still recorded as failure).  ``fence_lost`` counts tasks
    whose renewer lost its lease; the takeover worker owns those.

    Use :attr:`had_failures` to decide whether the worker exit should
    be nonzero: any failed task or any fence loss makes the run
    unsuccessful even if some tasks also completed.
    """

    processed: int = 0
    failed: int = 0
    fence_lost: int = 0

    @property
    def had_failures(self) -> bool:
        return bool(self.failed or self.fence_lost)

    def __int__(self) -> int:
        return int(self.processed)


class _LeaseRenewer:
    """Fenced lease-renewal thread for long in-process dispatches.

    The renewer opens its own SQLite connection (SQLite connections are
    not safe across threads).  Each tick, it calls ``renew_claim``
    fenced by ``owner+generation``.  A fence loss is logged and the
    thread stops — the caller surfaces the fence loss truthfully and
    keeps the dispatch result at-least-once.
    """

    def __init__(
        self,
        *,
        control_db_path: Path,
        owner: str,
        ttl_seconds: int,
        interval_seconds: int,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._control_db_path = control_db_path
        self._owner = owner
        self._ttl_seconds = ttl_seconds
        self._interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._fence_lost = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def fence_lost(self) -> bool:
        return self._fence_lost.is_set()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name=f"lease-renewer[{self._owner}]",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self._interval_seconds * 2 + 1.0)

    def _run(self) -> None:
        connection = open_db(self._control_db_path)
        try:
            while not self._stop.is_set():
                # Sleep first so the caller has time to claim.
                if self._stop.wait(self._interval_seconds):
                    return
                if self._task_id is None or self._generation is None:
                    continue
                try:
                    renew_claim(
                        connection,
                        task_id=self._task_id,
                        owner=self._owner,
                        generation=self._generation,
                        ttl_seconds=self._ttl_seconds,
                    )
                except ClaimMismatchError:
                    # Fence loss — the claim has been taken over or
                    # expired.  Stop renewing; the caller will surface
                    # this truthfully.
                    self._fence_lost.set()
                    return
                except sqlite3.Error:
                    # Connection error — stop renewing; the dispatch
                    # may still complete but we cannot fence it.
                    self._fence_lost.set()
                    return
        finally:
            try:
                connection.close()
            except sqlite3.Error:
                pass

    _task_id: str | None = None
    _generation: int | None = None

    def bind(self, task_id: str, generation: int) -> None:
        self._task_id = task_id
        self._generation = generation
        self._fence_lost.clear()


def run_worker(
    connection: sqlite3.Connection,
    config: WorkerConfig,
    *,
    dispatch: Callable[[list[str]], DispatchResult] | None = None,
) -> WorkerOutcome:
    """Run the worker loop.  Returns a :class:`WorkerOutcome`.

    Ordering invariants:

    * The selector runs without the lock to find the next selectable
      row (pending or expired-claimed of the worker's own kind).
    * The per-kind ``kind_flock`` is acquired **before** ``claim`` so
      that lock contention cannot strand a claimed row.
    * ``complete`` runs inside the same flock so a takeover worker
      cannot race the completion.
    * If the flock is contended, the worker simply continues — no
      claim was made, so the row remains selectable on the next
      sweep.

    Failure handling:

    * Dispatch returning ``status='failed'`` increments ``failed``
      *and* ``processed`` (a truthful run row was written).
    * ``complete()`` raising (claim expired / taken over) increments
      ``failed`` and does NOT increment ``processed`` — the run row
      was not persisted, so the dispatch must NOT be silently counted
      as success.
    * Fence loss during renewal increments ``fence_lost`` and leaves
      the task selectable for the takeover worker; the dispatch is
      NOT silently counted as success.
    """
    if config.kind not in ALLOWED_KINDS:
        raise UnknownKindError(f"unknown worker kind {config.kind!r}")

    # Pre-open path containment — done *before* opening any DB
    # connection, bootstrap, or flock so a misconfigured deployment
    # cannot read or write a byte.  The lexical + symlink check runs
    # unconditionally so an "outside sentinel" (lockfile, parent dir)
    # cannot be created even when no canary root is configured.  When
    # ``canary_root`` is set, the realpath containment check is added
    # on top.
    for path in (config.control_db, config.state_db, config.artifact_root, config.lock_path):
        assert_path_within_canary(path, canary_root=config.canary_root)
    if config.canary_root is not None:
        assert_canary_mode(True, config.canary_root)
    # Production safety net — the canary_root may be None, but the
    # caller is still required to wire the four roots beneath any
    # operator-approved base.  We expose a dedicated production
    # rejection via ``PRODUCTION_FORBIDDEN`` env var.
    if os.environ.get("NEWS_CONTAINER_PRODUCTION") == "1":
        raise RuntimeError("worker refuses to run under NEWS_CONTAINER_PRODUCTION=1")
    if any(part in {"prod", "production"} for part in config.control_db.parts):
        raise RuntimeError(f"control DB path {config.control_db} looks like production")

    # Build the unique invocation owner once per worker run so a
    # process restart cannot inherit a same-role lease by accident.
    invocation_owner = _unique_owner(config.owner, config.owner_counter)

    processed = 0
    failed = 0
    fence_lost = 0
    iterations = 0
    while True:
        iterations += 1
        if config.max_iterations and iterations > config.max_iterations:
            return WorkerOutcome(
                processed=processed, failed=failed, fence_lost=fence_lost
            )
        task = _select_next(connection, config.kind)
        if task is None:
            if config.max_iterations:
                return WorkerOutcome(
                    processed=processed, failed=failed, fence_lost=fence_lost
                )
            import time
            time.sleep(1.0)
            continue

        task_id = task["task_id"]
        due_slot_utc = task["due_slot_utc"]
        payload = json.loads(task["payload_json"]) if task["payload_json"] else {}

        # Acquire the per-kind flock **before** claim so a contention
        # outcome never strands a claimed row.  The selector already
        # filtered out rows whose claim is held by another live owner.
        try:
            flock_cm = kind_flock(config.lock_path, canary_root=config.canary_root)
            with flock_cm:
                try:
                    generation, _attempt, _expires_at = claim(
                        connection,
                        task_id=task_id,
                        owner=invocation_owner,
                        ttl_seconds=config.claim_ttl_seconds,
                    )
                except ControlStoreError:
                    # Another worker claimed it between select and
                    # claim (e.g. expired takeover race) — try again.
                    continue

                renewer: _LeaseRenewer | None = None
                if config.lease_renew_seconds > 0:
                    renewer = _LeaseRenewer(
                        control_db_path=config.lease_renew_db_path or config.control_db,
                        owner=invocation_owner,
                        ttl_seconds=config.claim_ttl_seconds,
                        interval_seconds=config.lease_renew_seconds,
                    )
                    renewer.bind(task_id, generation)
                    renewer.start()

                try:
                    if config.kind == "validate":
                        result = run_validate(connection, state_db=config.state_db)
                    else:
                        argv_builder = _KIND_ARGV_BUILDERS[config.kind]
                        ctx = DispatchContext(
                            db_path=config.state_db,
                            artifact_root=config.artifact_root,
                            task_id=task_id,
                            due_slot_utc=due_slot_utc,
                            payload=payload,
                            control_db=config.control_db,
                            owner=invocation_owner,
                            generation=generation,
                        )
                        argv = argv_builder(ctx)
                        if dispatch is not None:
                            result = dispatch(argv)
                        else:
                            result = _dispatch(argv)
                except DeliveryForbiddenError as exc:
                    result = DispatchResult(
                        exit_code=2,
                        stdout_bytes=0,
                        stdout_hash=hash_stdout(b""),
                        status="failed",
                        error_class=type(exc).__name__,
                        error_message=sanitize_error_message(str(exc)),
                    )
                except Exception as exc:
                    result = DispatchResult(
                        exit_code=1,
                        stdout_bytes=0,
                        stdout_hash=hash_stdout(b""),
                        status="failed",
                        error_class=sanitize_error_class(type(exc).__name__),
                        error_message=sanitize_error_message(str(exc)),
                    )

                fence_lost_event = renewer is not None and renewer.fence_lost
                if renewer is not None:
                    renewer.stop()

                if fence_lost_event:
                    # Fence loss: do NOT record a completed run row.
                    # The takeover worker will pick the task up.  We
                    # log a truthful fence-loss marker to stderr and
                    # count it as a failure so the worker exits nonzero.
                    sys.stderr.write(
                        f"fence loss for {task_id} (owner={invocation_owner!r}); "
                        "leaving task selectable for takeover\n"
                    )
                    fence_lost += 1
                    continue

                try:
                    complete(
                        connection,
                        task_id=task_id,
                        owner=invocation_owner,
                        generation=generation,
                        status=result.status,
                        exit_code=result.exit_code,
                        stdout_hash=result.stdout_hash,
                        error_class=result.error_class,
                        error_message=result.error_message,
                        canary_root=str(config.canary_root) if config.canary_root is not None else None,
                    )
                except ControlStoreError as exc:
                    # The claim expired (or was taken over) mid-run.
                    # We do not write a run row; the takeover worker
                    # owns the outcome.  The dispatch must NOT be
                    # silently counted as success, so we count it as a
                    # failure and exit nonzero.
                    sys.stderr.write(f"complete failed for {task_id}: {exc}\n")
                    failed += 1
                    continue

                if result.status == "failed":
                    failed += 1
                processed += 1
        except LockContendedError:
            # No claim was made — simply try the next selectable row.
            continue


class UnknownKindError(ControlStoreError):
    """The worker kind is outside the allow-list."""


def _select_next(connection: sqlite3.Connection, kind: str) -> sqlite3.Row | None:
    """Selector: pending or expired-claimed work for ``kind`` only."""
    rows = selectable_tasks(connection, kind=kind, limit=1)
    return rows[0] if rows else None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="news-container-worker")
    parser.add_argument("--kind", required=True, choices=sorted(ALLOWED_KINDS))
    parser.add_argument("--control-db", required=True, type=Path)
    parser.add_argument("--state-db", required=True, type=Path)
    parser.add_argument("--artifact-root", required=True, type=Path)
    parser.add_argument("--lock-path", required=True, type=Path)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--claim-ttl-seconds", type=int, default=120)
    parser.add_argument("--max-iterations", type=int, default=0)
    parser.add_argument("--canary-root", type=Path, default=None)
    parser.add_argument("--schedule", type=Path, default=None)
    parser.add_argument("--bootstrap-once", action="store_true",
                        help="Before claiming, enqueue all due slots once (test helper).")
    parser.add_argument("--bootstrap-at", default=None,
                        help="UTC-Z timestamp for --bootstrap-once (required with --bootstrap-once).")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    canary_root = args.canary_root or (
        Path(os.environ["NEWS_CANARY_ROOT"]) if os.environ.get("NEWS_CANARY_ROOT") else None
    )
    if canary_root is not None:
        canary_root = assert_path_within_canary(canary_root, canary_root=None)

    # Validate every configured path before optional bootstrap can open
    # or create the control DB, state DB, artifact directory, or lock.
    for path in (args.control_db, args.state_db, args.artifact_root, args.lock_path):
        assert_path_within_canary(path, canary_root=canary_root)

    if args.bootstrap_once:
        if not args.schedule or not args.bootstrap_at:
            raise SystemExit("--bootstrap-once requires --schedule and --bootstrap-at")
        from .scheduler import enqueue_due_slots
        from .control_store import open as open_db_for_bootstrap
        from datetime import datetime as _dt
        blocks = parse_schedule_toml(args.schedule.read_text(encoding="utf-8"))
        bootstrap_at = datetime.fromisoformat(args.bootstrap_at.replace("Z", "+00:00"))
        bootstrap_conn = open_db_for_bootstrap(args.control_db)
        try:
            enqueue_due_slots(bootstrap_conn, blocks, now_utc=bootstrap_at, horizon_minutes=0)
        finally:
            bootstrap_conn.close()

    connection = open_db(args.control_db)
    try:
        config = WorkerConfig(
            kind=args.kind,
            control_db=args.control_db,
            state_db=args.state_db,
            artifact_root=args.artifact_root,
            lock_path=args.lock_path,
            owner=args.owner,
            claim_ttl_seconds=args.claim_ttl_seconds,
            canary_root=canary_root,
            max_iterations=args.max_iterations,
        )
        outcome = run_worker(connection, config)
        print(
            json.dumps(
                {
                    "kind": config.kind,
                    "processed": outcome.processed,
                    "failed": outcome.failed,
                    "fence_lost": outcome.fence_lost,
                },
                ensure_ascii=False,
            )
        )
        return 1 if outcome.had_failures else 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))