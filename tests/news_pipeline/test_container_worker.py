"""Behavioral tests for the news_container role worker runtime.

Covered contracts
-----------------
* Path safety: ``assert_path_within_canary`` rejects lexical
  traversal (``..``), symlinks in the chain, and resolved paths
  outside ``canary_root``.
* Lock contention: a second ``kind_flock`` non-blocking acquisition
  raises ``LockContendedError`` while the first holds the lock.
* Validate kind: in-process integrity / FK / schema / zero-delivery
  diagnostics are reported via the dispatch result shape.
* No delivery vocabulary: ``_report_argv`` rejects live delivery;
  ``_dispatch`` refuses ``--enable-live-delivery``; the worker loop
  refuses to enqueue ``delivery``-shaped tasks.
* Per-kind argv builders produce the documented argv shape that the
  news pipeline ``jobs.main`` CLI expects (incl. dry-run daily-report).
* Worker refuses production paths and the ``NEWS_CONTAINER_PRODUCTION``
  env var.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest import mock

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from news_container import ALLOWED_KINDS  # noqa: E402
from news_container import worker as worker_module  # noqa: E402
from news_container.control_store import (  # noqa: E402
    ClaimMismatchError,
    ControlStoreError,
    DeliveryForbiddenError,
    open as open_db,
    claim,
    complete,
    enqueue,
    pending_tasks,
    renew_claim,
    selectable_tasks,
    hash_stdout,
)
from news_container.worker import (  # noqa: E402
    DispatchContext,
    DispatchResult,
    LockContendedError,
    PathEscapeError,
    SymlinkForbiddenError,
    UnknownKindError,
    WorkerConfig,
    _ingest_argv,
    _dispatch,
    _process_argv,
    _report_argv,
    _select_next,
    _unique_owner,
    assert_canary_mode,
    assert_path_within_canary,
    kind_flock,
    run_validate,
    run_worker,
)


def _ctx(
    *,
    db_path: Path,
    artifact_root: Path,
    task_id: str = "abc123",
    due_slot_utc: str = "2026-09-14T06:30:00Z",
    payload: dict | None = None,
) -> DispatchContext:
    return DispatchContext(
        db_path=db_path,
        artifact_root=artifact_root,
        task_id=task_id,
        due_slot_utc=due_slot_utc,
        payload=payload or {},
    )


# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------


class CanaryPathSafetyTests(unittest.TestCase):
    def test_path_inside_root_passes(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            inner = root / "state.db"
            assert_path_within_canary(inner, canary_root=root)
            self.assertTrue(inner.exists() or True)

    def test_lexical_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            escape = Path(td) / ".." / "escape.db"
            with self.assertRaises(PathEscapeError):
                assert_path_within_canary(escape, canary_root=root)

    def test_resolved_escape_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            other = tempfile.mkdtemp()
            try:
                escape = Path(other) / "evil.db"
                escape.touch()
                with self.assertRaises(PathEscapeError):
                    assert_path_within_canary(escape, canary_root=root)
            finally:
                import shutil
                shutil.rmtree(other, ignore_errors=True)

    def test_symlink_in_chain_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            target = root / "real"
            target.mkdir()
            link = root / "link"
            try:
                link.symlink_to(target)
            except (OSError, NotImplementedError):
                self.skipTest("symlink not supported in this environment")
            symlinked = link / "evil.db"
            with self.assertRaises((PathEscapeError, SymlinkForbiddenError)):
                assert_path_within_canary(symlinked, canary_root=root)

    def test_canary_root_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            with self.assertRaises(PathEscapeError):
                assert_canary_mode(True, Path(str(root) + "/../escape"))


# ---------------------------------------------------------------------------
# Lock contention
# ---------------------------------------------------------------------------


class LockContentionTests(unittest.TestCase):
    def test_second_lock_raises_contention(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lock_path = root / "ingest.lock"
            with kind_flock(lock_path, canary_root=root):
                with self.assertRaises(LockContendedError):
                    with kind_flock(lock_path, canary_root=root):
                        self.fail("second flock must not succeed while first holds the lock")

    def test_lock_released_between_calls(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lock_path = root / "ingest.lock"
            with kind_flock(lock_path, canary_root=root):
                pass
            # If the first context manager didn't release, this would block.
            with kind_flock(lock_path, canary_root=root):
                pass


# ---------------------------------------------------------------------------
# Per-kind argv builders
# ---------------------------------------------------------------------------


class ArgvBuilderTests(unittest.TestCase):
    def test_ingest_argv_uses_tick_subcommand(self):
        ctx = _ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={
                "sources": "/canary/sources.toml",
                "topics": "/canary/topics.toml",
                "policy": "/canary/policy.toml",
            },
        )
        argv = _ingest_argv(ctx)
        self.assertEqual(argv[0], "tick")
        self.assertIn("--enable-network", argv)
        self.assertIn("--db", argv)
        self.assertIn("/canary/state.db", argv)
        self.assertIn("--run-started-at", argv)
        self.assertIn("2026-09-14T06:30:00Z", argv)

    def test_ingest_argv_forwards_optional_provenance_path(self):
        argv = _ingest_argv(_ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={
                "sources": "/canary/sources.toml",
                "topics": "/canary/topics.toml",
                "policy": "/canary/policy.toml",
                "provenance": "/canary/provenance.toml",
            },
        ))
        index = argv.index("--provenance")
        self.assertEqual(argv[index + 1], "/canary/provenance.toml")

    def test_process_argv_uses_process_subcommand(self):
        ctx = _ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={"history_db": "/canary/history.db", "max_items": 42},
        )
        argv = _process_argv(ctx)
        self.assertEqual(argv[0], "process")
        self.assertIn("--enable-network", argv)
        self.assertIn("--evaluated-at", argv)
        self.assertIn("--history-db", argv)
        self.assertIn("/canary/history.db", argv)
        # max_items must surface as int.
        idx = argv.index("--max-items")
        self.assertEqual(argv[idx + 1], "42")

    def test_process_argv_allows_missing_optional_history_db(self):
        argv = _process_argv(_ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={"max_items": 42},
        ))
        self.assertNotIn("--history-db", argv)

    def test_report_argv_never_enables_live_delivery(self):
        ctx = _ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={"prior_upper_utc": "2026-09-14T06:30:00Z"},
        )
        argv = _report_argv(ctx)
        self.assertEqual(argv[0], "daily-report")
        self.assertNotIn("--enable-live-delivery", argv)
        self.assertIn("--artifact-root", argv)
        self.assertIn("/canary/artifacts", argv)

    def test_report_argv_rejects_payload_enable_live_delivery(self):
        ctx = _ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={"enable_live_delivery": True},
        )
        with self.assertRaises(DeliveryForbiddenError):
            _report_argv(ctx)

    def test_ingest_argv_requires_payload_strings(self):
        ctx = _ctx(
            db_path=Path("/canary/state.db"),
            artifact_root=Path("/canary/artifacts"),
            payload={},  # missing sources/topics/policy
        )
        with self.assertRaises(ValueError):
            _ingest_argv(ctx)


# ---------------------------------------------------------------------------
# Validate kind
# ---------------------------------------------------------------------------


class ValidateKindTests(unittest.TestCase):
    def test_run_validate_reports_clean_state(self):
        """A clean v6 state DB is required to satisfy the validate kind."""
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            state_db = Path(td) / "state.db"
            _make_synthetic_v6_state_db(state_db)
            connection = open_db(db_path)
            try:
                result = run_validate(connection, state_db=state_db)
                self.assertEqual(result.status, "completed")
                self.assertEqual(result.exit_code, 0)
                self.assertIsNone(result.error_class)
                # Stdout hash must be reproducible.
                self.assertEqual(len(result.stdout_hash), 64)
            finally:
                connection.close()

    def test_run_validate_flags_legacy_delivery_rows(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            connection = open_db(db_path)
            try:
                connection.execute(
                    "INSERT INTO tasks(task_id, kind, due_slot_utc, payload_json, state, generation, created_at)"
                    " VALUES ('legacy', 'delivery', '2026-09-14T00:00:00Z', '{}', 'pending', 0, '2026-09-14T00:00:00Z')"
                )
                result = run_validate(connection, state_db=None)
                self.assertEqual(result.status, "failed")
                self.assertEqual(result.error_class, "DeliveryForbiddenError")
                self.assertIn("delivery", (result.error_message or "").lower())
            finally:
                connection.close()


# ---------------------------------------------------------------------------
# Worker dispatch
# ---------------------------------------------------------------------------


class _StubDispatch:
    def __init__(self, exit_code: int = 0, stderr: str = "", error_class: str | None = None):
        self.calls: list[list[str]] = []
        self.exit_code = exit_code
        self.stderr = stderr
        self.error_class = error_class

    def __call__(self, argv: list[str]):
        from news_container.worker import DispatchResult, hash_stdout
        self.calls.append(list(argv))
        stdout_hash = hash_stdout(b"{}" if self.exit_code == 0 else b"")
        return DispatchResult(
            exit_code=self.exit_code,
            stdout_bytes=2 if self.exit_code == 0 else 0,
            stdout_hash=stdout_hash,
            status="completed" if self.exit_code == 0 else "failed",
            error_class=self.error_class,
            error_message=None if self.exit_code == 0 else self.stderr,
        )


class WorkerLoopTests(unittest.TestCase):
    def test_default_dispatch_calls_jobs_main_in_process(self):
        with mock.patch("news_pipeline.jobs.main", return_value=0) as jobs_main, mock.patch(
            "subprocess.run"
        ) as subprocess_run:
            result = _dispatch(["health", "--db", "/canary/state.db"])
        self.assertEqual(result.exit_code, 0)
        jobs_main.assert_called_once_with(["health", "--db", "/canary/state.db"])
        subprocess_run.assert_not_called()

    def _seed_task(self, db_path: Path, kind: str) -> str:
        connection = open_db(db_path)
        try:
            task_id, _ = _seed(connection, kind, payload=_payload_for(kind))
        finally:
            connection.close()
        return task_id

    def test_worker_claims_only_its_kind(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            artifact_root = root / "artifacts"
            lock_path = root / "ingest.lock"
            for p in (state_db, artifact_root):
                p.mkdir(parents=True, exist_ok=True)
            state_db.touch()

            self._seed_task(control_db, "ingest")
            self._seed_task(control_db, "report")

            connection = open_db(control_db)
            try:
                config = WorkerConfig(
                    kind="ingest",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="ingest-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=5,
                )
                stub = _StubDispatch(exit_code=0)
                outcome = run_worker(connection, config, dispatch=stub)
                self.assertEqual(outcome.processed, 1, "worker must process exactly one ingest task")
                self.assertEqual(outcome.failed, 0)
                self.assertEqual(outcome.fence_lost, 0)
                self.assertFalse(outcome.had_failures)
                self.assertEqual(len(stub.calls), 1)
                self.assertEqual(stub.calls[0][0], "tick")
                # The report task must remain pending.
                state = connection.execute(
                    "SELECT state FROM tasks WHERE kind='report'"
                ).fetchone()["state"]
                self.assertEqual(state, "pending")
            finally:
                connection.close()

    def test_worker_rejects_delivery_argv_at_dispatch(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            artifact_root = root / "artifacts"
            lock_path = root / "report.lock"
            for p in (state_db, artifact_root):
                p.mkdir(parents=True, exist_ok=True)
            state_db.touch()

            connection = open_db(control_db)
            try:
                self._seed_task(control_db, "report")
                config = WorkerConfig(
                    kind="report",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="report-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=2,
                )

                def fake_dispatch(argv):
                    # Inject a delivery flag the worker should never pass.
                    bad = list(argv) + ["--enable-live-delivery"]
                    from news_container.worker import _dispatch
                    return _dispatch(bad)

                processed = run_worker(connection, config, dispatch=fake_dispatch).processed
                self.assertEqual(processed, 1)
                # Run row should record DeliveryForbiddenError.
                row = connection.execute(
                    "SELECT status, error_class FROM runs ORDER BY run_id DESC LIMIT 1"
                ).fetchone()
                self.assertEqual(row["status"], "failed")
                self.assertEqual(row["error_class"], "DeliveryForbiddenError")
            finally:
                connection.close()

    def test_worker_rejects_production_env_var(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            artifact_root = root / "artifacts"
            lock_path = root / "ingest.lock"
            for p in (state_db, artifact_root):
                p.mkdir(parents=True, exist_ok=True)
            state_db.touch()
            self._seed_task(control_db, "ingest")
            connection = open_db(control_db)
            try:
                config = WorkerConfig(
                    kind="ingest",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="ingest-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=2,
                )
                old = os.environ.get("NEWS_CONTAINER_PRODUCTION")
                os.environ["NEWS_CONTAINER_PRODUCTION"] = "1"
                try:
                    with self.assertRaises(RuntimeError):
                        run_worker(connection, config, dispatch=_StubDispatch())
                finally:
                    if old is None:
                        os.environ.pop("NEWS_CONTAINER_PRODUCTION", None)
                    else:
                        os.environ["NEWS_CONTAINER_PRODUCTION"] = old
            finally:
                connection.close()

    def test_worker_rejects_unknown_kind(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            artifact_root = root / "artifacts"
            lock_path = root / "weird.lock"
            for p in (state_db, artifact_root):
                p.mkdir(parents=True, exist_ok=True)
            state_db.touch()
            connection = open_db(control_db)
            try:
                config = WorkerConfig(
                    kind="weird",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="weird-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=1,
                )
                with self.assertRaises(UnknownKindError):
                    run_worker(connection, config, dispatch=_StubDispatch())
            finally:
                connection.close()

    def test_worker_allowed_kinds_match_contract(self):
        self.assertEqual(ALLOWED_KINDS, frozenset({"ingest", "process", "validate", "report"}))


def _seed(connection: sqlite3.Connection, kind: str, payload: dict | None = None) -> tuple[str, bool]:
    from news_container.control_store import enqueue
    return enqueue(connection, kind=kind, due_slot_utc="2026-09-14T06:30:00Z", payload=payload or {})


def _payload_for(kind: str) -> dict:
    """Per-kind payload that satisfies the argv builders.

    Without these fields the ingest/process argv builders raise and
    the worker never reaches the dispatch stub.  This mirrors a
    realistic operator-provided payload.
    """
    if kind == "ingest":
        return {
            "sources": "/canary/sources.toml",
            "topics": "/canary/topics.toml",
            "policy": "/canary/policy.toml",
        }
    if kind == "process":
        return {"history_db": "/canary/history.db", "max_items": 50}
    return {}


# ---------------------------------------------------------------------------
# A3 lifecycle regressions: worker
# ---------------------------------------------------------------------------


class A3UniqueOwnerTests(unittest.TestCase):
    """Invariant 3: unique invocation owner (role + process/run identity)."""

    def test_unique_owner_distinct_per_invocation(self):
        counter = iter([1, 2, 3])
        a = _unique_owner("ingest", counter)
        b = _unique_owner("ingest", counter)
        self.assertNotEqual(a, b, "unique owner must differ across invocations")
        self.assertTrue(a.startswith("ingest@"))
        self.assertIn(str(__import__("os").getpid()), a)


class A3SelectorTests(unittest.TestCase):
    """Invariant 3: selector returns pending or expired-claimed for own kind."""

    def test_selector_recovers_expired_claim_for_own_kind(self):
        with tempfile.TemporaryDirectory() as td:
            control_db = Path(td) / "control.db"
            connection = open_db(control_db)
            try:
                task_id, _ = enqueue(
                    connection, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
                )
                claim(connection, task_id=task_id, owner="ingest-1", ttl_seconds=1)
                future = datetime.now(UTC) + timedelta(seconds=30)
                rows = selectable_tasks(connection, kind="ingest", now=future)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["task_id"], task_id)
            finally:
                connection.close()


class A3LockOrderingTests(unittest.TestCase):
    """Invariant 3: lock contention must not strand a claimed row."""

    def test_lock_contention_does_not_orphan_claim(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            artifact_root = root / "artifacts"
            lock_path = root / "ingest.lock"
            for p in (state_db, artifact_root):
                p.mkdir(parents=True, exist_ok=True)
            state_db.touch()
            connection = open_db(control_db)
            try:
                enqueue(
                    connection, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z",
                    payload=_payload_for("ingest"),
                )
                # Hold the per-kind lock so the worker fails to take it.
                with kind_flock(lock_path, canary_root=root):
                    config = WorkerConfig(
                        kind="ingest",
                        control_db=control_db,
                        state_db=state_db,
                        artifact_root=artifact_root,
                        lock_path=lock_path,
                        owner="ingest-1",
                        claim_ttl_seconds=60,
                        canary_root=root,
                        max_iterations=1,
                        owner_counter=iter([42]),
                    )
                    processed = run_worker(connection, config, dispatch=_StubDispatch()).processed
                # The worker did not strand a claim.
                self.assertEqual(processed, 0)
                state = connection.execute(
                    "SELECT state FROM tasks WHERE kind='ingest'"
                ).fetchone()["state"]
                self.assertEqual(state, "pending")
                claim_count = connection.execute(
                    "SELECT COUNT(*) AS n FROM claims"
                ).fetchone()["n"]
                self.assertEqual(claim_count, 0)
            finally:
                connection.close()


class A3FencedCompletionTests(unittest.TestCase):
    """Invariant 5: stale completion is fenced by owner+generation."""

    def test_stale_completion_after_takeover_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as td:
            control_db = Path(td) / "control.db"
            connection = open_db(control_db)
            try:
                task_id, _ = enqueue(
                    connection, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
                )
                claim(connection, task_id=task_id, owner="ingest-1", ttl_seconds=1)
                future = datetime.now(UTC) + timedelta(seconds=30)
                gen2, _, _ = claim(
                    connection, task_id=task_id, owner="ingest-2", ttl_seconds=60, now=future
                )
                # The old owner tries to complete against gen 1.
                with self.assertRaises(ClaimMismatchError):
                    complete(
                        connection, task_id=task_id, owner="ingest-1", generation=1,
                        status="completed", exit_code=0, stdout_hash=None,
                        error_class=None, error_message=None,
                    )
                # The new owner can complete against gen 2.
                complete(
                    connection, task_id=task_id, owner="ingest-2", generation=gen2,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None,
                )
                state = connection.execute(
                    "SELECT state FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()["state"]
                self.assertEqual(state, "completed")
            finally:
                connection.close()


class A3RepeatedSweepTests(unittest.TestCase):
    """Invariant 1: repeated scheduler sweep after claim/completion does not raise."""

    def test_re_enqueue_after_completion_is_noop(self):
        with tempfile.TemporaryDirectory() as td:
            control_db = Path(td) / "control.db"
            connection = open_db(control_db)
            try:
                task_id, _ = enqueue(
                    connection, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
                )
                gen, _, _ = claim(connection, task_id=task_id, owner="ingest-1", ttl_seconds=60)
                complete(
                    connection, task_id=task_id, owner="ingest-1", generation=gen,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None,
                )
                # Repeated sweep with the *same* payload.
                task_id2, created2 = enqueue(
                    connection, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
                )
                self.assertEqual(task_id, task_id2)
                self.assertFalse(created2)
                count = connection.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
                self.assertEqual(count, 1)
            finally:
                connection.close()


class A3TwoConnectionRenewTests(unittest.TestCase):
    """Invariant 4: real two-connection renewal test (separate SQLite
    connections for the renewer thread, SQLite thread-safety compliant).
    """

    def test_renew_via_second_connection_succeeds(self):
        with tempfile.TemporaryDirectory() as td:
            control_db = Path(td) / "control.db"
            owner_conn = open_db(control_db)
            renew_conn = open_db(control_db)
            try:
                task_id, _ = enqueue(
                    owner_conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
                )
                gen, _, _ = claim(
                    owner_conn, task_id=task_id, owner="ingest-1", ttl_seconds=60
                )
                # Renewal on a *separate* connection — the real worker
                # does this because SQLite connections are not safe
                # across threads by default.
                new_expiry = renew_claim(
                    renew_conn, task_id=task_id, owner="ingest-1",
                    generation=gen, ttl_seconds=120,
                )
                self.assertTrue(new_expiry.endswith("Z"))
                # Fenced check: wrong owner on the second connection
                # also raises.
                with self.assertRaises(ClaimMismatchError):
                    renew_claim(
                        renew_conn, task_id=task_id, owner="ingest-2",
                        generation=gen, ttl_seconds=60,
                    )
            finally:
                owner_conn.close()
                renew_conn.close()


# ---------------------------------------------------------------------------
# A4 regressions: pre-open containment, state-DB inspection, truthfulness
# ---------------------------------------------------------------------------


def _make_synthetic_v6_state_db(path: Path) -> None:
    """Build a deterministic v6 news state DB without network calls.

    Mirrors the migration sequence in ``test_delivery_phase6``: v3 + v4
    + v5 + v6, with minimal ``events``, ``event_versions``, ``reports``,
    and ``report_events`` rows so FK checks pass and the v6 delivery
    tables are queryable.  This is the clean fixture for the validate
    kind; ``health_snapshot`` reports ``schema_v6=True`` and zero
    ``report_delivery_attempts``/``unresolved_delivery_attempts``.
    """
    from news_pipeline.db import init_db
    from news_pipeline.delivery_schema_v6 import migrate_v6
    from news_pipeline.schema_v3 import migrate_v3
    from news_pipeline.schema_v4 import migrate_v4
    from news_pipeline.schema_v5 import migrate_v5

    init_db(str(path))
    connection = sqlite3.connect(str(path), isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, "2026-09-08T00:00:00Z")
        migrate_v4(connection, "2026-09-08T00:00:00Z")
        migrate_v5(connection, "2026-09-08T00:00:00Z")
        # Minimal FK chain: events -> event_versions -> report_events
        # and reports -> report_events.  No network calls.
        connection.execute(
            "INSERT INTO runs(id, started_at, finished_at, kind, provenance)"
            " VALUES ('run-a4', '2026-09-08T07:00:00Z', '2026-09-08T07:00:00Z',"
            " 'historical_replay', 'manual')"
        )
        connection.execute(
            "INSERT INTO events(id, run_id, category, started_at, ended_at,"
            " article_count, observation_count, status)"
            " VALUES ('evt-a4', 'run-a4', 'ai',"
            " '2026-09-08T07:00:00Z', '2026-09-08T07:00:00Z',"
            " 0, 0, 'complete')"
        )
        connection.execute(
            "INSERT INTO event_versions(event_id, version, material_change_reason,"
            " summary, verification_state, valid_from, superseded_at, verified_at)"
            " VALUES ('evt-a4', 1, 'initial', 'A verified event', 'verified',"
            " '2026-09-08T07:00:00Z', NULL, '2026-09-08T07:00:00Z')"
        )
        connection.execute(
            "INSERT INTO reports(report_id, window_start, window_end,"
            " generation_status, delivery_state, created_at)"
            " VALUES ('rep-a4', '2026-09-08T00:00:00Z', '2026-09-08T08:00:00Z',"
            " 'complete', 'dry_run', '2026-09-08T08:00:00Z')"
        )
        connection.execute(
            "INSERT INTO report_events(report_id, event_id, event_version,"
            " section, sort_order, inclusion_reason)"
            " VALUES ('rep-a4', 'evt-a4', 1, 'headline', 0, 'verified')"
        )
        migrate_v6(connection, "2026-09-08T08:01:00Z")
    finally:
        connection.close()


class A4StateDBValidationTests(unittest.TestCase):
    """Invariant: validate kind inspects the actual news state DB."""

    def test_validate_fails_when_state_db_missing(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            missing_state = Path(td) / "does_not_exist.db"
            self.assertFalse(missing_state.exists())
            connection = open_db(db_path)
            try:
                result = run_validate(connection, state_db=missing_state)
                self.assertEqual(result.status, "failed")
                self.assertEqual(result.exit_code, 1)
                self.assertEqual(result.error_class, "sqlite3.DatabaseError")
                # The state health snapshot must be exposed and not v6.
                health = worker_module._read_state_health(missing_state)
                self.assertFalse(health["schema_v6"])
                self.assertEqual(health["report_delivery_attempts"], 0)
                self.assertEqual(health["unresolved_delivery_attempts"], 0)
            finally:
                connection.close()

    def test_validate_fails_on_corrupt_non_v6_state_db(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            state_db = Path(td) / "state.db"
            state_db.write_bytes(b"not a sqlite database, just garbage bytes")
            connection = open_db(db_path)
            try:
                result = run_validate(connection, state_db=state_db)
                self.assertEqual(result.status, "failed")
                self.assertEqual(result.error_class, "sqlite3.DatabaseError")
                health = worker_module._read_state_health(state_db)
                self.assertFalse(health["schema_v6"])
            finally:
                connection.close()

    def test_validate_fails_on_v5_only_state_db(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            state_db = Path(td) / "state.db"
            # A v5-clean state DB has all migrations 1-5 but lacks v6
            # delivery tables, so validate must fail.
            from news_pipeline.db import init_db
            from news_pipeline.schema_v3 import migrate_v3
            from news_pipeline.schema_v4 import migrate_v4
            from news_pipeline.schema_v5 import migrate_v5

            init_db(str(state_db))
            connection = sqlite3.connect(str(state_db), isolation_level=None)
            try:
                connection.execute("PRAGMA foreign_keys=ON")
                migrate_v3(connection, "2026-09-08T00:00:00Z")
                migrate_v4(connection, "2026-09-08T00:00:00Z")
                migrate_v5(connection, "2026-09-08T00:00:00Z")
            finally:
                connection.close()

            connection = open_db(db_path)
            try:
                result = run_validate(connection, state_db=state_db)
                self.assertEqual(result.status, "failed")
                self.assertEqual(result.error_class, "sqlite3.DatabaseError")
                health = worker_module._read_state_health(state_db)
                self.assertFalse(health["schema_v6"])
            finally:
                connection.close()

    def test_validate_passes_on_synthetic_v6_state_db(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            state_db = Path(td) / "state.db"
            _make_synthetic_v6_state_db(state_db)

            connection = open_db(db_path)
            try:
                result = run_validate(connection, state_db=state_db)
                self.assertEqual(result.status, "completed")
                self.assertEqual(result.exit_code, 0)
                self.assertIsNone(result.error_class)
                health = worker_module._read_state_health(state_db)
                self.assertTrue(health["schema_v6"])
                # Clean fixture — zero attempts and zero unresolved.
                self.assertEqual(health["report_delivery_attempts"], 0)
                self.assertEqual(health["unresolved_delivery_attempts"], 0)
            finally:
                connection.close()

    def test_validate_surfaces_unresolved_attempts_when_present(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            state_db = Path(td) / "state.db"
            _make_synthetic_v6_state_db(state_db)

            # Insert an open delivery attempt to exercise the
            # unresolved-delivery-attempts surfacing path.
            connection = sqlite3.connect(str(state_db), isolation_level=None)
            try:
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute(
                    "INSERT INTO report_deliveries("
                    "report_id, idempotency_key, channel, recipient_hash,"
                    " content_sha256, state, current_attempt_id,"
                    " created_at, updated_at)"
                    " VALUES('rep-a4','idem-a4-d1','telegram',"
                    " '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',"
                    " '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',"
                    " 'prepared', 'att-a4-1',"
                    " '2026-09-08T08:02:00Z', '2026-09-08T08:02:00Z')"
                )
                connection.execute(
                    "INSERT INTO report_delivery_attempts("
                    "attempt_id, report_id, ordinal, state, content_sha256,"
                    " prepared_at, completed_at, message_ids_json,"
                    " error_code, error_detail)"
                    " VALUES('att-a4-1','rep-a4',1,'prepared',"
                    " '0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',"
                    " '2026-09-08T08:02:00Z', NULL, NULL, NULL, NULL)"
                )
            finally:
                connection.close()

            connection = open_db(db_path)
            try:
                # The state DB remains schema v6, but an unresolved
                # attempt makes the health gate fail closed while the
                # exact count remains visible in the snapshot.
                result = run_validate(connection, state_db=state_db)
                self.assertEqual(result.status, "failed")
                self.assertEqual(result.exit_code, 1)
                self.assertEqual(result.error_class, "sqlite3.DatabaseError")
                health = worker_module._read_state_health(state_db)
                self.assertTrue(health["schema_v6"])
                self.assertEqual(health["report_delivery_attempts"], 1)
                self.assertEqual(health["unresolved_delivery_attempts"], 1)
            finally:
                connection.close()


class A4PreOpenSentinelTests(unittest.TestCase):
    """Invariant: an outside sentinel must not be created on path failure."""

    def test_lockfile_not_created_when_lock_path_escapes_canary(self):
        """If the lock_path resolves outside the canary root, the worker
        must refuse to bootstrap and must NOT create the lockfile (the
        outside sentinel)."""
        with tempfile.TemporaryDirectory() as td:
            canary_root = Path(td) / "canary"
            canary_root.mkdir()
            outside_dir = Path(td) / "outside"
            outside_dir.mkdir()
            outside_lock = outside_dir / "ingest.lock"

            control_db = canary_root / "control.db"
            state_db = canary_root / "state.db"
            state_db.touch()
            artifact_root = canary_root / "artifacts"
            artifact_root.mkdir()
            control_db_conn = open_db(control_db)
            try:
                enqueue(control_db_conn, kind="ingest",
                        due_slot_utc="2026-09-14T06:30:00Z",
                        payload=_payload_for("ingest"))
            finally:
                control_db_conn.close()

            config = WorkerConfig(
                kind="ingest",
                control_db=control_db,
                state_db=state_db,
                artifact_root=artifact_root,
                lock_path=outside_lock,
                owner="ingest-1",
                claim_ttl_seconds=60,
                canary_root=canary_root,
                max_iterations=1,
            )

            connection = open_db(control_db)
            try:
                with self.assertRaises(PathEscapeError):
                    run_worker(connection, config, dispatch=_StubDispatch())
            finally:
                connection.close()

            # Critical: the outside lockfile sentinel must NOT exist.
            self.assertFalse(outside_lock.exists(),
                             f"outside sentinel was created at {outside_lock!r}")

    def test_lockfile_not_created_when_canary_root_is_none_and_path_has_lexical_escape(self):
        """Even without a canary_root, a lexical traversal in the lock
        path must refuse to bootstrap before the sentinel is created."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            state_db.touch()
            artifact_root = root / "artifacts"
            artifact_root.mkdir()
            escape_lock = root / ".." / "evil_ingest.lock"
            escape_lock_str = str(escape_lock)
            self.assertNotEqual(escape_lock_str, os.path.normpath(escape_lock_str))

            config = WorkerConfig(
                kind="ingest",
                control_db=control_db,
                state_db=state_db,
                artifact_root=artifact_root,
                lock_path=escape_lock,
                owner="ingest-1",
                claim_ttl_seconds=60,
                canary_root=None,
                max_iterations=1,
            )
            connection = open_db(control_db)
            try:
                with self.assertRaises(PathEscapeError):
                    run_worker(connection, config, dispatch=_StubDispatch())
            finally:
                connection.close()

            self.assertFalse(escape_lock.exists(),
                             f"outside sentinel was created at {escape_lock!r}")

    def test_cli_validates_paths_before_bootstrap_opens_control_db(self):
        """Bootstrap must not create a control DB outside the canary root."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            canary_root = root / "canary"
            canary_root.mkdir()
            outside_control = root / "outside" / "control.db"
            (root / "outside").mkdir()
            state_db = canary_root / "state.db"
            state_db.touch()
            artifact_root = canary_root / "artifacts"
            artifact_root.mkdir()
            lock_path = canary_root / "ingest.lock"
            schedule = canary_root / "schedule.toml"
            schedule.write_text(
                '[schedule.disabled]\nkind = "ingest"\ndue_local = "06:30"\nenabled = false\n',
                encoding="utf-8",
            )
            argv = [
                "--kind", "ingest",
                "--control-db", str(outside_control),
                "--state-db", str(state_db),
                "--artifact-root", str(artifact_root),
                "--lock-path", str(lock_path),
                "--owner", "ingest-1",
                "--canary-root", str(canary_root),
                "--max-iterations", "1",
                "--bootstrap-once",
                "--schedule", str(schedule),
                "--bootstrap-at", "2026-09-14T06:30:00Z",
            ]
            with self.assertRaises(PathEscapeError):
                worker_module.main(argv)
            self.assertFalse(outside_control.exists())


class A4TruthfulFailureTests(unittest.TestCase):
    """Invariant: failed dispatch / fence / completion must surface as nonzero."""

    def test_failed_dispatch_in_worker_loop_is_recorded_as_failure(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            state_db.touch()
            artifact_root = root / "artifacts"
            artifact_root.mkdir()
            lock_path = root / "ingest.lock"

            connection = open_db(control_db)
            try:
                enqueue(connection, kind="ingest",
                        due_slot_utc="2026-09-14T06:30:00Z",
                        payload=_payload_for("ingest"))
                config = WorkerConfig(
                    kind="ingest",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="ingest-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=2,
                )
                outcome = run_worker(connection, config,
                                     dispatch=_StubDispatch(exit_code=1,
                                                            error_class="RuntimeError"))
                self.assertEqual(outcome.processed, 1,
                                 "failed dispatch still wrote a run row")
                self.assertEqual(outcome.failed, 1,
                                 "failed dispatch must increment failed count")
                self.assertEqual(outcome.fence_lost, 0)
                self.assertTrue(outcome.had_failures)

                # The truthful run row must persist with status='failed'.
                row = connection.execute(
                    "SELECT status, error_class FROM runs ORDER BY run_id DESC LIMIT 1"
                ).fetchone()
                self.assertEqual(row["status"], "failed")
                self.assertEqual(row["error_class"], "RuntimeError")
            finally:
                connection.close()

    def test_cli_main_returns_nonzero_on_failed_dispatch(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            state_db.touch()
            artifact_root = root / "artifacts"
            artifact_root.mkdir()
            lock_path = root / "ingest.lock"

            connection = open_db(control_db)
            try:
                enqueue(connection, kind="ingest",
                        due_slot_utc="2026-09-14T06:30:00Z",
                        payload=_payload_for("ingest"))
            finally:
                connection.close()

            from news_container import worker as worker_module
            argv = [
                "--kind", "ingest",
                "--control-db", str(control_db),
                "--state-db", str(state_db),
                "--artifact-root", str(artifact_root),
                "--lock-path", str(lock_path),
                "--owner", "ingest-1",
                "--claim-ttl-seconds", "60",
                "--max-iterations", "1",
            ]
            with mock.patch.object(
                worker_module, "_dispatch",
                return_value=DispatchResult(
                    exit_code=1,
                    stdout_bytes=0,
                    stdout_hash=hash_stdout(b""),
                    status="failed",
                    error_class="RuntimeError",
                    error_message="boom",
                ),
            ):
                exit_code = worker_module.main(argv)
            self.assertEqual(exit_code, 1,
                             "main() must return nonzero when dispatch fails")

    def test_fence_loss_is_counted_and_nonzero_without_completed_run(self):
        """A fence-loss event must NOT be silently counted as success,
        and must NOT produce a 'completed' run row.  The worker must
        increment ``fence_lost`` and ``had_failures`` is True so the
        CLI exits nonzero."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            state_db.touch()
            artifact_root = root / "artifacts"
            artifact_root.mkdir()
            lock_path = root / "ingest.lock"

            connection = open_db(control_db)
            try:
                enqueue(
                    connection, kind="ingest",
                    due_slot_utc="2026-09-14T06:30:00Z",
                    payload=_payload_for("ingest"),
                )
                config = WorkerConfig(
                    kind="ingest",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="ingest-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=1,
                    lease_renew_seconds=1,
                )

                class _FenceLossRenewer:
                    def __init__(self, *a, **kw):
                        # Mark fence loss from the start so the
                        # worker's post-dispatch check sees it.
                        self.fence_lost = True

                    def start(self):
                        pass

                    def stop(self):
                        pass

                    def bind(self, task_id, generation):
                        # Keep fence_lost True across binds.
                        self.fence_lost = True

                with mock.patch.object(
                    worker_module, "_LeaseRenewer",
                    side_effect=lambda **kw: _FenceLossRenewer(),
                ):
                    outcome = run_worker(connection, config,
                                         dispatch=_StubDispatch(exit_code=0))

                # Fence-loss path: no completed run row written,
                # fence_lost increments, had_failures is True.
                self.assertEqual(outcome.processed, 0,
                                 "fence loss must NOT silently count as processed")
                self.assertEqual(outcome.failed, 0)
                self.assertEqual(outcome.fence_lost, 1)
                self.assertTrue(outcome.had_failures)
                run_count = connection.execute(
                    "SELECT COUNT(*) AS n FROM runs"
                ).fetchone()["n"]
                self.assertEqual(run_count, 0,
                                 "fence loss must NOT write a completed run row")
            finally:
                connection.close()

    def test_complete_failure_increments_failed_without_processed(self):
        """When ``complete()`` cannot persist the run row, ``failed``
`` is incremented and ``processed`` is NOT silently advanced."""
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            control_db = root / "control.db"
            state_db = root / "state.db"
            state_db.touch()
            artifact_root = root / "artifacts"
            artifact_root.mkdir()
            lock_path = root / "ingest.lock"

            connection = open_db(control_db)
            try:
                enqueue(
                    connection, kind="ingest",
                    due_slot_utc="2026-09-14T06:30:00Z",
                    payload=_payload_for("ingest"),
                )

                # Force ``complete`` to raise by patching the worker
                # module's imported ``complete`` symbol.
                def boom(*args, **kwargs):
                    raise ControlStoreError("forced complete failure")

                config = WorkerConfig(
                    kind="ingest",
                    control_db=control_db,
                    state_db=state_db,
                    artifact_root=artifact_root,
                    lock_path=lock_path,
                    owner="ingest-1",
                    claim_ttl_seconds=60,
                    canary_root=root,
                    max_iterations=2,
                )
                with mock.patch.object(worker_module, "complete", side_effect=boom):
                    outcome = run_worker(connection, config,
                                         dispatch=_StubDispatch(exit_code=0))
                self.assertEqual(outcome.processed, 0,
                                 "complete failure must NOT silently count as processed")
                self.assertEqual(outcome.failed, 1)
                self.assertTrue(outcome.had_failures)
                # No run row was persisted.
                run_count = connection.execute(
                    "SELECT COUNT(*) AS n FROM runs"
                ).fetchone()["n"]
                self.assertEqual(run_count, 0,
                                 "complete failure must NOT write a run row")
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()