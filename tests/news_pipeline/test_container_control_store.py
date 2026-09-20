"""Behavioral tests for the news_container control store.

These tests were authored with a red-green discipline: each test
defines a contract that the implementation must satisfy.  When a
test fails the corresponding behavior was missing from the store.

Covered contracts
-----------------
* Idempotent enqueue: re-enqueuing the same (kind, due_slot) returns
  ``created=False`` and never duplicates the row.
* Claim fencing/takeover: a fresh owner can take over a claim only
  after the original claim expires.
* Completion requires exact owner + generation + nonexpired claim.
* The ``validate`` job's diagnostics (integrity_check, FK, schema
  version, zero-delivery check) work end-to-end.
* Captured-stdout hashing and error-class sanitization are bounded.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from news_container import ALLOWED_KINDS  # noqa: E402
from news_container.control_store import (  # noqa: E402
    ClaimMismatchError,
    ControlStoreError,
    DeliveryForbiddenError,
    PayloadConflictError,
    UnknownKindError,
    claim,
    complete,
    deterministic_task_id,
    enqueue,
    foreign_key_check,
    hash_stdout,
    integrity_check,
    open as open_db,
    pending_tasks,
    renew_claim,
    sanitize_error_class,
    sanitize_error_message,
    schema_version,
    selectable_tasks,
    zero_delivery_check,
)


def _tmp_db(tmpdir: Path | None = None) -> sqlite3.Connection:
    import tempfile
    if tmpdir is None:
        tmpdir = Path(tempfile.mkdtemp(prefix="nc_test_"))
    return open_db(tmpdir / "control.db")


class EnqueueIdempotencyTests(unittest.TestCase):
    def test_enqueue_same_kind_due_slot_is_idempotent(self):
        conn = _tmp_db()
        try:
            task_id_1, created_1 = enqueue(
                conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
            )
            task_id_2, created_2 = enqueue(
                conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
            )
            self.assertEqual(task_id_1, task_id_2, "task id must be deterministic")
            self.assertTrue(created_1)
            self.assertFalse(created_2, "second enqueue must be a no-op")
            rows = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()
            self.assertEqual(rows["n"], 1, "exactly one row must exist")
        finally:
            conn.close()

    def test_deterministic_task_id_independent_of_payload(self):
        conn = _tmp_db()
        try:
            task_id_1, _ = enqueue(
                conn, kind="process", due_slot_utc="2026-09-14T06:30:00Z",
                payload={"x": 1},
            )
            # Re-enqueueing the same identity with a *conflicting*
            # payload fails closed (invariant 1 of the A3 lifecycle).
            with self.assertRaises(PayloadConflictError):
                enqueue(
                    conn, kind="process", due_slot_utc="2026-09-14T06:30:00Z",
                    payload={"x": 2, "y": 3},
                )
            # Pre-flight: the id is the same shape as a sha256 prefix.
            self.assertEqual(len(deterministic_task_id("process", "2026-09-14T06:30:00Z")), 16)
            # Id remains deterministic regardless of the payload that
            # was first stored.
            self.assertEqual(task_id_1, deterministic_task_id("process", "2026-09-14T06:30:00Z"))
        finally:
            conn.close()

    def test_enqueue_rejects_delivery_kind(self):
        conn = _tmp_db()
        try:
            with self.assertRaises(DeliveryForbiddenError):
                enqueue(conn, kind="delivery", due_slot_utc="2026-09-14T06:30:00Z")
        finally:
            conn.close()

    def test_enqueue_rejects_unknown_kind(self):
        conn = _tmp_db()
        try:
            with self.assertRaises(UnknownKindError):
                enqueue(conn, kind="unicorn", due_slot_utc="2026-09-14T06:30:00Z")
        finally:
            conn.close()

    def test_payload_refuses_delivery_flag(self):
        conn = _tmp_db()
        try:
            with self.assertRaises(DeliveryForbiddenError):
                enqueue(
                    conn,
                    kind="report",
                    due_slot_utc="2026-09-14T06:30:00Z",
                    payload={"enable_live_delivery": True},
                )
            with self.assertRaises(DeliveryForbiddenError):
                enqueue(
                    conn,
                    kind="report",
                    due_slot_utc="2026-09-14T06:30:00Z",
                    payload={"delivery": {"enabled": True}},
                )
        finally:
            conn.close()

    def test_allowed_kinds_are_exactly_the_documented_set(self):
        self.assertEqual(ALLOWED_KINDS, frozenset({"ingest", "investigate", "process", "validate", "report"}))


class ClaimFencingTests(unittest.TestCase):
    def _bootstrap(self, conn: sqlite3.Connection) -> str:
        task_id, _ = enqueue(
            conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z"
        )
        return task_id

    def test_initial_claim_assigns_generation_one(self):
        conn = _tmp_db()
        try:
            task_id = self._bootstrap(conn)
            gen, attempt, expires = claim(
                conn, task_id=task_id, owner="ingest-1", ttl_seconds=60
            )
            self.assertEqual(gen, 1)
            self.assertEqual(attempt, 1)
            self.assertTrue(expires.endswith("Z"))
        finally:
            conn.close()

    def test_same_owner_re_claim_is_idempotent(self):
        conn = _tmp_db()
        try:
            task_id = self._bootstrap(conn)
            gen1, attempt1, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            gen2, attempt2, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            self.assertEqual(gen1, gen2)
            self.assertEqual(attempt1, attempt2)
        finally:
            conn.close()

    def test_stale_claim_is_taken_over(self):
        conn = _tmp_db()
        try:
            task_id = self._bootstrap(conn)
            # First worker claims with a *very short* TTL.
            gen1, attempt1, _ = claim(
                conn, task_id=task_id, owner="ingest-1", ttl_seconds=1
            )
            # Fast-forward "now" past the expiry without sleeping.
            future = datetime.now(UTC) + timedelta(seconds=30)
            gen2, attempt2, _ = claim(
                conn, task_id=task_id, owner="ingest-2", ttl_seconds=60, now=future
            )
            self.assertEqual(gen1, 1)
            self.assertEqual(gen2, 2, "takeover must bump generation")
            self.assertGreater(attempt2, attempt1, "takeover must bump attempt")
        finally:
            conn.close()

    def test_live_claim_cannot_be_taken_over(self):
        conn = _tmp_db()
        try:
            task_id = self._bootstrap(conn)
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=120)
            with self.assertRaises(ControlStoreError):
                claim(conn, task_id=task_id, owner="ingest-2", ttl_seconds=60)
        finally:
            conn.close()

    def test_complete_requires_exact_owner_and_generation(self):
        conn = _tmp_db()
        try:
            task_id = self._bootstrap(conn)
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            with self.assertRaises(ClaimMismatchError):
                complete(
                    conn, task_id=task_id, owner="ingest-2", generation=1,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None,
                )
            with self.assertRaises(ClaimMismatchError):
                complete(
                    conn, task_id=task_id, owner="ingest-1", generation=99,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None,
                )
        finally:
            conn.close()

    def test_complete_rejects_expired_claim(self):
        conn = _tmp_db()
        try:
            task_id = self._bootstrap(conn)
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=1)
            future = datetime.now(UTC) + timedelta(seconds=30)
            with self.assertRaises(ClaimMismatchError):
                complete(
                    conn, task_id=task_id, owner="ingest-1", generation=1,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None, now=future,
                )
        finally:
            conn.close()


class ValidateDiagnosticsTests(unittest.TestCase):
    def test_validate_diagnostics_pass_on_clean_db(self):
        conn = _tmp_db()
        try:
            enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            self.assertEqual(integrity_check(conn), ["ok"])
            self.assertEqual(foreign_key_check(conn), [])
            self.assertEqual(schema_version(conn), 2)
            count, kinds = zero_delivery_check(conn)
            self.assertEqual(count, 0)
            self.assertEqual(kinds, [])
        finally:
            conn.close()

    def test_zero_delivery_check_flags_smuggled_rows(self):
        # Bypass enqueue() so we can simulate a stale/legacy DB row.
        conn = _tmp_db()
        try:
            conn.execute(
                "INSERT INTO tasks(task_id, kind, due_slot_utc, payload_json, state, generation, created_at)"
                " VALUES ('legacy', 'delivery', '2026-09-14T00:00:00Z', '{}', 'pending', 0, '2026-09-14T00:00:00Z')"
            )
            count, kinds = zero_delivery_check(conn)
            self.assertEqual(count, 1)
            self.assertEqual(kinds, ["delivery"])
        finally:
            conn.close()


class SanitizationTests(unittest.TestCase):
    def test_sanitize_error_class_whitelist(self):
        self.assertEqual(sanitize_error_class("RuntimeError"), "RuntimeError")
        self.assertEqual(sanitize_error_class("NotAnError"), "ControlStoreError")
        self.assertEqual(sanitize_error_class(None), None)

    def test_sanitize_error_message_strips_control_chars(self):
        dirty = "boom\x00\x01\x02\n\tEnd"
        cleaned = sanitize_error_message(dirty)
        self.assertNotIn("\x00", cleaned)
        self.assertIn("End", cleaned)

    def test_sanitize_error_message_clips_length(self):
        long = "x" * 5000
        cleaned = sanitize_error_message(long, max_length=128)
        self.assertTrue(cleaned.endswith("..."))
        self.assertLessEqual(len(cleaned), 128)

    def test_hash_stdout_deterministic(self):
        self.assertEqual(hash_stdout(b"hello"), hash_stdout("hello"))
        self.assertEqual(
            hash_stdout(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        )


class PendingTasksTests(unittest.TestCase):
    def test_pending_tasks_returns_only_pending(self):
        conn = _tmp_db()
        try:
            enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            enqueue(conn, kind="process", due_slot_utc="2026-09-14T06:45:00Z")
            # Mark one completed by completing through a real claim.
            enqueue(conn, kind="report", due_slot_utc="2026-09-14T23:30:00Z")
            report_id = pending_tasks(conn, limit=10)[-1]["task_id"]
            gen, _, _ = claim(conn, task_id=report_id, owner="report-1", ttl_seconds=60)
            complete(
                conn, task_id=report_id, owner="report-1", generation=gen,
                status="completed", exit_code=0, stdout_hash=None,
                error_class=None, error_message=None,
            )
            rows = pending_tasks(conn)
            self.assertEqual(len(rows), 2)
            self.assertEqual({row["kind"] for row in rows}, {"ingest", "process"})
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# A3 lifecycle regressions
# ---------------------------------------------------------------------------


class EnqueueReenqueueContractTests(unittest.TestCase):
    """Invariant 1: enqueue with identical canonical payload is a no-op
    in pending/claimed/completed; conflicting payload fails closed.
    """

    def test_re_enqueue_pending_with_same_payload_is_noop(self):
        conn = _tmp_db()
        try:
            task_id, created = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1})
            self.assertTrue(created)
            again_id, again_created = enqueue(
                conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1}
            )
            self.assertEqual(task_id, again_id)
            self.assertFalse(again_created)
            count = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
            self.assertEqual(count, 1)
        finally:
            conn.close()

    def test_re_enqueue_claimed_with_same_payload_is_noop(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1})
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            again_id, again_created = enqueue(
                conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1}
            )
            self.assertEqual(task_id, again_id)
            self.assertFalse(again_created, "claimed row re-enqueue must not raise or duplicate")
        finally:
            conn.close()

    def test_re_enqueue_completed_with_same_payload_is_noop(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1})
            gen, _, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            complete(
                conn, task_id=task_id, owner="ingest-1", generation=gen,
                status="completed", exit_code=0, stdout_hash=None,
                error_class=None, error_message=None,
            )
            again_id, again_created = enqueue(
                conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1}
            )
            self.assertEqual(task_id, again_id)
            self.assertFalse(again_created, "completed row re-enqueue must not raise or duplicate")
            count = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
            self.assertEqual(count, 1)
        finally:
            conn.close()

    def test_re_enqueue_with_conflicting_payload_raises(self):
        conn = _tmp_db()
        try:
            enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z", payload={"x": 1})
            with self.assertRaises(PayloadConflictError):
                enqueue(
                    conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z",
                    payload={"x": 2, "y": 3},
                )
        finally:
            conn.close()


class ClaimExpiryBeforeSameOwnerTests(unittest.TestCase):
    """Invariant 2: claim examines expiry before same-owner idempotence."""

    def test_expired_same_owner_takeover_bumps_generation_and_attempt(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            gen1, attempt1, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=1)
            future = datetime.now(UTC) + timedelta(seconds=30)
            gen2, attempt2, _ = claim(
                conn, task_id=task_id, owner="ingest-1", ttl_seconds=60, now=future
            )
            self.assertEqual(gen1, 1)
            self.assertEqual(gen2, 2, "expired same-owner must bump generation")
            self.assertEqual(attempt2, attempt1 + 1, "expired same-owner must bump attempt")
        finally:
            conn.close()

    def test_live_same_owner_reclaim_is_idempotent(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            gen1, attempt1, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            gen2, attempt2, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            self.assertEqual(gen1, gen2)
            self.assertEqual(attempt1, attempt2)
        finally:
            conn.close()

    def test_expired_different_owner_takeover_bumps_generation(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=1)
            future = datetime.now(UTC) + timedelta(seconds=30)
            gen2, attempt2, _ = claim(
                conn, task_id=task_id, owner="ingest-2", ttl_seconds=60, now=future
            )
            self.assertEqual(gen2, 2)
            self.assertGreater(attempt2, 1)
        finally:
            conn.close()

    def test_stale_completion_after_takeover_is_refused(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=1)
            future = datetime.now(UTC) + timedelta(seconds=30)
            claim(conn, task_id=task_id, owner="ingest-2", ttl_seconds=60, now=future)
            # Old owner tries to complete against the bumped generation.
            with self.assertRaises(ClaimMismatchError):
                complete(
                    conn, task_id=task_id, owner="ingest-1", generation=1,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None,
                )
        finally:
            conn.close()


class SelectorTests(unittest.TestCase):
    """Invariant 3: selector returns pending or expired-claimed work for its own kind."""

    def test_selector_returns_pending(self):
        conn = _tmp_db()
        try:
            enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            rows = selectable_tasks(conn, kind="ingest")
            self.assertEqual(len(rows), 1)
        finally:
            conn.close()

    def test_selector_returns_expired_claimed(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=1)
            future = datetime.now(UTC) + timedelta(seconds=30)
            rows = selectable_tasks(conn, kind="ingest", now=future)
            self.assertEqual(len(rows), 1, "expired claim must be selectable")
        finally:
            conn.close()

    def test_selector_excludes_live_claimed(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=120)
            rows = selectable_tasks(conn, kind="ingest")
            self.assertEqual(rows, [], "live claim must not be selectable")
        finally:
            conn.close()

    def test_selector_excludes_other_kinds(self):
        conn = _tmp_db()
        try:
            enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            enqueue(conn, kind="report", due_slot_utc="2026-09-14T06:30:00Z")
            rows = selectable_tasks(conn, kind="ingest")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["kind"], "ingest")
        finally:
            conn.close()


class LeaseRenewTests(unittest.TestCase):
    """Invariant 4: lease renewal is fenced by owner+generation."""

    def test_renew_extends_expiry(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            gen, _, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            future = datetime.now(UTC) + timedelta(seconds=30)
            new_expiry = renew_claim(
                conn, task_id=task_id, owner="ingest-1", generation=gen,
                ttl_seconds=120, now=future,
            )
            self.assertTrue(new_expiry.endswith("Z"))
            row = conn.execute(
                "SELECT expires_at_utc FROM claims WHERE task_id=?", (task_id,)
            ).fetchone()
            self.assertEqual(row["expires_at_utc"], new_expiry)
        finally:
            conn.close()

    def test_renew_with_wrong_owner_raises(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            gen, _, _ = claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=60)
            with self.assertRaises(ClaimMismatchError):
                renew_claim(
                    conn, task_id=task_id, owner="ingest-2", generation=gen,
                    ttl_seconds=60,
                )
        finally:
            conn.close()

    def test_renew_after_expiry_is_fence_loss(self):
        conn = _tmp_db()
        try:
            task_id, _ = enqueue(conn, kind="ingest", due_slot_utc="2026-09-14T06:30:00Z")
            claim(conn, task_id=task_id, owner="ingest-1", ttl_seconds=1)
            future = datetime.now(UTC) + timedelta(seconds=30)
            with self.assertRaises(ClaimMismatchError):
                renew_claim(
                    conn, task_id=task_id, owner="ingest-1", generation=1,
                    ttl_seconds=60, now=future,
                )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()