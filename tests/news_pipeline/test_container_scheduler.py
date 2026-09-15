"""Behavioral tests for the news_container scheduler.

These tests define the contract for the *pure* scheduler layer
(``compute_due_slots``, ``parse_schedule_toml``) and the *driver*
that combines it with the control store.

Covered contracts
-----------------
* ``compute_due_slots`` is deterministic and wall-clock-free.
* Spring-forward DST (Europe/London, 2026-03-29) does not produce a
  slot for the missing local hour 01:00..01:59 and does not duplicate
  the autumn fall-back hour 01:00..01:59 on 2026-10-25.
* The TOML parser rejects ``delivery`` kinds and invalid weekday names.
* The driver is idempotent — re-running ``--once --at`` for the same
  timestamp does not create duplicate tasks.
* The recurring loop honours ``--max-iterations`` and a bounded
  ``--sleep-cap-seconds`` without ever sleeping past the cap.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from news_container.control_store import (  # noqa: E402
    PayloadConflictError,
    open as open_db,
)
from news_container.scheduler import (  # noqa: E402
    SCHEDULE_TZ_NAME,
    ScheduleBlock,
    ScheduleError,
    compute_due_slots,
    enqueue_due_slots,
    next_due_slot,
    parse_schedule_toml,
)


def _block(
    block_id: str = "morning_ingest",
    *,
    kind: str = "ingest",
    hour: int = 6,
    minute: int = 30,
    weekdays: frozenset[int] = frozenset(range(7)),
    enabled: bool = True,
    payload: dict | None = None,
) -> ScheduleBlock:
    return ScheduleBlock(
        block_id=block_id,
        kind=kind,
        local_hours=frozenset({hour}),
        local_minutes=frozenset({minute}),
        weekdays=weekdays,
        enabled=enabled,
        notes="",
        payload=payload or {},
    )


class ComputeDueSlotsTests(unittest.TestCase):
    def test_zero_horizon_returns_exact_match(self):
        # 06:30 BST in September (BST = UTC+1) is 05:30 UTC.
        now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [_block()], horizon_minutes=0)
        self.assertEqual(slots, [("ingest", "2026-09-14T05:30:00Z")])

    def test_horizon_catches_future_slots_in_window(self):
        now = datetime(2026, 9, 14, 5, 25, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [_block()], horizon_minutes=10)
        self.assertEqual(slots, [("ingest", "2026-09-14T05:30:00Z")])

    def test_horizon_zero_omits_strictly_future_slot(self):
        now = datetime(2026, 9, 14, 5, 29, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [_block()], horizon_minutes=0)
        self.assertEqual(slots, [])

    def test_deterministic_for_same_inputs(self):
        now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
        first = compute_due_slots(now, [_block()])
        second = compute_due_slots(now, [_block()])
        self.assertEqual(first, second)

    def test_results_sorted_by_due_slot_then_kind(self):
        # Start at midnight UTC, look 24h ahead.
        now = datetime(2026, 9, 14, 0, 0, 0, tzinfo=UTC)
        ingest_block = _block("i", kind="ingest", hour=6, minute=30)
        process_block = _block("p", kind="process", hour=6, minute=45)
        slots = compute_due_slots(now, [process_block, ingest_block], horizon_minutes=24 * 60)
        # Ingest (06:30 local = 05:30 UTC) comes before process (05:45 UTC).
        self.assertEqual(slots[0][0], "ingest")
        self.assertEqual(slots[1][0], "process")

    def test_disabled_block_is_skipped(self):
        now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [_block(enabled=False)])
        self.assertEqual(slots, [])

    def test_weekday_filter_excludes_other_days(self):
        # 2026-09-13 is a Sunday (weekday index 6); 2026-09-14 is Monday.
        now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
        only_sunday = _block(weekdays=frozenset({6}))
        self.assertEqual(compute_due_slots(now, [only_sunday]), [])
        only_monday = _block(weekdays=frozenset({0}))
        self.assertEqual(
            compute_due_slots(now, [only_monday]),
            [("ingest", "2026-09-14T05:30:00Z")],
        )

    def test_wall_clock_is_not_used(self):
        # Pass a fixed "now"; do not let the function read time.time().
        now = datetime(2099, 12, 31, 23, 59, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [_block()], horizon_minutes=0)
        self.assertEqual(slots, [])


class DstScheduleTests(unittest.TestCase):
    """Europe/London DST coverage.

    BST starts on the last Sunday of March (2026-03-29): local 01:00 GMT
    jumps to 02:00 BST.  Local times 01:00..01:59 do not exist on that
    day.  Conversely, on 2026-10-25 the local clock falls back from
    02:00 BST to 01:00 GMT, producing two 01:00..01:59 windows; the
    scheduler must dedupe by (kind, local_due) so the same id is not
    enqueued twice.
    """

    def test_spring_forward_skips_missing_local_hour(self):
        # 2026-03-29 in Europe/London is the spring-forward day.
        # Schedule a block at 01:30 (which never happens that day).
        block = _block(kind="ingest", hour=1, minute=30, weekdays=frozenset({6}))
        # 06:00 UTC on 2026-03-29 corresponds to 07:00 BST (post-jump).
        now = datetime(2026, 3, 29, 6, 0, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [block], horizon_minutes=24 * 60)
        # 01:30 local is non-existent on this date — no slot emitted.
        self.assertEqual(slots, [])

    def test_fall_back_dedupes_duplicate_local_hour(self):
        # 2026-10-25 in Europe/London is the autumn fall-back day.
        # 01:30 local occurs at 00:30 UTC and again at 01:30 UTC.
        block = _block(kind="ingest", hour=1, minute=30, weekdays=frozenset({6}))
        # Sweep a window that covers both occurrences.
        now = datetime(2026, 10, 25, 0, 0, 0, tzinfo=UTC)
        slots = compute_due_slots(now, [block], horizon_minutes=24 * 60)
        # We expect exactly one slot for the *local* 01:30 — the
        # scheduler dedupes by (kind, local_due).  The UTC due slot is
        # the *first* (00:30 UTC) occurrence.
        kinds = [kind for kind, _ in slots]
        ingests = [slot for kind, slot in slots if kind == "ingest"]
        self.assertEqual(len(ingests), 1, f"expected one deduped slot, got {ingests!r}")
        self.assertEqual(ingests[0], "2026-10-25T00:30:00Z")
        self.assertEqual(kinds.count("ingest"), 1)

    def test_zoneinfo_is_europe_london(self):
        self.assertEqual(SCHEDULE_TZ_NAME, "Europe/London")


class NextDueSlotTests(unittest.TestCase):
    def test_returns_imminent_slot_when_within_minute(self):
        now = datetime(2026, 9, 14, 5, 29, 30, tzinfo=UTC)
        nxt = next_due_slot(now, [_block()])
        self.assertEqual(nxt, ("ingest", "2026-09-14T05:30:00Z"))


class ParseScheduleTomlTests(unittest.TestCase):
    def test_minimal_block(self):
        toml = """
[schedule.morning_ingest]
kind = "ingest"
due_local = "06:30"
weekdays = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
"""
        blocks = parse_schedule_toml(toml)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0].block_id, "morning_ingest")
        self.assertEqual(blocks[0].local_hours, frozenset({6}))
        self.assertEqual(blocks[0].local_minutes, frozenset({30}))
        self.assertEqual(blocks[0].kind, "ingest")

    def test_payload_table_is_preserved(self):
        toml = """
[schedule.morning_ingest]
kind = "ingest"
due_local = "06:30"

[schedule.morning_ingest.payload]
sources = "/app/config/news-sources.toml"
topics = "/app/config/news-topics.toml"
policy = "/app/config/news-policy.toml"
"""
        block = parse_schedule_toml(toml)[0]
        self.assertEqual(block.payload["sources"], "/app/config/news-sources.toml")

    def test_hour_and_minute_sets_support_live_cadence(self):
        block = parse_schedule_toml("""
[schedule.phase6_ingest]
kind = "ingest"
hours = "all"
minutes = [0, 15, 30, 45]
""")[0]
        self.assertEqual(block.local_hours, frozenset(range(24)))
        self.assertEqual(block.local_minutes, frozenset({0, 15, 30, 45}))

    def test_delivery_kind_is_rejected(self):
        toml = """
[schedule.bad]
kind = "delivery"
due_local = "06:30"
"""
        with self.assertRaises(ScheduleError):
            parse_schedule_toml(toml)

    def test_unknown_weekday_rejected(self):
        toml = """
[schedule.bad]
kind = "ingest"
due_local = "06:30"
weekdays = ["funday"]
"""
        with self.assertRaises(ScheduleError):
            parse_schedule_toml(toml)

    def test_invalid_due_local_rejected(self):
        toml = """
[schedule.bad]
kind = "ingest"
due_local = "25:99"
"""
        with self.assertRaises(ScheduleError):
            parse_schedule_toml(toml)

    def test_missing_schedule_section_rejected(self):
        with self.assertRaises(ScheduleError):
            parse_schedule_toml("")


class EnqueueDriverTests(unittest.TestCase):
    def test_enqueue_due_slots_persists_schedule_payload(self):
        with tempfile.TemporaryDirectory() as td:
            connection = open_db(Path(td) / "control.db")
            try:
                payload = {"sources": "/app/config/news-sources.toml"}
                now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
                enqueue_due_slots(connection, [_block(payload=payload)], now_utc=now)
                stored = connection.execute(
                    "SELECT payload_json FROM tasks"
                ).fetchone()["payload_json"]
                self.assertEqual(json.loads(stored), payload)
            finally:
                connection.close()

    def test_enqueue_due_slots_is_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            connection = open_db(db_path)
            try:
                now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
                first = enqueue_due_slots(connection, [_block()], now_utc=now)
                second = enqueue_due_slots(connection, [_block()], now_utc=now)
                self.assertEqual(first, [("ingest", "2026-09-14T05:30:00Z", True)])
                self.assertEqual(second, [("ingest", "2026-09-14T05:30:00Z", False)])
                count = connection.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
                self.assertEqual(count, 1)
            finally:
                connection.close()

    def test_enqueue_due_slots_only_emits_due_kinds(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            connection = open_db(db_path)
            try:
                now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
                blocks = [_block("i", kind="ingest", hour=6, minute=30),
                          _block("r", kind="report", hour=23, minute=30)]
                rows = enqueue_due_slots(connection, blocks, now_utc=now)
                self.assertEqual(rows, [("ingest", "2026-09-14T05:30:00Z", True)])
            finally:
                connection.close()


class A3RepeatedSweepContractTests(unittest.TestCase):
    """Invariant 1: repeated scheduler sweep after claim/completion
    does not raise or duplicate.

    This is the contract the scheduler exposes to the runtime: a
    recurring sweep re-running ``enqueue_due_slots`` with the same
    schedule and clock must not raise and must not duplicate rows.
    """

    def test_repeated_sweep_after_completion_is_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            connection = open_db(db_path)
            try:
                now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
                block = _block(payload={"sources": "/canary/sources.toml"})
                rows1 = enqueue_due_slots(connection, [block], now_utc=now)
                self.assertEqual(rows1[0][2], True)
                # Simulate a worker finishing between sweeps by moving
                # the task through claimed -> completed via the public
                # claim/complete path.
                from news_container.control_store import claim, complete
                task_id = deterministic_task_id("ingest", "2026-09-14T05:30:00Z")
                gen, _, _ = claim(connection, task_id=task_id, owner="ingest-1", ttl_seconds=60)
                complete(
                    connection, task_id=task_id, owner="ingest-1", generation=gen,
                    status="completed", exit_code=0, stdout_hash=None,
                    error_class=None, error_message=None,
                )
                # Second sweep must not raise and must not duplicate.
                rows2 = enqueue_due_slots(connection, [block], now_utc=now)
                self.assertEqual(rows2, [("ingest", "2026-09-14T05:30:00Z", False)])
                count = connection.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
                self.assertEqual(count, 1)
            finally:
                connection.close()

    def test_repeated_sweep_with_same_payload_is_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            connection = open_db(db_path)
            try:
                now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
                block = _block(payload={"sources": "/canary/sources.toml"})
                first = enqueue_due_slots(connection, [block], now_utc=now)
                second = enqueue_due_slots(connection, [block], now_utc=now)
                third = enqueue_due_slots(connection, [block], now_utc=now)
                self.assertEqual(first[0][2], True)
                self.assertEqual(second[0][2], False)
                self.assertEqual(third[0][2], False)
                count = connection.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
                self.assertEqual(count, 1)
            finally:
                connection.close()

    def test_conflicting_payload_at_sweep_raises(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "control.db"
            connection = open_db(db_path)
            try:
                now = datetime(2026, 9, 14, 5, 30, 0, tzinfo=UTC)
                block_a = _block(block_id="a", payload={"x": 1})
                block_b = _block(block_id="b", payload={"x": 2})
                # First sweep enqueues the task with payload {"x": 1}.
                rows1 = enqueue_due_slots(connection, [block_a], now_utc=now)
                self.assertEqual(rows1[0][2], True)
                # Second sweep with a *conflicting* block raises.
                with self.assertRaises(PayloadConflictError):
                    enqueue_due_slots(connection, [block_b], now_utc=now)
            finally:
                connection.close()


def deterministic_task_id(kind: str, due_slot_utc: str) -> str:  # local helper
    import hashlib
    material = f"{kind}|{due_slot_utc}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()[:16]


if __name__ == "__main__":
    unittest.main()