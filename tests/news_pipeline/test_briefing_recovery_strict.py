"""Regression coverage for strict stale-run recovery cutoff equality."""
from __future__ import annotations

import sqlite3
import unittest
from datetime import datetime, timezone

from news_pipeline import briefing_ledger


class TestBriefingRecoveryStrict(unittest.TestCase):
    def test_recovery_excludes_equality_at_whole_second_and_microsecond_cutoffs(self) -> None:
        conn = sqlite3.connect(":memory:", isolation_level=None)
        try:
            ledger = briefing_ledger.BriefingLedger(conn)
            ledger.initialize_schema()

            ledger.begin_run(
                "whole-second",
                "2026-07-01T00:00:00Z",
                "2026-07-02T00:00:00Z",
                "2026-07-01T00:00:00Z",
            )
            whole_second = ledger.recover_stale_runs(
                datetime(2026, 7, 1, 0, 0, 0, tzinfo=timezone.utc),
                "2026-07-01T00:00:01Z",
            )

            ledger.begin_run(
                "microsecond",
                "2026-07-02T00:00:00Z",
                "2026-07-03T00:00:00Z",
                "2026-07-01T00:00:01Z",
            )
            microsecond = ledger.recover_stale_runs(
                datetime(2026, 7, 1, 0, 0, 1, 123456, tzinfo=timezone.utc),
                "2026-07-01T00:00:02Z",
            )

            self.assertNotIn("whole-second", whole_second)
            self.assertNotIn("microsecond", microsecond)
        finally:
            conn.close()
