from __future__ import annotations

import unittest
from dataclasses import FrozenInstanceError

from news_pipeline.novelty import (
    AttemptYield,
    CategoryYield,
    NoveltyRecord,
    summarize_yield,
)


class NoveltyTests(unittest.TestCase):
    def test_yield_is_immutable_and_validated(self) -> None:
        summary = CategoryYield("ai", distinct_event_count=1, material_update_count=1)
        self.assertEqual(summary.novel_count, 2)
        with self.assertRaises(FrozenInstanceError):
            summary.returned_count = 4
        with self.assertRaises(ValueError):
            CategoryYield("ai", stale_count=-1)

    def test_merges_attempt_and_semantic_counts(self) -> None:
        attempts = (
            AttemptYield("ai", "success", returned_count=6, duplicate_count=2),
        )
        records = (
            NoveltyRecord("ai", "1", "suppress_exact_url", "bypass_phase2_terminal"),
            NoveltyRecord("ai", "2", "drop_stale", "bypass_phase2_terminal"),
            NoveltyRecord("ai", "3", "keep", "rewrite"),
            NoveltyRecord("ai", "4", "keep", "material_update"),
            NoveltyRecord("ai", "5", "keep", "distinct_event"),
            NoveltyRecord("ai", "6", "pending_possible_update", "pending_review"),
        )
        summary = summarize_yield(records, attempts)[0]
        self.assertEqual(summary.completed_rounds, 1)
        self.assertEqual(summary.returned_count, 6)
        self.assertEqual(summary.processed_count, 6)
        self.assertEqual(summary.ingest_duplicate_count, 3)
        self.assertEqual(summary.stale_count, 1)
        self.assertEqual(summary.rewrite_count, 1)
        self.assertEqual(summary.material_update_count, 1)
        self.assertEqual(summary.distinct_event_count, 1)
        self.assertEqual(summary.pending_count, 1)
        self.assertEqual(summary.novel_count, 2)

    def test_empty_success_round_needs_expansion(self) -> None:
        summary = summarize_yield((), (AttemptYield("ai", "success"),))[0]
        self.assertTrue(summary.needs_expansion())

    def test_transport_failure_alone_never_triggers_expansion(self) -> None:
        summary = summarize_yield(
            (), (AttemptYield("ai", "failed", returned_count=0),)
        )[0]
        self.assertEqual(summary.transport_failure_count, 1)
        self.assertEqual(summary.completed_rounds, 0)
        self.assertFalse(summary.needs_expansion())

    def test_mixed_failure_and_completed_low_yield_can_expand(self) -> None:
        summary = summarize_yield(
            (),
            (
                AttemptYield("ai", "failed"),
                AttemptYield("ai", "success", returned_count=2, duplicate_count=2),
            ),
        )[0]
        self.assertEqual(summary.transport_failure_count, 1)
        self.assertEqual(summary.completed_rounds, 1)
        self.assertTrue(summary.needs_expansion())


if __name__ == "__main__":
    unittest.main()
