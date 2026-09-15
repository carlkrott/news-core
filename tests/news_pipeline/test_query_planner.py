from __future__ import annotations

import unittest

from news_pipeline.live_contracts import QueryPlanContract, stable_id
from news_pipeline.query_planner import (
    EXPANSION_REASON,
    MAX_EXPANSION_ROUNDS,
    build_expansion_queries,
    validate_query_plan,
    validate_searxng_query_target,
)

EVALUATED_AT = "2026-09-06T12:00:00Z"


class QueryPlannerTests(unittest.TestCase):
    def test_queries_are_deterministic_bounded_and_sanitized(self) -> None:
        kwargs = {
            "source_id": "searx",
            "category": "ai",
            "evaluated_at": EVALUATED_AT,
            "titles": (
                "The Product Launch: https://reddit.com/r/test",
                "Product pricing correction announced",
                "A third title must not create a third round",
            ),
            "cooldown_seconds": 900,
        }
        first = build_expansion_queries(**kwargs)
        second = build_expansion_queries(**kwargs)
        self.assertEqual(first, second)
        self.assertEqual(len(first), MAX_EXPANSION_ROUNDS)
        self.assertTrue(all(plan.max_rounds == 2 for plan in first))
        self.assertTrue(all(plan.reason_selected == EXPANSION_REASON for plan in first))
        self.assertTrue(all("reddit" not in plan.query_text for plan in first))

    def test_fallback_uses_category_and_contract_identity(self) -> None:
        plan = build_expansion_queries(
            source_id="searx",
            category="audio_engineering",
            evaluated_at=EVALUATED_AT,
        )[0]
        self.assertIs(type(plan), QueryPlanContract)
        self.assertEqual(plan.query_text, "audio engineering official update")
        self.assertEqual(
            plan.query_plan_id,
            stable_id(
                "query-plan",
                "searx",
                "audio_engineering",
                plan.query_text,
                "audio_engineering",
            ),
        )

    def test_query_validation_rejects_direct_urls_and_wrong_reason(self) -> None:
        valid = build_expansion_queries(
            source_id="searx",
            category="ai",
            evaluated_at=EVALUATED_AT,
        )[0]
        with self.assertRaises(ValueError):
            validate_query_plan(
                QueryPlanContract(
                    query_plan_id="x",
                    source_id="searx",
                    query_text="https://reddit.com/r/news",
                    category="ai",
                    reason_selected=EXPANSION_REASON,
                    cooldown_seconds=60,
                    max_rounds=2,
                    created_at=EVALUATED_AT,
                )
            )
        with self.assertRaises(ValueError):
            validate_query_plan(
                QueryPlanContract(
                    query_plan_id="x",
                    source_id="searx",
                    query_text="safe query",
                    category="ai",
                    reason_selected="configured",
                    cooldown_seconds=60,
                    max_rounds=2,
                    created_at=EVALUATED_AT,
                )
            )
        self.assertIs(validate_query_plan(valid), valid)

    def test_target_validation_accepts_local_searxng(self) -> None:
        target = "http://127.0.0.1:8888/search"
        self.assertEqual(validate_searxng_query_target(target), target)

    def test_target_validation_rejects_unsafe_or_non_searxng_targets(self) -> None:
        targets = (
            "https://reddit.com/search",
            "https://user:pass@search.test/search",
            "file:///tmp/search",
            "https://search.test/other",
            "https://search.test/search?q=x",
            "https://search.test:99999/search",
        )
        for target in targets:
            with self.subTest(target=target), self.assertRaises(ValueError):
                validate_searxng_query_target(target)


if __name__ == "__main__":
    unittest.main()
