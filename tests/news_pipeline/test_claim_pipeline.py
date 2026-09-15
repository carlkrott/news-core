from __future__ import annotations

import hashlib
import sqlite3
import unittest
from decimal import Decimal

from news_pipeline.claim_pipeline import claims_from_observation, observation_id_to_source_item_id
from news_pipeline.live_contracts import ObservationContract, ObservationKind


class ClaimPipelineTests(unittest.TestCase):
    def observation(self):
        return ObservationContract(
            observation_id="obs-1", source_id="src", category="ai", kind=ObservationKind.PARSED_ARTICLE,
            original_url="https://example.test/a", canonical_url="https://example.test/a", publisher="Example",
            retrieval_method="rss", raw_content_hash="a" * 64, observed_at="2026-09-07T00:00:00Z",
            title="Product v2.0 launched", body="Product v2.0 launched on 2026-09-06 for $10.",
        )

    def test_claims_are_deterministic_and_exactly_hashed(self):
        first = claims_from_observation(self.observation())
        second = claims_from_observation(self.observation())
        self.assertEqual(first, second)
        self.assertTrue(first.claims)
        for evidence in first.evidence:
            self.assertEqual(evidence.excerpt_hash, hashlib.sha256(evidence.exact_excerpt.encode()).hexdigest())

    def test_observation_mapping_rejects_unmapped_id(self):
        con = sqlite3.connect(":memory:")
        con.execute("CREATE TABLE source_items(source_item_id TEXT PRIMARY KEY, external_id TEXT)")
        try:
            with self.assertRaises(ValueError):
                observation_id_to_source_item_id(con, "missing")
            con.executemany("INSERT INTO source_items VALUES (?,?)", [("a", "dup"), ("b", "dup")])
            with self.assertRaises(ValueError):
                observation_id_to_source_item_id(con, "dup")
            con.execute("INSERT INTO source_items VALUES (?,?)", ("c", "exact"))
            self.assertEqual(observation_id_to_source_item_id(con, "c"), "c")
        finally:
            con.close()

    def test_source_statement_fallback_preserves_exact_body_and_excerpt(self):
        observation = ObservationContract(
            observation_id="obs-plain", source_id="src", category="ai", kind=ObservationKind.PARSED_ARTICLE,
            original_url="https://example.test/a", canonical_url="https://example.test/a", publisher="Example",
            retrieval_method="rss", raw_content_hash="a" * 64, observed_at="2026-09-07T00:00:00Z", body="verbatim body")
        rows = claims_from_observation(observation)
        self.assertEqual(rows.claims[0].predicate, "source_statement")
        self.assertEqual(rows.claims[0].object_value, "verbatim body")
        self.assertEqual(rows.evidence[0].exact_excerpt, "verbatim body")


if __name__ == "__main__":
    unittest.main()
