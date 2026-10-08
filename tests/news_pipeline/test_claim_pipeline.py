from __future__ import annotations

import hashlib
import sqlite3
import unittest
from dataclasses import replace
from decimal import Decimal

from news_pipeline.claim_pipeline import (
    claims_from_observation,
    observation_from_source_item,
    observation_id_to_source_item_id,
)
from news_pipeline.live_contracts import ObservationContract, ObservationKind, SourceRole


class ClaimPipelineTests(unittest.TestCase):
    def observation(self):
        return ObservationContract(
            observation_id="obs-1", source_id="src", category="ai", kind=ObservationKind.PARSED_ARTICLE,
            original_url="https://example.test/a", canonical_url="https://example.test/a", publisher="Example",
            retrieval_method="rss", raw_content_hash="a" * 64, observed_at="2026-09-07T00:00:00Z",
            title="Product v2.0 launched", body="Product v2.0 launched on 2026-09-06 for $10.",
        )

    def test_numeric_title_token_is_never_the_claim_subject(self):
        observation = replace(
            self.observation(),
            title="Widgetd v3.1 released with 0 regressions",
            body="Widgetd v3.1 released with 0 regressions.",
        )
        subjects = {claim.subject for claim in claims_from_observation(observation).claims}
        self.assertTrue(subjects)
        self.assertNotIn("0", subjects)
        self.assertEqual(subjects, {"regressions"})

    def test_all_numeric_title_falls_back_to_source_item_subject(self):
        observation = replace(self.observation(), title="0 1 2", body="Version v2.0 shipped.")
        subjects = {claim.subject for claim in claims_from_observation(observation).claims}
        self.assertEqual(subjects, {"source item"})

    def test_claims_are_deterministic_and_exactly_hashed(self):
        first = claims_from_observation(self.observation())
        second = claims_from_observation(self.observation())
        self.assertEqual(first, second)
        self.assertTrue(first.claims)
        for evidence in first.evidence:
            self.assertEqual(evidence.excerpt_hash, hashlib.sha256(evidence.exact_excerpt.encode()).hexdigest())

    def test_official_llamacpp_release_uses_only_policy_bound_tag_version(self):
        observation = replace(
            self.observation(),
            source_id="github-llamacpp-release",
            category="our_setup",
            original_url="https://api.github.com/repos/ggml-org/llama.cpp/releases/latest",
            canonical_url="https://github.com/ggml-org/llama.cpp/releases/tag/v0.6.0",
            publisher="llama.cpp",
            retrieval_method="github-release-api",
            observed_at="2026-10-06T07:02:00Z",
            published_at="2026-10-05T16:56:22Z",
            title="v0.6.0",
            body="Release notes include dependency versions v0.26.0 and v0.5.0.",
            publisher_host="github.com",
            effective_source_role=SourceRole.PRIMARY,
            independence_group="ggml-org-llama.cpp-origin",
            matched_rule_id="llama-cpp-own-release",
            authority_match=True,
            classification_reason="matched_rule",
            authority_scope=("our_setup",),
            authority_entities=("llama.cpp",),
        )

        rows = claims_from_observation(observation)

        self.assertEqual(len(rows.claims), 1)
        self.assertEqual(rows.claims[0].subject, "llama.cpp")
        self.assertEqual(rows.claims[0].predicate, "has_version")
        self.assertEqual(rows.claims[0].object_value, "0.6.0")
        self.assertEqual(rows.evidence[0].exact_excerpt, "v0.6.0")

    def test_llamacpp_release_tag_context_never_overrides_failed_source_authority(self):
        observation = replace(
            self.observation(),
            source_id="github-llamacpp-release",
            category="our_setup",
            original_url="https://api.github.com/repos/ggml-org/llama.cpp/releases/latest",
            canonical_url="https://github.com/ggml-org/llama.cpp/releases/tag/v0.6.0",
            publisher="llama.cpp",
            retrieval_method="github-release-api",
            title="v0.6.0",
            body="Release notes include dependency version v0.26.0.",
            publisher_host="github.com",
            effective_source_role=SourceRole.PRIMARY,
            independence_group="ggml-org-llama.cpp-origin",
            matched_rule_id="llama-cpp-own-release",
            authority_match=False,
            classification_reason="authority_not_matched",
            authority_scope=("our_setup",),
            authority_entities=("llama.cpp",),
        )

        rows = claims_from_observation(observation)

        self.assertTrue(rows.claims)
        self.assertFalse(any(claim.subject == "llama.cpp" for claim in rows.claims))

    def test_first_party_mkinitcpio_state_fact_uses_exact_named_authority(self):
        observation = ObservationContract(
            observation_id="arch-capture-1",
            source_id="first-party-rss",
            category="our_setup",
            kind=ObservationKind.PARSED_ARTICLE,
            original_url="https://example.test/advisory",
            canonical_url="https://example.test/advisory",
            publisher="Example Project",
            retrieval_method="publisher-article-fetch",
            raw_content_hash="a" * 64,
            observed_at="2026-09-24T19:40:28Z",
            published_at="2026-09-22T09:09:27Z",
            title="mkinitcpio hook advisory",
            body=(
                "2026-09-22 - Maintainer Starting with package version 42-1, "
                "the mkinitcpio systemd hook now includes systemd-example.service "
                "(as intended by systemd v261)."
            ),
            publisher_host="example.test",
            effective_source_role=SourceRole.PRIMARY,
            independence_group="example-origin",
            matched_rule_id="example-primary-rule",
            classification_reason="matched_rule",
            authority_scope=("our_setup",),
            authority_entities=("Example Project", "mkinitcpio"),
        )

        rows = claims_from_observation(observation)

        self.assertEqual(len(rows.claims), 1)
        self.assertEqual(rows.claims[0].subject, "mkinitcpio")
        self.assertEqual(rows.claims[0].predicate, "systemd_hook_includes_unit")
        self.assertEqual(
            rows.claims[0].object_value,
            "systemd-example.service starting with package version 42-1",
        )
        self.assertEqual(
            rows.evidence[0].exact_excerpt,
            "Starting with package version 42-1, the mkinitcpio systemd hook now "
            "includes systemd-example.service (as intended by systemd v261).",
        )
        self.assertEqual(
            rows.evidence[0].excerpt_hash,
            hashlib.sha256(rows.evidence[0].exact_excerpt.encode("utf-8")).hexdigest(),
        )

    def test_mkinitcpio_fact_fails_closed_for_leads_wrong_scope_and_numeric_only_text(self):
        eligible = ObservationContract(
            observation_id="arch-capture-negative-controls",
            source_id="first-party-rss",
            category="our_setup",
            kind=ObservationKind.PARSED_ARTICLE,
            original_url="https://example.test/advisory",
            canonical_url="https://example.test/advisory",
            publisher="Example Project",
            retrieval_method="publisher-article-fetch",
            raw_content_hash="b" * 64,
            observed_at="2026-09-24T19:40:28Z",
            published_at="2026-09-22T09:09:27Z",
            title="mkinitcpio hook advisory",
            body=(
                "Starting with package version 42-1, the mkinitcpio systemd hook "
                "now includes systemd-example.service."
            ),
            publisher_host="example.test",
            effective_source_role=SourceRole.PRIMARY,
            independence_group="example-origin",
            matched_rule_id="example-primary-rule",
            classification_reason="matched_rule",
            authority_scope=("our_setup",),
            authority_entities=("Example Project", "mkinitcpio"),
        )
        negative_observations = (
            replace(eligible, retrieval_method="rss-poll"),
            replace(eligible, effective_source_role=SourceRole.SPECIALIST),
            replace(
                eligible,
                effective_source_role=SourceRole.NEUTRAL,
                independence_group="syndicated-arch-origin",
            ),
            replace(eligible, authority_entities=("Example Project",)),
            replace(eligible, category="ai", authority_scope=("ai",)),
            replace(eligible, body="The mkinitcpio release references package 42-1 and systemd v261."),
            replace(eligible, body="The mkinitcpio systemd hook was discussed in a release."),
            replace(
                eligible,
                body=(
                    "Starting with package version 42-1, the vendor systemd hook "
                    "now includes systemd-example.service."
                ),
            ),
        )
        for index, observation in enumerate(negative_observations):
            with self.subTest(control=index):
                rows = claims_from_observation(observation)
                self.assertFalse(
                    any(claim.predicate == "systemd_hook_includes_unit" for claim in rows.claims)
                )

    def test_evidence_uses_reviewed_independence_group_not_publisher_text(self):
        observation = replace(
            self.observation(),
            publisher="Same Publisher",
            effective_source_role=SourceRole.SPECIALIST,
            independence_group="reviewed-trade-family",
            publisher_host="trade.example.com",
            classification_reason="matched_rule",
        )
        rows = claims_from_observation(observation)
        self.assertEqual(rows.evidence[0].independence_group, "reviewed-trade-family")
        self.assertNotEqual(rows.evidence[0].independence_group, observation.publisher.casefold())

    def test_transport_only_retrieval_cannot_claim_primary_authority(self):
        for method, reason in (
            ("rss-poll", "feed_transport_not_claim_evidence"),
            ("searxng-query", "search_result_not_claim_evidence"),
            ("social-post", "social_lead_not_claim_evidence"),
        ):
            with self.subTest(method=method):
                observation = observation_from_source_item({
                    "source_item_id": "lead-item",
                    "source_id": "transport",
                    "category": "ai",
                    "original_url": "https://example.test/story",
                    "canonical_url": "https://example.test/story",
                    "publisher": "Example",
                    "retrieval_method": method,
                    "raw_content_hash": "a" * 64,
                    "retrieved_at": "2026-09-21T00:00:00Z",
                    "effective_source_role": "primary",
                    "normalized_publisher_host": "example.test",
                    "independence_group": "example-origin",
                    "matched_rule_id": "example-primary",
                    "authority_match": 1,
                    "classification_reason": "matched_rule",
                    "authority_scope_json": '["ai"]',
                    "authority_entities_json": '["Product"]',
                })
                self.assertEqual(observation.effective_source_role, SourceRole.DISCOVERY)
                self.assertFalse(observation.authority_match)
                self.assertEqual(observation.classification_reason, reason)

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

    def test_missing_publication_date_maps_reason_without_false_date_evidence(self):
        observation = observation_from_source_item(
            {
                "source_item_id": "item-missing-date",
                "source_id": "search",
                "category": "world",
                "original_url": "https://example.test/story",
                "canonical_url": "https://example.test/story",
                "publisher": "Example",
                "retrieval_method": "searxng",
                "raw_content_hash": "a" * 64,
                "retrieved_at": "2026-09-21T00:00:00Z",
                "published_at": None,
                "updated_at": None,
                "publication_evidence": "missing",
            }
        )
        self.assertIsNone(observation.published_at)
        self.assertIsNone(observation.publication_evidence)
        self.assertEqual(observation.unknown_date_reason, "missing")


if __name__ == "__main__":
    unittest.main()
