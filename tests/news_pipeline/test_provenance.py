from __future__ import annotations

import unittest

from news_pipeline.live_contracts import SourceRole, VerificationState
from news_pipeline.provenance import PublisherRule, PublisherRegistry, load_provenance
from news_pipeline.verification import verify_evidence


CLASSIFIED_AT = "2026-09-19T22:00:00Z"


class PublisherRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.registry = PublisherRegistry(
            (
                PublisherRule(
                    rule_id="maker-example",
                    host="manufacturer.example.com",
                    source_role=SourceRole.PRIMARY,
                    independence_group="manufacturer-example",
                    categories=("ai", "hardware"),
                    authority_entities=("Example Manufacturer",),
                    audit_note="first-party release authority",
                ),
                PublisherRule(
                    rule_id="trade-example",
                    host="trade.example.com",
                    source_role=SourceRole.SPECIALIST,
                    independence_group="trade-example",
                    categories=("ai",),
                    audit_note="specialist trade publication",
                ),
            )
        )

    def test_first_party_rule_matches_exact_host_and_authority(self) -> None:
        result = self.registry.classify(
            "https://manufacturer.example.com/releases/v2",
            category="ai",
            claim_subject="Example Manufacturer",
            classified_at=CLASSIFIED_AT,
        )
        self.assertEqual(result.normalized_publisher_host, "manufacturer.example.com")
        self.assertEqual(result.effective_source_role, SourceRole.PRIMARY)
        self.assertEqual(result.independence_group, "manufacturer-example")
        self.assertEqual(result.matched_rule_id, "maker-example")
        self.assertTrue(result.authority_match)

    def test_trade_subdomain_is_specialist_not_primary(self) -> None:
        result = self.registry.classify(
            "https://news.trade.example.com/release",
            category="ai",
            claim_subject="Example Manufacturer",
            classified_at=CLASSIFIED_AT,
        )
        self.assertEqual(result.effective_source_role, SourceRole.SPECIALIST)
        self.assertEqual(result.independence_group, "trade-example")
        self.assertFalse(result.authority_match)

    def test_unknown_publisher_is_discovery_and_unverified(self) -> None:
        result = self.registry.classify(
            "https://unknown.example.com/story",
            category="ai",
            claim_subject="Example Manufacturer",
            classified_at=CLASSIFIED_AT,
        )
        self.assertEqual(result.effective_source_role, SourceRole.DISCOVERY)
        self.assertEqual(result.independence_group, "unknown")
        self.assertIsNone(result.matched_rule_id)
        self.assertFalse(result.authority_match)
        self.assertEqual(result.classification_reason, "unknown_publisher")

    def test_reviewed_groups_drive_verification_not_publisher_text(self) -> None:
        self.assertEqual(
            verify_evidence(
                (
                    {
                        "role": "supports",
                        "effective_source_role": "primary",
                        "independence_group": "manufacturer-example",
                        "authority_match": True,
                    },
                )
            ),
            VerificationState.VERIFIED,
        )
        self.assertEqual(
            verify_evidence(
                (
                    {
                        "role": "supports",
                        "effective_source_role": "primary",
                        "independence_group": "manufacturer-example",
                        "authority_match": False,
                    },
                )
            ),
            VerificationState.UNVERIFIED,
        )
        self.assertEqual(
            verify_evidence(
                (
                    {
                        "role": "supports",
                        "effective_source_role": "neutral",
                        "independence_group": "group-a",
                        "authority_match": False,
                    },
                    {
                        "role": "supports",
                        "effective_source_role": "specialist",
                        "independence_group": "group-b",
                        "authority_match": False,
                    },
                )
            ),
            VerificationState.VERIFIED,
        )
        self.assertEqual(
            verify_evidence(
                (
                    {
                        "role": "supports",
                        "effective_source_role": "neutral",
                        "independence_group": "same-family",
                        "authority_match": False,
                    },
                    {
                        "role": "supports",
                        "effective_source_role": "specialist",
                        "independence_group": "same-family",
                        "authority_match": False,
                    },
                )
            ),
            VerificationState.UNVERIFIED,
        )
        self.assertEqual(
            verify_evidence(
                (
                    {
                        "role": "contradicts",
                        "effective_source_role": "primary",
                        "independence_group": "manufacturer-example",
                        "authority_match": True,
                    },
                )
            ),
            VerificationState.WATCHLIST,
        )

    def test_missing_effective_provenance_fails_closed(self) -> None:
        self.assertEqual(
            verify_evidence(
                ({"role": "supports", "independence_group": "publisher-text"},)
            ),
            VerificationState.UNVERIFIED,
        )

    def test_public_example_registry_loads_without_live_values(self) -> None:
        config = load_provenance("config/news-provenance.example.toml")
        self.assertTrue(config.rules)
        self.assertTrue(all(rule.host.endswith(".example.com") for rule in config.rules))


if __name__ == "__main__":
    unittest.main()
