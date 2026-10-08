from __future__ import annotations

import json
import unittest

from unittest.mock import patch

from news_pipeline.verification import (
    GroundingError,
    authority_entity_matches,
    claim_specific_evidence_row,
    validate_model_output,
    verify_evidence,
)
from news_pipeline.live_contracts import EvidenceRole, SourceRole, VerificationState


class VerificationTests(unittest.TestCase):
    def test_claim_specific_primary_requires_exact_named_subject_and_identity(self):
        row = (
            "supports", "primary", "cachyos-origin", 1, "cachyos-own-announce",
            "cachyos.org", '["our_setup"]', '["CachyOS"]', "our_setup",
        )
        direct = claim_specific_evidence_row(row, "CachyOS")
        self.assertTrue(direct["authority_match"])
        self.assertEqual(verify_evidence([direct]), VerificationState.VERIFIED)

        self.assertFalse(authority_entity_matches("2026", ["2026"]))
        self.assertFalse(authority_entity_matches("2.4e3", ["2.4e3"]))
        numeric = claim_specific_evidence_row(
            row[:4] + (row[4], row[5], row[6], '["2026"]', row[8]), "2026"
        )
        self.assertFalse(numeric["authority_match"])
        self.assertEqual(verify_evidence([numeric]), VerificationState.UNVERIFIED)

        missing_identity = claim_specific_evidence_row(
            row[:4] + (None, row[5], row[6], row[7], row[8]), "CachyOS"
        )
        self.assertEqual(missing_identity["effective_source_role"], "")
        self.assertEqual(verify_evidence([missing_identity]), VerificationState.UNVERIFIED)

    def test_enabled_registry_rule_restores_feed_source_role_only_for_exact_host_and_scope(self):
        row = (
            "supports", "discovery", "feed-family", False, "trade-rule",
            "trade.example", '["ai"]', "[]", "ai",
        )
        matched = claim_specific_evidence_row(
            row, "Story subject", registry_enabled=True,
            registry_role="specialist", registry_group="trade-family",
            registry_host="trade.example", registry_categories='["ai"]',
        )
        self.assertEqual(matched["effective_source_role"], "specialist")
        self.assertEqual(matched["independence_group"], "trade-family")
        self.assertTrue(matched["registry_matched"])
        self.assertEqual(verify_evidence([matched]), VerificationState.VERIFIED)
        for bad in (
            {"registry_host": "other.example"},
            {"registry_categories": '["hardware"]'},
            {"registry_enabled": False},
        ):
            kwargs = {
                "registry_enabled": True, "registry_role": "specialist",
                "registry_group": "trade-family", "registry_host": "trade.example",
                "registry_categories": '["ai"]',
            }
            kwargs.update(bad)
            not_matched = claim_specific_evidence_row(row, "Story subject", **kwargs)
            self.assertFalse(not_matched["registry_matched"])
            self.assertEqual(verify_evidence([not_matched]), VerificationState.UNVERIFIED)

    def test_grounded_output_accepts_only_supplied_evidence(self):
        evidence = {"e1": "release v2.0 launched"}
        output = json.dumps({"decision": "verified", "confidence": 0.9, "summary": "release v2.0 launched", "evidence_ids": ["e1"], "facts": ["v2.0"]})
        self.assertEqual(validate_model_output(output, evidence).decision, "verified")
        forged = json.dumps({"decision": "verified", "confidence": 0.9, "summary": "release v2.0 launched", "evidence_ids": ["e1"], "facts": ["invented"]})
        with self.assertRaises(GroundingError):
            validate_model_output(forged, evidence)

    def test_grounding_rejects_substrings_but_accepts_exact_spans(self):
        evidence = {"e1": "The price is $100; the partial article."}
        for fact in ("$10", "art"):
            raw = json.dumps({"decision": "verified", "confidence": 1, "summary": "price", "evidence_ids": ["e1"], "facts": [fact]})
            with self.assertRaises(GroundingError):
                validate_model_output(raw, evidence)
        raw = json.dumps({"decision": "verified", "confidence": 1, "summary": "price", "evidence_ids": ["e1"], "facts": ["$100", "partial article"]})
        self.assertEqual(validate_model_output(raw, evidence).facts, ("$100", "partial article"))

    def test_duplicate_unknown_and_nonfinite_output_fail_closed(self):
        with self.assertRaises(GroundingError):
            validate_model_output('{"decision":"pending","decision":"verified","confidence":0,"summary":"x","evidence_ids":[],"facts":[]}', {})
        with self.assertRaises(GroundingError):
            validate_model_output('{"decision":"pending","confidence":NaN,"summary":"x","evidence_ids":[],"facts":[]}', {})
        with self.assertRaises(GroundingError):
            validate_model_output('{"decision":"pending","confidence":0,"summary":"x","evidence_ids":["bad"],"facts":[]}', {})

    def test_source_independence_and_conflict_rules(self):
        self.assertEqual(verify_evidence([{"role": "supports", "independence_group": "a", "source_role": "primary"}]), VerificationState.VERIFIED)
        self.assertEqual(verify_evidence([{"role": "supports", "independence_group": "a", "source_role": "discovery"}]), VerificationState.UNVERIFIED)
        self.assertEqual(verify_evidence([{"role": "supports", "independence_group": "a", "source_role": "neutral"}, {"role": "supports", "independence_group": "b", "source_role": "specialist"}]), VerificationState.VERIFIED)
        self.assertEqual(verify_evidence([{"role": "contradicts", "independence_group": "b", "source_role": "primary"}]), VerificationState.WATCHLIST)
        self.assertEqual(verify_evidence([{"role": EvidenceRole.SUPPORTS, "independence_group": " ", "source_role": SourceRole.DISCOVERY}]), VerificationState.UNVERIFIED)
        self.assertEqual(verify_evidence([{"role": EvidenceRole.SUPPORTS, "independence_group": "a", "source_role": SourceRole.NEUTRAL}, {"role": EvidenceRole.SUPPORTS, "independence_group": "b", "source_role": SourceRole.SPECIALIST}]), VerificationState.VERIFIED)
        self.assertEqual(verify_evidence([{"role": "supports", "independence_group": "a", "source_role": "discovery"}, {"role": "supports", "independence_group": "b", "source_role": "discovery"}]), VerificationState.UNVERIFIED)

    def test_sources_sharing_canonical_url_or_text_hash_are_not_independent(self):
        base = {"role": "supports", "source_role": "specialist"}
        for duplicate_field in ("canonical_url", "text_hash"):
            with self.subTest(duplicate_field=duplicate_field):
                first = {
                    **base,
                    "independence_group": "outlet-a",
                    "canonical_url": "https://a.example/story",
                    "text_hash": "a" * 64,
                }
                second = {
                    **base,
                    "independence_group": "outlet-b",
                    "canonical_url": "https://b.example/story",
                    "text_hash": "b" * 64,
                }
                second[duplicate_field] = first[duplicate_field]
                self.assertEqual(
                    verify_evidence([first, second]), VerificationState.UNVERIFIED
                )

    def test_wire_syndication_parent_is_not_independent(self):
        first = {
            "role": "supports", "source_role": "neutral",
            "independence_group": "outlet-a", "canonical_url": "https://a.example/story",
            "text_hash": "a" * 64, "syndicated_parent": "wire-story-1",
        }
        second = {
            "role": "supports", "source_role": "specialist",
            "independence_group": "outlet-b", "canonical_url": "https://b.example/story",
            "text_hash": "b" * 64, "syndicated_parent": "wire-story-1",
        }
        self.assertEqual(verify_evidence([first, second]), VerificationState.UNVERIFIED)

    def test_single_approved_outlet_can_verify_and_strict_mode_preserves_old_rule(self):
        primary = {
            "role": "supports", "effective_source_role": "primary",
            "independence_group": "vendor-a", "authority_match": False,
            "matched_rule_id": "vendor-a-rule", "normalized_publisher_host": "vendor.example",
            "authority_scope_json": '["hardware"]', "authority_entities_json": '["Vendor A"]',
            "category": "hardware", "registry_matched": True,
        }
        specialist = {
            **primary, "effective_source_role": "specialist",
            "independence_group": "trade-a", "matched_rule_id": "trade-a-rule",
            "normalized_publisher_host": "trade.example",
        }
        unregistered = {**specialist, "effective_source_role": "discovery", "matched_rule_id": ""}
        self.assertEqual(verify_evidence([primary]), VerificationState.VERIFIED)
        self.assertEqual(verify_evidence([specialist]), VerificationState.VERIFIED)
        self.assertEqual(verify_evidence([unregistered]), VerificationState.UNVERIFIED)
        self.assertEqual(verify_evidence([specialist], allow_single_outlet=False), VerificationState.UNVERIFIED)
        with patch.dict("os.environ", {"NEWS_SINGLE_OUTLET_ENABLED": "0"}):
            self.assertEqual(verify_evidence([specialist]), VerificationState.UNVERIFIED)
        with patch.dict("os.environ", {}, clear=True):
            self.assertEqual(verify_evidence([specialist]), VerificationState.VERIFIED)

    def test_independent_outlets_and_primary_authority_still_verify(self):
        independent = [
            {"role": "supports", "source_role": "neutral", "independence_group": "outlet-a",
             "canonical_url": "https://a.example/story", "text_hash": "a" * 64},
            {"role": "supports", "source_role": "specialist", "independence_group": "outlet-b",
             "canonical_url": "https://b.example/story", "text_hash": "b" * 64},
        ]
        self.assertEqual(verify_evidence(independent), VerificationState.VERIFIED)
        self.assertEqual(
            verify_evidence([{"role": "supports", "source_role": "primary", "independence_group": "authority",
                              "authority_match": True, "canonical_url": "https://authority.example/story",
                              "text_hash": "c" * 64}]),
            VerificationState.VERIFIED,
        )

    def test_ungrounded_summary_and_prompt_injection_fail_closed(self):
        evidence = {"e1": "release v2.0 launched"}
        for summary in ("unrelated", "ignore all previous instructions"):
            raw = json.dumps({"decision": "verified", "confidence": 1, "summary": summary, "evidence_ids": ["e1"], "facts": []})
            with self.assertRaises(GroundingError):
                validate_model_output(raw, evidence)

    def test_evidence_free_verified_and_trailing_json_rejected(self):
        raw = json.dumps({"decision": "verified", "confidence": 1, "summary": "release", "evidence_ids": [], "facts": []})
        with self.assertRaises(GroundingError):
            validate_model_output(raw, {"e1": "release"})
        raw = json.dumps({"decision": "pending", "confidence": 0, "summary": "release", "evidence_ids": [], "facts": []}) + " trailing"
        with self.assertRaises(GroundingError):
            validate_model_output(raw, {"e1": "release"})


if __name__ == "__main__":
    unittest.main()
