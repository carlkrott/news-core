from __future__ import annotations

import json
import unittest

from news_pipeline.verification import GroundingError, validate_model_output, verify_evidence
from news_pipeline.live_contracts import EvidenceRole, SourceRole, VerificationState


class VerificationTests(unittest.TestCase):
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
