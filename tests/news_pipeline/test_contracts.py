"""Tests for the Phase 2 deterministic dry-run filtering contracts.

The package under test exposes:
- CandidateArticle (frozen/slotted dataclass)
- DecisionCode enum
- ReasonCode enum
- FilterResult dataclass
- HistoryMatch dataclass
- PublishedEvidence Literal type alias
- validate_utc_iso helper
"""
from __future__ import annotations

import dataclasses
import unittest
from datetime import UTC, datetime, timedelta

from news_pipeline.contracts import (
    CandidateArticle,
    DecisionCode,
    FilterResult,
    HistoryMatch,
    PublishedEvidence,
    ReasonCode,
    TrustTier,
    validate_utc_iso,
)
from news_pipeline.models import Category


EVAL = "2026-07-14T22:50:33Z"
PUB = "2026-07-14T20:50:33Z"
OBS = "2026-07-14T19:50:33Z"


def _candidate(**overrides):
    base = dict(
        candidate_id="cand-1",
        category=Category.AI,
        query_group="ai",
        title="Some title",
        snippet="snippet body",
        original_url="https://example.com/a",
        canonical_url="https://example.com/a",
        published_at=PUB,
        published_evidence="source",
        observed_at=OBS,
        evaluated_at=EVAL,
    )
    base.update(overrides)
    return CandidateArticle(**base)


class CandidateArticleTests(unittest.TestCase):
    def test_frozen_and_slotted(self):
        self.assertTrue(dataclasses.is_dataclass(CandidateArticle))
        f = dataclasses.fields(CandidateArticle)
        for field in f:
            self.assertIsInstance(field.name, str)
        c = _candidate()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            c.title = "other"  # type: ignore[misc]
        # __slots__ prevents arbitrary attributes
        # CPython 3.14's generated frozen+slots __setattr__ raises TypeError for
        # undeclared attributes; older versions raise AttributeError. Both prove
        # that arbitrary attributes cannot be added.
        with self.assertRaises((AttributeError, TypeError)):
            c.undeclared = "x"  # type: ignore[attr-defined]

    def test_required_fields_present(self):
        names = {f.name for f in dataclasses.fields(CandidateArticle)}
        for required in (
            "candidate_id",
            "category",
            "query_group",
            "title",
            "snippet",
            "original_url",
            "canonical_url",
            "published_at",
            "published_evidence",
            "observed_at",
            "evaluated_at",
        ):
            self.assertIn(required, names)

    def test_published_evidence_literal_values(self):
        # Force only the documented literals
        for value in ("source", "metadata", "missing", "unparseable"):
            c = _candidate(published_evidence=value)
            self.assertEqual(c.published_evidence, value)
        # PublishedEvidence alias itself
        self.assertIn("source", PublishedEvidence.__args__)

    def test_validate_utc_iso_accepts_z_suffix(self):
        self.assertEqual(validate_utc_iso("2026-07-14T22:50:33Z"), "2026-07-14T22:50:33Z")

    def test_validate_utc_iso_accepts_offset(self):
        self.assertEqual(
            validate_utc_iso("2026-07-14T22:50:33+00:00"),
            "2026-07-14T22:50:33Z",
        )

    def test_validate_utc_iso_rejects_naive(self):
        with self.assertRaises(ValueError):
            validate_utc_iso("2026-07-14T22:50:33")

    def test_validate_utc_iso_rejects_garbage(self):
        with self.assertRaises(ValueError):
            validate_utc_iso("not-a-date")
        with self.assertRaises(ValueError):
            validate_utc_iso("")


class DecisionCodeEnumTests(unittest.TestCase):
    def test_required_decisions(self):
        names = {d.name for d in DecisionCode}
        for required in (
            "KEEP",
            "SUPPRESS_BATCH_EXACT",
            "SUPPRESS_EXACT_URL",
            "SUPPRESS_EXACT_IDENTITY",
            "SUPPRESS_RECENT_TITLE",
            "DROP_STALE",
            "DROP_BLOCKED_SOURCE",
            "PENDING_MISSING_EVIDENCE",
            "PENDING_INVALID_EVIDENCE",
            "PENDING_POSSIBLE_UPDATE",
            "PENDING_HISTORY_UNAVAILABLE",
        ):
            self.assertIn(required, names, required)


class ReasonCodeEnumTests(unittest.TestCase):
    def test_minimum_reasons(self):
        names = {r.name for r in ReasonCode}
        for required in (
            "OK_KEEP",
            "OK_BLOCKED_SOURCE",
            "OK_OBSERVED_FALLBACK",
            "OK_TITLE_ONLY_LOW_CONFIDENCE",
            "OK_TRUSTED_SOURCE",
            "OK_UNKNOWN_SOURCE",
            "BLOCKED_SOURCE_EXACT",
            "BLOCKED_SOURCE_SUBDOMAIN",
            "STALE",
            "MISSING_DATE",
            "INVALID_DATE",
            "FUTURE_DATE",
            "WITHIN_BATCH_EXACT",
            "HISTORY_EXACT_URL",
            "HISTORY_EXACT_IDENTITY",
            "HISTORY_RECENT_TITLE",
            "HISTORY_URL_CHANGED_CONTENT",
            "HISTORY_TITLE_CHANGED_SNIPPET",
            "UNKNOWN_QUERY_GROUP",
            "MALFORMED_CANONICAL_URL",
            "MISSING_URL",
            "HISTORY_UNAVAILABLE",
            "CROSS_CATEGORY_PASSTHROUGH",
        ):
            self.assertIn(required, names, required)


class FilterResultTests(unittest.TestCase):
    def test_filter_result_carries_ordinal_and_trust(self):
        c = _candidate()
        result = FilterResult(
            candidate=c,
            decision=DecisionCode.KEEP,
            reasons=(ReasonCode.OK_KEEP,),
            matched_article_ids=(),
            matched_observation_ids=(),
            trust_tier=TrustTier.UNKNOWN,
            evaluated_publication_time=PUB,
            ordinal=3,
        )
        self.assertEqual(result.decision, DecisionCode.KEEP)
        self.assertEqual(result.trust_tier, TrustTier.UNKNOWN)
        self.assertEqual(result.ordinal, 3)
        self.assertEqual(result.candidate.candidate_id, "cand-1")


class HistoryMatchTests(unittest.TestCase):
    def test_history_match_typed_fields(self):
        m = HistoryMatch(
            article_id="art-1",
            observation_id="obs-1",
            category=Category.AI,
            occurred_at=OBS,
            title="t",
            snippet="s",
            canonical_url="https://example.com/x",
            identity_basis="canonical_url",
        )
        self.assertEqual(m.identity_basis, "canonical_url")


class TrustTierTests(unittest.TestCase):
    def test_tiers(self):
        names = {t.name for t in TrustTier}
        self.assertEqual(
            names,
            {"BLOCKED", "ALLOWED", "TRUSTED", "UNKNOWN"},
        )


if __name__ == "__main__":
    unittest.main()
