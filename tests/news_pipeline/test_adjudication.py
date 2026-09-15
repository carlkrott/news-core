"""Phase 3 — Slice 4 rules, Slice 5 model boundary, Slice 6 batch API."""
from __future__ import annotations

import json
import unittest
from datetime import timedelta
from decimal import Decimal

from news_pipeline.contracts import CandidateArticle, DecisionCode, FilterResult, HistoryMatch, ReasonCode, TrustTier
from news_pipeline.event_contracts import EventCandidate, InternalRuleDecision, ModelDecision, ModelErrorCategory, SemanticDecision, SemanticReasonCode
from news_pipeline.models import Category
from news_pipeline.policies import QueryPolicy
from news_pipeline.clustering import score_pair
from news_pipeline.adjudication import classify_pair
from news_pipeline.phase3_api import (
    FIXED_MODEL_INSTRUCTION,
    ModelTransportError,
    build_model_request,
    evaluate_semantic_updates,
    parse_model_response,
    render_model_payload,
)


def _policy(category=Category.AI, cross=True):
    return QueryPolicy(category=category, allowed_query_groups=(category.value,), recency=timedelta(days=7), missing_date_fallback=True, exact_title_lookback=timedelta(days=7), exact_url_lookback=timedelta(days=7), exact_identity_lookback=timedelta(days=7), cross_category_exact_url=cross)


def _candidate(cid="c1", title="Product launch", snippet="A product update", category=Category.AI, url=None, evaluated="2026-07-01T00:00:00Z"):
    return CandidateArticle(candidate_id=cid, category=category, query_group=category.value, title=title, snippet=snippet, original_url=url, canonical_url=url, published_at="2026-06-30T00:00:00Z", published_evidence="source", observed_at="2026-07-01T00:00:00Z", evaluated_at=evaluated)


def _history(aid="h1", title="Product launch", snippet="A product update", category=Category.AI, url=None, occurred="2026-06-01T00:00:00Z", oid=None):
    return HistoryMatch(article_id=aid, observation_id=oid or "o-" + aid, category=category, occurred_at=occurred, title=title, snippet=snippet, canonical_url=url, identity_basis="title_only")


def _event(c, decision=DecisionCode.KEEP, reasons=(ReasonCode.OK_KEEP,), article_ids=(), obs_ids=(), ordinal=0):
    f = FilterResult(candidate=c, decision=decision, reasons=reasons, matched_article_ids=tuple(article_ids), matched_observation_ids=tuple(obs_ids), trust_tier=TrustTier.UNKNOWN, evaluated_publication_time=None, ordinal=ordinal)
    return EventCandidate(candidate=c, filter_result=f, query_policy=_policy(c.category))


def _scored(c, h):
    return score_pair(c, h)


class TestRules(unittest.TestCase):
    def test_correction_precedence(self):
        c = _candidate(title="Product launch correction", snippet="The correction changes the report")
        h = _history(title="Product launch announced", snippet="The original report")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.material_update)
        self.assertEqual(v.reasons, (SemanticReasonCode.CORRECTION_OR_RETRACTION,))

    def test_rumor_to_official(self):
        c = _candidate(title="Product launch official", snippet="Officially confirmed today")
        h = _history(title="Product launch rumor", snippet="Sources say it is coming")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.material_update)
        self.assertEqual(v.reasons, (SemanticReasonCode.CONFIRMED_RUMOR,))

    def test_announcement_or_preorder_to_launch_or_ga(self):
        c = _candidate(title="Product launch launched", snippet="The product is available now")
        h = _history(title="Product launch announced", snippet="Announcement details")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.material_update)
        self.assertEqual(v.reasons, (SemanticReasonCode.LAUNCHED_OR_SHIPPED,))

    def test_negation_previous_three_tokens(self):
        c = _candidate(title="Product launch official", snippet="It was not officially confirmed")
        h = _history(title="Product launch rumor", snippet="Sources say it is coming")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.model_required)
        self.assertIn(SemanticReasonCode.UNRESOLVED_CONFLICT, v.reasons)

    def test_conflicting_lifecycle_requires_model(self):
        c = _candidate(title="Product rumor launched", snippet="The product launched")
        h = _history(title="Product rumor", snippet="A rumor")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.model_required)
        self.assertIn(SemanticReasonCode.UNRESOLVED_CONFLICT, v.reasons)

    def test_single_fact_revision_material(self):
        c = _candidate(title="Product pricing update", snippet="The price is $20")
        h = _history(title="Product pricing update", snippet="The price is $10")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.material_update)
        self.assertEqual(v.reasons, (SemanticReasonCode.NUMERIC_REVISION,))
        self.assertEqual(len(v.fact_deltas), 1)
        self.assertEqual(v.fact_deltas[0].topic_gate, Decimal("1.000000"))

    def test_multiple_fact_values_require_model(self):
        c = _candidate(title="Product pricing update", snippet="Prices are $20 and $30")
        h = _history(title="Product pricing update", snippet="The price is $10")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.model_required)
        self.assertIn(SemanticReasonCode.UNRESOLVED_CONFLICT, v.reasons)

    def test_rewrite_requires_score_facts_and_no_lifecycle(self):
        c = _candidate(title="Product specifications", snippet="The price is $20 and 10 users")
        h = _history(title="Product specifications", snippet="The price is $20 and 10 users")
        v = classify_pair(c, h, _scored(c, h))
        self.assertEqual(v.decision, InternalRuleDecision.rewrite)
        self.assertEqual(v.reasons, (SemanticReasonCode.SAME_FACTS,))




    def test_phrase_tokens_remove_stopwords_and_keep_negation_order(self):
        from news_pipeline.adjudication import _family_hits, _phrase_tokens

        tokens = _phrase_tokens("It is not officially confirmed today")
        self.assertEqual(tokens, ("it", "not", "officially", "confirmed", "today"))
        self.assertNotIn("is", tokens)
        hits = _family_hits("It is not officially confirmed today")
        self.assertIn(False, hits["new_official"])

    def test_phrase_token_stream_preserves_text_order(self):
        from news_pipeline.adjudication import _phrase_tokens

        self.assertEqual(
            _phrase_tokens("not officially confirmed"),
            ("not", "officially", "confirmed"),
        )


if __name__ == "__main__":
    unittest.main()
