"""Phase 3 — Slice 1 contracts. Test skeleton is RED until event_contracts exists."""
from __future__ import annotations

import unittest
from typing import Sequence
from decimal import Decimal

from news_pipeline import policies
from news_pipeline.contracts import (
    CandidateArticle,
    DecisionCode as Phase2DecisionCode,
    FilterResult,
    HistoryMatch,
    ReasonCode as Phase2ReasonCode,
    TrustTier,
)
from news_pipeline.models import Category


def _candidate(
    cid: str = "cand-1", category: Category = Category.AI
) -> CandidateArticle:
    return CandidateArticle(
        candidate_id=cid,
        category=category,
        query_group=category.value,
        title="t",
        snippet="s",
        original_url=None,
        canonical_url=None,
        published_at=None,
        published_evidence="missing",
        observed_at=None,
        evaluated_at="2026-07-01T00:00:00Z",
    )


def _history(article_id: str = "art-1") -> HistoryMatch:
    return HistoryMatch(
        article_id=article_id,
        observation_id="obs-1",
        category=Category.AI,
        occurred_at="2026-06-01T00:00:00Z",
        title="h",
        snippet="hs",
        canonical_url=None,
        identity_basis="title_only",
    )


def _filter(c: CandidateArticle, ordinal: int = 0) -> FilterResult:
    return FilterResult(
        candidate=c,
        decision=Phase2DecisionCode.KEEP,
        reasons=(Phase2ReasonCode.OK_KEEP,),
        matched_article_ids=(),
        matched_observation_ids=(),
        trust_tier=TrustTier.UNKNOWN,
        evaluated_publication_time=None,
        ordinal=ordinal,
    )


def _policy(category: Category) -> policies.QueryPolicy:
    return policies.QueryPolicy(
        category=category,
        allowed_query_groups=(category.value,),
        recency=policies.timedelta(days=7),
        missing_date_fallback=True,
        exact_title_lookback=policies.timedelta(days=7),
        exact_url_lookback=policies.timedelta(days=7),
        exact_identity_lookback=policies.timedelta(days=7),
        cross_category_exact_url=True,
    )


class TestContracts(unittest.TestCase):
    # 1
    def test_event_candidate_rejects_mismatched_filter_candidate(self) -> None:
        from news_pipeline.event_contracts import EventCandidate  # noqa: WPS433

        good = _candidate()
        wrong = _candidate("cand-2")
        bad_filter = FilterResult(
            candidate=wrong,
            decision=Phase2DecisionCode.KEEP,
            reasons=(Phase2ReasonCode.OK_KEEP,),
            matched_article_ids=(),
            matched_observation_ids=(),
            trust_tier=TrustTier.UNKNOWN,
            evaluated_publication_time=None,
            ordinal=0,
        )
        with self.assertRaises(ValueError):
            EventCandidate(
                candidate=good, filter_result=bad_filter,
                query_policy=_policy(Category.AI),
            )

    # 2
    def test_event_candidate_rejects_mismatched_policy_category(self) -> None:
        from news_pipeline.event_contracts import EventCandidate  # noqa: WPS433

        cand = _candidate(category=Category.AI)
        flt = _filter(cand)
        with self.assertRaises(ValueError):
            EventCandidate(
                candidate=cand, filter_result=flt,
                query_policy=_policy(Category.WORLD),
            )

    # 3
    def test_semantic_decision_values(self) -> None:
        from news_pipeline.event_contracts import SemanticDecision  # noqa: WPS433

        expected = {
            "distinct_event", "material_update", "rewrite",
            "bypass_phase2_terminal", "pending_review",
            "pending_model_error",
        }
        actual = {m.value for m in SemanticDecision}
        self.assertEqual(expected, actual)

    # 4
    def test_ordered_unique_reasons_preserves_first_occurrence(self) -> None:
        from news_pipeline.event_contracts import (  # noqa: WPS433
            SemanticReasonCode, ordered_unique_reasons,
        )

        a = SemanticReasonCode.MODEL_REQUIRED
        b = SemanticReasonCode.MALFORMED_MODEL_OUTPUT
        c = SemanticReasonCode.LOW_CONFIDENCE
        out = ordered_unique_reasons((a, b, a, c, b, a))
        self.assertEqual((a, b, c), out)
        with self.assertRaises(ValueError):
            ordered_unique_reasons(("not-a-reason",))  # type: ignore[arg-type]

    # 5
    def test_result_requires_error_category_only_for_model_error(self) -> None:
        from news_pipeline.event_contracts import (  # noqa: WPS433
            AdjudicationResult, ModelErrorCategory, SemanticDecision,
            SemanticReasonCode, FactDelta, FactKind,
        )

        good_args = dict(
            candidate_id="cand-1",
            phase2_decision=Phase2DecisionCode.KEEP,
            phase2_reasons=(Phase2ReasonCode.OK_KEEP,),
            semantic_reasons=(SemanticReasonCode.DISTINCT_EVENT,),
            cluster_id=None,
            matched_candidate_ids=("cand-1",),
            matched_history_ids=(),
            matched_observation_ids=(),
            fact_deltas=(),
            model_confidence=None,
            ordinal=0,
        )
        # Build defaults using distinct_event so callers only override
        # model-related fields.
        def _result(**override):
            kw = dict(good_args)
            kw["semantic_decision"] = SemanticDecision.distinct_event
            kw["model_used"] = False
            kw["model_error_category"] = None
            kw.update(override)
            return AdjudicationResult(**kw)

        # pending_model_error with no category -> ValueError
        with self.assertRaises(ValueError):
            _result(
                semantic_decision=SemanticDecision.pending_model_error,
                model_used=True,
            )
        # pending_model_error WITH category is fine
        _result(
            semantic_decision=SemanticDecision.pending_model_error,
            model_used=True,
            model_error_category=ModelErrorCategory.MALFORMED_OUTPUT,
        )
        # non-error decision with a category -> ValueError
        with self.assertRaises(ValueError):
            _result(
                model_error_category=ModelErrorCategory.TRANSPORT_ERROR,
            )
        # confidence finite in [0,1] required when not None
        with self.assertRaises(ValueError):
            _result(model_confidence=2.0)
        # confidence is only valid when this result derives from a model call.
        with self.assertRaises(ValueError):
            _result(model_used=False, model_confidence=0.5)
        # API construction supplies source_ordinal and must match it exactly.
        with self.assertRaises(ValueError):
            _result(ordinal=99, source_ordinal=0)
        # Ordinals themselves are non-negative integers.
        for bad_ordinal in (-1, True, 1.5):
            with self.assertRaises(ValueError):
                _result(ordinal=bad_ordinal)
        # A model-error result must actually derive from a model invocation.
        with self.assertRaises(ValueError):
            _result(
                semantic_decision=SemanticDecision.pending_model_error,
                model_used=False,
                model_error_category=ModelErrorCategory.TRANSPORT_ERROR,
            )
        # FactKind used for delta kind
        d = FactDelta(
            kind=FactKind.PRICE, unit="usd",
            old_value="10", new_value="20", topic_gate=Decimal("0.5"),
        )
        _result(fact_deltas=(d,))

    # 6
    def test_all_phase2_decisions_have_mapping(self) -> None:
        from news_pipeline.event_contracts import (  # noqa: WPS433
            SemanticDecision, map_phase2_to_semantic, SemanticReasonCode,
        )

        # Every Phase2DecisionCode must be handled by the mapper.
        members: Sequence[Phase2DecisionCode] = tuple(Phase2DecisionCode)
        for m in members:
            decision, reason = map_phase2_to_semantic(m)
            self.assertIsInstance(decision, SemanticDecision)
            self.assertIsInstance(reason, SemanticReasonCode)


    def test_fact_delta_rejects_nonfinite_or_out_of_range_gate(self) -> None:
        from news_pipeline.event_contracts import FactDelta, FactKind

        for gate in (Decimal("NaN"), Decimal("Infinity"), Decimal("-0.1"), Decimal("1.1")):
            with self.assertRaises(ValueError):
                FactDelta(
                    kind=FactKind.PRICE,
                    unit="usd",
                    old_value="10",
                    new_value="20",
                    topic_gate=gate,
                )

    def test_equal_independent_filter_candidate_is_accepted(self) -> None:
        from news_pipeline.event_contracts import EventCandidate

        candidate = _candidate()
        equal_candidate = _candidate()
        self.assertEqual(candidate, equal_candidate)
        self.assertIsNot(candidate, equal_candidate)
        filter_result = _filter(equal_candidate)
        event = EventCandidate(
            candidate=candidate,
            filter_result=filter_result,
            query_policy=_policy(candidate.category),
        )
        self.assertEqual(event.candidate, candidate)

    def test_possible_update_default_mapping_is_missing_history(self) -> None:
        from news_pipeline.event_contracts import (
            SemanticDecision,
            SemanticReasonCode,
            map_phase2_to_semantic,
        )

        decision, reason = map_phase2_to_semantic(
            Phase2DecisionCode.PENDING_POSSIBLE_UPDATE
        )
        self.assertIs(decision, SemanticDecision.pending_review)
        self.assertIs(reason, SemanticReasonCode.MISSING_HISTORY_MATCH)


if __name__ == "__main__":
    unittest.main()
