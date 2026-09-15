"""Phase 4 — Slice 1 contracts (briefing input / time / eligibility) tests.

Twenty unittest methods for the singular ``BriefingInput`` contract approved
by the Phase 4 supervisor addendum (§2.1, §2.2, §2.5). Slice 1 owns only
this file plus ``news_pipeline.briefing_contracts``; every other Phase 4
file is forbidden here.
"""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from typing import Tuple
from zoneinfo import ZoneInfo

from news_pipeline.contracts import (
    CandidateArticle,
    DecisionCode as Phase2DecisionCode,
    FilterResult,
    ReasonCode as Phase2ReasonCode,
    TrustTier,
    validate_utc_iso,
)
from news_pipeline.event_contracts import (
    AdjudicationResult,
    EventCandidate,
    FactDelta,
    ModelErrorCategory,
    SemanticDecision,
    SemanticReasonCode,
)
from news_pipeline.models import Category
from news_pipeline.policies import QueryPolicy


_LOCAL_TZ = ZoneInfo("Europe/London")


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _candidate(
    cid: str = "cand-1",
    category: Category = Category.AI,
    evaluated_at: str = "2026-07-01T07:00:00Z",
    title: str = "A meaningful headline",
    snippet: str = "A meaningful snippet",
    url: str | None = "https://example.test/article",
) -> CandidateArticle:
    return CandidateArticle(
        candidate_id=cid,
        category=category,
        query_group=category.value,
        title=title,
        snippet=snippet,
        original_url=url,
        canonical_url=url,
        published_at=None,
        published_evidence="missing",
        observed_at=None,
        evaluated_at=evaluated_at,
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


def _policy(category: Category) -> QueryPolicy:
    return QueryPolicy(
        category=category,
        allowed_query_groups=(category.value,),
        recency=timedelta(days=7),
        missing_date_fallback=True,
        exact_title_lookback=timedelta(days=7),
        exact_url_lookback=timedelta(days=7),
        exact_identity_lookback=timedelta(days=7),
        cross_category_exact_url=True,
    )


def _event(c: CandidateArticle, ordinal: int = 0) -> EventCandidate:
    return EventCandidate(
        candidate=c,
        filter_result=_filter(c, ordinal=ordinal),
        query_policy=_policy(c.category),
    )


def _adjudication(
    c: CandidateArticle,
    decision: SemanticDecision,
    ordinal: int,
    semantic_reasons: Tuple[SemanticReasonCode, ...] = (SemanticReasonCode.DISTINCT_EVENT,),
    fact_deltas: Tuple[FactDelta, ...] = (),
    matched_candidate_ids: Tuple[str, ...] = (),
    matched_history_ids: Tuple[str, ...] = (),
    matched_observation_ids: Tuple[str, ...] = (),
    model_used: bool = False,
    model_confidence: float | None = None,
    model_error_category: ModelErrorCategory | None = None,
    cluster_id: str | None = None,
    phase2_decision: Phase2DecisionCode = Phase2DecisionCode.KEEP,
    phase2_reasons: Tuple[Phase2ReasonCode, ...] = (Phase2ReasonCode.OK_KEEP,),
) -> AdjudicationResult:
    return AdjudicationResult(
        candidate_id=c.candidate_id,
        semantic_decision=decision,
        phase2_decision=phase2_decision,
        phase2_reasons=phase2_reasons,
        semantic_reasons=semantic_reasons,
        cluster_id=cluster_id,
        matched_candidate_ids=matched_candidate_ids,
        matched_history_ids=matched_history_ids,
        matched_observation_ids=matched_observation_ids,
        fact_deltas=fact_deltas,
        model_used=model_used,
        model_confidence=model_confidence,
        model_error_category=model_error_category,
        ordinal=ordinal,
        source_ordinal=ordinal,
    )


# Each six-decision entry pairs a default ``semantic_reasons`` value with any
# adjudication kwargs the existing ``AdjudicationResult`` invariant requires
# (e.g. ``pending_model_error`` needs ``model_used=True`` + error category).
_DECISION_FIXTURES = {
    SemanticDecision.distinct_event: {},
    SemanticDecision.material_update: {"semantic_reasons": (SemanticReasonCode.NUMERIC_REVISION,)},
    SemanticDecision.rewrite: {"semantic_reasons": (SemanticReasonCode.SAME_FACTS,)},
    SemanticDecision.bypass_phase2_terminal: {
        "semantic_reasons": (SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,)
    },
    SemanticDecision.pending_review: {
        "semantic_reasons": (SemanticReasonCode.MISSING_HISTORY_MATCH,)
    },
    SemanticDecision.pending_model_error: {
        "semantic_reasons": (SemanticReasonCode.TRANSPORT_ERROR,),
        "model_used": True,
        "model_error_category": ModelErrorCategory.TRANSPORT_ERROR,
    },
}


def _all_six_decisions() -> Tuple[SemanticDecision, ...]:
    return (
        SemanticDecision.distinct_event,
        SemanticDecision.material_update,
        SemanticDecision.rewrite,
        SemanticDecision.bypass_phase2_terminal,
        SemanticDecision.pending_review,
        SemanticDecision.pending_model_error,
    )


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestBriefingContracts(unittest.TestCase):
    # 1
    def test_dry_run_is_literal_true(self) -> None:
        from news_pipeline import briefing_contracts  # noqa: WPS433

        # The supervisor addendum requires the literal ``True``; identity
        # checks prove it isn't e.g. ``1`` (int) or a derived expression.
        self.assertIs(briefing_contracts._DRY_RUN, True)
        self.assertEqual(type(briefing_contracts._DRY_RUN), bool)

    # 2
    def test_briefing_input_accepts_all_six_semantic_decisions(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput  # noqa: WPS433

        decisions = _all_six_decisions()
        self.assertEqual(len(SemanticDecision), 6)
        self.assertEqual(set(SemanticDecision), set(decisions))

        for index, dec in enumerate(decisions):
            cid = f"cand-{index}"
            c = _candidate(cid=cid)
            ev = _event(c, ordinal=index)
            adj = _adjudication(c, decision=dec, ordinal=index, **_DECISION_FIXTURES[dec])

            inp = BriefingInput(event_candidate=ev, adjudication=adj)
            # The exact same object instances are retained.
            self.assertIs(inp.event_candidate, ev)
            self.assertIs(inp.adjudication, adj)
            self.assertEqual(inp.adjudication.semantic_decision, dec)
            self.assertEqual(inp.event_candidate.candidate.candidate_id, cid)

    # 3
    def test_briefing_input_rejects_candidate_id_mismatch(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput  # noqa: WPS433

        c1 = _candidate(cid="cand-A")
        c2 = _candidate(cid="cand-B")
        ev1 = _event(c1, ordinal=0)
        adj_bad = _adjudication(c2, decision=SemanticDecision.distinct_event, ordinal=0)
        # Two different candidate IDs: pair-wise check raises ValueError.
        with self.assertRaises(ValueError):
            BriefingInput(event_candidate=ev1, adjudication=adj_bad)

    # 4
    def test_briefing_input_rejects_ordinal_mismatch(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput  # noqa: WPS433

        c = _candidate(cid="cand-A")
        ev = _event(c, ordinal=0)
        adj_bad = _adjudication(c, decision=SemanticDecision.distinct_event, ordinal=7)
        with self.assertRaises(ValueError):
            BriefingInput(event_candidate=ev, adjudication=adj_bad)

    # 5
    def test_briefing_input_properties_do_not_truncate(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput  # noqa: WPS433

        # Build one CandidateArticle whose title, snippet, and URL exceed
        # the summarizer bounds (titles 1..512, snippets 0..2048, URLs
        # 1..2048). The contract is read-only exact access — never slice.
        long_title = "T" * 1024  # 2x the summarizer cap
        long_snippet = "S" * 4096  # 2x the summarizer cap
        long_url = "https://example.test/" + ("a" * 4096)  # 2x the summarizer cap
        c = _candidate(
            cid="cand-long",
            title=long_title,
            snippet=long_snippet,
            url=long_url,
        )
        ev = _event(c, ordinal=0)
        adj = _adjudication(c, decision=SemanticDecision.distinct_event, ordinal=0)
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        # Object identity is preserved — properties do not copy or slice.
        self.assertIs(inp.event_candidate, ev)
        self.assertIs(inp.adjudication, adj)

        # Every property returns the *exact* underlying value with full
        # length intact. Properties never truncate.
        self.assertEqual(inp.candidate_id, c.candidate_id)
        self.assertIs(inp.candidate_id, c.candidate_id)

        self.assertIs(inp.category, c.category)
        self.assertEqual(inp.title, long_title)
        self.assertEqual(len(inp.title), 1024)
        self.assertIs(inp.title, c.title)
        self.assertEqual(inp.snippet, long_snippet)
        self.assertEqual(len(inp.snippet), 4096)
        self.assertIs(inp.snippet, c.snippet)
        self.assertEqual(inp.url, long_url)
        self.assertEqual(len(inp.url), 4096 + len("https://example.test/"))
        self.assertIs(inp.url, c.canonical_url)

        self.assertEqual(inp.evaluated_at, c.evaluated_at)
        # evaluated_at property returns the exact stored string identity.
        self.assertIs(inp.evaluated_at, c.evaluated_at)
        self.assertEqual(inp.ordinal, 0)
        self.assertIs(inp.ordinal, ev.filter_result.ordinal)

        # ``url`` alias for ``canonical_url``: same exact object.
        self.assertIs(inp.url, c.canonical_url)

    # 6
    def test_map_distinct_event_included(self) -> None:
        from news_pipeline.briefing_contracts import (
            BriefingInput,
            EligibilityResult,
            map_eligibility,
        )  # noqa: WPS433

        c = _candidate(cid="cand-de")
        ev = _event(c, ordinal=0)
        adj = _adjudication(c, decision=SemanticDecision.distinct_event, ordinal=0)
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        result = map_eligibility(inp)
        self.assertIsInstance(result, EligibilityResult)
        self.assertTrue(result.included)
        self.assertFalse(result.excluded)
        self.assertEqual(result.inclusion_reason, SemanticReasonCode.DISTINCT_EVENT)
        self.assertIsNone(result.exclusion_reason)
        # The result references the exact input instance.
        self.assertIs(result.briefing_input, inp)

    # 7
    def test_map_material_update_included(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput, map_eligibility  # noqa: WPS433

        c = _candidate(cid="cand-mu")
        ev = _event(c, ordinal=0)
        adj = _adjudication(
            c,
            decision=SemanticDecision.material_update,
            ordinal=0,
            semantic_reasons=(SemanticReasonCode.NUMERIC_REVISION,),
        )
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        result = map_eligibility(inp)
        self.assertTrue(result.included)
        self.assertEqual(result.inclusion_reason, SemanticReasonCode.NUMERIC_REVISION)
        self.assertIsNone(result.exclusion_reason)
        self.assertIs(result.briefing_input, inp)

    # 8
    def test_map_rewrite_excluded(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput, map_eligibility  # noqa: WPS433

        c = _candidate(cid="cand-rw")
        ev = _event(c, ordinal=0)
        adj = _adjudication(
            c,
            decision=SemanticDecision.rewrite,
            ordinal=0,
            semantic_reasons=(SemanticReasonCode.SAME_FACTS,),
        )
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        result = map_eligibility(inp)
        self.assertFalse(result.included)
        self.assertTrue(result.excluded)
        self.assertIsNone(result.inclusion_reason)
        self.assertEqual(result.exclusion_reason, SemanticReasonCode.SAME_FACTS)
        self.assertIs(result.briefing_input, inp)

    # 9
    def test_map_bypass_excluded(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput, map_eligibility  # noqa: WPS433

        c = _candidate(cid="cand-bp")
        ev = _event(c, ordinal=0)
        adj = _adjudication(
            c,
            decision=SemanticDecision.bypass_phase2_terminal,
            ordinal=0,
            semantic_reasons=(SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,),
        )
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        result = map_eligibility(inp)
        self.assertFalse(result.included)
        self.assertEqual(result.exclusion_reason, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED)
        self.assertIsNone(result.inclusion_reason)
        self.assertIs(result.briefing_input, inp)

    # 10
    def test_map_pending_review_excluded(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput, map_eligibility  # noqa: WPS433

        c = _candidate(cid="cand-pr")
        ev = _event(c, ordinal=0)
        adj = _adjudication(
            c,
            decision=SemanticDecision.pending_review,
            ordinal=0,
            semantic_reasons=(SemanticReasonCode.MISSING_HISTORY_MATCH,),
        )
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        result = map_eligibility(inp)
        self.assertFalse(result.included)
        self.assertEqual(result.exclusion_reason, SemanticReasonCode.MISSING_HISTORY_MATCH)
        self.assertIsNone(result.inclusion_reason)
        self.assertIs(result.briefing_input, inp)

    # 11
    def test_map_pending_model_error_excluded(self) -> None:
        from news_pipeline.briefing_contracts import BriefingInput, map_eligibility  # noqa: WPS433

        c = _candidate(cid="cand-pme")
        ev = _event(c, ordinal=0)
        adj = _adjudication(
            c,
            decision=SemanticDecision.pending_model_error,
            ordinal=0,
            semantic_reasons=(SemanticReasonCode.TRANSPORT_ERROR,),
            model_used=True,
            model_error_category=ModelErrorCategory.TRANSPORT_ERROR,
        )
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        result = map_eligibility(inp)
        self.assertFalse(result.included)
        self.assertEqual(result.exclusion_reason, SemanticReasonCode.TRANSPORT_ERROR)
        self.assertIsNone(result.inclusion_reason)
        self.assertIs(result.briefing_input, inp)

    # 12
    def test_eligibility_result_requires_exactly_one_reason(self) -> None:
        from news_pipeline.briefing_contracts import (  # noqa: WPS433
            BriefingInput,
            EligibilityResult,
        )

        c = _candidate(cid="cand-er1")
        ev = _event(c, ordinal=0)
        adj = _adjudication(c, decision=SemanticDecision.distinct_event, ordinal=0)
        inp = BriefingInput(event_candidate=ev, adjudication=adj)

        # No inclusion or exclusion reason → ValueError.
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=False,
                excluded=True,
                inclusion_reason=None,
                exclusion_reason=None,
            )
        # Both inclusion and exclusion reasons → ValueError (XOR violation).
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=True,
                excluded=False,
                inclusion_reason=SemanticReasonCode.DISTINCT_EVENT,
                exclusion_reason=SemanticReasonCode.SAME_FACTS,
            )
        # Wrong enum family on inclusion reason → ValueError.
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=True,
                excluded=False,
                inclusion_reason=Phase2ReasonCode.OK_KEEP,
                exclusion_reason=None,
            )
        # Wrong enum family on exclusion reason → ValueError.
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=False,
                excluded=True,
                inclusion_reason=None,
                exclusion_reason=Phase2ReasonCode.OK_KEEP,
            )
        # Decision/flag inconsistency: included=True with exclusion_reason
        # set and inclusion_reason=None is a contract violation — the two
        # flags must agree with the XOR reasons.
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=True,
                excluded=False,
                inclusion_reason=None,
                exclusion_reason=SemanticReasonCode.SAME_FACTS,
            )
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=False,
                excluded=True,
                inclusion_reason=SemanticReasonCode.DISTINCT_EVENT,
                exclusion_reason=None,
            )
        # The flags must also agree with the underlying semantic decision,
        # not merely with which reason slot is populated.
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=False,
                excluded=True,
                inclusion_reason=None,
                exclusion_reason=SemanticReasonCode.DISTINCT_EVENT,
            )
        # A reason foreign to the paired adjudication is inconsistent even
        # when the included/excluded flags otherwise match the decision.
        with self.assertRaises(ValueError):
            EligibilityResult(
                briefing_input=inp,
                included=True,
                excluded=False,
                inclusion_reason=SemanticReasonCode.SAME_FACTS,
                exclusion_reason=None,
            )

        # Valid construction round-trips.
        good = EligibilityResult(
            briefing_input=inp,
            included=True,
            excluded=False,
            inclusion_reason=SemanticReasonCode.DISTINCT_EVENT,
            exclusion_reason=None,
        )
        self.assertEqual(good.inclusion_reason, SemanticReasonCode.DISTINCT_EVENT)
        self.assertIsNone(good.exclusion_reason)
        self.assertTrue(good.included)
        self.assertIs(good.briefing_input, inp)

    # 13
    def test_window_before_0800_uses_previous_boundary(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        as_of = datetime(2026, 7, 15, 6, 30, 0, tzinfo=timezone.utc)
        lower, upper = compute_morning_window(as_of, None)
        expected_upper_local = datetime(2026, 7, 14, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_upper_utc = expected_upper_local.astimezone(timezone.utc)
        self.assertEqual(upper, expected_upper_utc)
        expected_cap_local = datetime(2026, 7, 7, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_cap_utc = expected_cap_local.astimezone(timezone.utc)
        self.assertEqual(lower, expected_cap_utc)

    # 14
    def test_window_exactly_0800_uses_current_boundary(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        # 1 July 2026 is BST (UTC+1) — 08:00 local == 07:00 UTC.
        as_of = datetime(2026, 7, 15, 7, 0, 0, tzinfo=timezone.utc)
        lower, upper = compute_morning_window(as_of, None)
        expected_upper_local = datetime(2026, 7, 15, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_upper_utc = expected_upper_local.astimezone(timezone.utc)
        self.assertEqual(upper, expected_upper_utc)
        expected_cap_local = datetime(2026, 7, 8, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_cap_utc = expected_cap_local.astimezone(timezone.utc)
        self.assertEqual(lower, expected_cap_utc)

    # 15
    def test_window_after_0800_uses_current_boundary(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        # 12:00 local BST == 11:00 UTC.
        as_of = datetime(2026, 7, 15, 11, 0, 0, tzinfo=timezone.utc)
        lower, upper = compute_morning_window(as_of, None)
        expected_upper_local = datetime(2026, 7, 15, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_upper_utc = expected_upper_local.astimezone(timezone.utc)
        self.assertEqual(upper, expected_upper_utc)
        expected_cap_local = datetime(2026, 7, 8, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_cap_utc = expected_cap_local.astimezone(timezone.utc)
        self.assertEqual(lower, expected_cap_utc)

    # 16
    def test_window_spring_dst_uses_local_calendar(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        # UK clocks go forward on the last Sunday in March. In 2026 the BST
        # change is at 01:00 UTC on 29 March. After that, 08:00 BST == 07:00
        # UTC. We evaluate at 09:00 BST == 08:00 UTC on 30 March 2026.
        as_of = datetime(2026, 3, 30, 8, 0, 0, tzinfo=timezone.utc)
        lower, upper = compute_morning_window(as_of, None)
        expected_upper_local = datetime(2026, 3, 30, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_upper_utc = expected_upper_local.astimezone(timezone.utc)
        self.assertEqual(upper, expected_upper_utc)
        expected_cap_local = datetime(2026, 3, 23, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_cap_utc = expected_cap_local.astimezone(timezone.utc)
        self.assertEqual(lower, expected_cap_utc)

    # 17
    def test_window_fall_dst_uses_local_calendar(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        # UK clocks go back on the last Sunday in October. In 2026 the change
        # is at 02:00 BST → 01:00 GMT on 25 October. After 25 Oct, 08:00 GMT
        # == 08:00 UTC. Evaluate at 09:30 UTC on 26 October 2026.
        as_of = datetime(2026, 10, 26, 9, 30, 0, tzinfo=timezone.utc)
        lower, upper = compute_morning_window(as_of, None)
        expected_upper_local = datetime(2026, 10, 26, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_upper_utc = expected_upper_local.astimezone(timezone.utc)
        self.assertEqual(upper, expected_upper_utc)
        expected_cap_local = datetime(2026, 10, 19, 8, 0, 0, tzinfo=_LOCAL_TZ)
        expected_cap_utc = expected_cap_local.astimezone(timezone.utc)
        self.assertEqual(lower, expected_cap_utc)

    # 18
    def test_window_uses_recent_last_completed_boundary(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        as_of = datetime(2026, 7, 15, 11, 0, 0, tzinfo=timezone.utc)
        last_completed_local = datetime(2026, 7, 14, 8, 0, 0, tzinfo=_LOCAL_TZ)
        last_completed_utc = last_completed_local.astimezone(timezone.utc)
        lower, upper = compute_morning_window(as_of, last_completed_utc)
        expected_upper_local = datetime(2026, 7, 15, 8, 0, 0, tzinfo=_LOCAL_TZ)
        self.assertEqual(upper, expected_upper_local.astimezone(timezone.utc))
        self.assertEqual(lower, last_completed_utc)

    # 19
    def test_window_caps_old_last_completed_at_seven_local_days(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        as_of = datetime(2026, 7, 15, 11, 0, 0, tzinfo=timezone.utc)
        ancient = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        lower, upper = compute_morning_window(as_of, ancient)
        expected_upper_local = datetime(2026, 7, 15, 8, 0, 0, tzinfo=_LOCAL_TZ)
        self.assertEqual(upper, expected_upper_local.astimezone(timezone.utc))
        expected_cap_local = datetime(2026, 7, 8, 8, 0, 0, tzinfo=_LOCAL_TZ)
        self.assertEqual(lower, expected_cap_local.astimezone(timezone.utc))
        self.assertNotEqual(lower, ancient)

    # 20
    def test_window_returns_empty_when_already_completed(self) -> None:
        from news_pipeline.briefing_contracts import compute_morning_window  # noqa: WPS433

        as_of = datetime(2026, 7, 15, 11, 0, 0, tzinfo=timezone.utc)
        upper_local = datetime(2026, 7, 15, 8, 0, 0, tzinfo=_LOCAL_TZ)
        upper_utc = upper_local.astimezone(timezone.utc)
        lower, upper = compute_morning_window(as_of, upper_utc)
        self.assertEqual(lower, upper)
        self.assertEqual(upper, upper_utc)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
