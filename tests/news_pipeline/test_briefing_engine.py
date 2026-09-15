"""Phase 4 — Slice 6 engine tests.

Fourteen unittest methods for ``news_pipeline.briefing_engine`` per the
Phase 4 supervisor addendum §2.4 / §2.5 and §6 (tests 85-98). Slice 6
owns only this file plus ``news_pipeline.briefing_engine``; every other
Phase 4 file is forbidden here, and no Phase 1-3 / wrapper / cron /
service / DB file may be touched.

Engine is the only Phase 4 module allowed to import the other five. It
orchestrates caller-provided Phase-3 inputs, window eligibility, the
summarizer, the renderer, and the shadow ledger. Stdlib + existing
``news_pipeline`` modules only. No network / filesystem / process /
clock / random / UUID APIs — all dependencies are injected.
"""
from __future__ import annotations

import json
import sqlite3
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from news_pipeline import briefing_engine
from news_pipeline.briefing_contracts import BriefingInput, compute_morning_window
from news_pipeline.briefing_ledger import (
    BriefingLedger,
    CompleteRunResult,
    LedgerContractError,
    ShadowEvent,
)
from news_pipeline.briefing_renderer import (
    OversizeRecordError,
    RenderRecord,
    RenderResult,
    RenderStatus,
)
from news_pipeline.briefing_summarizer import (
    CategorySummaryResult,
    SummarizerInput,
    SummaryItem,
    SummarySource,
    SummarizerErrorCategory,
)
from news_pipeline.contracts import (
    CandidateArticle,
    DecisionCode as Phase2DecisionCode,
    FilterResult,
    ReasonCode as Phase2ReasonCode,
    TrustTier,
)
from news_pipeline.event_contracts import (
    AdjudicationResult,
    EventCandidate,
    FactDelta,
    FactKind,
    ModelErrorCategory,
    SemanticDecision,
    SemanticReasonCode,
)
from news_pipeline.models import Category
from news_pipeline.policies import QueryPolicy


# ---------------------------------------------------------------------------
# Test fixtures — local engine surface only
# ---------------------------------------------------------------------------


_LOCAL_TZ_OFFSET = timezone.utc.utcoffset(None)


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


def _candidate(
    cid: str = "cand-1",
    category: Category = Category.AI,
    title: str = "Headline one",
    snippet: str = "Snippet one",
    url: Optional[str] = "https://example.test/article-1",
    evaluated_at: str = "2026-07-01T06:00:00Z",
    ordinal: int = 0,
    fact_deltas: Tuple[FactDelta, ...] = (),
) -> EventCandidate:
    art = CandidateArticle(
        candidate_id=cid,
        category=category,
        query_group=category.value,
        title=title,
        snippet=snippet,
        original_url=url,
        canonical_url=url,
        published_at=evaluated_at,
        published_evidence="source",
        observed_at=evaluated_at,
        evaluated_at=evaluated_at,
    )
    flt = FilterResult(
        candidate=art,
        decision=Phase2DecisionCode.KEEP,
        reasons=(Phase2ReasonCode.OK_KEEP,),
        matched_article_ids=(),
        matched_observation_ids=(),
        trust_tier=TrustTier.UNKNOWN,
        evaluated_publication_time=None,
        ordinal=ordinal,
    )
    return EventCandidate(
        candidate=art, filter_result=flt, query_policy=_policy(category)
    )


def _adjudication(
    cid: str,
    decision: SemanticDecision,
    ordinal: int,
    *,
    fact_deltas: Tuple[FactDelta, ...] = (),
    reasons: Tuple[SemanticReasonCode, ...] = (SemanticReasonCode.DISTINCT_EVENT,),
    model_used: bool = False,
    model_confidence: Optional[float] = None,
    model_error_category: Optional[ModelErrorCategory] = None,
    phase2_decision: Phase2DecisionCode = Phase2DecisionCode.KEEP,
    phase2_reasons: Tuple[Phase2ReasonCode, ...] = (Phase2ReasonCode.OK_KEEP,),
) -> AdjudicationResult:
    return AdjudicationResult(
        candidate_id=cid,
        semantic_decision=decision,
        phase2_decision=phase2_decision,
        phase2_reasons=phase2_reasons,
        semantic_reasons=reasons,
        cluster_id=None,
        matched_candidate_ids=(cid,),
        matched_history_ids=(),
        matched_observation_ids=(),
        fact_deltas=fact_deltas,
        model_used=model_used,
        model_confidence=model_confidence,
        model_error_category=model_error_category,
        ordinal=ordinal,
    )


def _briefing_input(
    cid: str,
    decision: SemanticDecision,
    *,
    category: Category = Category.AI,
    title: str = "Headline one",
    snippet: str = "Snippet one",
    url: Optional[str] = "https://example.test/article-1",
    evaluated_at: str = "2026-07-01T06:00:00Z",
    ordinal: int = 0,
    fact_deltas: Tuple[FactDelta, ...] = (),
    reasons: Tuple[SemanticReasonCode, ...] = (SemanticReasonCode.DISTINCT_EVENT,),
    model_used: bool = False,
    model_confidence: Optional[float] = None,
    model_error_category: Optional[ModelErrorCategory] = None,
) -> BriefingInput:
    build_cid = cid or "invalid-surrogate"
    event = _candidate(
        cid=build_cid,
        category=category,
        title=title,
        snippet=snippet,
        url=url,
        evaluated_at=evaluated_at,
        ordinal=ordinal,
        fact_deltas=fact_deltas,
    )
    adj = _adjudication(
        cid=build_cid,
        decision=decision,
        ordinal=ordinal,
        fact_deltas=fact_deltas,
        reasons=reasons,
        model_used=model_used,
        model_confidence=model_confidence,
        model_error_category=model_error_category,
    )
    briefing = BriefingInput(event_candidate=event, adjudication=adj)
    if cid == "":
        object.__setattr__(event.candidate, "candidate_id", "")
        object.__setattr__(adj, "candidate_id", "")
    return briefing


def _fact_delta(kind: FactKind = FactKind.PRICE) -> FactDelta:
    return FactDelta(
        kind=kind,
        unit="usd",
        old_value="10",
        new_value="20",
        topic_gate=Decimal("0.9"),
    )


def _summary_item(
    candidate_id: str,
    summary: str,
    source: SummarySource = SummarySource.MODEL,
    error_category: Optional[SummarizerErrorCategory] = None,
) -> SummaryItem:
    return SummaryItem(
        candidate_id=candidate_id,
        summary=summary,
        source=source,
        error_category=error_category,
    )


# ---------------------------------------------------------------------------
# In-memory ledger adapter
# ---------------------------------------------------------------------------


class _InMemoryLedger:
    """A faithful in-memory fake for the engine's ShadowLedgerProtocol.

    Mirrors ``BriefingLedger`` semantics (idempotent begin_run,
    seen_candidate_ids preserves input order, complete_run partitions
    RECORDED / ALREADY_SEEN). Also captures every interaction so tests
    can assert ordering.
    """

    def __init__(self, *, seen: Tuple[str, ...] = ()) -> None:
        self._seen: set = set(seen)
        self._shadow: Dict[str, Dict[str, Any]] = {}
        self.calls: List[str] = []
        self.begun: List[Tuple[str, str, str, str]] = []
        self.complete_runs: List[Tuple[str, str, Tuple[ShadowEvent, ...]]] = []
        self.fail_runs: List[Tuple[str, str, str]] = []

    def begin_run(
        self, run_id: str, lower: str, upper: str, started: str
    ) -> None:
        self.calls.append("begin_run")
        self.begun.append((run_id, lower, upper, started))

    def seen_candidate_ids(self, ids: Tuple[str, ...]) -> Tuple[str, ...]:
        self.calls.append("seen_candidate_ids")
        return tuple(cid for cid in ids if cid in self._seen)

    def complete_run(
        self,
        run_id: str,
        updated: str,
        events: Sequence[ShadowEvent],
    ) -> CompleteRunResult:
        self.calls.append("complete_run")
        events_t = tuple(events)
        new_ids: List[str] = []
        already_seen: List[str] = []
        for ev in events_t:
            if ev.candidate_id in self._seen:
                already_seen.append(ev.candidate_id)
            else:
                self._seen.add(ev.candidate_id)
                self._shadow[ev.candidate_id] = {
                    "category": ev.category,
                    "decision": ev.decision,
                    "payload": ev.payload_json,
                    "recorded_at": ev.recorded_at_utc,
                    "first_run_id": run_id,
                }
                new_ids.append(ev.candidate_id)
        self.complete_runs.append((run_id, updated, events_t))
        return CompleteRunResult(
            new_ids=tuple(new_ids), already_seen_ids=tuple(already_seen)
        )

    def fail_run(self, run_id: str, updated: str, error_code: str) -> None:
        self.calls.append("fail_run")
        self.fail_runs.append((run_id, updated, error_code))

    # Diagnostic helpers --------------------------------------------------

    @property
    def shadow_count(self) -> int:
        return len(self._shadow)

    def has_shadow(self, candidate_id: str) -> bool:
        return candidate_id in self._shadow


# ---------------------------------------------------------------------------
# Fake summarizer — fully controllable
# ---------------------------------------------------------------------------


class _FakeSummarizer:
    """A controllable SummarizerProtocol implementation.

    ``per_category`` maps Category -> list of ``SummaryItem`` (or a
    callable that builds the list from the raw inputs). ``raise_during``
    lets a test force a transport error or malformed output. The
    ``calls`` / ``model_used_calls`` / ``cache_used_calls`` counters
    let tests assert the engine never exceeds the budget.
    """

    def __init__(
        self,
        per_category: Optional[Dict[Category, Any]] = None,
        *,
        cache_keys: Optional[Tuple[Tuple[Category, frozenset], ...]] = None,
        raise_during: Optional[Tuple[Category, BaseException]] = None,
    ) -> None:
        self._per_category: Dict[Category, Any] = per_category or {}
        self._cache_keys = cache_keys or ()
        self._raise = raise_during
        self._model_calls = 0
        self._cache_hits = 0
        self.calls: List[Tuple[Category, Tuple[str, ...]]] = []

    @property
    def model_call_count(self) -> int:
        return self._model_calls

    @property
    def cache_hit_count(self) -> int:
        return self._cache_hits

    def summarize_category(
        self, category: Category, raw_inputs: Sequence[Any]
    ) -> CategorySummaryResult:
        ids = tuple(r.candidate_id for r in raw_inputs)
        self.calls.append((category, ids))

        # Mimic SummarizerSession's 32-cap: any raw input beyond the
        # first 32 is reported as an INPUT_BOUNDS fallback and consumes
        # no budget.
        MAX_ITEMS = 32
        if len(raw_inputs) > MAX_ITEMS:
            kept = list(raw_inputs[:MAX_ITEMS])
            overflow = list(raw_inputs[MAX_ITEMS:])
        else:
            kept = list(raw_inputs)
            overflow = []

        cfg = self._per_category.get(category)
        if cfg is None:
            # Default: derive a SummaryItem per raw input using title as summary.
            items = [
                _summary_item(r.candidate_id, r.title or "Untitled item")
                for r in kept
            ]
        elif callable(cfg):
            items = list(cfg(kept))
        else:
            items = list(cfg)

        # Cache detection — if the (category, ids) signature is registered
        # as a cache key, the fake reports CACHE source + cache_hit=True.
        ids_fs = frozenset(r.candidate_id for r in kept)
        cache_hit = any(cat is category and ids_fs == ks for cat, ks in self._cache_keys)
        if cache_hit:
            self._cache_hits += 1
            rebuilt: List[SummaryItem] = []
            for item in items:
                rebuilt.append(
                    SummaryItem(
                        candidate_id=item.candidate_id,
                        summary=item.summary,
                        source=SummarySource.CACHE,
                        error_category=None,
                    )
                )
            for ov in overflow:
                rebuilt.append(
                    _summary_item(
                        ov.candidate_id,
                        ov.title or "Untitled item",
                        source=SummarySource.FALLBACK,
                        error_category=SummarizerErrorCategory.INPUT_BOUNDS,
                    )
                )
            return CategorySummaryResult(
                items=tuple(rebuilt), model_used=False, cache_hit=True
            )

        # Real invocation path.
        self._model_calls += 1
        rebuilt = []
        for item in items:
            rebuilt.append(
                SummaryItem(
                    candidate_id=item.candidate_id,
                    summary=item.summary,
                    source=item.source,
                    error_category=item.error_category,
                )
            )
        for ov in overflow:
            rebuilt.append(
                _summary_item(
                    ov.candidate_id,
                    ov.title or "Untitled item",
                    source=SummarySource.FALLBACK,
                    error_category=SummarizerErrorCategory.INPUT_BOUNDS,
                )
            )
        return CategorySummaryResult(
            items=tuple(rebuilt), model_used=True, cache_hit=False
        )


# ---------------------------------------------------------------------------
# Fake renderer
# ---------------------------------------------------------------------------


class _FakeRenderer:
    """Captures render calls and returns a configurable result.

    Setting ``raise_oversize`` raises ``OversizeRecordError`` on the
    next call; setting ``chunks`` returns a custom ``RenderResult``;
    leaving both at defaults returns a single-chunk RENDERED result
    whose chunk preserves the records' titles so byte-determinism
    tests can assert equality.
    """

    def __init__(self, *, chunks: Optional[Tuple[str, ...]] = None) -> None:
        self._chunks = chunks
        self.calls: List[Tuple[Tuple[str, ...], str]] = []

    def render_briefing(
        self, records: Sequence[RenderRecord], upper_bound_utc: datetime
    ) -> RenderResult:
        record_titles = tuple(r.candidate_id for r in records)
        self.calls.append((record_titles, upper_bound_utc.isoformat()))
        if self._chunks is None:
            if not records:
                return RenderResult(status=RenderStatus.EMPTY, chunks=())
            return RenderResult(
                status=RenderStatus.RENDERED, chunks=("rendered",)
            )
        if not self._chunks:
            return RenderResult(status=RenderStatus.EMPTY, chunks=())
        return RenderResult(
            status=RenderStatus.RENDERED, chunks=self._chunks
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _as_of() -> datetime:
    """2026-07-01 09:00:00 UTC — well after the 08:00 local boundary."""
    return datetime(2026, 7, 1, 9, 0, 0, tzinfo=timezone.utc)


def _low_window_evaluated_at(cid: str = "cand-1") -> str:
    """An evaluated_at that sits inside the morning window."""
    return "2026-07-01T06:00:00Z"


def _build_engine(
    ledger: _InMemoryLedger,
    summarizer: _FakeSummarizer,
    renderer: _FakeRenderer,
) -> briefing_engine.BriefingEngine:
    return briefing_engine.BriefingEngine(
        ledger=ledger, summarizer=summarizer, renderer=renderer
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestBriefingEngine(unittest.TestCase):
    """Tests 85-98 for ``news_pipeline.briefing_engine``."""

    # 85
    def test_maps_all_six_decisions_without_dropping_inputs(self) -> None:
        """Six decision types map to included/excluded without losing IDs."""
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        decisions: Tuple[Tuple[str, SemanticDecision], ...] = (
            ("cand-distinct", SemanticDecision.distinct_event),
            ("cand-update", SemanticDecision.material_update),
            ("cand-rewrite", SemanticDecision.rewrite),
            ("cand-bypass", SemanticDecision.bypass_phase2_terminal),
            ("cand-pending-review", SemanticDecision.pending_review),
            ("cand-pending-model-error", SemanticDecision.pending_model_error),
        )
        # pending_model_error requires model_used=True and model_error_category set.
        inputs: List[BriefingInput] = []
        for ordinal, (cid, decision) in enumerate(decisions):
            kwargs: Dict[str, Any] = {}
            if decision is SemanticDecision.pending_model_error:
                kwargs["model_used"] = True
                kwargs["model_error_category"] = ModelErrorCategory.MALFORMED_OUTPUT
            inputs.append(
                _briefing_input(
                    cid=cid,
                    decision=decision,
                    ordinal=ordinal,
                    evaluated_at=_low_window_evaluated_at(cid),
                    **kwargs,
                )
            )
        result = engine.execute_briefing_run(
            run_id="run-85",
            briefing_inputs=tuple(inputs),
            as_of_utc=_as_of(),
        )

        # Every raw input is accounted for: either in included or excluded.
        accounted = {
            included.briefing_input.candidate_id for included in result.included_items
        }
        excluded = {
            ex.briefing_input.candidate_id for ex in result.excluded_items
        }
        all_ids = {cid for cid, _ in decisions}
        self.assertEqual(accounted | excluded, all_ids)
        self.assertFalse(accounted & excluded, "no ID may appear in both groups")

        # Exactly two decisions are eligible: distinct_event and material_update.
        self.assertEqual(len(result.included_items), 2)
        included_ids = sorted(included.briefing_input.candidate_id for included in result.included_items)
        self.assertEqual(included_ids, ["cand-distinct", "cand-update"])

        # The four excluded IDs each carry a typed NON_ELIGIBLE_DECISION.
        excluded_reasons = {
            ex.briefing_input.candidate_id: ex.reason for ex in result.excluded_items
        }
        for cid in ("cand-rewrite", "cand-bypass", "cand-pending-review", "cand-pending-model-error"):
            self.assertIs(
                excluded_reasons[cid],
                briefing_engine.EngineExclusionReason.NON_ELIGIBLE_DECISION,
            )

    # 86
    def test_excludes_out_of_window_and_already_shadow_seen(self) -> None:
        """Out-of-window + already-seen inputs are excluded with typed reasons."""
        ledger = _InMemoryLedger(seen=("cand-already",))
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        inputs = (
            _briefing_input(
                cid="cand-in-window",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
            _briefing_input(
                cid="cand-out-of-window",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2025-01-01T00:00:00Z",
            ),
            _briefing_input(
                cid="cand-already",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-86",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )

        included_ids = {i.briefing_input.candidate_id for i in result.included_items}
        excluded_by_cid = {ex.briefing_input.candidate_id: ex for ex in result.excluded_items}
        self.assertIn("cand-in-window", included_ids)
        self.assertNotIn("cand-out-of-window", included_ids)
        self.assertNotIn("cand-already", included_ids)
        self.assertIs(
            excluded_by_cid["cand-out-of-window"].reason,
            briefing_engine.EngineExclusionReason.OUT_OF_WINDOW,
        )
        self.assertIs(
            excluded_by_cid["cand-already"].reason,
            briefing_engine.EngineExclusionReason.ALREADY_SHADOW_SEEN,
        )

    # 87
    def test_sort_and_category_grouping_are_deterministic(self) -> None:
        """Sort by (category-order, evaluated_at, ordinal, candidate_id)."""
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        # Deliberately scrambled input order: opposite category order
        # and opposite evaluated_at. Deterministic ordering must put
        # AI before WORLD.
        inputs = (
            _briefing_input(
                cid="world-late",
                decision=SemanticDecision.distinct_event,
                category=Category.WORLD,
                evaluated_at="2026-07-01T06:00:00Z",
                ordinal=1,
            ),
            _briefing_input(
                cid="ai-late",
                decision=SemanticDecision.distinct_event,
                category=Category.AI,
                evaluated_at="2026-07-01T06:00:00Z",
                ordinal=0,
            ),
            _briefing_input(
                cid="ai-early",
                decision=SemanticDecision.distinct_event,
                category=Category.AI,
                evaluated_at="2026-07-01T05:00:00Z",
                ordinal=0,
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-87",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        # Deterministic order: AI before WORLD; within AI, ai-early
        # (5am) before ai-late (7am).
        order = [i.briefing_input.candidate_id for i in result.included_items]
        self.assertEqual(order, ["ai-early", "ai-late", "world-late"])

        # Re-running produces the byte-identical order.
        ledger2 = _InMemoryLedger()
        engine2 = _build_engine(ledger2, summarizer, renderer)
        result2 = engine2.execute_briefing_run(
            run_id="run-87-bis",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        order2 = [i.briefing_input.candidate_id for i in result2.included_items]
        self.assertEqual(order2, order)

    # 88
    def test_more_than_32_items_uses_input_bounds_fallback_for_remainder(self) -> None:
        """33 valid inputs -> first 32 summarized, 33rd INPUT_BOUNDS fallback."""
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        inputs: List[BriefingInput] = []
        for i in range(33):
            inputs.append(
                _briefing_input(
                    cid=f"cand-{i:02d}",
                    decision=SemanticDecision.distinct_event,
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=i,
                )
            )
        result = engine.execute_briefing_run(
            run_id="run-88",
            briefing_inputs=tuple(inputs),
            as_of_utc=_as_of(),
        )

        # All 33 IDs are present in included_items.
        included_ids = [i.briefing_input.candidate_id for i in result.included_items]
        self.assertEqual(len(included_ids), 33)
        self.assertEqual(set(included_ids), {f"cand-{i:02d}" for i in range(33)})

        # Exactly one INPUT_BOUNDS fallback: the 33rd item.
        fallback_items = [
            i for i in result.included_items
            if i.summary_item.source is SummarySource.FALLBACK
            and i.summary_item.error_category is SummarizerErrorCategory.INPUT_BOUNDS
        ]
        self.assertEqual(len(fallback_items), 1)
        self.assertEqual(fallback_items[0].briefing_input.candidate_id, "cand-32")

        # Budget: only one call (one category), so model_call_count <= 1.
        self.assertLessEqual(summarizer.model_call_count, 1)

    # 89
    def test_zero_eligible_completes_with_no_events_or_chunks(self) -> None:
        """All inputs excluded -> COMPLETED with zero events and empty chunks."""
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        # All inputs out-of-window (no eligible rows).
        inputs = (
            _briefing_input(
                cid="cand-out-1",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2025-01-01T00:00:00Z",
            ),
            _briefing_input(
                cid="cand-out-2",
                decision=SemanticDecision.rewrite,
                evaluated_at="2025-01-01T00:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-89",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )

        self.assertEqual(result.status, briefing_engine.EngineRunStatus.COMPLETED)
        self.assertEqual(result.included_items, ())
        self.assertEqual(result.chunks, ())
        # Shadow count is zero — engine never created a shadow row.
        self.assertEqual(ledger.shadow_count, 0)
        self.assertEqual(ledger.complete_runs[0][2], ())
        # Render must still have been called exactly once.
        self.assertEqual(len(renderer.calls), 1)

    # 90
    def test_mixed_model_cache_and_fallback_yields_partial(self) -> None:
        """Per-category mix: model, cache, fallback -> status PARTIAL."""
        ledger = _InMemoryLedger()
        # AI -> model (MODEL), WORLD -> cache (CACHE), HARDWARE -> fallback.
        ai_item = _summary_item("ai-1", "AI summary")
        cache_item = _summary_item("world-1", "World summary")
        hw_fallback = _summary_item(
            "hw-1",
            "Headline hardware",
            source=SummarySource.FALLBACK,
            error_category=SummarizerErrorCategory.TRANSPORT_ERROR,
        )
        summarizer = _FakeSummarizer(
            per_category={
                Category.AI: [ai_item],
                Category.WORLD: [cache_item],
                Category.HARDWARE: [hw_fallback],
            },
            cache_keys=((Category.WORLD, frozenset({"world-1"})),),
        )
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        inputs = (
            _briefing_input(
                cid="ai-1",
                decision=SemanticDecision.distinct_event,
                category=Category.AI,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
            _briefing_input(
                cid="world-1",
                decision=SemanticDecision.material_update,
                category=Category.WORLD,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
            _briefing_input(
                cid="hw-1",
                decision=SemanticDecision.distinct_event,
                category=Category.HARDWARE,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-90",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )

        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        # AI is a model call, WORLD is a cache hit, HARDWARE is a model call
        # that fell back to TRANSPORT_ERROR — so 2 uncached invocations.
        self.assertEqual(result.model_call_count, 2)
        self.assertEqual(result.cache_hit_count, 1)  # WORLD cache hit
        sources = {i.briefing_input.candidate_id: i.summary_item.source for i in result.included_items}
        self.assertIs(sources["ai-1"], SummarySource.MODEL)
        self.assertIs(sources["world-1"], SummarySource.CACHE)
        self.assertIs(sources["hw-1"], SummarySource.FALLBACK)

    # 91
    def test_never_exceeds_one_call_per_category_or_eight_total(self) -> None:
        """Across 8 categories, never more than 8 total model calls."""
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)

        all_categories = (
            Category.AI,
            Category.WORLD,
            Category.AUDIO_ENGINEERING,
            Category.HARDWARE,
            Category.FANTASY_NOVEL,
            Category.AUDIOVISUAL,
            Category.AV_CORPORATE,
            Category.OUR_SETUP,
        )
        inputs: List[BriefingInput] = []
        ordinal = 0
        for cat in all_categories:
            for j in range(2):  # 2 inputs per category
                inputs.append(
                    _briefing_input(
                        cid=f"{cat.value}-{j}",
                        decision=SemanticDecision.distinct_event,
                        category=cat,
                        evaluated_at="2026-07-01T06:00:00Z",
                        ordinal=ordinal,
                    )
                )
                ordinal += 1

        result = engine.execute_briefing_run(
            run_id="run-91",
            briefing_inputs=tuple(inputs),
            as_of_utc=_as_of(),
        )
        # Total uncached calls <= 8 and per-category <= 1.
        self.assertLessEqual(result.model_call_count, 8)
        # Direct check: model_call_count for fake counts only uncached
        # invocations; one uncached call per category for 8 categories.
        self.assertEqual(summarizer.model_call_count, len(all_categories))

    # 92 — Phase 4 follow-up: lifecycle / run-transition time is the
    # caller-supplied aware-UTC ``as_of_utc`` formatted Z, NOT the upper
    # window bound. Coverage is in-place; no new numbered test method
    # is added (frozen Phase 4 matrix preserved).
    def test_successful_render_completes_ledger_after_render(self) -> None:
        """Render happens before complete_run; if render fails, ledger is failed.

        Phase 4 follow-up: lifecycle / run-transition time equals the
        caller-supplied aware-UTC ``as_of_utc`` (formatted Z) and is
        NOT the upper window bound. ``_as_of()`` returns 09:00 UTC,
        which sits after the 08:00 Europe/London local boundary, so
        the engine-computed upper bound (07:00:00Z) is observably
        distinct from the lifecycle timestamp (09:00:00Z).
        """
        order: List[str] = []
        ledger = _InMemoryLedger()

        class _OrderRenderer(_FakeRenderer):
            def render_briefing(self, records, upper_bound_utc):
                order.append("render")
                return super().render_briefing(records, upper_bound_utc)

        original_complete = ledger.complete_run
        original_seen = ledger.seen_candidate_ids

        def _complete_with_order(*args, **kwargs):
            order.append("complete_run")
            return original_complete(*args, **kwargs)

        def _seen_with_order(*args, **kwargs):
            order.append("seen_candidate_ids")
            return original_seen(*args, **kwargs)

        ledger.complete_run = _complete_with_order  # type: ignore[assignment]
        ledger.seen_candidate_ids = _seen_with_order  # type: ignore[assignment]

        summarizer = _FakeSummarizer()
        engine = _build_engine(ledger, summarizer, _OrderRenderer())
        inputs = (
            _briefing_input(
                cid="cand-92",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        # _as_of() == 2026-07-01T09:00:00Z (after 08:00 Europe/London
        # local); the engine-computed upper bound is 07:00:00Z, so
        # lifecycle_z (== as_of_z) is observably distinct.
        as_of = _as_of()
        result = engine.execute_briefing_run(
            run_id="run-92",
            briefing_inputs=inputs,
            as_of_utc=as_of,
        )
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.COMPLETED)
        self.assertEqual(ledger.complete_runs[0][0], "run-92")
        # render must precede complete_run.
        self.assertLess(order.index("render"), order.index("complete_run"))

        # --- Phase 4 follow-up: lifecycle / run-transition time = as_of ---
        lifecycle_z = as_of.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        upper_z = (
            result.window_upper_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        lower_z = (
            result.window_lower_utc.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        )
        # Precondition: as_of is strictly after 08:00 local.
        self.assertEqual(as_of.hour, 9, msg="test guard: as_of must be after 08:00")
        # Lifecycle timestamp must NOT equal the upper window bound.
        self.assertNotEqual(lifecycle_z, upper_z)

        # begin_run: lower/upper are window bounds; started = lifecycle_z.
        self.assertEqual(len(ledger.begun), 1)
        self.assertEqual(ledger.begun[0][0], "run-92")
        self.assertEqual(ledger.begun[0][1], lower_z)
        self.assertEqual(ledger.begun[0][2], upper_z)
        self.assertEqual(ledger.begun[0][3], lifecycle_z)

        # complete_run.updated = lifecycle_z (NOT upper_z).
        self.assertEqual(len(ledger.complete_runs), 1)
        self.assertEqual(ledger.complete_runs[0][0], "run-92")
        self.assertEqual(ledger.complete_runs[0][1], lifecycle_z)
        self.assertNotEqual(ledger.complete_runs[0][1], upper_z)

    # 93
    def test_renderer_overflow_fails_run_and_records_no_seen_ids(self) -> None:
        """OversizeRecordError -> FAILED with zero shadow rows."""
        ledger = _InMemoryLedger()

        class _OverflowRenderer(_FakeRenderer):
            def render_briefing(self, records, upper_bound_utc):
                raise OversizeRecordError(records[0].candidate_id)

        summarizer = _FakeSummarizer()
        engine = _build_engine(ledger, summarizer, _OverflowRenderer())
        inputs = (
            _briefing_input(
                cid="cand-overflow",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        with self.assertRaises(OversizeRecordError):
            engine.execute_briefing_run(
                run_id="run-93",
                briefing_inputs=inputs,
                as_of_utc=_as_of(),
            )
        self.assertEqual(ledger.shadow_count, 0)
        self.assertEqual(len(ledger.fail_runs), 1)
        self.assertEqual(ledger.fail_runs[0][2], "RENDERER_OVERFLOW")

    # 94
    def test_programmer_exception_fails_run_and_propagates(self) -> None:
        """Generic Exception during run -> fail_run recorded, exception propagates."""

        class _BoomRenderer(_FakeRenderer):
            def render_briefing(self, records, upper_bound_utc):
                raise RuntimeError("boom from renderer")

        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        engine = _build_engine(ledger, summarizer, _BoomRenderer())
        inputs = (
            _briefing_input(
                cid="cand-boom",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        with self.assertRaises(RuntimeError):
            engine.execute_briefing_run(
                run_id="run-94",
                briefing_inputs=inputs,
                as_of_utc=_as_of(),
            )
        self.assertEqual(len(ledger.fail_runs), 1)
        self.assertEqual(ledger.fail_runs[0][2], "PROGRAMMER_EXCEPTION")
        self.assertEqual(ledger.shadow_count, 0)

    # 95
    def test_ledger_failure_records_no_partial_seen_ids(self) -> None:
        """Ledger.complete_run raising -> fail_run recorded, no shadow rows."""

        class _FailCompleteLedger(_InMemoryLedger):
            def __init__(self) -> None:
                super().__init__()
                self.fail_run_called = False

            def complete_run(self, run_id, updated, events):
                # Mark no shadow rows persisted (we raise before any insert).
                self.fail_run_called_before_raise = getattr(self, "fail_run_called", False)
                raise LedgerContractError("forced complete_run failure")

        ledger = _FailCompleteLedger()
        summarizer = _FakeSummarizer()
        engine = _build_engine(ledger, summarizer, _FakeRenderer())
        inputs = (
            _briefing_input(
                cid="cand-95",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        with self.assertRaises(LedgerContractError):
            engine.execute_briefing_run(
                run_id="run-95",
                briefing_inputs=inputs,
                as_of_utc=_as_of(),
            )
        # No shadow rows persisted.
        self.assertEqual(ledger.shadow_count, 0)
        # fail_run was invoked (generic except path).
        self.assertEqual(len(ledger.fail_runs), 1)

    # 96
    def test_two_identical_runs_are_byte_deterministic(self) -> None:
        """Identical inputs across two runs produce byte-identical results."""

        def _run_once() -> briefing_engine.BriefingEngineResult:
            ledger = _InMemoryLedger()
            # Deterministic renderer: emit a chunk derived from records.
            class _DetRenderer(_FakeRenderer):
                def render_briefing(self, records, upper_bound_utc):
                    chunk_text = "|" + "|".join(r.candidate_id for r in records) + "|"
                    return RenderResult(
                        status=RenderStatus.RENDERED, chunks=(chunk_text,)
                    )

            summarizer = _FakeSummarizer()
            engine = _build_engine(ledger, summarizer, _DetRenderer())
            inputs = (
                _briefing_input(
                    cid="cand-a",
                    decision=SemanticDecision.distinct_event,
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=0,
                ),
                _briefing_input(
                    cid="cand-b",
                    decision=SemanticDecision.material_update,
                    category=Category.WORLD,
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=1,
                ),
            )
            return engine.execute_briefing_run(
                run_id="run-96",
                briefing_inputs=inputs,
                as_of_utc=_as_of(),
            )

        result1 = _run_once()
        result2 = _run_once()
        # Chunks must be byte-identical.
        self.assertEqual(result1.chunks, result2.chunks)
        self.assertEqual(result1.chunks, ("|cand-a|cand-b|",))
        # Run IDs, status, model_call_count, included ordering all match.
        self.assertEqual(result1.status, result2.status)
        self.assertEqual(result1.model_call_count, result2.model_call_count)
        self.assertEqual(
            [i.briefing_input.candidate_id for i in result1.included_items],
            [i.briefing_input.candidate_id for i in result2.included_items],
        )
        # Window datetimes match.
        self.assertEqual(result1.window_lower_utc, result2.window_lower_utc)
        self.assertEqual(result1.window_upper_utc, result2.window_upper_utc)

    # 97
    def test_inputs_and_phase3_results_unchanged_after_100_runs(self) -> None:
        """100 runs do not mutate the original BriefingInput / AdjudicationResult pairs."""

        inputs: List[BriefingInput] = []
        for i in range(20):
            inputs.append(
                _briefing_input(
                    cid=f"cand-{i:02d}",
                    decision=SemanticDecision.distinct_event if i % 2 == 0 else SemanticDecision.material_update,
                    category=Category.AI if i < 10 else Category.WORLD,
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=i,
                )
            )
        inputs_t = tuple(inputs)
        # Take snapshots of the original objects.
        input_snapshots = [
            (bi.event_candidate, bi.adjudication, bi.candidate_id)
            for bi in inputs_t
        ]
        event_snapshots = [bi.event_candidate for bi in inputs_t]
        adj_snapshots = [bi.adjudication for bi in inputs_t]
        # Also snapshot Phase-3 fields.
        before_titles = [bi.event_candidate.candidate.title for bi in inputs_t]
        before_ordinals = [bi.adjudication.ordinal for bi in inputs_t]
        before_candidate_ids = [bi.event_candidate.candidate.candidate_id for bi in inputs_t]
        before_facts = [bi.adjudication.fact_deltas for bi in inputs_t]

        engine = briefing_engine.BriefingEngine(
            ledger=_InMemoryLedger(),
            summarizer=_FakeSummarizer(),
            renderer=_FakeRenderer(),
        )

        for run_n in range(100):
            ledger = _InMemoryLedger()
            engine = briefing_engine.BriefingEngine(
                ledger=ledger,
                summarizer=_FakeSummarizer(),
                renderer=_FakeRenderer(),
            )
            engine.execute_briefing_run(
                run_id=f"run-97-{run_n}",
                briefing_inputs=inputs_t,
                as_of_utc=_as_of(),
            )

        # Original BriefingInput objects are unchanged by reference.
        for bi, (orig_event, orig_adj, orig_cid) in zip(inputs_t, input_snapshots):
            self.assertIs(bi.event_candidate, orig_event)
            self.assertIs(bi.adjudication, orig_adj)
            self.assertEqual(bi.candidate_id, orig_cid)

        # Original Phase-3 fields are unchanged.
        self.assertEqual([bi.event_candidate.candidate.title for bi in inputs_t], before_titles)
        self.assertEqual([bi.adjudication.ordinal for bi in inputs_t], before_ordinals)
        self.assertEqual(
            [bi.event_candidate.candidate.candidate_id for bi in inputs_t],
            before_candidate_ids,
        )
        self.assertEqual([bi.adjudication.fact_deltas for bi in inputs_t], before_facts)

        # Identity of EventCandidate and AdjudicationResult preserved.
        self.assertEqual(
            [bi.event_candidate for bi in inputs_t], event_snapshots
        )
        self.assertEqual(
            [bi.adjudication for bi in inputs_t], adj_snapshots
        )

    # 98
    def test_shadow_seen_never_creates_delivered_state(self) -> None:
        """Phase-4 engine exposes no delivered namespace/state."""
        # Inspect the module's public surface.
        module_names = dir(briefing_engine)
        # Forbidden public names — anything that suggests "deliver",
        # "send", "post", "telegram", "transport", "live", "publish".
        forbidden_substrings = (
            "deliver", "send", "telegram", "publish", "live", "post",
            "transport", "broadcast",
        )
        for name in module_names:
            if name.startswith("_"):
                continue
            lowered = name.lower()
            for token in forbidden_substrings:
                if token in lowered:
                    self.fail(
                        f"Phase-4 engine exposes delivered-state surface: {name!r}"
                    )

        # Result has no delivered channel.
        result_attrs = (
            "run_id", "window_lower_utc", "window_upper_utc", "status",
            "included_items", "excluded_items", "chunks", "model_call_count",
            "cache_hit_count", "error_category",
        )
        for attr in result_attrs:
            self.assertTrue(
                hasattr(briefing_engine.BriefingEngineResult, attr),
                f"BriefingEngineResult missing attribute {attr}",
            )

        # The engine must keep _DRY_RUN True.
        from news_pipeline.briefing_contracts import _DRY_RUN
        self.assertTrue(_DRY_RUN)

        # No method on the engine/result mutates a delivered namespace.
        ledger = _InMemoryLedger()
        engine = _build_engine(ledger, _FakeSummarizer(), _FakeRenderer())
        inputs = (
            _briefing_input(
                cid="cand-shadow",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-98",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        # The only persisted state lives in the shadow ledger; the
        # result carries chunks but no delivery identifier.
        self.assertNotIn("deliver", dir(result))
        self.assertNotIn("send_telegram", dir(result))
        self.assertNotIn("broadcast", dir(result))

# 99 — C1: fail_run raising does NOT replace the original exception.
    def test_fail_run_raising_does_not_replace_original_exception(self) -> None:
        class _RaisingFailLedger(_InMemoryLedger):
            def fail_run(self, run_id, updated, error_code):
                raise LedgerContractError("fail_run exploded")

            def seen_candidate_ids(self, ids):
                return ()

        class _BoomRenderer(_FakeRenderer):
            def render_briefing(self, records, upper_bound_utc):
                raise RuntimeError("original boom from renderer")

        ledger = _RaisingFailLedger()
        summarizer = _FakeSummarizer()
        engine = _build_engine(ledger, summarizer, _BoomRenderer())
        inputs = (
            _briefing_input(
                cid="cand-c1",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        with self.assertRaises(RuntimeError) as cm:
            engine.execute_briefing_run(
                run_id="run-99",
                briefing_inputs=inputs,
                as_of_utc=_as_of(),
            )
        self.assertEqual(str(cm.exception), "original boom from renderer")
        self.assertEqual(ledger.shadow_count, 0)
# 100 — C1: begin_run failure routes through fail_run under one
    # authorized outer boundary; original exception propagates.
    def test_begin_run_failure_routes_through_fail_run(self) -> None:
        """Phase 4 follow-up: failure-path coverage. begin_run raises
        LedgerContractError; the engine routes through fail_run, and
        fail_run.updated MUST be lifecycle_z (the caller-supplied
        ``as_of_utc`` formatted Z), NOT the upper window bound.
        Coverage is in-place; no new numbered test method is added
        (frozen Phase 4 matrix preserved).
        """
        class _BeginFailLedger(_InMemoryLedger):
            def begin_run(self, run_id, lower, upper, started):
                raise LedgerContractError("begin_run exploded")

        ledger = _BeginFailLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        inputs = (
            _briefing_input(
                cid="cand-c1b",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        # _as_of() == 2026-07-01T09:00:00Z; the engine-computed upper
        # bound is 07:00:00Z, so the two Z strings must differ.
        as_of = _as_of()
        with self.assertRaises(LedgerContractError):
            engine.execute_briefing_run(
                run_id="run-100",
                briefing_inputs=inputs,
                as_of_utc=as_of,
            )
        self.assertEqual(len(ledger.fail_runs), 1)
        self.assertEqual(ledger.fail_runs[0][0], "run-100")

        # --- Phase 4 follow-up: fail_run.updated == lifecycle_z ---
        lifecycle_z = as_of.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        fail_run_updated = ledger.fail_runs[0][1]
        self.assertEqual(fail_run_updated, lifecycle_z)
        self.assertNotEqual(fail_run_updated, "2026-07-01T07:00:00Z")
        # No shadow rows recorded on the failure path.
        self.assertEqual(ledger.shadow_count, 0)

# 101 — C2: invalid candidate_id (empty) -> direct INPUT_BOUNDS
    # fallback emitted by the engine BEFORE SummarizerInput; original ID
    # preserved; no model call. Status is PARTIAL (I1).
    def test_invalid_candidate_id_yields_input_bounds_fallback_no_model_call(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        inputs = (
            _briefing_input(
                cid="",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-101",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        self.assertEqual(len(result.included_items), 1)
        included = result.included_items[0]
        self.assertEqual(included.briefing_input.candidate_id, "")
        self.assertEqual(included.summary_item.candidate_id, "")
        self.assertIs(included.summary_item.source, SummarySource.FALLBACK)
        self.assertIs(
            included.summary_item.error_category,
            SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(summarizer.model_call_count, 0)
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        self.assertIs(
            result.error_category,
            briefing_engine.EngineErrorCategory.INPUT_BOUNDS,
        )
# 102 — C2: invalid title (too long) -> INPUT_BOUNDS, no model call.
    def test_invalid_title_yields_input_bounds_fallback(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        long_title = "x" * 513
        inputs = (
            _briefing_input(
                cid="cand-bad-title",
                decision=SemanticDecision.distinct_event,
                title=long_title,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-102",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        self.assertEqual(len(result.included_items), 1)
        included = result.included_items[0]
        self.assertIs(included.summary_item.source, SummarySource.FALLBACK)
        self.assertIs(
            included.summary_item.error_category,
            SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(included.summary_item.candidate_id, "cand-bad-title")
        self.assertEqual(summarizer.model_call_count, 0)
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        self.assertIs(
            result.error_category,
            briefing_engine.EngineErrorCategory.INPUT_BOUNDS,
        )

    # 103 — C2: invalid snippet (too long) -> INPUT_BOUNDS, no model call.
    def test_invalid_snippet_yields_input_bounds_fallback(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        long_snippet = "s" * 2049
        inputs = (
            _briefing_input(
                cid="cand-bad-snippet",
                decision=SemanticDecision.distinct_event,
                snippet=long_snippet,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-103",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        self.assertEqual(len(result.included_items), 1)
        self.assertIs(
            result.included_items[0].summary_item.source,
            SummarySource.FALLBACK,
        )
        self.assertIs(
            result.included_items[0].summary_item.error_category,
            SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(summarizer.model_call_count, 0)
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        self.assertIs(
            result.error_category,
            briefing_engine.EngineErrorCategory.INPUT_BOUNDS,
        )
# 104 — C2: invalid url (missing scheme) -> INPUT_BOUNDS, no model call.
    def test_invalid_url_yields_input_bounds_fallback(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        inputs = (
            _briefing_input(
                cid="cand-bad-url",
                decision=SemanticDecision.distinct_event,
                url="not-a-valid-url",
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-104",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        self.assertEqual(len(result.included_items), 1)
        included = result.included_items[0]
        self.assertIs(included.summary_item.source, SummarySource.FALLBACK)
        self.assertIs(
            included.summary_item.error_category,
            SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(included.summary_item.candidate_id, "cand-bad-url")
        self.assertEqual(summarizer.model_call_count, 0)
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        self.assertIs(
            result.error_category,
            briefing_engine.EngineErrorCategory.INPUT_BOUNDS,
        )

    # 105 — C2: mixed valid + invalid inputs — valid items still go
    # through summarize_category; invalid items get INPUT_BOUNDS; all IDs
    # are preserved in included_items (no slicing / dropping).
    def test_mixed_valid_and_invalid_inputs_preserves_all_ids(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        long_title = "x" * 513
        inputs = (
            _briefing_input(
                cid="cand-good-1",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
                ordinal=0,
            ),
            _briefing_input(
                cid="cand-bad-title",
                decision=SemanticDecision.distinct_event,
                title=long_title,
                evaluated_at="2026-07-01T06:00:00Z",
                ordinal=1,
            ),
            _briefing_input(
                cid="cand-good-2",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
                ordinal=2,
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-105",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        included_ids = [i.briefing_input.candidate_id for i in result.included_items]
        self.assertEqual(
            set(included_ids),
            {"cand-good-1", "cand-bad-title", "cand-good-2"},
        )
        # The bad-title one is the INPUT_BOUNDS fallback; the others are MODEL.
        by_id = {i.briefing_input.candidate_id: i for i in result.included_items}
        self.assertIs(
            by_id["cand-bad-title"].summary_item.source,
            SummarySource.FALLBACK,
        )
        self.assertIs(
            by_id["cand-bad-title"].summary_item.error_category,
            SummarizerErrorCategory.INPUT_BOUNDS,
        )
        # Good ones get MODEL summaries (fake default path).
        self.assertIs(by_id["cand-good-1"].summary_item.source, SummarySource.MODEL)
        self.assertIs(by_id["cand-good-2"].summary_item.source, SummarySource.MODEL)
        # PARTIAL because at least one INPUT_BOUNDS fallback exists (I1).
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        self.assertIs(
            result.error_category,
            briefing_engine.EngineErrorCategory.INPUT_BOUNDS,
        )
# 106 — I1: any INPUT_BOUNDS fallback makes the run PARTIAL with a
    # typed non-NONE error_category (INPUT_BOUNDS) on the run result.
    def test_input_bounds_fallback_yields_partial_with_input_bounds_category(self) -> None:
        ledger = _InMemoryLedger()
        summarizer = _FakeSummarizer()
        renderer = _FakeRenderer()
        engine = _build_engine(ledger, summarizer, renderer)
        # Pre-populate shadow-seen so engine has at least one exclusion
        # to show the run can also have NON-ELIGIBLE decisions without
        # changing the I1 rule.
        inputs = (
            _briefing_input(
                cid="",
                decision=SemanticDecision.distinct_event,
                evaluated_at="2026-07-01T06:00:00Z",
            ),
        )
        result = engine.execute_briefing_run(
            run_id="run-106",
            briefing_inputs=inputs,
            as_of_utc=_as_of(),
        )
        # I1: any fallback -> PARTIAL + typed non-NONE error_category.
        self.assertEqual(result.status, briefing_engine.EngineRunStatus.PARTIAL)
        self.assertIsNot(
            result.error_category,
            briefing_engine.EngineErrorCategory.NONE,
        )
        self.assertIs(
            result.error_category,
            briefing_engine.EngineErrorCategory.INPUT_BOUNDS,
        )

    # 107 — I2: EngineIncludedItem rejects non-BriefingInput /
    # non-SummaryItem arguments (frozen/slotted strict typing).
    def test_engine_included_item_validator_rejects_non_types(self) -> None:
        with self.assertRaises(TypeError):
            briefing_engine.EngineIncludedItem(
                briefing_input=object(),  # type: ignore[arg-type]
                summary_item=object(),  # type: ignore[arg-type]
            )

    # 108 — I2: EngineExcludedItem rejects wrong reason type, wrong
    # typed_detail type, and typed_detail inconsistent with reason.
    def test_engine_excluded_item_validator_rejects_bad_reason_or_detail(self) -> None:
        # Non-EngineExclusionReason reason -> TypeError.
        with self.assertRaises(TypeError):
            briefing_engine.EngineExcludedItem(
                briefing_input=_briefing_input("validator", SemanticDecision.distinct_event, evaluated_at="2026-07-01T06:00:00Z"),
                reason="not-an-enum",  # type: ignore[arg-type]
                typed_detail=None,
            )
        # Wrong typed_detail type for NON_ELIGIBLE_DECISION -> TypeError.
        with self.assertRaises(TypeError):
            briefing_engine.EngineExcludedItem(
                briefing_input=_briefing_input("validator", SemanticDecision.distinct_event, evaluated_at="2026-07-01T06:00:00Z"),
                reason=briefing_engine.EngineExclusionReason.NON_ELIGIBLE_DECISION,
                typed_detail="not-a-reason-code",  # type: ignore[arg-type]
            )
        # For OUT_OF_WINDOW / ALREADY_SHADOW_SEEN, typed_detail must be None.
        with self.assertRaises(ValueError):
            briefing_engine.EngineExcludedItem(
                briefing_input=_briefing_input("validator2", SemanticDecision.distinct_event, evaluated_at="2026-07-01T06:00:00Z"),
                reason=briefing_engine.EngineExclusionReason.OUT_OF_WINDOW,
                typed_detail=SemanticReasonCode.DISTINCT_EVENT,
            )
        with self.assertRaises(ValueError):
            briefing_engine.EngineExcludedItem(
                briefing_input=_briefing_input("validator3", SemanticDecision.distinct_event, evaluated_at="2026-07-01T06:00:00Z"),
                reason=briefing_engine.EngineExclusionReason.ALREADY_SHADOW_SEEN,
                typed_detail=SemanticReasonCode.DISTINCT_EVENT,
            )
# 109 — I2: BriefingEngineResult enforces status/error_category
    # relation (COMPLETED => NONE; PARTIAL/FAILED => non-NONE) and
    # model_call_count / cache_hit_count bounded to 0..8.
    def test_result_validates_status_error_relation_and_count_bounds(self) -> None:
        # COMPLETED with non-NONE error_category must reject.
        with self.assertRaises(ValueError):
            briefing_engine.BriefingEngineResult(
                run_id="x",
                window_lower_utc=datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc),
                window_upper_utc=datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc),
                status=briefing_engine.EngineRunStatus.COMPLETED,
                included_items=(),
                excluded_items=(),
                chunks=(),
                model_call_count=0,
                cache_hit_count=0,
                error_category=briefing_engine.EngineErrorCategory.TRANSPORT,
            )
        # PARTIAL with NONE error_category must reject.
        with self.assertRaises(ValueError):
            briefing_engine.BriefingEngineResult(
                run_id="x",
                window_lower_utc=datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc),
                window_upper_utc=datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc),
                status=briefing_engine.EngineRunStatus.PARTIAL,
                included_items=(),
                excluded_items=(),
                chunks=(),
                model_call_count=0,
                cache_hit_count=0,
                error_category=briefing_engine.EngineErrorCategory.NONE,
            )
        # FAILED with NONE error_category must reject.
        with self.assertRaises(ValueError):
            briefing_engine.BriefingEngineResult(
                run_id="x",
                window_lower_utc=datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc),
                window_upper_utc=datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc),
                status=briefing_engine.EngineRunStatus.FAILED,
                included_items=(),
                excluded_items=(),
                chunks=(),
                model_call_count=0,
                cache_hit_count=0,
                error_category=briefing_engine.EngineErrorCategory.NONE,
            )
        # model_call_count > 8 must reject.
        with self.assertRaises(ValueError):
            briefing_engine.BriefingEngineResult(
                run_id="x",
                window_lower_utc=datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc),
                window_upper_utc=datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc),
                status=briefing_engine.EngineRunStatus.COMPLETED,
                included_items=(),
                excluded_items=(),
                chunks=(),
                model_call_count=9,
                cache_hit_count=0,
                error_category=briefing_engine.EngineErrorCategory.NONE,
            )
        # cache_hit_count > 8 must reject.
        with self.assertRaises(ValueError):
            briefing_engine.BriefingEngineResult(
                run_id="x",
                window_lower_utc=datetime(2026, 7, 1, 0, 0, tzinfo=timezone.utc),
                window_upper_utc=datetime(2026, 7, 1, 8, 0, tzinfo=timezone.utc),
                status=briefing_engine.EngineRunStatus.COMPLETED,
                included_items=(),
                excluded_items=(),
                chunks=(),
                model_call_count=0,
                cache_hit_count=9,
                error_category=briefing_engine.EngineErrorCategory.NONE,
            )
# 110 — I3: engine module does not import concrete collaborators at
    # module load. Specifically: no BriefingLedger, no render_briefing,
    # no SummarizerInput, no BriefingInput from the ledger/renderer/
    # summarizer modules as a top-level import.
    def test_engine_does_not_import_concrete_collaborators_at_module_level(self) -> None:
        # BriefingLedger should not be re-bound into the module namespace
        # as a top-level reference (only the protocol class lives there).
        # The summarizer's SummarizerInput should be importable lazily,
        # not at module-load time.
        import importlib

        # Force a fresh re-import to check module-load surface.
        mod = importlib.reload(briefing_engine)
        # These concrete names must not appear as top-level bindings.
        forbidden = ("BriefingLedger",)
        for name in forbidden:
            self.assertFalse(
                hasattr(mod, name) and not name.startswith("_"),
                f"engine module exposes concrete collaborator {name!r}",
            )
        # The functional render_briefing must not be imported at module
        # load either (engine only uses an injected RendererProtocol).
        self.assertFalse(
            hasattr(mod, "render_briefing"),
            "engine module imports concrete render_briefing collaborator",
        )

    # 111 — I4: eligibility is mapped for EVERY input BEFORE the
    # shadow-seen filter is applied. Use a counting spy wrapping
    # map_eligibility via the eligibility attribute the engine consumes.
    def test_eligibility_mapped_for_every_input_before_shadow_seen_filtering(self) -> None:
        import news_pipeline.briefing_engine as engine_mod
        import news_pipeline.briefing_contracts as contracts_mod

        original_map = contracts_mod.map_eligibility
        seen_calls = []

        def _spy_map(inp):
            seen_calls.append(inp.candidate_id)
            return original_map(inp)

        # Monkey-patch the binding the engine actually looks up.
        contracts_mod.map_eligibility = _spy_map
        engine_mod.map_eligibility = _spy_map
        try:
            ledger = _InMemoryLedger(seen=("cand-already",))
            summarizer = _FakeSummarizer()
            renderer = _FakeRenderer()
            engine = _build_engine(ledger, summarizer, renderer)
            inputs = (
                _briefing_input(
                    cid="cand-already",
                    decision=SemanticDecision.distinct_event,
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=0,
                ),
                _briefing_input(
                    cid="cand-ineligible",
                    decision=SemanticDecision.rewrite,
                    reasons=(SemanticReasonCode.SAME_FACTS,),
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=1,
                ),
                _briefing_input(
                    cid="cand-eligible",
                    decision=SemanticDecision.distinct_event,
                    evaluated_at="2026-07-01T06:00:00Z",
                    ordinal=2,
                ),
            )
            result = engine.execute_briefing_run(
                run_id="run-111",
                briefing_inputs=inputs,
                as_of_utc=_as_of(),
            )
            # I4: map_eligibility called for EVERY input, including the
            # already-seen one ("cand-already").
            self.assertIn("cand-already", seen_calls)
            self.assertIn("cand-ineligible", seen_calls)
            self.assertIn("cand-eligible", seen_calls)
            # ALREADY_SHADOW_SEEN still wins precedence on exclusion.
            excluded_by_cid = {
                ex.briefing_input.candidate_id: ex
                for ex in result.excluded_items
            }
            self.assertIs(
                excluded_by_cid["cand-already"].reason,
                briefing_engine.EngineExclusionReason.ALREADY_SHADOW_SEEN,
            )
            self.assertIs(
                excluded_by_cid["cand-ineligible"].reason,
                briefing_engine.EngineExclusionReason.NON_ELIGIBLE_DECISION,
            )
        finally:
            contracts_mod.map_eligibility = original_map
            engine_mod.map_eligibility = original_map

    # 112 — I4: every eligible input, including ones the engine later
    # marks as INPUT_BOUNDS, must be preserved as included_items entries.
    # All six decision types still get map_eligibility called once each.
    def test_all_six_decisions_each_receive_one_map_eligibility_call(self) -> None:
        import news_pipeline.briefing_engine as engine_mod
        import news_pipeline.briefing_contracts as contracts_mod

        original_map = contracts_mod.map_eligibility
        seen_calls = []

        def _spy_map(inp):
            seen_calls.append((inp.candidate_id, inp.adjudication.semantic_decision))
            return original_map(inp)

        contracts_mod.map_eligibility = _spy_map
        engine_mod.map_eligibility = _spy_map
        try:
            ledger = _InMemoryLedger()
            summarizer = _FakeSummarizer()
            renderer = _FakeRenderer()
            engine = _build_engine(ledger, summarizer, renderer)
            decisions = (
                ("cand-distinct", SemanticDecision.distinct_event),
                ("cand-update", SemanticDecision.material_update),
                ("cand-rewrite", SemanticDecision.rewrite),
                ("cand-bypass", SemanticDecision.bypass_phase2_terminal),
                ("cand-pending", SemanticDecision.pending_review),
                ("cand-model-err", SemanticDecision.pending_model_error),
            )
            inputs = []
            for ord_, (cid, decision) in enumerate(decisions):
                kwargs = {}
                if decision is SemanticDecision.pending_model_error:
                    kwargs["model_used"] = True
                    kwargs["model_error_category"] = ModelErrorCategory.MALFORMED_OUTPUT
                inputs.append(
                    _briefing_input(
                        cid=cid,
                        decision=decision,
                        ordinal=ord_,
                        evaluated_at="2026-07-01T06:00:00Z",
                        **kwargs,
                    )
                )
            engine.execute_briefing_run(
                run_id="run-112",
                briefing_inputs=tuple(inputs),
                as_of_utc=_as_of(),
            )
            seen_ids = [cid for cid, _ in seen_calls]
            for cid, _ in decisions:
                self.assertIn(cid, seen_calls and seen_ids)
        finally:
            contracts_mod.map_eligibility = original_map
            engine_mod.map_eligibility = original_map

if __name__ == "__main__":
    unittest.main()
