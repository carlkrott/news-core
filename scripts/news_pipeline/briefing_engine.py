#!/usr/bin/env python3
"""Phase 4 — Slice 6 briefing_engine.

The engine is the only Phase 4 module allowed to import the other five
(contracts / ledger / summarizer / renderer / reliability). It is
strictly stdlib + ``news_pipeline``; it never calls network, filesystem,
process, wall-clock, random, or UUID APIs.

Phase 4 supervisor addendum §2.4 and §2.5 are normative. The engine:

  * accepts caller-provided ``BriefingInput`` paired inputs, an injected
    ``ShadowLedgerProtocol`` (or any object exposing
    ``begin_run / seen_candidate_ids / complete_run / fail_run``), an
    injected summarizer session (the only thing allowed to invoke the
    transport), and ``as_of_utc`` plus optional
    ``last_completed_upper_utc``;
  * computes the morning window via ``compute_morning_window``;
  * excludes out-of-window, non-eligible, and already-seen candidates
    with typed reasons;
  * sorts by ``(category-order, evaluated_at_datetime, ordinal, candidate_id)``;
  * groups by Category order, summarizes at most the first 32 eligible
    unseen items per category, then renders via the injected renderer,
    then completes the ledger only after a successful render.

All run results are frozen/slotted. The only generic ``except Exception``
in production Phase 4 lives at the outermost boundary of
``execute_briefing_run`` and is solely there to attempt a
``ledger.fail_run`` and re-raise the original exception.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from contextlib import suppress
from typing import Any, Dict, Mapping, Optional, Protocol, Sequence, Tuple

from .briefing_contracts import (
    BriefingInput,
    EligibilityResult,
    SemanticReasonCode,  # re-exported for excluded / included reasons
    _DRY_RUN,
    compute_morning_window,
    map_eligibility,
)
from .briefing_ledger import (
    CompleteRunResult,
    LedgerError,
    LedgerContractError,
    LockTimeoutError,
    RunConflictError,
    RunStateError,
    ShadowEvent,
    normalize_canonical_payload,
)
from .briefing_renderer import (
    OversizeRecordError,
    RenderRecord,
    RenderResult,
    RenderStatus,
    utf16_units,
)
from .briefing_summarizer import (
    CANDIDATE_ID_MAX,
    CANDIDATE_ID_MIN,
    SNIPPET_MAX,
    SNIPPET_MIN,
    TITLE_MAX,
    TITLE_MIN,
    URL_MAX,
    URL_MIN,
    SummarizerErrorCategory,
    SummaryItem,
    SummarySource,
)
from .contracts import validate_utc_iso
from .event_contracts import EventCandidate, FactDelta, SemanticDecision, SemanticReasonCode
from .models import Category


# Phase 4 / Phase 5 must not flip this flag.
__all__ = (
    "BriefingEngine",
    "EngineRunStatus",
    "EngineExclusionReason",
    "EngineErrorCategory",
    "ShadowLedgerProtocol",
    "RendererProtocol",
    "SummarizerProtocol",
    "EngineExcludedItem",
    "EngineIncludedItem",
    "BriefingEngineResult",
    "execute_briefing_run",
)


# ---------------------------------------------------------------------------
# Enums (use existing enums where addendum allows, only new enums here)
# -----------------------------------------------------------------------------


class EngineRunStatus(str, Enum):
    """Typed terminal status for one briefing run.

    The addendum §2.4 names three terminal outcomes: COMPLETED, PARTIAL,
    FAILED. PARTIAL means a transport / malformed / budget / input
    fallback was used and the run is still renderable; FAILED means
    programmer / renderer-overflow / ledger failure that prevented a
    full successful render.
    """

    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class EngineExclusionReason(str, Enum):
    """Typed reasons a BriefingInput was excluded from a run."""

    OUT_OF_WINDOW = "out_of_window"
    NON_ELIGIBLE_DECISION = "non_eligible_decision"
    ALREADY_SHADOW_SEEN = "already_shadow_seen"


class EngineErrorCategory(str, Enum):
    """Typed non-secret error categories on a run result.

    Only the categories named in §2.4 / §2.5 are surfaced.
    """

    NONE = "none"
    RENDERER_OVERFLOW = "renderer_overflow"
    PROGRAMMER = "programmer"
    LEDGER = "ledger"
    TRANSPORT = "transport"
    MALFORMED_OUTPUT = "malformed_output"
    BUDGET_EXHAUSTED = "budget_exhausted"
    INPUT_BOUNDS = "input_bounds"


# -----------------------------------------------------------------------------=
# Protocols — every collaborator is injected; the engine never opens paths
# -----------------------------------------------------------------------------


class ShadowLedgerProtocol(Protocol):
    """Structural surface the engine uses on a shadow ledger.

    The production type ``BriefingLedger`` conforms; a fake / spy in
    tests does too. No methods on this protocol call network or
    filesystem APIs.
    """

    def begin_run(
        self, run_id: str, lower: str, upper: str, started: str
    ) -> None: ...

    def seen_candidate_ids(self, ids: Tuple[str, ...]) -> Tuple[str, ...]: ...

    def complete_run(
        self,
        run_id: str,
        updated: str,
        events: Sequence[ShadowEvent],
    ) -> CompleteRunResult: ...

    def fail_run(self, run_id: str, updated: str, error_code: str) -> None: ...


class RendererProtocol(Protocol):
    """Structural surface the engine uses on a renderer."""

    def render_briefing(
        self, records: Sequence[RenderRecord], upper_bound_utc: datetime
    ) -> RenderResult: ...


class SummarizerProtocol(Protocol):
    """Structural surface the engine uses on a summarizer session.

    ``summarize_category`` must accept a category and a sequence of
    raw inputs and return a ``CategorySummaryResult``-shaped object
    with tuple ``items``, plus ``model_used`` / ``cache_hit`` flags.
    The standard library-side attribute surface
    ``model_call_count`` / ``cache_hit_count`` is read at the end of
    the run.
    """

    def summarize_category(
        self, category: Category, raw_inputs: Sequence[Any]
    ) -> Any: ...

    @property
    def model_call_count(self) -> int: ...

    @property
    def cache_hit_count(self) -> int: ...


# -----------------------------------------------------------------------------=
# Result value objects
# -----------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EngineIncludedItem:
    """One BriefingInput that survived filtering and was rendered.

    The original ``BriefingInput`` and ``SummaryItem`` are referenced
    by identity; the engine never copies or truncates them.
    """

    briefing_input: BriefingInput
    summary_item: SummaryItem

    def __post_init__(self) -> None:
        if not isinstance(self.briefing_input, BriefingInput):
            raise TypeError("briefing_input must be BriefingInput")
        if not isinstance(self.summary_item, SummaryItem):
            raise TypeError("summary_item must be SummaryItem")


@dataclass(frozen=True, slots=True)
class EngineExcludedItem:
    """One BriefingInput that was excluded from the rendered run.

    ``reason`` is one of the three ``EngineExclusionReason`` members;
    ``typed_detail`` carries the underlying typed reason (window reason,
    eligibility reason, or ``None`` for shadow-seen suppression).
    """

    briefing_input: BriefingInput
    reason: EngineExclusionReason
    typed_detail: Optional[SemanticReasonCode]

    def __post_init__(self) -> None:
        if not isinstance(self.briefing_input, BriefingInput):
            raise TypeError("briefing_input must be BriefingInput")
        if not isinstance(self.reason, EngineExclusionReason):
            raise TypeError("reason must be EngineExclusionReason")
        if self.typed_detail is not None and not isinstance(self.typed_detail, SemanticReasonCode):
            raise TypeError("typed_detail must be SemanticReasonCode or None")
        if self.reason is not EngineExclusionReason.NON_ELIGIBLE_DECISION and self.typed_detail is not None:
            raise ValueError("typed_detail is only valid for non-eligible decisions")


@dataclass(frozen=True, slots=True)
class BriefingEngineResult:
    """Frozen result of one ``execute_briefing_run`` call.

    Invariants enforced by ``__post_init__``:

      * run_id is a non-empty str;
      * status is one of the three ``EngineRunStatus`` members;
      * chunks is an exact tuple of ``str``;
      * included / excluded items are exact tuples of the right type;
      * model_call_count, cache_hit_count are non-negative ints;
      * error_category is one of ``EngineErrorCategory`` members.
    """

    run_id: str
    window_lower_utc: datetime
    window_upper_utc: datetime
    status: EngineRunStatus
    included_items: Tuple[EngineIncludedItem, ...]
    excluded_items: Tuple[EngineExcludedItem, ...]
    chunks: Tuple[str, ...]
    model_call_count: int
    cache_hit_count: int
    error_category: EngineErrorCategory

    def __post_init__(self) -> None:
        if type(self.run_id) is not str or not self.run_id:
            raise TypeError(
                f"BriefingEngineResult.run_id must be non-empty str, got {self.run_id!r}"
            )
        for name, dt in (
            ("window_lower_utc", self.window_lower_utc),
            ("window_upper_utc", self.window_upper_utc),
        ):
            if not isinstance(dt, datetime):
                raise TypeError(
                    f"BriefingEngineResult.{name} must be datetime, got {type(dt).__name__}"
                )
            if dt.tzinfo is None or dt.utcoffset() != timezone.utc.utcoffset(dt):
                raise ValueError(
                    f"BriefingEngineResult.{name} must be aware UTC, got {dt!r}"
                )
        if not isinstance(self.status, EngineRunStatus):
            raise TypeError(
                f"BriefingEngineResult.status must be EngineRunStatus, got {type(self.status).__name__}"
            )
        if type(self.included_items) is not tuple:
            raise TypeError(
                "BriefingEngineResult.included_items must be tuple, got "
                f"{type(self.included_items).__name__}"
            )
        for item in self.included_items:
            if not isinstance(item, EngineIncludedItem):
                raise TypeError(
                    f"BriefingEngineResult.included_items entry must be EngineIncludedItem, got {item!r}"
                )
        if type(self.excluded_items) is not tuple:
            raise TypeError(
                "BriefingEngineResult.excluded_items must be tuple, got "
                f"{type(self.excluded_items).__name__}"
            )
        for item in self.excluded_items:
            if not isinstance(item, EngineExcludedItem):
                raise TypeError(
                    f"BriefingEngineResult.excluded_items entry must be EngineExcludedItem, got {item!r}"
                )
        if type(self.chunks) is not tuple:
            raise TypeError(
                f"BriefingEngineResult.chunks must be tuple, got {type(self.chunks).__name__}"
            )
        for chunk in self.chunks:
            if type(chunk) is not str:
                raise TypeError(
                    f"BriefingEngineResult.chunks entry must be str, got {chunk!r}"
                )
        if (
            type(self.model_call_count) is not int
            or isinstance(self.model_call_count, bool)
            or not (0 <= self.model_call_count <= 8)
        ):
            raise ValueError(
                f"BriefingEngineResult.model_call_count must be non-negative int, got {self.model_call_count!r}"
            )
        if (
            type(self.cache_hit_count) is not int
            or isinstance(self.cache_hit_count, bool)
            or not (0 <= self.cache_hit_count <= 8)
        ):
            raise ValueError(
                f"BriefingEngineResult.cache_hit_count must be non-negative int, got {self.cache_hit_count!r}"
            )
        if not isinstance(self.error_category, EngineErrorCategory):
            raise TypeError(
                f"BriefingEngineResult.error_category must be EngineErrorCategory, got {type(self.error_category).__name__}"
            )
        if self.status is EngineRunStatus.FAILED and self.error_category is EngineErrorCategory.NONE:
            raise ValueError("FAILED status requires a typed error_category")
        if self.status is EngineRunStatus.COMPLETED and self.error_category is not EngineErrorCategory.NONE:
            raise ValueError("COMPLETED status requires error_category NONE")
        if self.status is EngineRunStatus.PARTIAL and self.error_category is EngineErrorCategory.NONE:
            raise ValueError("PARTIAL status requires a typed error_category")


# -----------------------------------------------------------------------------=
# Engine helpers
# -----------------------------------------------------------------------------


_CATEGORY_ORDER: Dict[Category, int] = {
    category: index for index, category in enumerate(Category)
}


def _aware_utc(dt: datetime, name: str) -> datetime:
    """Reject non-datetime / naive / non-zero-offset datetimes."""
    if not isinstance(dt, datetime):
        raise TypeError(f"{name} must be datetime, got {type(dt).__name__}")
    if dt.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware, got {dt!r}")
    if dt.utcoffset() != timezone.utc.utcoffset(dt):
        raise ValueError(f"{name} must be UTC, got {dt!r}")
    return dt.astimezone(timezone.utc)


def _format_utc_z(dt: datetime) -> str:
    """Render an aware UTC datetime in the ``Z`` ISO-8601 form."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_evaluated_at(value: str) -> datetime:
    """Parse a ``validated_utc_iso`` string back to an aware UTC datetime.

    Per §2.5 #2, the sort tuple must contain an actual aware datetime,
    not a string.
    """
    normalized = validate_utc_iso(value)
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    return datetime.fromisoformat(normalized)


def _semantic_decision_to_render_decision(decision: SemanticDecision) -> SemanticDecision:
    """Renderer accepts only ``distinct_event`` / ``material_update``.

    The map_eligibility gate already filters out everything else, so
    this is a defensive equality check.
    """
    if decision not in (SemanticDecision.distinct_event, SemanticDecision.material_update):
        raise ValueError(
            f"cannot render ineligible semantic decision: {decision!r}"
        )
    return decision


def _raw_bounds_invalid(briefing_input: BriefingInput) -> bool:
    candidate = briefing_input.event_candidate.candidate
    cid = briefing_input.candidate_id
    title = candidate.title
    snippet = candidate.snippet
    url = briefing_input.url
    if type(cid) is not str or not (CANDIDATE_ID_MIN <= len(cid) <= CANDIDATE_ID_MAX):
        return True
    if type(title) is not str or not (TITLE_MIN <= len(title) <= TITLE_MAX):
        return True
    if type(snippet) is not str or not (SNIPPET_MIN <= len(snippet) <= SNIPPET_MAX):
        return True
    if url is not None:
        if type(url) is not str or not (URL_MIN <= len(url) <= URL_MAX):
            return True
        if not (url.startswith("http://") or url.startswith("https://")):
            return True
        if any(ch.isspace() or ord(ch) < 32 or 0x7F <= ord(ch) <= 0x9F for ch in url):
            return True
        authority = url.split("://", 1)[1]
        positions = [authority.find(term) for term in ("/", "?", "#") if authority.find(term) >= 0]
        if positions:
            authority = authority[:min(positions)]
        if not authority:
            return True
    return False


def _build_render_record(
    included: EngineIncludedItem,
    fact_deltas: Tuple[FactDelta, ...],
    ordinal: int,
) -> RenderRecord:
    """Build one ``RenderRecord`` from an engine-included item.

    ``ordinal`` is the engine's deterministic per-category counter (it
    restarts at 0 inside each category group, after sorting). The
    renderer re-sorts by (category-order, ordinal, candidate_id) so the
    ordinal is stable across runs.
    """
    briefing_input = included.briefing_input
    candidate = briefing_input.event_candidate.candidate
    title = candidate.title or included.summary_item.summary
    return RenderRecord(
        candidate_id=briefing_input.candidate_id,
        category=briefing_input.category,
        decision=_semantic_decision_to_render_decision(
            briefing_input.adjudication.semantic_decision
        ),
        title=title,
        summary=included.summary_item.summary,
        url=briefing_input.url,
        semantic_reasons=tuple(briefing_input.adjudication.semantic_reasons),
        fact_deltas=tuple(fact_deltas),
        ordinal=ordinal,
    )


def _canonical_payload_for_briefing_input(
    included: EngineIncludedItem,
    decision_value: str,
) -> bytes:
    """Build the canonical payload bytes for a single ``ShadowEvent``.

    The payload is the minimum JSON object that round-trips through
    ``normalize_canonical_payload`` byte-identically. It is never
    allowed to carry any sensitive key.
    """
    candidate = included.briefing_input.event_candidate.candidate
    return normalize_canonical_payload(
        {
            "candidate_id": included.briefing_input.candidate_id,
            "category": included.briefing_input.category.value,
            "decision": decision_value,
            "title": candidate.title,
            "summary": included.summary_item.summary,
        }
    )


# -----------------------------------------------------------------------------=
# Engine
# -----------------------------------------------------------------------------=


class BriefingEngine:
    """Deterministic, dependency-injected briefing orchestrator."""

    def __init__(
        self,
        ledger: ShadowLedgerProtocol,
        summarizer: SummarizerProtocol,
        renderer: RendererProtocol,
    ) -> None:
        if not _DRY_RUN:
            raise RuntimeError("briefing_engine must keep _DRY_RUN=True")
        if ledger is None:
            raise TypeError("BriefingEngine.ledger is required")
        if summarizer is None:
            raise TypeError("BriefingEngine.summarizer is required")
        if renderer is None:
            raise TypeError("BriefingEngine.renderer is required")
        self._ledger = ledger
        self._summarizer = summarizer
        self._renderer = renderer

    # -- public surface ----------------------------------------------

    def execute_briefing_run(
        self,
        run_id: str,
        briefing_inputs: Tuple[BriefingInput, ...],
        as_of_utc: datetime,
        last_completed_upper_utc: Optional[datetime] = None,
    ) -> BriefingEngineResult:
        """Run one briefing, returning a frozen ``BriefingEngineResult``.

        Phases (in order): validate pairings; compute window; map all
        decisions; exclude out-of-window / non-eligible / already-seen
        with typed reasons; sort; group in Category order; summarize;
        render; complete ledger only after successful render.
        """
        if not isinstance(run_id, str) or not run_id:
            raise TypeError("execute_briefing_run.run_id must be non-empty str")
        if not isinstance(briefing_inputs, tuple):
            raise TypeError(
                f"execute_briefing_run.briefing_inputs must be tuple, got "
                f"{type(briefing_inputs).__name__}"
            )
        for idx, item in enumerate(briefing_inputs):
            if not isinstance(item, BriefingInput):
                raise TypeError(
                    f"briefing_inputs[{idx}] must be BriefingInput, got {type(item).__name__}"
                )
        as_of = _aware_utc(as_of_utc, "as_of_utc")
        if last_completed_upper_utc is not None:
            last_completed_upper_utc = _aware_utc(
                last_completed_upper_utc, "last_completed_upper_utc"
            )
            if last_completed_upper_utc > as_of:
                raise ValueError(
                    "last_completed_upper_utc must be <= as_of_utc"
                )

        lower_utc, upper_utc = compute_morning_window(
            as_of_utc=as_of, last_completed_upper_utc=last_completed_upper_utc
        )
        lower_z = _format_utc_z(lower_utc)
        upper_z = _format_utc_z(upper_utc)
        # Phase 4 follow-up: lifecycle / run-transition time uses the
        # caller-supplied, already-validated aware-UTC ``as_of`` itself.
        # ``lifecycle_z`` is the deterministic logical timestamp of
        # THIS run transition (begin/complete/fail); it intentionally
        # does NOT equal ``upper_z`` because the upper window bound
        # is a content-time boundary (the 08:00 Europe/London
        # cut-off), not when the run actually happened. Window
        # bounds lower/upper remain governed by
        # ``compute_morning_window`` and are passed to
        # ``begin_run(lower=..., upper=...)`` unchanged. fail_run's
        # ``updated`` is the caller-supplied transition time too.
        lifecycle_z = _format_utc_z(as_of)
        started_z = lifecycle_z
        updated_z = lifecycle_z

        # Begin the shadow ledger run so a later exception can call fail_run.
        # Begin and execute under the same outer failure boundary so a
        # begin_run failure is also routed through the shadow failure hook.
        try:
            self._ledger.begin_run(
                run_id=run_id,
                lower=lower_z,
                upper=upper_z,
                started=started_z,
            )
            return self._run_after_begin(
                run_id=run_id,
                briefing_inputs=briefing_inputs,
                lower_utc=lower_utc,
                upper_utc=upper_utc,
                updated_z=updated_z,
            )
        except (OversizeRecordError, LedgerError) as exc:
            # Pass lifecycle_z (caller transition time) to fail_run,
            # NOT upper_z (window bound).
            self._safe_fail_run(run_id, lifecycle_z, _classify_for_failure(exc))
            raise
        except Exception:
            # Use lifecycle_z (caller transition time), NOT upper_z.
            self._safe_fail_run(run_id, lifecycle_z, EngineErrorCategory.PROGRAMMER)
            raise

    # -- internal helpers -------------------------------------------

    def _run_after_begin(
        self,
        run_id: str,
        briefing_inputs: Tuple[BriefingInput, ...],
        lower_utc: datetime,
        upper_utc: datetime,
        updated_z: str,
    ) -> BriefingEngineResult:
        included_items: list[EngineIncludedItem] = []
        excluded_items: list[EngineExcludedItem] = []
        seen_before: set = set()

        # Pre-collect candidate IDs for a single shadow-seen lookup.
        ordered_ids: list[str] = []
        candidate_id_set: set = set()
        for bi in briefing_inputs:
            cid = bi.candidate_id
            if cid in candidate_id_set:
                raise ValueError(
                    f"briefing_inputs contains duplicate candidate_id: {cid!r}"
                )
            candidate_id_set.add(cid)
            ordered_ids.append(cid)
        shadow_seen = self._ledger.seen_candidate_ids(tuple(ordered_ids))
        seen_before = set(shadow_seen)

        # Map every decision before shadow-seen filtering so overlap cases
        # still receive the typed eligibility result.
        eligibility_by_id = {
            bi.candidate_id: map_eligibility(bi) for bi in briefing_inputs
        }
        eligible_inputs: list[Tuple[BriefingInput, EligibilityResult]] = []
        for bi in briefing_inputs:
            cid = bi.candidate_id
            eligibility = eligibility_by_id[cid]
            if cid in seen_before:
                excluded_items.append(
                    EngineExcludedItem(
                        briefing_input=bi,
                        reason=EngineExclusionReason.ALREADY_SHADOW_SEEN,
                        typed_detail=eligibility.exclusion_reason,
                    )
                )
                continue
            evaluated_at_dt = _parse_evaluated_at(bi.evaluated_at)
            if not (lower_utc <= evaluated_at_dt < upper_utc):
                excluded_items.append(
                    EngineExcludedItem(
                        briefing_input=bi,
                        reason=EngineExclusionReason.OUT_OF_WINDOW,
                        typed_detail=None,
                    )
                )
                continue
            if not eligibility.included:
                excluded_items.append(
                    EngineExcludedItem(
                        briefing_input=bi,
                        reason=EngineExclusionReason.NON_ELIGIBLE_DECISION,
                        typed_detail=eligibility.exclusion_reason,
                    )
                )
                continue
            eligible_inputs.append((bi, eligibility))

        # Sort by (category-order, evaluated_at_dt, ordinal, candidate_id).
        eligible_inputs.sort(
            key=lambda pair: (
                _CATEGORY_ORDER[pair[0].category],
                _parse_evaluated_at(pair[0].evaluated_at),
                pair[0].ordinal,
                pair[0].candidate_id,
            )
        )

        # Group by category in declaration order.
        groups: Dict[Category, list[BriefingInput]] = {}
        category_order_seen: list[Category] = []
        for bi, _elig in eligible_inputs:
            if bi.category not in groups:
                groups[bi.category] = []
                category_order_seen.append(bi.category)
            groups[bi.category].append(bi)

        # Summarize per category in declaration order; the engine may
        # never exceed one uncached call per category and 8 total.
        any_fallback = False
        for category in category_order_seen:
            items_in_category = groups[category]
            raw_inputs: list[Any] = []
            direct_fallbacks: Dict[str, SummaryItem] = {}
            for bi in items_in_category:
                candidate = bi.event_candidate.candidate
                if _raw_bounds_invalid(bi):
                    direct_fallbacks[bi.candidate_id] = SummaryItem(
                        candidate_id=bi.candidate_id,
                        summary="Input bounds exceeded",
                        source=SummarySource.FALLBACK,
                        error_category=SummarizerErrorCategory.INPUT_BOUNDS,
                    )
                else:
                    raw_inputs.append(self._summarizer_input_for(bi))
            summary_items: Tuple[SummaryItem, ...] = ()
            if raw_inputs:
                summary_result = self._summarizer.summarize_category(
                    category, raw_inputs
                )
                summary_items = tuple(summary_result.items)
                if any(item.source is SummarySource.FALLBACK for item in summary_items):
                    any_fallback = True
            cid_to_summary = {item.candidate_id: item for item in summary_items}
            cid_to_summary.update(direct_fallbacks)
            for bi in items_in_category:
                included_items.append(
                    EngineIncludedItem(
                        briefing_input=bi,
                        summary_item=cid_to_summary[bi.candidate_id],
                    )
                )
            if direct_fallbacks:
                any_fallback = True

        included_items_tuple: Tuple[EngineIncludedItem, ...] = tuple(included_items)
        excluded_items_tuple: Tuple[EngineExcludedItem, ...] = tuple(excluded_items)

        # Build deterministic per-category ordinals then render only valid
        # records; invalid raw inputs remain represented as fallbacks but
        # never reach renderer or shadow ledger contracts.
        render_records: list[RenderRecord] = []
        ordinal_by_id: Dict[str, int] = {}
        per_category_counter: Dict[Category, int] = {}
        for included in included_items_tuple:
            if _raw_bounds_invalid(included.briefing_input):
                continue
            cat = included.briefing_input.category
            per_category_counter[cat] = per_category_counter.get(cat, 0)
            ordinal_by_id[included.briefing_input.candidate_id] = (
                per_category_counter[cat]
            )
            per_category_counter[cat] += 1

        fact_deltas_by_id: Dict[str, Tuple[FactDelta, ...]] = {
            included.briefing_input.candidate_id: tuple(
                included.briefing_input.adjudication.fact_deltas
            )
            for included in included_items_tuple
        }

        for included in included_items_tuple:
            if _raw_bounds_invalid(included.briefing_input):
                continue
            render_records.append(
                _build_render_record(
                    included=included,
                    fact_deltas=fact_deltas_by_id[
                        included.briefing_input.candidate_id
                    ],
                    ordinal=ordinal_by_id[included.briefing_input.candidate_id],
                )
            )

        # Render — renderer overflow is the only path that fails the run.
        render_result: RenderResult = self._renderer.render_briefing(
            render_records, upper_bound_utc=upper_utc
        )

        # Build ShadowEvent list and complete the ledger ONLY after render.
        decision_by_id: Dict[str, SemanticDecision] = {
            included.briefing_input.candidate_id: (
                included.briefing_input.adjudication.semantic_decision
            )
            for included in included_items_tuple
        }
        shadow_events: list[ShadowEvent] = []
        for included in included_items_tuple:
            if _raw_bounds_invalid(included.briefing_input):
                continue
            decision = decision_by_id[included.briefing_input.candidate_id]
            payload = _canonical_payload_for_briefing_input(included, decision.value)
            shadow_events.append(
                ShadowEvent(
                    candidate_id=included.briefing_input.candidate_id,
                    category=included.briefing_input.category,
                    decision=decision,
                    payload_json=payload,
                    recorded_at_utc=updated_z,
                )
            )

        # Empty-eligible path still calls complete_run with zero events; the
        # ledger partition is then ((), ()) and no shadow rows are added.
        self._ledger.complete_run(
            run_id=run_id, updated=updated_z, events=tuple(shadow_events)
        )

        model_calls = int(getattr(self._summarizer, "model_call_count", 0))
        cache_hits = int(getattr(self._summarizer, "cache_hit_count", 0))

        partial = any(
            included.summary_item.source is SummarySource.FALLBACK
            for included in included_items_tuple
        )
        status = EngineRunStatus.PARTIAL if partial else EngineRunStatus.COMPLETED
        error_category = EngineErrorCategory.NONE
        if partial:
            categories = {
                SummarizerErrorCategory.INPUT_BOUNDS: EngineErrorCategory.INPUT_BOUNDS,
                SummarizerErrorCategory.TRANSPORT_ERROR: EngineErrorCategory.TRANSPORT,
                SummarizerErrorCategory.MALFORMED_OUTPUT: EngineErrorCategory.MALFORMED_OUTPUT,
                SummarizerErrorCategory.BUDGET_EXHAUSTED: EngineErrorCategory.BUDGET_EXHAUSTED,
            }
            error_category = next(
                (categories.get(item.summary_item.error_category, EngineErrorCategory.INPUT_BOUNDS)
                 for item in included_items_tuple
                 if item.summary_item.source is SummarySource.FALLBACK),
                EngineErrorCategory.INPUT_BOUNDS,
            )

        # The renderer returns EMPTY when no records; ``chunks`` is ``()``.
        # The renderer's own chunks tuple is preserved byte-identically.
        return BriefingEngineResult(
            run_id=run_id,
            window_lower_utc=lower_utc,
            window_upper_utc=upper_utc,
            status=status,
            included_items=included_items_tuple,
            excluded_items=excluded_items_tuple,
            chunks=render_result.chunks,
            model_call_count=model_calls,
            cache_hit_count=cache_hits,
            error_category=error_category,
        )

    # -- helpers ----------------------------------------------------

    def _summarizer_input_for(self, bi: BriefingInput) -> Any:
        """Build a ``SummarizerInput`` for one ``BriefingInput``.

        Imported lazily so the engine module loads even when the
        summarizer types are unavailable in some test contexts.
        """
        from .briefing_summarizer import SummarizerInput
        candidate = bi.event_candidate.candidate
        return SummarizerInput(
            candidate_id=bi.candidate_id,
            title=candidate.title,
            snippet=candidate.snippet,
            url=bi.url,
        )

    def _safe_fail_run(
        self,
        run_id: str,
        updated_z: str,
        error_category: EngineErrorCategory,
    ) -> None:
        """Best-effort ``fail_run``; never raises.

        This helper is invoked from the single permitted outermost
        ``except Exception`` boundary of ``execute_briefing_run``.
        It must swallow every possible failure of ``fail_run`` so the
        original active exception can be re-raised by the caller.
        Per §2.5 item 4 the only generic ``except Exception`` is at
        the outermost boundary itself; this helper catches only the
        specific failure modes the ledger can surface.
        """
        error_code = _error_code_for(error_category)
        with suppress(BaseException):
            self._ledger.fail_run(
                run_id=run_id, updated=updated_z, error_code=error_code
            )


def _classify_for_failure(exc: BaseException) -> EngineErrorCategory:
    if isinstance(exc, OversizeRecordError):
        return EngineErrorCategory.RENDERER_OVERFLOW
    if isinstance(exc, (LedgerError, LedgerContractError, RunConflictError, RunStateError, LockTimeoutError)):
        return EngineErrorCategory.LEDGER
    return EngineErrorCategory.PROGRAMMER


def _error_code_for(category: EngineErrorCategory) -> str:
    if category is EngineErrorCategory.RENDERER_OVERFLOW:
        return "RENDERER_OVERFLOW"
    if category is EngineErrorCategory.LEDGER:
        return "LEDGER_FAILURE"
    if category is EngineErrorCategory.PROGRAMMER:
        return "PROGRAMMER_EXCEPTION"
    return "ENGINE_FAILURE"


def execute_briefing_run(
    run_id: str,
    briefing_inputs: Tuple[BriefingInput, ...],
    ledger: ShadowLedgerProtocol,
    summarizer: SummarizerProtocol,
    renderer: RendererProtocol,
    as_of_utc: datetime,
    last_completed_upper_utc: Optional[datetime] = None,
) -> BriefingEngineResult:
    """Functional entry point mirroring ``BriefingEngine.execute_briefing_run``.

    The addendum accepts either an object or a functional entry point;
    this wrapper exists so callers can compose the engine without
    instantiating ``BriefingEngine``. It shares the exact same code
    path as the object method.
    """
    engine = BriefingEngine(
        ledger=ledger, summarizer=summarizer, renderer=renderer
    )
    return engine.execute_briefing_run(
        run_id=run_id,
        briefing_inputs=briefing_inputs,
        as_of_utc=as_of_utc,
        last_completed_upper_utc=last_completed_upper_utc,
    )
