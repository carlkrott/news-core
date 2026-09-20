"""Phase 3 deterministic dry-run adjudication — contract types.

These dataclasses and enums are the surface every other module in
``news_pipeline.{clustering,adjudication,phase3_api}`` consumes. Strictly
frozen/slotted with tuple-only sequence fields. The phase-2 ``DecisionCode``
and ``ReasonCode`` are re-used verbatim to keep ordinals/dumps identical.

The module is stdlib-only. No time, no DB, no network. The
``map_phase2_to_semantic`` table is the source of truth for any caller
mapping Phase 2 outputs to Phase 3 ``SemanticDecision`` /
``SemanticReasonCode``.
"""
from __future__ import annotations

from dataclasses import InitVar, dataclass
from decimal import Decimal
from enum import Enum
from typing import Tuple, Optional

from .contracts import DecisionCode as Phase2DecisionCode
from .contracts import ReasonCode
from .models import Category
from .policies import QueryPolicy


class FactKind(str, Enum):
    PRICE = "price"
    VERSION = "version"
    DATE = "date"
    PERCENT = "percent"
    COUNT = "count"


class SemanticDecision(str, Enum):
    distinct_event = "distinct_event"
    material_update = "material_update"
    rewrite = "rewrite"
    bypass_phase2_terminal = "bypass_phase2_terminal"
    pending_review = "pending_review"
    pending_model_error = "pending_model_error"


class InternalRuleDecision(str, Enum):
    material_update = "material_update"
    rewrite = "rewrite"
    model_required = "model_required"


class ModelDecision(str, Enum):
    MATERIAL_UPDATE = "MATERIAL_UPDATE"
    REWRITE = "REWRITE"
    UNCERTAIN = "UNCERTAIN"


class ModelErrorCategory(str, Enum):
    MALFORMED_OUTPUT = "malformed_output"
    TRANSPORT_ERROR = "transport_error"


class SemanticReasonCode(str, Enum):
    CONFIRMED_RUMOR = "CONFIRMED_RUMOR"
    LAUNCHED_OR_SHIPPED = "LAUNCHED_OR_SHIPPED"
    NUMERIC_REVISION = "NUMERIC_REVISION"
    CORRECTION_OR_RETRACTION = "CORRECTION_OR_RETRACTION"
    SAME_FACTS = "SAME_FACTS"
    UNRESOLVED_CONFLICT = "UNRESOLVED_CONFLICT"
    PHASE2_SUPPRESSED_OR_DROPPED = "PHASE2_SUPPRESSED_OR_DROPPED"
    PHASE2_MISSING_EVIDENCE = "PHASE2_MISSING_EVIDENCE"
    PHASE2_INVALID_EVIDENCE = "PHASE2_INVALID_EVIDENCE"
    PHASE2_HISTORY_UNAVAILABLE = "PHASE2_HISTORY_UNAVAILABLE"
    MISSING_HISTORY_MATCH = "MISSING_HISTORY_MATCH"
    MODEL_REQUIRED = "MODEL_REQUIRED"
    MALFORMED_MODEL_OUTPUT = "MALFORMED_MODEL_OUTPUT"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    DISTINCT_EVENT = "DISTINCT_EVENT"


# Public Phase 2 -> Phase 3 mapping.  Every Phase2DecisionCode MUST appear.
_PHASE2_TO_SEMANTIC: dict[Phase2DecisionCode, tuple[SemanticDecision, SemanticReasonCode]] = {
    Phase2DecisionCode.KEEP: (
        SemanticDecision.distinct_event,  # KEEP -> retrieval; default if no match
        SemanticReasonCode.DISTINCT_EVENT,
    ),
    Phase2DecisionCode.SUPPRESS_BATCH_EXACT: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.SUPPRESS_EXACT_URL: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.SUPPRESS_EXACT_IDENTITY: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.SUPPRESS_RECENT_TITLE: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.DROP_STALE: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.DROP_BLOCKED_SOURCE: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.DROP_NON_ARTICLE_URL: (
        SemanticDecision.bypass_phase2_terminal,
        SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,
    ),
    Phase2DecisionCode.PENDING_MISSING_EVIDENCE: (
        SemanticDecision.pending_review,
        SemanticReasonCode.PHASE2_MISSING_EVIDENCE,
    ),
    Phase2DecisionCode.PENDING_INVALID_EVIDENCE: (
        SemanticDecision.pending_review,
        SemanticReasonCode.PHASE2_INVALID_EVIDENCE,
    ),
    Phase2DecisionCode.PENDING_HISTORY_UNAVAILABLE: (
        SemanticDecision.pending_review,
        SemanticReasonCode.PHASE2_HISTORY_UNAVAILABLE,
    ),
    Phase2DecisionCode.PENDING_POSSIBLE_UPDATE: (
        SemanticDecision.pending_review,
        SemanticReasonCode.MISSING_HISTORY_MATCH,  # default; resolved by API
    ),
}


def map_phase2_to_semantic(
    decision: Phase2DecisionCode,
) -> tuple[SemanticDecision, SemanticReasonCode]:
    """Deterministic mapping for terminal Phase 2 decisions.

    PENDING_POSSIBLE_UPDATE returns ``pending_review`` here; the API layer
    resolves it to rule/model verdicts and overwrites this default when every
    referenced article ID resolves.
    """
    if decision not in _PHASE2_TO_SEMANTIC:
        raise ValueError(f"no Phase 3 mapping for Phase 2 decision: {decision!r}")
    return _PHASE2_TO_SEMANTIC[decision]


def all_phase2_decisions() -> tuple[Phase2DecisionCode, ...]:
    """All Phase 2 decision members (used by tests to spot missing mappings)."""
    return tuple(Phase2DecisionCode)


def ordered_unique_reasons(
    reasons: Tuple[SemanticReasonCode, ...],
) -> Tuple[SemanticReasonCode, ...]:
    """Return ``reasons`` deduplicated by first occurrence.

    Only ``SemanticReasonCode`` entries are accepted. Strings or other
    enums raise ValueError.
    """
    out: list[SemanticReasonCode] = []
    for r in reasons:
        if not isinstance(r, SemanticReasonCode):
            raise ValueError(
                f"ordered_unique_reasons only accepts SemanticReasonCode, got {r!r}"
            )
        if r not in out:
            out.append(r)
    return tuple(out)


@dataclass(frozen=True, slots=True)
class TypedFact:
    kind: FactKind
    unit: str
    value_normalized: str
    context: Tuple[str, ...]
    evidence: str


@dataclass(frozen=True, slots=True)
class ExtractedFacts:
    prices: Tuple[TypedFact, ...]
    versions: Tuple[TypedFact, ...]
    dates: Tuple[TypedFact, ...]
    percentages: Tuple[TypedFact, ...]
    counts: Tuple[TypedFact, ...]

    def __post_init__(self) -> None:
        for field_name in ("prices", "versions", "dates", "percentages", "counts"):
            value = getattr(self, field_name)
            if value is None:
                raise ValueError(f"{field_name} must not be None")
            if not isinstance(value, tuple):
                raise ValueError(f"{field_name} must be a tuple")
            for fact in value:
                if not isinstance(fact, TypedFact):
                    raise ValueError(
                        f"{field_name} contains non-TypedFact entry: {fact!r}"
                    )


@dataclass(frozen=True, slots=True)
class FactDelta:
    kind: FactKind
    unit: str
    old_value: str
    new_value: str
    topic_gate: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.topic_gate, Decimal):
            raise ValueError(
                f"FactDelta.topic_gate must be Decimal, got {type(self.topic_gate)!r}"
            )
        if not self.topic_gate.is_finite() or not Decimal("0") <= self.topic_gate <= Decimal("1"):
            raise ValueError("FactDelta.topic_gate must be finite and in [0,1]")


@dataclass(frozen=True, slots=True)
class EventCandidate:
    candidate: object  # CandidateArticle (avoid import cycle comment-only)
    filter_result: object  # FilterResult
    query_policy: QueryPolicy

    def __post_init__(self) -> None:
        # Imports kept local to avoid module-level cycle pressure.
        from .contracts import CandidateArticle, FilterResult

        if not isinstance(self.candidate, CandidateArticle):
            raise ValueError(
                f"EventCandidate.candidate must be CandidateArticle, got {type(self.candidate)!r}"
            )
        if not isinstance(self.filter_result, FilterResult):
            raise ValueError(
                f"EventCandidate.filter_result must be FilterResult, got {type(self.filter_result)!r}"
            )
        if not isinstance(self.query_policy, QueryPolicy):
            raise ValueError(
                f"EventCandidate.query_policy must be QueryPolicy, got {type(self.query_policy)!r}"
            )
        if not getattr(self.candidate, "candidate_id", ""):
            raise ValueError("EventCandidate.candidate.candidate_id must be non-empty")
        if self.filter_result.candidate != self.candidate:
            raise ValueError("filter_result.candidate must equal candidate")
        if self.query_policy.category is not self.candidate.category:
            raise ValueError("query_policy.category must equal candidate.category")
        if self.filter_result.ordinal < 0:
            raise ValueError("filter_result.ordinal must be non-negative")


@dataclass(frozen=True, slots=True)
class ScoredHistoryMatch:
    match: object  # HistoryMatch
    title_score: Decimal
    snippet_score: Decimal
    combined_score: Decimal
    exact_url: bool

    def __post_init__(self) -> None:
        from .contracts import HistoryMatch

        if not isinstance(self.match, HistoryMatch):
            raise ValueError(
                f"ScoredHistoryMatch.match must be HistoryMatch, got {type(self.match)!r}"
            )
        for name in ("title_score", "snippet_score", "combined_score"):
            if not isinstance(getattr(self, name), Decimal):
                raise ValueError(
                    f"ScoredHistoryMatch.{name} must be Decimal"
                )


@dataclass(frozen=True, slots=True)
class RuleVerdict:
    decision: InternalRuleDecision
    reasons: Tuple[SemanticReasonCode, ...]
    fact_deltas: Tuple[FactDelta, ...]

    def __post_init__(self) -> None:
        for reason in self.reasons:
            if not isinstance(reason, SemanticReasonCode):
                raise ValueError(
                    f"RuleVerdict.reasons must be SemanticReasonCode, got {reason!r}"
                )
        for fd in self.fact_deltas:
            if not isinstance(fd, FactDelta):
                raise ValueError(
                    f"RuleVerdict.fact_deltas must be FactDelta, got {fd!r}"
                )


@dataclass(frozen=True, slots=True)
class ModelRequest:
    instruction: str
    candidate_title: str
    candidate_snippet: str
    candidate_url: str | None
    candidate_category: str
    candidate_published_at: str | None
    candidate_evaluated_at: str
    history_title: str
    history_snippet: str
    history_url: str | None
    history_category: str
    history_occurred_at: str

    def __post_init__(self) -> None:
        if not isinstance(self.instruction, str) or not self.instruction:
            raise ValueError("ModelRequest.instruction must be a non-empty string")
        if not isinstance(self.candidate_url, (str, type(None))):
            raise ValueError("ModelRequest.candidate_url must be str or None")
        if not isinstance(self.history_url, (str, type(None))):
            raise ValueError("ModelRequest.history_url must be str or None")


@dataclass(frozen=True, slots=True)
class ParsedModelVerdict:
    decision: ModelDecision
    confidence: float
    reason: str
    facts: Tuple[str, ...]

    def __post_init__(self) -> None:
        import math

        if not isinstance(self.confidence, float) or isinstance(self.confidence, bool):
            raise ValueError("ParsedModelVerdict.confidence must be float (not bool)")
        if math.isnan(self.confidence) or math.isinf(self.confidence):
            raise ValueError("ParsedModelVerdict.confidence must be finite")
        if self.confidence < 0.0 or self.confidence > 1.0:
            raise ValueError("ParsedModelVerdict.confidence must be in [0,1]")
        if not isinstance(self.reason, str) or not (1 <= len(self.reason) <= 512):
            raise ValueError(
                "ParsedModelVerdict.reason must be a string of length 1..512"
            )
        if not isinstance(self.facts, tuple):
            raise ValueError("ParsedModelVerdict.facts must be a tuple")
        for f in self.facts:
            if not isinstance(f, str) or not (1 <= len(f) <= 256):
                raise ValueError(
                    "ParsedModelVerdict.facts entries must be strings of length 1..256"
                )


@dataclass(frozen=True, slots=True)
class CachedModelOutcome:
    final_decision: SemanticDecision
    reasons: Tuple[SemanticReasonCode, ...]
    confidence: float | None
    error_category: ModelErrorCategory | None
    facts: Tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AdjudicationResult:
    candidate_id: str
    semantic_decision: SemanticDecision
    phase2_decision: Phase2DecisionCode
    phase2_reasons: Tuple[ReasonCode, ...]
    semantic_reasons: Tuple[SemanticReasonCode, ...]
    cluster_id: Optional[str]
    matched_candidate_ids: Tuple[str, ...]
    matched_history_ids: Tuple[str, ...]
    matched_observation_ids: Tuple[str, ...]
    fact_deltas: Tuple[FactDelta, ...]
    model_used: bool
    model_confidence: Optional[float]
    model_error_category: Optional[ModelErrorCategory]
    ordinal: int
    source_ordinal: InitVar[int | None] = None

    def __post_init__(self, source_ordinal: int | None) -> None:
        import math

        if not self.candidate_id:
            raise ValueError("AdjudicationResult.candidate_id must be non-empty")
        if type(self.ordinal) is not int or self.ordinal < 0:
            raise ValueError("AdjudicationResult.ordinal must be a non-negative int")
        if source_ordinal is not None:
            if type(source_ordinal) is not int or source_ordinal < 0:
                raise ValueError("AdjudicationResult.source_ordinal must be a non-negative int")
            if self.ordinal != source_ordinal:
                raise ValueError("AdjudicationResult.ordinal must equal source filter ordinal")
        if type(self.model_used) is not bool:
            raise ValueError("AdjudicationResult.model_used must be bool")
        if not self.model_used and self.model_confidence is not None:
            raise ValueError("model_used=False requires model_confidence=None")
        for r in self.phase2_reasons:
            if not isinstance(r, ReasonCode):
                raise ValueError(
                    f"AdjudicationResult.phase2_reasons only accepts ReasonCode, got {r!r}"
                )
        for r in self.semantic_reasons:
            if not isinstance(r, SemanticReasonCode):
                raise ValueError(
                    f"AdjudicationResult.semantic_reasons only accepts SemanticReasonCode, got {r!r}"
                )
        for cid in self.matched_candidate_ids:
            if not isinstance(cid, str) or not cid:
                raise ValueError(
                    f"AdjudicationResult.matched_candidate_ids contains bad entry: {cid!r}"
                )
        for hid in self.matched_history_ids:
            if not isinstance(hid, str) or not hid:
                raise ValueError(
                    f"AdjudicationResult.matched_history_ids contains bad entry: {hid!r}"
                )
        for oid in self.matched_observation_ids:
            if not isinstance(oid, str) or not oid:
                raise ValueError(
                    f"AdjudicationResult.matched_observation_ids contains bad entry: {oid!r}"
                )
        for fd in self.fact_deltas:
            if not isinstance(fd, FactDelta):
                raise ValueError(
                    f"AdjudicationResult.fact_deltas only accepts FactDelta, got {fd!r}"
                )
        # Reasons already deduped in first-occurrence order.
        if tuple(self.semantic_reasons) != ordered_unique_reasons(self.semantic_reasons):
            raise ValueError(
                "AdjudicationResult.semantic_reasons must be first-occurrence-unique"
            )
        if self.model_confidence is not None:
            mc = self.model_confidence
            if isinstance(mc, bool) or not isinstance(mc, float):
                raise ValueError(
                    "AdjudicationResult.model_confidence must be a finite float in [0,1] or None"
                )
            if math.isnan(mc) or math.isinf(mc) or mc < 0.0 or mc > 1.0:
                raise ValueError(
                    "AdjudicationResult.model_confidence must be finite and in [0,1]"
                )
        # Invariant: pending_model_error requires a category; everything else
        # forbids one.
        if self.semantic_decision is SemanticDecision.pending_model_error:
            if not self.model_used:
                raise ValueError("pending_model_error requires model_used=True")
            if self.model_error_category is None:
                raise ValueError(
                    "pending_model_error requires model_error_category"
                )
        else:
            if self.model_error_category is not None:
                raise ValueError(
                    f"{self.semantic_decision!r} forbids model_error_category"
                )
