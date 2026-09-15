"""Immutable Phase 3 novelty and query-yield accounting."""
from __future__ import annotations

from dataclasses import dataclass, replace

from .contracts import DecisionCode
from .event_contracts import SemanticDecision

_TERMINAL_SUCCESS = frozenset({"success", "partial"})
_TERMINAL_FAILURE = frozenset({"failed", "rate_limited"})
_DUPLICATE_DECISIONS = frozenset(
    {
        DecisionCode.SUPPRESS_EXACT_URL.value,
        DecisionCode.SUPPRESS_EXACT_IDENTITY.value,
        DecisionCode.SUPPRESS_RECENT_TITLE.value,
        DecisionCode.SUPPRESS_BATCH_EXACT.value,
    }
)


@dataclass(frozen=True, slots=True)
class AttemptYield:
    category: str
    status: str
    returned_count: int = 0
    duplicate_count: int = 0

    def __post_init__(self) -> None:
        if type(self.category) is not str or not self.category:
            raise ValueError("category must be a non-empty string")
        if self.status not in _TERMINAL_SUCCESS | _TERMINAL_FAILURE:
            raise ValueError("attempt status must be terminal")
        for name in ("returned_count", "duplicate_count"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative int")


@dataclass(frozen=True, slots=True)
class NoveltyRecord:
    category: str
    source_item_id: str
    decision: str
    semantic_decision: str | None
    source_id: str | None = None
    title: str = ""

    def __post_init__(self) -> None:
        if type(self.category) is not str or not self.category:
            raise ValueError("category must be a non-empty string")
        if type(self.source_item_id) is not str or not self.source_item_id:
            raise ValueError("source_item_id must be a non-empty string")


@dataclass(frozen=True, slots=True)
class CategoryYield:
    category: str
    completed_rounds: int = 0
    returned_count: int = 0
    processed_count: int = 0
    ingest_duplicate_count: int = 0
    stale_count: int = 0
    rewrite_count: int = 0
    material_update_count: int = 0
    distinct_event_count: int = 0
    pending_count: int = 0
    transport_failure_count: int = 0

    def __post_init__(self) -> None:
        if type(self.category) is not str or not self.category:
            raise ValueError("category must be a non-empty string")
        for name in self.__dataclass_fields__:
            if name == "category":
                continue
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative int")

    @property
    def novel_count(self) -> int:
        return self.distinct_event_count + self.material_update_count

    def needs_expansion(self, minimum_novelty_target: int = 1) -> bool:
        if type(minimum_novelty_target) is not int or minimum_novelty_target < 0:
            raise ValueError("minimum_novelty_target must be a non-negative int")
        return self.completed_rounds > 0 and self.novel_count < minimum_novelty_target


def _increment(summary: CategoryYield, field: str, amount: int = 1) -> CategoryYield:
    return replace(summary, **{field: getattr(summary, field) + amount})


def summarize_yield(
    records: tuple[NoveltyRecord, ...] | list[NoveltyRecord],
    attempts: tuple[AttemptYield, ...] | list[AttemptYield] = (),
) -> tuple[CategoryYield, ...]:
    """Merge attributable attempt counts with deterministic item decisions by category."""
    groups: dict[str, CategoryYield] = {}
    for attempt in attempts:
        summary = groups.setdefault(attempt.category, CategoryYield(attempt.category))
        summary = _increment(summary, "returned_count", attempt.returned_count)
        summary = _increment(summary, "ingest_duplicate_count", attempt.duplicate_count)
        if attempt.status in _TERMINAL_SUCCESS:
            summary = _increment(summary, "completed_rounds")
        else:
            summary = _increment(summary, "transport_failure_count")
        groups[attempt.category] = summary

    for record in records:
        summary = groups.setdefault(record.category, CategoryYield(record.category))
        summary = _increment(summary, "processed_count")
        if record.decision in _DUPLICATE_DECISIONS:
            summary = _increment(summary, "ingest_duplicate_count")
        if record.decision == DecisionCode.DROP_STALE.value:
            summary = _increment(summary, "stale_count")
        if record.semantic_decision == SemanticDecision.rewrite.value:
            summary = _increment(summary, "rewrite_count")
        elif record.semantic_decision == SemanticDecision.material_update.value:
            summary = _increment(summary, "material_update_count")
        elif record.semantic_decision == SemanticDecision.distinct_event.value:
            summary = _increment(summary, "distinct_event_count")
        elif record.semantic_decision in {
            SemanticDecision.pending_review.value,
            SemanticDecision.pending_model_error.value,
        }:
            summary = _increment(summary, "pending_count")
        groups[record.category] = summary

    return tuple(groups[category] for category in sorted(groups))


def category_yield(
    records: tuple[NoveltyRecord, ...] | list[NoveltyRecord],
    attempts: tuple[AttemptYield, ...] | list[AttemptYield] = (),
) -> tuple[CategoryYield, ...]:
    return summarize_yield(records, attempts)
