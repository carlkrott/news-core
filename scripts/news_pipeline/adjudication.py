"""Deterministic Phase 3 lifecycle and fact adjudication rules."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import unicodedata

from .clustering import score_pair
from .event_contracts import (
    FactDelta,
    InternalRuleDecision,
    RuleVerdict,
    SemanticReasonCode,
)
from .fact_extraction import extract_facts


OLD_RUMOR = (("rumor",), ("rumoured",), ("reportedly",), ("sources", "say"))
NEW_OFFICIAL = (("official",), ("officially", "confirmed"), ("confirmed",), ("announced",))
OLD_ANNOUNCED = (("announced",), ("announcement",))
OLD_PREORDER = (("preorder",), ("pre", "order"), ("preorders", "open"))
NEW_LAUNCHED = (("launched",), ("released",))
NEW_SHIPPED_GA = (("shipped",), ("shipping",), ("generally", "available"), ("available", "now"))
CORRECTION = (("correction",), ("corrected",), ("error", "corrected"))
RETRACTION = (("retracted",), ("retraction",), ("withdrawn",))
REVERSAL = (("reversed",), ("reversal",), ("overturned",))
NEGATIONS = ("no", "not", "never", "denies", "denied")
_STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "but", "is", "are", "was", "were",
    "in", "on", "at", "to", "for", "of", "with",
})

_FAMILIES = {
    "old_rumor": OLD_RUMOR,
    "new_official": NEW_OFFICIAL,
    "old_announced": OLD_ANNOUNCED,
    "old_preorder": OLD_PREORDER,
    "new_launched": NEW_LAUNCHED,
    "new_shipped_ga": NEW_SHIPPED_GA,
    "correction": CORRECTION,
    "retraction": RETRACTION,
    "reversal": REVERSAL,
}


def _phrase_tokens(text: str) -> tuple[str, ...]:
    """Normalize to an ordered token stream, unlike clustering's sorted set."""
    text = unicodedata.normalize("NFKC", text).casefold()
    chars = []
    for ch in text:
        category = unicodedata.category(ch)
        chars.append(ch if category.startswith("L") or category.startswith("N") else " ")
    return tuple(
        token for token in "".join(chars).split()
        if (len(token) >= 2 or token.isdigit()) and token not in _STOPWORDS
    )


def _family_hits(text: str) -> dict[str, tuple[bool, ...]]:
    tokens = _phrase_tokens(text)
    hits: dict[str, list[bool]] = {name: [] for name in _FAMILIES}
    for family, phrases in _FAMILIES.items():
        for phrase in phrases:
            width = len(phrase)
            for index in range(len(tokens) - width + 1):
                if tokens[index:index + width] != phrase:
                    continue
                previous = tokens[max(0, index - 3):index]
                hits[family].append(not any(token in NEGATIONS for token in previous))
    return {family: tuple(values) for family, values in hits.items()}


def _has_positive(hits: dict[str, tuple[bool, ...]], family: str) -> bool:
    return any(hits[family])


def _has_negative(hits: dict[str, tuple[bool, ...]], family: str) -> bool:
    return any(not value for value in hits[family])


def _has_conflicting_family(hits: dict[str, tuple[bool, ...]]) -> bool:
    return any(_has_positive(hits, family) and _has_negative(hits, family) for family in hits)


def _has_any_lifecycle_mention(hits: dict[str, tuple[bool, ...]]) -> bool:
    """Return true for any lifecycle mention, including a negated mention."""
    return any(hits[family] for family in ("old_rumor", "new_official", "old_announced", "old_preorder", "new_launched", "new_shipped_ga"))


def _has_incompatible_lifecycle(hits: dict[str, tuple[bool, ...]]) -> bool:
    old = any(_has_positive(hits, family) for family in ("old_rumor", "old_announced", "old_preorder"))
    new = any(_has_positive(hits, family) for family in ("new_official", "new_launched", "new_shipped_ga"))
    return old and new


def _fact_values(facts) -> dict[tuple[object, str], list[str]]:
    output: dict[tuple[object, str], list[str]] = {}
    for field in ("prices", "versions", "dates", "percentages", "counts"):
        for fact in getattr(facts, field):
            output.setdefault((fact.kind, fact.unit), []).append(fact.value_normalized)
    return output


def _fact_deltas(candidate_facts, history_facts, topic_gate: Decimal) -> tuple[tuple[FactDelta, ...], bool]:
    candidate_values = _fact_values(candidate_facts)
    history_values = _fact_values(history_facts)
    deltas: list[FactDelta] = []
    conflict = False
    for key in sorted(set(candidate_values) | set(history_values), key=lambda item: (item[0].value, item[1])):
        old_values = sorted(set(history_values.get(key, [])))
        new_values = sorted(set(candidate_values.get(key, [])))
        if len(old_values) > 1 or len(new_values) > 1:
            if set(old_values) != set(new_values) or len(old_values) > 1 or len(new_values) > 1:
                conflict = True
            continue
        if len(old_values) == 1 and len(new_values) == 1 and old_values[0] != new_values[0]:
            deltas.append(FactDelta(kind=key[0], unit=key[1], old_value=old_values[0], new_value=new_values[0], topic_gate=topic_gate))
    return tuple(deltas), conflict


def classify_pair(candidate, history, scored: "object") -> RuleVerdict:
    """Apply the addendum's six-rule precedence chain to one scored pair."""
    candidate_text = candidate.title + " " + candidate.snippet
    history_text = history.title + " " + history.snippet
    candidate_hits = _family_hits(candidate_text)
    history_hits = _family_hits(history_text)
    topic_gate = scored.title_score
    conflict = _has_conflicting_family(candidate_hits) or _has_incompatible_lifecycle(candidate_hits)
    if conflict:
        return RuleVerdict(InternalRuleDecision.model_required, (SemanticReasonCode.UNRESOLVED_CONFLICT,), ())

    correction_families = ("correction", "retraction", "reversal")
    if topic_gate >= Decimal("0.500000") and any(_has_positive(candidate_hits, family) and not _has_positive(history_hits, family) for family in correction_families):
        return RuleVerdict(InternalRuleDecision.material_update, (SemanticReasonCode.CORRECTION_OR_RETRACTION,), ())

    if topic_gate >= Decimal("0.500000") and _has_positive(history_hits, "old_rumor") and _has_positive(candidate_hits, "new_official"):
        return RuleVerdict(InternalRuleDecision.material_update, (SemanticReasonCode.CONFIRMED_RUMOR,), ())

    old_advance = any(_has_positive(history_hits, family) for family in ("old_announced", "old_preorder"))
    new_advance = any(_has_positive(candidate_hits, family) for family in ("new_launched", "new_shipped_ga"))
    if topic_gate >= Decimal("0.500000") and old_advance and new_advance:
        return RuleVerdict(InternalRuleDecision.material_update, (SemanticReasonCode.LAUNCHED_OR_SHIPPED,), ())

    candidate_facts = extract_facts(candidate.title, candidate.snippet)
    history_facts = extract_facts(history.title, history.snippet)
    deltas, fact_conflict = _fact_deltas(candidate_facts, history_facts, topic_gate)
    if topic_gate >= Decimal("0.500000") and fact_conflict:
        return RuleVerdict(InternalRuleDecision.model_required, (SemanticReasonCode.UNRESOLVED_CONFLICT,), deltas)
    if topic_gate >= Decimal("0.500000") and len(deltas) == 1:
        return RuleVerdict(InternalRuleDecision.material_update, (SemanticReasonCode.NUMERIC_REVISION,), deltas)

    has_lifecycle_or_correction = any(_has_any_lifecycle_mention(hits) or any(hits[f] for f in ("correction", "retraction", "reversal")) for hits in (candidate_hits, history_hits))
    if scored.combined_score >= Decimal("0.950000") and candidate_facts == history_facts and not has_lifecycle_or_correction:
        return RuleVerdict(InternalRuleDecision.rewrite, (SemanticReasonCode.SAME_FACTS,), ())
    return RuleVerdict(InternalRuleDecision.model_required, (SemanticReasonCode.MODEL_REQUIRED,), deltas)
