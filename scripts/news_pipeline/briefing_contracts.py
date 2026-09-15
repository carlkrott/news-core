"""Phase 4 — Slice 1 contracts.

Singular ``BriefingInput`` and the eligibility / morning-window helpers
required by every later Phase 4 slice. Phase 4 supervisor addendum §2.1,
§2.2, §2.5 is the only normative source. Stdlib + existing
``news_pipeline`` modules only.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from .event_contracts import (
    AdjudicationResult,
    EventCandidate,
    SemanticDecision,
    SemanticReasonCode,
)
from .models import Category


# Phase 4 / Phase 5 must not flip this flag.
_DRY_RUN: bool = True


_LOCAL_TZ_NAME = "Europe/London"
_LOCAL_TZ = ZoneInfo(_LOCAL_TZ_NAME)


# ---------------------------------------------------------------------------
# BriefingInput
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BriefingInput:
    """One paired ``(EventCandidate, AdjudicationResult)`` row.

    The Phase 4 supervisor addendum §2.1 names three structural invariants:

    1. ``adjudication.candidate_id == event_candidate.candidate.candidate_id``.
    2. ``adjudication.ordinal == event_candidate.filter_result.ordinal``.
    3. The supplied objects are stored by reference — no copy, no slice, no
       mutation. Read-only exact properties expose candidate fields.

    All six ``SemanticDecision`` members are accepted; exclusion is the
    ``map_eligibility`` mapper's job, not this constructor's.
    """

    event_candidate: EventCandidate
    adjudication: AdjudicationResult

    def __post_init__(self) -> None:
        if not isinstance(self.event_candidate, EventCandidate):
            raise TypeError(
                "BriefingInput.event_candidate must be EventCandidate, got "
                f"{type(self.event_candidate)!r}"
            )
        if not isinstance(self.adjudication, AdjudicationResult):
            raise TypeError(
                "BriefingInput.adjudication must be AdjudicationResult, got "
                f"{type(self.adjudication)!r}"
            )
        # Candidate-ID equality across the pair.
        event_cid = self.event_candidate.candidate.candidate_id
        adj_cid = self.adjudication.candidate_id
        if event_cid != adj_cid:
            raise ValueError(
                "BriefingInput candidate_id mismatch: "
                f"event={event_cid!r} adjudication={adj_cid!r}"
            )
        # Ordinal equality across the pair. AdjudicationResult already
        # rejects non-int / negative ordinals; the filter ordinal must
        # match exactly.
        ev_ord = self.event_candidate.filter_result.ordinal
        adj_ord = self.adjudication.ordinal
        if type(ev_ord) is not int or ev_ord < 0:
            raise ValueError(
                "BriefingInput.event_candidate.filter_result.ordinal must be a "
                f"non-negative int, got {ev_ord!r}"
            )
        if ev_ord != adj_ord:
            raise ValueError(
                "BriefingInput ordinal mismatch: "
                f"event={ev_ord} adjudication={adj_ord}"
            )

    # -- Read-only exact properties --------------------------------------
    # Properties never copy, slice, or rewrite. They return the exact
    # underlying value — including identity for mutable internals — so
    # downstream code (and tests) can prove no truncation.

    @property
    def candidate_id(self) -> str:
        return self.event_candidate.candidate.candidate_id

    @property
    def category(self) -> Category:
        return self.event_candidate.candidate.category

    @property
    def title(self) -> str:
        return self.event_candidate.candidate.title

    @property
    def snippet(self) -> str:
        return self.event_candidate.candidate.snippet

    @property
    def url(self) -> Optional[str]:
        return self.event_candidate.candidate.canonical_url

    @property
    def evaluated_at(self) -> str:
        return self.event_candidate.candidate.evaluated_at

    @property
    def ordinal(self) -> int:
        return self.event_candidate.filter_result.ordinal


# ---------------------------------------------------------------------------
# EligibilityResult + map_eligibility
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class EligibilityResult:
    """Frozen inclusion / exclusion verdict for one ``BriefingInput``.

    Exactly one of ``inclusion_reason`` and ``exclusion_reason`` must be set
    (the §2.1 XOR). The ``included`` / ``excluded`` flags must agree with
    the chosen reason. Cross-enum reason values raise ``ValueError``. The
    result references the exact ``BriefingInput`` instance — no copy.
    """

    briefing_input: BriefingInput
    included: bool
    excluded: bool
    inclusion_reason: Optional[SemanticReasonCode]
    exclusion_reason: Optional[SemanticReasonCode]

    def __post_init__(self) -> None:
        if not isinstance(self.briefing_input, BriefingInput):
            raise TypeError(
                "EligibilityResult.briefing_input must be BriefingInput, got "
                f"{type(self.briefing_input)!r}"
            )
        if type(self.included) is not bool or type(self.excluded) is not bool:
            raise TypeError(
                "EligibilityResult.included / excluded must be bool, got "
                f"included={self.included!r} excluded={self.excluded!r}"
            )
        has_inc = self.inclusion_reason is not None
        has_exc = self.exclusion_reason is not None
        if has_inc == has_exc:
            raise ValueError(
                "EligibilityResult requires exactly one of inclusion_reason / "
                "exclusion_reason, got inclusion_reason="
                f"{self.inclusion_reason!r} exclusion_reason={self.exclusion_reason!r}"
            )
        chosen = self.inclusion_reason if has_inc else self.exclusion_reason
        if not isinstance(chosen, SemanticReasonCode):
            raise ValueError(
                "EligibilityResult reason must be SemanticReasonCode, got "
                f"{chosen!r}"
            )
        if self.included != has_inc or self.excluded != has_exc:
            raise ValueError(
                "EligibilityResult included/excluded flags must agree with the "
                f"presence of inclusion_reason / exclusion_reason "
                f"(included={self.included}, excluded={self.excluded}, "
                f"inclusion_reason={self.inclusion_reason!r}, "
                f"exclusion_reason={self.exclusion_reason!r})"
            )
        decision = self.briefing_input.adjudication.semantic_decision
        should_include = decision in (
            SemanticDecision.distinct_event,
            SemanticDecision.material_update,
        )
        if self.included is not should_include or self.excluded is should_include:
            raise ValueError(
                "EligibilityResult flags disagree with semantic_decision: "
                f"decision={decision!r} included={self.included} excluded={self.excluded}"
            )
        if chosen not in self.briefing_input.adjudication.semantic_reasons:
            raise ValueError(
                "EligibilityResult reason must be present in the paired adjudication: "
                f"reason={chosen!r} semantic_reasons="
                f"{self.briefing_input.adjudication.semantic_reasons!r}"
            )


def _first_reason(inp: BriefingInput) -> SemanticReasonCode:
    """Return the first ``semantic_reasons`` entry from the adjudication.

    ``AdjudicationResult`` always enforces a non-empty tuple of
    ``SemanticReasonCode`` values, so the first entry is always valid.
    """
    reasons = inp.adjudication.semantic_reasons
    if not reasons:
        # Defensive: the result XOR check requires a real reason, and a
        # future caller could construct a non-validated tuple; never let
        # the result constructor swallow a programmer bug.
        raise ValueError(
            "map_eligibility requires non-empty adjudication.semantic_reasons"
        )
    return reasons[0]


def _include(inp: BriefingInput) -> EligibilityResult:
    return EligibilityResult(
        briefing_input=inp,
        included=True,
        excluded=False,
        inclusion_reason=_first_reason(inp),
        exclusion_reason=None,
    )


def _exclude(inp: BriefingInput) -> EligibilityResult:
    return EligibilityResult(
        briefing_input=inp,
        included=False,
        excluded=True,
        inclusion_reason=None,
        exclusion_reason=_first_reason(inp),
    )


def map_eligibility(inp: BriefingInput) -> EligibilityResult:
    """Map one ``BriefingInput`` to a frozen eligibility row.

    Exhaustive over the six ``SemanticDecision`` members. Non-enum / unknown
    decisions raise ``TypeError`` (per §2.5 #3). A future addition of a new
    ``SemanticDecision`` member must be wired in here or this function
    fails fast with ``TypeError`` rather than silently including.
    """
    if not isinstance(inp, BriefingInput):
        raise TypeError(
            f"map_eligibility requires BriefingInput, got {type(inp)!r}"
        )
    decision = inp.adjudication.semantic_decision
    if not isinstance(decision, SemanticDecision):
        raise TypeError(
            f"map_eligibility decision must be SemanticDecision, got "
            f"{decision!r} (type={type(decision).__name__})"
        )
    if decision is SemanticDecision.distinct_event:
        return _include(inp)
    if decision is SemanticDecision.material_update:
        return _include(inp)
    if decision is SemanticDecision.rewrite:
        return _exclude(inp)
    if decision is SemanticDecision.bypass_phase2_terminal:
        return _exclude(inp)
    if decision is SemanticDecision.pending_review:
        return _exclude(inp)
    if decision is SemanticDecision.pending_model_error:
        return _exclude(inp)
    # Defensive: a future ``SemanticDecision`` member that is not handled
    # must surface as a programmer contract failure.
    raise TypeError(
        f"map_eligibility reached an unhandled SemanticDecision member: {decision!r}"
    )


# ---------------------------------------------------------------------------
# compute_morning_window
# ---------------------------------------------------------------------------


def _require_aware_utc(value: datetime, name: str) -> datetime:
    """Coerce ``value`` to aware UTC; reject naive and non-UTC offsets."""
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime, got {type(value).__name__}")
    if value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware, got {value!r}")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{name} must be UTC (offset 0), got {value!r}")
    return value.astimezone(timezone.utc)


def _local_0800_utc_for_date(local_date) -> datetime:
    """Return the local 08:00 boundary for the given local date, in UTC."""
    return datetime(
        local_date.year, local_date.month, local_date.day, 8, 0, 0, tzinfo=_LOCAL_TZ
    ).astimezone(timezone.utc)


def compute_morning_window(
    as_of_utc: datetime,
    last_completed_upper_utc: Optional[datetime],
) -> Tuple[datetime, datetime]:
    """Return ``(lower, upper)`` aware-UTC datetimes for the briefing window.

    Phase 4 supervisor addendum §2.2 is normative.
    """
    as_of = _require_aware_utc(as_of_utc, "as_of_utc")

    if last_completed_upper_utc is not None:
        last_completed = _require_aware_utc(
            last_completed_upper_utc, "last_completed_upper_utc"
        )
        if last_completed > as_of:
            raise ValueError(
                "last_completed_upper_utc must be <= as_of_utc, got "
                f"last_completed_upper_utc={last_completed!r} as_of_utc={as_of!r}"
            )
    else:
        last_completed = None

    as_of_local = as_of.astimezone(_LOCAL_TZ)
    local_today_0800 = as_of_local.replace(hour=8, minute=0, second=0, microsecond=0)
    if as_of_local >= local_today_0800:
        upper_local_date = as_of_local.date()
    else:
        upper_local_date = as_of_local.date() - timedelta(days=1)
    upper_utc = _local_0800_utc_for_date(upper_local_date)

    cap_local_date = upper_local_date - timedelta(days=7)
    cap_utc = _local_0800_utc_for_date(cap_local_date)

    if not cap_utc < upper_utc:
        raise AssertionError(
            "compute_morning_window cap must be strictly before upper; "
            f"got cap={cap_utc!r} upper={upper_utc!r}"
        )

    if last_completed is None:
        lower = cap_utc
    else:
        if last_completed >= upper_utc:
            return (upper_utc, upper_utc)
        if last_completed < cap_utc:
            lower = cap_utc
        else:
            lower = last_completed

    if lower > upper_utc:
        raise AssertionError(
            "compute_morning_window lower exceeded upper_utc; "
            f"lower={lower!r} upper_utc={upper_utc!r}"
        )
    return (lower, upper_utc)


__all__ = (
    "_DRY_RUN",
    "BriefingInput",
    "EligibilityResult",
    "map_eligibility",
    "compute_morning_window",
)
