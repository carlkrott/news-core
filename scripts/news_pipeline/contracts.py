"""Phase 2 deterministic dry-run filtering — contract types.

These dataclasses, enums, and helpers are the surface every other module in
``news_pipeline.filtering`` consumes. They are deliberately stdlib-only,
frozen/slotted, and never call ``now()`` themselves — every timestamp comes
from explicit caller input so evaluation is fully deterministic.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from typing import Literal, get_args

from .models import Category


# Literal alias mirroring the Phase 1 publication-evidence vocabulary.
PublishedEvidence = Literal["source", "metadata", "missing", "unparseable"]


def validate_utc_iso(value: str) -> str:
    """Normalize a UTC ISO-8601 timestamp into the Phase 1 ``...Z`` form.

    Accepts both ``...Z`` and ``...+00:00`` suffixes. Rejects naive timestamps
    and anything ``datetime.fromisoformat`` cannot parse.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(f"timestamp is not a non-empty string: {value!r}")
    if value.endswith("Z"):
        candidate = value[:-1] + "+00:00"
    elif "+" in value[10:] or value.count("-") > 2:
        # Either an explicit offset is present, or the date has a negative year.
        # datetime.fromisoformat handles offsets; reject naive strings explicitly.
        if "+" not in value[10:] and not value.endswith("Z") and value[10:].count("-") == 0:
            raise ValueError(f"timestamp lacks UTC offset: {value!r}")
        candidate = value
    else:
        raise ValueError(f"timestamp lacks UTC offset: {value!r}")
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise ValueError(f"timestamp is not ISO-8601: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"timestamp is not UTC: {value!r}")
    return parsed.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class DecisionCode(str, Enum):
    """Terminal decisions the engine can emit for a candidate."""

    KEEP = "keep"
    SUPPRESS_BATCH_EXACT = "suppress_batch_exact"
    SUPPRESS_EXACT_URL = "suppress_exact_url"
    SUPPRESS_EXACT_IDENTITY = "suppress_exact_identity"
    SUPPRESS_RECENT_TITLE = "suppress_recent_title"
    DROP_STALE = "drop_stale"
    DROP_BLOCKED_SOURCE = "drop_blocked_source"
    DROP_NON_ARTICLE_URL = "drop_non_article_url"
    PENDING_MISSING_EVIDENCE = "pending_missing_evidence"
    PENDING_INVALID_EVIDENCE = "pending_invalid_evidence"
    PENDING_POSSIBLE_UPDATE = "pending_possible_update"
    PENDING_HISTORY_UNAVAILABLE = "pending_history_unavailable"


class ReasonCode(str, Enum):
    """Structured reasons attached to each ``FilterResult``.

    Reason codes are informational and never imply the final decision on their
    own: the same reason (e.g. ``OK_UNKNOWN_SOURCE``) can coexist with both a
    KEEP and a DROP decision, depending on the path that produced it.
    """

    OK_KEEP = "ok_keep"
    OK_BLOCKED_SOURCE = "ok_blocked_source"
    OK_OBSERVED_FALLBACK = "ok_observed_fallback"
    OK_TITLE_ONLY_LOW_CONFIDENCE = "ok_title_only_low_confidence"
    OK_TRUSTED_SOURCE = "ok_trusted_source"
    OK_UNKNOWN_SOURCE = "ok_unknown_source"

    BLOCKED_SOURCE_EXACT = "blocked_source_exact"
    BLOCKED_SOURCE_SUBDOMAIN = "blocked_source_subdomain"
    STALE = "stale"
    MISSING_DATE = "missing_date"
    INVALID_DATE = "invalid_date"
    FUTURE_DATE = "future_date"
    FUTURE_DATE_CLAMPED = "future_date_clamped"
    WITHIN_BATCH_EXACT = "within_batch_exact"

    HISTORY_EXACT_URL = "history_exact_url"
    HISTORY_EXACT_IDENTITY = "history_exact_identity"
    HISTORY_RECENT_TITLE = "history_recent_title"
    HISTORY_URL_CHANGED_CONTENT = "history_url_changed_content"
    HISTORY_TITLE_CHANGED_SNIPPET = "history_title_changed_snippet"

    UNKNOWN_QUERY_GROUP = "unknown_query_group"
    MALFORMED_CANONICAL_URL = "malformed_canonical_url"
    CANONICAL_MISMATCH = "canonical_mismatch"
    MISSING_URL = "missing_url"
    NON_ARTICLE_URL = "non_article_url"
    HISTORY_UNAVAILABLE = "history_unavailable"
    CROSS_CATEGORY_PASSTHROUGH = "cross_category_passthrough"


class TrustTier(str, Enum):
    """Trust classification produced by ``SourcePolicy.classify_source``."""

    BLOCKED = "blocked"
    ALLOWED = "allowed"
    TRUSTED = "trusted"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CandidateArticle:
    """One inbound article candidate from the (future) live pipeline.

    Every field is supplied by the caller — the engine never invents a value.
    ``published_evidence`` is the literal alias described in ``PublishedEvidence``.
    """

    candidate_id: str
    category: Category
    query_group: str
    title: str
    snippet: str
    original_url: str | None
    canonical_url: str | None
    published_at: str | None
    published_evidence: PublishedEvidence
    observed_at: str | None
    evaluated_at: str

    def __post_init__(self) -> None:
        # Validate the literal alias without a hard import of typing.get_args here.
        if self.published_evidence not in get_args(PublishedEvidence):
            raise ValueError(
                f"published_evidence must be one of {list(get_args(PublishedEvidence))!r}, "
                f"got {self.published_evidence!r}"
            )
        if not isinstance(self.category, Category):
            raise ValueError(
                f"category must be a Category instance, got {self.category!r}"
            )
        # Per the supervisor contract, timestamps are validated *eagerly for type*
        # but bad evidence becomes PENDING in the engine — never a raised exception
        # from the batch API. We do not call ``validate_utc_iso`` here.


@dataclass(frozen=True, slots=True)
class FilterResult:
    """Output row for one candidate, preserving the deterministic ordinal."""

    candidate: CandidateArticle
    decision: DecisionCode
    reasons: tuple[ReasonCode, ...]
    matched_article_ids: tuple[str, ...]
    matched_observation_ids: tuple[str, ...]
    trust_tier: TrustTier
    evaluated_publication_time: str | None
    ordinal: int
    date_evidence: str = "unknown"
    recency_status: str = "unknown"
    audit_only: bool = False


@dataclass(frozen=True, slots=True)
class HistoryMatch:
    """A single historical Phase 1 appearance, used by suppression checks."""

    article_id: str
    observation_id: str
    category: Category
    occurred_at: str
    title: str
    snippet: str
    canonical_url: str | None
    identity_basis: str
