"""Deterministic, read-only Phase 2 candidate filtering."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Mapping

from .canonicalization import canonicalize_url
from .contracts import CandidateArticle, DecisionCode, FilterResult, HistoryMatch, ReasonCode, TrustTier, validate_utc_iso
from .db import article_id as phase1_article_id
from .history import HistoryUnavailable, fetch_history_match, find_exact_identity, find_exact_title, find_exact_url, open_history
from .models import Category
from .policies import QueryPolicy, SourceMatch, SourcePolicy, classify_source

FUTURE_TOLERANCE = timedelta(hours=6)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.casefold()).strip()


def _parse_iso(value: str) -> datetime:
    normalized = validate_utc_iso(value)
    return datetime.fromisoformat(normalized[:-1] + "+00:00").astimezone(timezone.utc)


def _format_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _unique(*groups: tuple[ReasonCode, ...] | list[ReasonCode]) -> tuple[ReasonCode, ...]:
    result: list[ReasonCode] = []
    for group in groups:
        for reason in group:
            if reason not in result:
                result.append(reason)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class _Context:
    candidate: CandidateArticle
    ordinal: int
    policy: QueryPolicy
    evaluated_at: datetime
    canonical_url: str | None
    normalized_title: str
    normalized_snippet: str
    identity: str | None


def _result(ctx: _Context, decision: DecisionCode, reasons: tuple[ReasonCode, ...] | list[ReasonCode], trust: TrustTier, publication_time: str | None = None, match: HistoryMatch | None = None) -> FilterResult:
    return FilterResult(
        candidate=ctx.candidate,
        decision=decision,
        reasons=_unique(reasons),
        matched_article_ids=(match.article_id,) if match else (),
        matched_observation_ids=(match.observation_id,) if match else (),
        trust_tier=trust,
        evaluated_publication_time=publication_time,
        ordinal=ctx.ordinal,
    )


def _context(candidate: CandidateArticle, ordinal: int, policy: QueryPolicy) -> tuple[_Context | None, FilterResult | None]:
    try:
        evaluated_at = _parse_iso(candidate.evaluated_at)
    except (TypeError, ValueError):
        dummy = _Context(candidate, ordinal, policy, datetime.fromtimestamp(0, timezone.utc), None, "", "", None)
        return None, _result(dummy, DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.INVALID_DATE,), TrustTier.UNKNOWN)

    normalized_title = _normalize_text(candidate.title)
    normalized_snippet = _normalize_text(candidate.snippet)
    try:
        supplied = canonicalize_url(candidate.canonical_url) if candidate.canonical_url else None
        original = canonicalize_url(candidate.original_url) if candidate.original_url else None
    except (TypeError, ValueError):
        dummy = _Context(candidate, ordinal, policy, evaluated_at, None, normalized_title, normalized_snippet, None)
        return None, _result(dummy, DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.MALFORMED_CANONICAL_URL,), TrustTier.UNKNOWN)
    if supplied is not None and original is not None and supplied != original:
        dummy = _Context(candidate, ordinal, policy, evaluated_at, supplied, normalized_title, normalized_snippet, None)
        return None, _result(dummy, DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.CANONICAL_MISMATCH,), TrustTier.UNKNOWN)
    canonical_url = supplied or original
    identity = None
    if canonical_url is None:
        identity = phase1_article_id(None, normalized_title, normalized_snippet, candidate.category.value, None)
    return _Context(candidate, ordinal, policy, evaluated_at, canonical_url, normalized_title, normalized_snippet, identity), None


def _recency(ctx: _Context, trust: TrustTier) -> tuple[DecisionCode | None, tuple[ReasonCode, ...], str | None]:
    c = ctx.candidate
    if c.published_evidence == "unparseable":
        return DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.INVALID_DATE,), None
    if c.published_evidence == "missing" and c.published_at not in (None, ""):
        return DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.INVALID_DATE,), None

    try:
        published = _parse_iso(c.published_at) if c.published_at else None
        observed = _parse_iso(c.observed_at) if c.observed_at else None
    except (TypeError, ValueError):
        return DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.INVALID_DATE,), None

    evidence_reasons: list[ReasonCode] = []
    if published is None:
        if observed is None:
            return DecisionCode.PENDING_MISSING_EVIDENCE, (ReasonCode.MISSING_DATE,), None
        if not ctx.policy.missing_date_fallback:
            return DecisionCode.PENDING_MISSING_EVIDENCE, (ReasonCode.MISSING_DATE,), None
        chosen = observed
        evidence_reasons.append(ReasonCode.OK_OBSERVED_FALLBACK)
    else:
        chosen = published

    publication_time = _format_iso(chosen)
    future = chosen - ctx.evaluated_at
    if future > FUTURE_TOLERANCE:
        return DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.FUTURE_DATE, ReasonCode.INVALID_DATE), publication_time
    if future > timedelta(0):
        evidence_reasons.append(ReasonCode.FUTURE_DATE_CLAMPED)
    compare_time = min(chosen, ctx.evaluated_at)
    if ctx.evaluated_at - compare_time > ctx.policy.recency:
        return DecisionCode.DROP_STALE, _unique(evidence_reasons, (ReasonCode.STALE,)), publication_time
    return None, tuple(evidence_reasons), publication_time


def evaluate_candidates(candidates: list[CandidateArticle], db_path: str, source_policy: SourcePolicy, query_policies: Mapping[Category, QueryPolicy]) -> tuple[FilterResult, ...]:
    """Evaluate candidates in stable input order without writing state."""
    history = None
    history_failed = False
    global_urls: dict[str, tuple[int, tuple[str, str]]] = {}
    category_urls: dict[tuple[Category, str], tuple[int, tuple[str, str]]] = {}
    category_titles: dict[tuple[Category, str], tuple[int, tuple[str, str]]] = {}
    results: list[FilterResult] = []

    try:
        for ordinal, candidate in enumerate(candidates, start=1):
            policy = query_policies.get(candidate.category)
            if policy is None:
                dummy = _Context(candidate, ordinal, next(iter(query_policies.values()), None), datetime.fromtimestamp(0, timezone.utc), None, "", "", None)  # type: ignore[arg-type]
                results.append(_result(dummy, DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.UNKNOWN_QUERY_GROUP,), TrustTier.UNKNOWN))
                continue
            ctx, short = _context(candidate, ordinal, policy)
            if short is not None:
                results.append(short)
                continue
            assert ctx is not None
            if candidate.query_group not in policy.allowed_query_groups:
                results.append(_result(ctx, DecisionCode.PENDING_INVALID_EVIDENCE, (ReasonCode.UNKNOWN_QUERY_GROUP,), TrustTier.UNKNOWN))
                continue

            source = classify_source(source_policy, ctx.canonical_url)
            source_reasons = (source.reason,)
            if source.tier is TrustTier.BLOCKED:
                results.append(_result(ctx, DecisionCode.DROP_BLOCKED_SOURCE, _unique(source_reasons, (ReasonCode.OK_BLOCKED_SOURCE,)), source.tier))
                continue

            recency_decision, evidence_reasons, publication_time = _recency(ctx, source.tier)
            base_reasons = _unique(source_reasons, evidence_reasons)
            if recency_decision is not None:
                results.append(_result(ctx, recency_decision, base_reasons, source.tier, publication_time))
                continue

            signature = (ctx.normalized_title, ctx.normalized_snippet)
            batch_match: tuple[int, tuple[str, str]] | None = None
            if ctx.canonical_url is not None:
                if policy.cross_category_exact_url:
                    batch_match = global_urls.get(ctx.canonical_url)
                else:
                    batch_match = category_urls.get((candidate.category, ctx.canonical_url))
            else:
                batch_match = category_titles.get((candidate.category, ctx.normalized_title))

            if batch_match is not None:
                _, winner_signature = batch_match
                decision = DecisionCode.SUPPRESS_BATCH_EXACT if winner_signature == signature else DecisionCode.PENDING_POSSIBLE_UPDATE
                results.append(_result(ctx, decision, _unique(base_reasons, (ReasonCode.WITHIN_BATCH_EXACT,)), source.tier, publication_time))
                continue

            if ctx.canonical_url is not None:
                global_urls.setdefault(ctx.canonical_url, (ordinal, signature))
                category_urls.setdefault((candidate.category, ctx.canonical_url), (ordinal, signature))
            else:
                category_titles.setdefault((candidate.category, ctx.normalized_title), (ordinal, signature))

            if history is None and not history_failed:
                try:
                    history = open_history(db_path)
                except HistoryUnavailable:
                    history_failed = True
            if history_failed or history is None:
                results.append(_result(ctx, DecisionCode.PENDING_HISTORY_UNAVAILABLE, _unique(base_reasons, (ReasonCode.HISTORY_UNAVAILABLE,)), source.tier, publication_time))
                continue

            try:
                url_match = None
                if ctx.canonical_url is not None:
                    url_match = find_exact_url(
                        history,
                        ctx.canonical_url,
                        candidate.evaluated_at,
                        policy.exact_url_lookback,
                        cross_category=policy.cross_category_exact_url,
                        category=None if policy.cross_category_exact_url else candidate.category.value,
                    )
                if url_match is not None:
                    latest = fetch_history_match(history, url_match.article_id, url_match.observation_id)
                    if latest is None:
                        latest = url_match
                    same_content = _normalize_text(latest.title) == ctx.normalized_title and _normalize_text(latest.snippet or "") == ctx.normalized_snippet
                    decision = DecisionCode.SUPPRESS_EXACT_URL if same_content else DecisionCode.PENDING_POSSIBLE_UPDATE
                    reason = ReasonCode.HISTORY_EXACT_URL if same_content else ReasonCode.HISTORY_URL_CHANGED_CONTENT
                    results.append(_result(ctx, decision, _unique(base_reasons, (reason,)), source.tier, publication_time, latest))
                    continue

                if ctx.identity is not None:
                    identity_match = find_exact_identity(history, ctx.identity, candidate.category.value, candidate.evaluated_at, policy.exact_identity_lookback)
                    if identity_match is not None:
                        results.append(_result(ctx, DecisionCode.SUPPRESS_EXACT_IDENTITY, _unique(base_reasons, (ReasonCode.HISTORY_EXACT_IDENTITY,)), source.tier, publication_time, identity_match))
                        continue

                title_match = find_exact_title(history, ctx.normalized_title, candidate.category.value, candidate.evaluated_at, policy.exact_title_lookback)
                if title_match is not None:
                    same_snippet = _normalize_text(title_match.snippet or "") == ctx.normalized_snippet
                    if same_snippet:
                        results.append(_result(ctx, DecisionCode.SUPPRESS_RECENT_TITLE, _unique(base_reasons, (ReasonCode.HISTORY_RECENT_TITLE,)), source.tier, publication_time, title_match))
                    else:
                        extra: tuple[ReasonCode, ...] = (ReasonCode.OK_TITLE_ONLY_LOW_CONFIDENCE,) if title_match.identity_basis == "title_only" else ()
                        results.append(_result(ctx, DecisionCode.PENDING_POSSIBLE_UPDATE, _unique(base_reasons, extra, (ReasonCode.HISTORY_TITLE_CHANGED_SNIPPET,)), source.tier, publication_time, title_match))
                    continue
            except HistoryUnavailable:
                history_failed = True
                results.append(_result(ctx, DecisionCode.PENDING_HISTORY_UNAVAILABLE, _unique(base_reasons, (ReasonCode.HISTORY_UNAVAILABLE,)), source.tier, publication_time))
                continue

            results.append(_result(ctx, DecisionCode.KEEP, base_reasons, source.tier, publication_time))
    finally:
        if history is not None:
            history.close()

    return tuple(results)
