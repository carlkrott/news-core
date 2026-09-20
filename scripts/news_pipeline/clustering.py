"""Phase 3 clustering: tokenization, jaccard, scoring, history selection.

Public surface (matches addendum §4 and §7):
  - ``tokenize(text) -> tuple[str, ...]``
  - ``jaccard(left, right) -> Decimal``
  - ``combined_score(title_score, snippet_score) -> Decimal``
  - ``score_pair(candidate, history) -> ScoredHistoryMatch``
  - ``select_history_matches(event_candidate, history) -> tuple[ScoredHistoryMatch, ...]``
  - ``cluster_id_for_url(canonical_url) -> str``
  - ``cluster_id_for_topic(category, anchor_article_id) -> str``

Stdlib-only. No DB, no network.
"""
from __future__ import annotations

import hashlib
import unicodedata
from datetime import timedelta
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Sequence, Tuple

from .contracts import (
    CandidateArticle,
    HistoryMatch,
)
from .event_contracts import ScoredHistoryMatch


# Stopword set per addendum §4.
_STOPWORDS: frozenset = frozenset({
    "the", "a", "an", "and", "or", "but", "is", "are", "was", "were",
    "in", "on", "at", "to", "for", "of", "with",
})


def tokenize(text: str) -> Tuple[str, ...]:
    """Addendum §4 tokenization."""
    norm = unicodedata.normalize("NFKC", text).casefold()
    out: list[str] = []
    chars: list[str] = []
    for ch in norm:
        cat = unicodedata.category(ch)
        if cat.startswith("L") or cat.startswith("N"):
            chars.append(ch)
        else:
            chars.append(" ")
    for tok in "".join(chars).split():
        if len(tok) >= 2 or tok.isdigit():
            if tok in _STOPWORDS:
                continue
            out.append(tok)
    return tuple(sorted(set(out)))


def jaccard(left: Tuple[str, ...], right: Tuple[str, ...]) -> Decimal:
    left_set = set(left)
    right_set = set(right)
    if not left_set and not right_set:
        return Decimal("0")
    union_size = len(left_set | right_set)
    if union_size == 0:
        return Decimal("0")
    inter_size = len(left_set & right_set)
    result = Decimal(inter_size) / Decimal(union_size)
    return result.quantize(Decimal("0.000001"), rounding=ROUND_HALF_EVEN)


# Same quantization constant used by combined_score.
_SCORE_QUANT = Decimal("0.000001")


def combined_score(title_score: Decimal, snippet_score: Decimal) -> Decimal:
    """Addendum §4: ``(0.75 * title + 0.25 * snippet)`` quantized to 6 dp."""
    val = (Decimal("0.75") * Decimal(title_score)
           + Decimal("0.25") * Decimal(snippet_score))
    return val.quantize(_SCORE_QUANT, rounding=ROUND_HALF_EVEN)


def _tokens_for(c: CandidateArticle) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    return tokenize(c.title), tokenize(c.snippet)


def score_pair(candidate: CandidateArticle, history: HistoryMatch) -> ScoredHistoryMatch:
    """Score a single candidate/history pair into a ScoredHistoryMatch."""
    cand_title_tokens, cand_snip_tokens = _tokens_for(candidate)
    hist_title_tokens = tokenize(history.title)
    hist_snip_tokens = tokenize(history.snippet)
    title = jaccard(cand_title_tokens, hist_title_tokens)
    snippet = jaccard(cand_snip_tokens, hist_snip_tokens)
    combined = combined_score(title, snippet)
    exact_url = (candidate.canonical_url is not None
                 and history.canonical_url is not None
                 and candidate.canonical_url == history.canonical_url)
    return ScoredHistoryMatch(
        match=history, title_score=title, snippet_score=snippet,
        combined_score=combined, exact_url=exact_url,
    )


def _lexical_threshold() -> Decimal:
    """0.700000 exactly."""
    return Decimal("0.700000")


def select_history_matches(
    event_candidate,  # EventCandidate; hint-typed for test circularity
    history: Sequence[HistoryMatch],
) -> Tuple[ScoredHistoryMatch, ...]:
    """Return ordered history matches that are valid under ``event_candidate``."""
    cand = event_candidate.candidate
    policy = event_candidate.query_policy
    cand_title_tokens, cand_snip_tokens = _tokens_for(cand)

    scored: list[ScoredHistoryMatch] = []
    for h in history:
        score = score_pair(cand, h)
        matched = False
        # Exact URL branch.
        if (score.exact_url
                and cand.canonical_url is not None
                and h.canonical_url is not None
                and cand.canonical_url == h.canonical_url):
            # Cross-category exact URL allowed only by policy.
            if h.category == cand.category or policy.cross_category_exact_url:
                matched = True
        # Lexical branch: same category + combined >= .70
        if not matched:
            if h.category == cand.category and score.combined_score >= _lexical_threshold():
                matched = True
        if not matched:
            continue
        scored.append(score)

    # Sort: exact_url first, then combined_score desc, occurred_at asc, article_id
    # asc, observation_id asc.
    scored.sort(key=lambda s: (
        0 if s.exact_url else 1,
        -s.combined_score,
        s.match.occurred_at,
        s.match.article_id,
        s.match.observation_id,
    ))
    return tuple(scored)


def cluster_id_for_url(canonical_url: str) -> str:
    """Addendum §4: ``"url|" + sha256(canonical_url.encode("utf-8")).hexdigest()``."""
    digest = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
    return "url|" + digest


def cluster_id_for_topic(category, anchor_article_id: str) -> str:
    """Addendum §4: ``"topic|" + sha256((category.value + "\\x1f" + anchor.article_id).encode("utf-8")).hexdigest()``."""
    payload = (category.value + "\x1f" + anchor_article_id).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return "topic|" + digest


def event_id_for_match(candidate: CandidateArticle, history: HistoryMatch | None = None) -> str:
    """Return the durable event ID for a candidate/history relationship.

    A persisted event ID is authoritative when history already carries one.
    Otherwise exact canonical URLs remain the fast stable identity and the
    topic fallback is deterministic for URL-less history.
    """
    persisted = getattr(history, "event_id", None) if history is not None else None
    if isinstance(persisted, str) and persisted:
        return persisted
    if candidate.canonical_url:
        return cluster_id_for_url(candidate.canonical_url)
    anchor = history.article_id if history is not None else candidate.candidate_id
    return cluster_id_for_topic(candidate.category, anchor)


def event_version_for_match(history: HistoryMatch | None = None) -> int:
    """Return the known event version, defaulting to the first version."""
    version = getattr(history, "event_version", None) if history is not None else None
    if type(version) is int and version > 0:
        return version
    return 1
