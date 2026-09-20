"""Deterministic, bounded bespoke-query planning for Phase 3."""
from __future__ import annotations

import re

from .live_contracts import QueryPlanContract, stable_id

MAX_EXPANSION_ROUNDS = 2
MAX_QUERY_LENGTH = 256
EXPANSION_REASON = "low_novelty_expansion"
INVESTIGATION_REASON = "event_specific_investigation"

_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "at",
        "by",
        "for",
        "from",
        "in",
        "is",
        "of",
        "on",
        "the",
        "to",
        "with",
        "www",
        "http",
        "https",
        "com",
        "reddit",
    }
)


def validate_searxng_query_target(target: str) -> str:
    """Validate a configured SearXNG HTTP(S) endpoint without doing DNS or I/O."""
    if type(target) is not str or target != target.strip() or any(ch in target for ch in "\r\n\t"):
        raise ValueError("query target must be a clean URL")
    match = re.fullmatch(r"(?P<scheme>https?)://(?P<authority>[^/?#\s]+)(?P<path>/[^?#\s]*)", target)
    if match is None:
        raise ValueError("query target must be an HTTP(S) URL without query or fragment")
    authority = match.group("authority")
    if "@" in authority:
        raise ValueError("query target must not contain credentials")
    if authority.startswith("["):
        close = authority.find("]")
        if close < 0 or authority[close + 1 :] not in ("",) and not authority[close + 1 :].startswith(":"):
            raise ValueError("query target has an invalid IPv6 authority")
        host = authority[1:close]
        port = authority[close + 2 :] if authority[close + 1 :].startswith(":") else ""
    else:
        host, separator, port = authority.rpartition(":")
        if not separator:
            host, port = authority, ""
    if not host or port and (not port.isdigit() or not 1 <= int(port) <= 65535):
        raise ValueError("query target has an invalid host or port")
    normalized_host = host.casefold().rstrip(".")
    if normalized_host in {"reddit.com", "redd.it", "www.redd.it"} or normalized_host.endswith(".reddit.com"):
        raise ValueError("direct Reddit targets are forbidden")
    path = match.group("path").rstrip("/")
    if path != "/search" and not path.endswith("/search"):
        raise ValueError("target is not a SearXNG search endpoint")
    return target


def validate_query_plan(plan: QueryPlanContract, *, target: str | None = None) -> QueryPlanContract:
    if type(plan) is not QueryPlanContract:
        raise TypeError("plan must be QueryPlanContract")
    if plan.reason_selected not in {EXPANSION_REASON, INVESTIGATION_REASON}:
        raise ValueError("unexpected expansion reason")
    if plan.max_rounds != MAX_EXPANSION_ROUNDS:
        raise ValueError("expansion plans must be capped at two rounds")
    if len(plan.query_text) > MAX_QUERY_LENGTH or any(ch in plan.query_text for ch in "\r\n\t"):
        raise ValueError("query contains unsafe or oversized text")
    if re.search(r"(?:https?://|www\.|reddit\.com|redd\.it)", plan.query_text, re.IGNORECASE):
        raise ValueError("query contains a direct URL or Reddit target")
    if target is not None:
        validate_searxng_query_target(target)
    return plan


def _query_terms(title: str) -> tuple[str, ...]:
    words = re.findall(r"[\w][\w'’-]{2,}", title.casefold(), flags=re.UNICODE)
    return tuple(dict.fromkeys(word for word in words if word not in _STOP_WORDS))


def _query_text(seed: str) -> str:
    words = _query_terms(seed)
    base = " ".join(words[:6])
    text = f"{base} official update" if base else "official update"
    if len(text) > MAX_QUERY_LENGTH:
        text = text[:MAX_QUERY_LENGTH].rstrip()
    return text


def build_expansion_queries(
    *,
    source_id: str,
    category: str,
    evaluated_at: str,
    titles: tuple[str, ...] = (),
    category_label: str | None = None,
    target: str | None = None,
    cooldown_seconds: int = 3600,
) -> tuple[QueryPlanContract, ...]:
    """Build at most two deterministic query plans for one category/tick."""
    if target is not None:
        validate_searxng_query_target(target)
    seeds = tuple(title for title in titles if type(title) is str and _query_terms(title))
    if not seeds:
        seeds = (category_label or category.replace("_", " "),)
    query_texts = tuple(dict.fromkeys(_query_text(seed) for seed in seeds))[:MAX_EXPANSION_ROUNDS]
    plans = tuple(
        QueryPlanContract(
            query_plan_id=stable_id("query-plan", source_id, category, query_text, category),
            source_id=source_id,
            query_text=query_text,
            category=category,
            reason_selected=EXPANSION_REASON,
            cooldown_seconds=cooldown_seconds,
            max_rounds=MAX_EXPANSION_ROUNDS,
            created_at=evaluated_at,
        )
        for query_text in query_texts
    )
    return tuple(validate_query_plan(plan, target=target) for plan in plans)


def plan_expansion(
    *,
    source_id: str,
    category: str,
    evaluated_at: str,
    titles: tuple[str, ...] = (),
    category_label: str | None = None,
    target: str | None = None,
    cooldown_seconds: int = 3600,
) -> tuple[QueryPlanContract, ...]:
    return build_expansion_queries(
        source_id=source_id,
        category=category,
        evaluated_at=evaluated_at,
        titles=titles,
        category_label=category_label,
        target=target,
        cooldown_seconds=cooldown_seconds,
    )


def build_investigation_queries(
    *,
    candidate_id: str,
    feed_lane_id: str,
    category: str,
    title: str,
    publisher_host: str | None,
    evaluated_at: str,
    round_number: int = 0,
) -> tuple[QueryPlanContract, ...]:
    """Build bounded, event-specific plans without combining candidates.

    The input is one candidate only.  The returned plans retain the
    candidate's feed lane so callers can persist and audit the isolation
    boundary without sharing a result list with discovery.
    """
    if type(candidate_id) is not str or not candidate_id.strip():
        raise ValueError("candidate_id must be a non-empty string")
    if type(feed_lane_id) is not str or not feed_lane_id.strip():
        raise ValueError("feed_lane_id must be a non-empty string")
    if round_number < 0 or round_number >= MAX_EXPANSION_ROUNDS:
        raise ValueError("investigation round_number must be between 0 and 1")
    seeds = [title]
    if publisher_host:
        host_terms = publisher_host.replace(".", " ").replace("-", " ")
        seeds.append(f"{title} {host_terms}")
    query_texts = tuple(dict.fromkeys(_query_text(seed) for seed in seeds))[:MAX_EXPANSION_ROUNDS]
    plans = tuple(
        QueryPlanContract(
            query_plan_id=stable_id(
                "investigation-query", candidate_id, feed_lane_id, str(round_number), query_text
            ),
            source_id=feed_lane_id,
            query_text=query_text,
            category=category,
            reason_selected=INVESTIGATION_REASON,
            cooldown_seconds=0,
            max_rounds=MAX_EXPANSION_ROUNDS,
            created_at=evaluated_at,
            entity=candidate_id,
            feed_lane_id=feed_lane_id,
        )
        for query_text in query_texts
    )
    return tuple(validate_query_plan(plan) for plan in plans)


def plan_investigation(
    **kwargs: object,
) -> tuple[QueryPlanContract, ...]:
    return build_investigation_queries(**kwargs)  # type: ignore[arg-type]
