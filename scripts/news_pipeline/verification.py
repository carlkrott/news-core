"""Pure, fail-closed Phase 4 grounding and verification rules."""
from __future__ import annotations

import hashlib
import json
import math
import os
import re
from dataclasses import dataclass
from enum import Enum
from json import JSONDecoder
from typing import Iterable, Mapping

from .live_contracts import EvidenceRole, QueryPlanContract, VerificationState, stable_id


class GroundingError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class GroundedModelOutput:
    decision: str
    confidence: float
    summary: str
    evidence_ids: tuple[str, ...]
    facts: tuple[str, ...]


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise GroundingError("duplicate JSON key")
        result[key] = value
    return result


def _constant(value):
    raise GroundingError("non-finite JSON constant is forbidden")


_INJECTION = re.compile(r"(?:ignore\s+(?:all\s+)?previous|system\s+prompt|developer\s+message|reveal\s+(?:the\s+)?prompt|call\s+(?:a\s+)?tool)", re.I)
_NUMERIC_AUTHORITY_SUBJECT = re.compile(
    r"^[+-]?(?:(?:\d+(?:\.\d*)?)|(?:\.\d+))(?:[eE][+-]?\d+)?%?$"
)


def lead_retrieval_reason(retrieval_method: object) -> str | None:
    """Return a fail-closed reason when retrieval supplies only a discovery lead."""
    if type(retrieval_method) is not str:
        return None
    method = retrieval_method.strip().casefold()
    if method in {"rss", "rss-poll"}:
        return "feed_transport_not_claim_evidence"
    if method in {"searxng", "searxng-query", "search"} or method.startswith("search-"):
        return "search_result_not_claim_evidence"
    if method == "social" or method.startswith("social-"):
        return "social_lead_not_claim_evidence"
    return None


def _grounded_span(value: str, corpus: str) -> bool:
    pattern = rf"(?<!\w){re.escape(value.casefold())}(?!\w)"
    return re.search(pattern, corpus, re.UNICODE) is not None


def validate_model_output(raw: str, evidence: Mapping[str, str] | Iterable[Mapping[str, str]]) -> GroundedModelOutput:
    if type(raw) is not str or len(raw) > 8192 or "```" in raw:
        raise GroundingError("model output must be plain bounded JSON")
    if isinstance(evidence, Mapping):
        ev = dict(evidence)
    else:
        ev = {}
        for row in evidence:
            if type(row) is not dict or set(row) != {"evidence_id", "exact_excerpt"}:
                raise GroundingError("evidence rows have an invalid shape")
            eid, excerpt = row["evidence_id"], row["exact_excerpt"]
            if type(eid) is not str or not eid or eid in ev or type(excerpt) is not str or not excerpt:
                raise GroundingError("evidence rows must be unique and non-empty")
            ev[eid] = excerpt
    if any(type(k) is not str or type(v) is not str or not k or not v for k, v in ev.items()):
        raise GroundingError("evidence must contain non-empty strings")
    decoder = JSONDecoder(object_pairs_hook=_pairs, parse_constant=_constant)
    trimmed = raw.strip(" \t\r\n")
    try:
        obj, end = decoder.raw_decode(trimmed)
    except (ValueError, json.JSONDecodeError) as exc:
        raise GroundingError("malformed model JSON") from exc
    if end != len(trimmed) or type(obj) is not dict or set(obj) != {"decision", "confidence", "summary", "evidence_ids", "facts"}:
        raise GroundingError("model schema mismatch")
    if obj["decision"] not in {"verified", "watchlist", "rejected", "pending"}:
        raise GroundingError("invalid decision")
    confidence = obj["confidence"]
    if type(confidence) not in (int, float) or isinstance(confidence, bool) or not math.isfinite(float(confidence)) or not 0 <= float(confidence) <= 1:
        raise GroundingError("invalid confidence")
    summary = obj["summary"]
    if type(summary) is not str or not 1 <= len(summary) <= 2048 or _INJECTION.search(summary):
        raise GroundingError("invalid or unsupported summary")
    ids, facts = obj["evidence_ids"], obj["facts"]
    if type(ids) is not list or len(ids) > 16 or any(type(x) is not str or not x for x in ids) or len(ids) != len(set(ids)):
        raise GroundingError("invalid evidence_ids")
    if any(eid not in ev for eid in ids):
        raise GroundingError("model referenced unknown evidence")
    if obj["decision"] in {"verified", "watchlist", "rejected"} and not ids:
        raise GroundingError("non-pending decisions require evidence")
    if type(facts) is not list or len(facts) > 16 or any(type(x) is not str or not x.strip() or len(x) > 1024 for x in facts) or len({x.casefold() for x in facts}) != len(facts):
        raise GroundingError("invalid facts")
    corpus = " ".join(ev[eid] for eid in ids).casefold()
    if _INJECTION.search(" ".join(facts)) or _INJECTION.search(corpus):
        raise GroundingError("prompt-injection-shaped assertion")
    if not _grounded_span(summary, corpus):
        raise GroundingError("summary is not grounded in supplied evidence")
    if any(not _grounded_span(fact, corpus) for fact in facts):
        raise GroundingError("model fact is not supported by supplied evidence")
    return GroundedModelOutput(obj["decision"], float(confidence), summary, tuple(ids), tuple(facts))


def _value(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Enum) and isinstance(value.value, str):
        return value.value.strip()
    return ""


def authority_entity_matches(subject: object, entities: object) -> bool:
    """Match only an exact named claim subject, never a numeric article detail."""
    if type(subject) is not str or not subject.strip() or not isinstance(entities, (list, tuple)):
        return False
    normalized = subject.strip()
    if _NUMERIC_AUTHORITY_SUBJECT.fullmatch(normalized):
        return False
    return any(
        type(entity) is str
        and bool(entity.strip())
        and normalized.casefold() == entity.strip().casefold()
        for entity in entities
    )


def _single_outlet_enabled() -> bool:
    """Read the reversible operator switch; absent means approved B behavior."""
    raw = os.environ.get("NEWS_SINGLE_OUTLET_ENABLED")
    if raw is None:
        return True
    if raw.strip().casefold() in {"1", "true", "yes", "on"}:
        return True
    if raw.strip().casefold() in {"0", "false", "no", "off"}:
        return False
    raise ValueError("NEWS_SINGLE_OUTLET_ENABLED must be a boolean")


def verify_evidence(
    evidence: Iterable[Mapping[str, object]], *, allow_single_outlet: bool | None = None
) -> VerificationState:
    """Verify by primary authority, independent corroboration, or approved single outlet.

    Single-outlet eligibility requires complete persisted registry provenance. The
    switch defaults on as authorized; set NEWS_SINGLE_OUTLET_ENABLED=0 to restore
    the prior strict primary-authority / two-independent-group behavior.
    """
    if allow_single_outlet is None:
        allow_single_outlet = _single_outlet_enabled()
    if type(allow_single_outlet) is not bool:
        raise TypeError("allow_single_outlet must be a boolean")
    rows = tuple(evidence)
    contradicts = []
    parents: dict[str, str] = {}

    def find(key: str) -> str:
        parents.setdefault(key, key)
        if parents[key] != key:
            parents[key] = find(parents[key])
        return parents[key]

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    candidates: list[tuple[dict[str, object], str]] = []
    for index, row in enumerate(rows):
        role = _value(row.get("role"))
        source_role_key = "effective_source_role" if "effective_source_role" in row else "source_role"
        source_role = _value(row.get(source_role_key))
        group = row.get("independence_group")
        group = group.strip() if isinstance(group, str) else ""
        authority = row.get("authority_match")
        # Legacy direct callers may still provide source_role. Persisted
        # verification rows must provide effective provenance explicitly.
        if source_role_key == "effective_source_role" and type(authority) is not bool:
            authority = None
        elif source_role_key == "source_role" and "authority_match" not in row:
            # Transitional compatibility for pre-v7 direct callers only; the
            # persisted v7 event path must provide explicit authority_match.
            authority = source_role == "primary"
        normalized = dict(row)
        normalized.update(
            role=role,
            effective_source_role=source_role,
            independence_group=group,
            authority_match=authority,
        )
        if role == EvidenceRole.CONTRADICTS.value:
            contradicts.append(normalized)
            continue
        if role != EvidenceRole.SUPPORTS.value or source_role not in {"primary", "neutral", "specialist"} or not group or type(authority) is not bool:
            continue
        # Publisher groups, canonical URLs, matching text and explicit wire
        # parents all identify one underlying source of corroboration.
        key = f"row:{index}"
        find(key)
        tokens = [f"group:{group}"]
        for field in ("canonical_url", "text_hash", "raw_content_hash", "syndicated_parent", "wire_parent"):
            value = row.get(field)
            if isinstance(value, str) and value.strip():
                tokens.append(f"{field}:{value.strip().casefold()}")
        for token in tokens:
            union(key, token)
        candidates.append((normalized, key))
    if contradicts:
        return VerificationState.WATCHLIST
    # A primary-authority match is an independent verification route and is
    # deliberately evaluated before cross-outlet deduplication.
    if any(row["effective_source_role"] == "primary" and row["authority_match"] is True for row, _ in candidates):
        return VerificationState.VERIFIED
    components: dict[str, set[str]] = {}
    for row, key in candidates:
        components.setdefault(find(key), set()).add(_value(row.get("effective_source_role")))
    if len(components) >= 2 and any(roles & {"neutral", "specialist"} for roles in components.values()):
        return VerificationState.VERIFIED
    if allow_single_outlet and len(components) == 1 and any(
        row.get("registry_matched") is True for row, _ in candidates
    ):
        return VerificationState.VERIFIED
    return VerificationState.UNVERIFIED


def claim_specific_evidence_row(
    row: tuple[object, ...],
    claim_subject: str,
    *,
    registry_enabled: bool = False,
    registry_role: object = None,
    registry_group: object = None,
    registry_host: object = None,
    registry_categories: object = None,
) -> dict[str, object]:
    """Adapt one schema-v11 provenance snapshot without trusting broad joins."""
    if len(row) != 9 or type(claim_subject) is not str or not claim_subject.strip():
        return {
            "role": row[0] if row else "",
            "effective_source_role": "",
            "independence_group": "",
            "authority_match": None,
            "registry_matched": False,
        }
    (
        evidence_role,
        effective_role,
        group,
        stored_authority_match,
        rule_id,
        publisher_host,
        authority_scope_json,
        authority_entities_json,
        category,
    ) = row
    try:
        scopes = json.loads(authority_scope_json) if isinstance(authority_scope_json, str) else None
        entities = json.loads(authority_entities_json) if isinstance(authority_entities_json, str) else None
    except json.JSONDecodeError:
        scopes, entities = None, None
    registry_scope = registry_categories
    try:
        if isinstance(registry_scope, str):
            registry_scope = json.loads(registry_scope)
    except json.JSONDecodeError:
        registry_scope = None
    registry_identity_valid = (
        registry_enabled is True
        and registry_role in {"primary", "specialist", "neutral"}
        and isinstance(registry_group, str)
        and bool(registry_group.strip())
        and registry_group.strip() != "unknown"
        and registry_host == publisher_host
        and type(registry_scope) is list
        and category in registry_scope
    )
    if registry_identity_valid:
        effective_role = registry_role
        group = registry_group
    identity_valid = (
        effective_role in {"primary", "neutral", "specialist"}
        and isinstance(group, str)
        and bool(group.strip())
        and group.strip() != "unknown"
        and isinstance(rule_id, str)
        and bool(rule_id.strip())
        and isinstance(publisher_host, str)
        and bool(publisher_host.strip())
        and publisher_host.strip() != "unknown"
        and type(scopes) is list
        and all(type(value) is str and bool(value.strip()) for value in scopes)
        and category in scopes
        and type(entities) is list
        and all(type(value) is str and bool(value.strip()) for value in entities)
    )
    entity_values = entities if type(entities) is list else []
    calculated_authority_match = (
        identity_valid
        and effective_role == "primary"
        and authority_entity_matches(claim_subject, entity_values)
    )
    authority_match = (
        type(stored_authority_match) in (int, bool)
        and bool(stored_authority_match)
        and calculated_authority_match
    )
    return {
        "role": evidence_role,
        "effective_source_role": effective_role if identity_valid else "",
        "independence_group": group.strip() if identity_valid else "",
        "authority_match": authority_match,
        "registry_matched": (
            registry_identity_valid
            and not _NUMERIC_AUTHORITY_SUBJECT.fullmatch(claim_subject.strip())
        ),
    }


def exact_excerpt_hash(excerpt: str) -> str:
    return hashlib.sha256(excerpt.encode("utf-8")).hexdigest()


def plan_corroboration_queries(claims, *, source_id: str, category: str, created_at: str, max_items: int = 2):
    if type(max_items) is not int or not 0 <= max_items <= 2:
        raise ValueError("max_items must be between 0 and 2")
    plans = []
    for claim in tuple(claims)[:max_items]:
        subject = claim.subject if hasattr(claim, "subject") else claim["subject"]
        predicate = claim.predicate if hasattr(claim, "predicate") else claim["predicate"]
        value = claim.object_value if hasattr(claim, "object_value") else claim["object_value"]
        query = f"{subject} {predicate} {value} official update"[:256]
        plans.append(QueryPlanContract(stable_id("corroboration", source_id, category, query), source_id, query, category, "claim_corroboration", 3600, 1, created_at, topic=subject))
    return tuple(plans)


validate_grounded_output = validate_model_output
source_independence_state = verify_evidence
