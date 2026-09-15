"""Pure, fail-closed Phase 4 grounding and verification rules."""
from __future__ import annotations

import hashlib
import json
import math
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


def verify_evidence(evidence: Iterable[Mapping[str, object]]) -> VerificationState:
    rows = tuple(evidence)
    supports = []
    contradicts = []
    for row in rows:
        role = _value(row.get("role"))
        source_role = _value(row.get("source_role"))
        group = row.get("independence_group")
        group = group.strip() if isinstance(group, str) else ""
        normalized = dict(row)
        normalized.update(role=role, source_role=source_role, independence_group=group)
        if role == EvidenceRole.CONTRADICTS.value:
            contradicts.append(normalized)
        elif role == EvidenceRole.SUPPORTS.value and source_role in {"primary", "neutral", "specialist"}:
            supports.append(normalized)
    if contradicts:
        return VerificationState.WATCHLIST
    if any(row["source_role"] == "primary" for row in supports):
        return VerificationState.VERIFIED
    groups = {row["independence_group"] for row in supports if row["independence_group"]}
    roles = {row["source_role"] for row in supports}
    if len(groups) >= 2 and roles & {"neutral", "specialist"}:
        return VerificationState.VERIFIED
    return VerificationState.UNVERIFIED


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
