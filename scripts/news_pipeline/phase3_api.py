"""Phase 3 strict model boundary and deterministic batch evaluator."""
from __future__ import annotations

import json
import math
from json import JSONDecoder
from typing import Protocol, Sequence, cast

from .adjudication import classify_pair
from .clustering import cluster_id_for_topic, cluster_id_for_url, score_pair, select_history_matches
from .contracts import CandidateArticle, DecisionCode as Phase2DecisionCode, FilterResult, HistoryMatch
from .event_contracts import (
    AdjudicationResult,
    CachedModelOutcome,
    EventCandidate,
    InternalRuleDecision,
    ModelDecision,
    ModelErrorCategory,
    ModelRequest,
    ParsedModelVerdict,
    RuleVerdict,
    SemanticDecision,
    SemanticReasonCode,
    ordered_unique_reasons,
)

FIXED_MODEL_INSTRUCTION = (
    "Compare the newer candidate article with the older history article. "
    "Return exactly one JSON object with keys decision, confidence, reason, and facts. "
    "Use MATERIAL_UPDATE only for a new factual development, REWRITE only when no material fact changed, "
    "and UNCERTAIN when evidence is ambiguous. Do not return Markdown or any text outside the JSON object."
)


class ModelAdjudicatorProtocol(Protocol):
    def __call__(self, request: ModelRequest) -> str: ...


class ModelTransportError(Exception):
    pass


def build_model_request(candidate, history) -> ModelRequest:
    return ModelRequest(
        instruction=FIXED_MODEL_INSTRUCTION,
        candidate_title=candidate.title[:512],
        candidate_snippet=candidate.snippet[:2048],
        candidate_url=None if candidate.canonical_url is None else candidate.canonical_url[:2048],
        candidate_category=candidate.category.value[:32],
        candidate_published_at=None if candidate.published_at is None else candidate.published_at[:32],
        candidate_evaluated_at=candidate.evaluated_at[:32],
        history_title=history.title[:512], history_snippet=history.snippet[:2048],
        history_url=None if history.canonical_url is None else history.canonical_url[:2048],
        history_category=history.category.value[:32], history_occurred_at=history.occurred_at[:32],
    )


def _payload_obj(request: ModelRequest) -> dict:
    return {
        "instruction": request.instruction,
        "candidate": {
            "title": request.candidate_title, "snippet": request.candidate_snippet,
            "canonical_url": request.candidate_url, "category": request.candidate_category,
            "published_at": request.candidate_published_at, "evaluated_at": request.candidate_evaluated_at,
        },
        "history": {
            "title": request.history_title, "snippet": request.history_snippet,
            "canonical_url": request.history_url, "category": request.history_category,
            "occurred_at": request.history_occurred_at,
        },
    }


def render_model_payload(request: ModelRequest) -> str:
    candidate_snippet = request.candidate_snippet
    history_snippet = request.history_snippet
    while True:
        current = ModelRequest(request.instruction, request.candidate_title, candidate_snippet, request.candidate_url, request.candidate_category, request.candidate_published_at, request.candidate_evaluated_at, request.history_title, history_snippet, request.history_url, request.history_category, request.history_occurred_at)
        rendered = json.dumps(_payload_obj(current), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        if len(rendered) <= 8192:
            return rendered
        if not candidate_snippet and not history_snippet:
            raise ValueError("model payload exceeds 8192 with empty snippets")
        if len(candidate_snippet) >= len(history_snippet) and candidate_snippet:
            candidate_snippet = candidate_snippet[:-1]
        elif history_snippet:
            history_snippet = history_snippet[:-1]


def _reject_pairs(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError("duplicate JSON key")
        output[key] = value
    return output


def _reject_constant(value):
    raise ValueError("JSON constant is not allowed: " + value)


def parse_model_response(raw: str) -> ParsedModelVerdict:
    if not isinstance(raw, str) or len(raw) > 4096:
        raise ValueError("model output must be a string <= 4096 characters")
    if "```" in raw:
        raise ValueError("Markdown fences are not allowed")
    trimmed = raw.strip(" \t\r\n")
    decoder = JSONDecoder(object_pairs_hook=_reject_pairs, parse_constant=_reject_constant)
    try:
        obj, end = decoder.raw_decode(trimmed)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ValueError("malformed model JSON") from exc
    if end != len(trimmed) or not isinstance(obj, dict) or set(obj) != {"decision", "confidence", "reason", "facts"}:
        raise ValueError("model JSON schema mismatch")
    decision = obj["decision"]
    confidence = obj["confidence"]
    reason = obj["reason"]
    facts = obj["facts"]
    if decision not in tuple(d.value for d in ModelDecision):
        raise ValueError("invalid model decision")
    if type(confidence) not in (int, float) or isinstance(confidence, bool):
        raise ValueError("confidence must be numeric, not bool")
    confidence = float(confidence)
    if not math.isfinite(confidence) or not 0.0 <= confidence <= 1.0:
        raise ValueError("confidence out of range")
    if not isinstance(reason, str) or not 1 <= len(reason) <= 512:
        raise ValueError("invalid reason")
    if not isinstance(facts, list) or len(facts) > 8:
        raise ValueError("invalid facts")
    if any(not isinstance(f, str) or not 1 <= len(f) <= 256 for f in facts) or len(set(facts)) != len(facts):
        raise ValueError("invalid facts")
    return ParsedModelVerdict(ModelDecision(decision), confidence, reason, tuple(facts))


def _result(event, decision, semantic_reasons, cluster_id=None, matched_candidates=(), matched_history=(), matched_observations=(), fact_deltas=(), model_used=False, confidence=None, error_category=None):
    return AdjudicationResult(candidate_id=event.candidate.candidate_id, semantic_decision=decision, phase2_decision=event.filter_result.decision, phase2_reasons=event.filter_result.reasons, semantic_reasons=ordered_unique_reasons(tuple(semantic_reasons)), cluster_id=cluster_id, matched_candidate_ids=tuple(matched_candidates), matched_history_ids=tuple(matched_history), matched_observation_ids=tuple(matched_observations), fact_deltas=tuple(fact_deltas), model_used=model_used, model_confidence=confidence, model_error_category=error_category, ordinal=event.filter_result.ordinal, source_ordinal=event.filter_result.ordinal)


def _model_outcome(model, candidate, history, calls, max_calls, cache, key):
    cached = cache.get(key)
    if cached is not None:
        return cached, calls
    if calls >= max_calls:
        return CachedModelOutcome(SemanticDecision.pending_review, (SemanticReasonCode.BUDGET_EXHAUSTED,), None, None, ()), calls
    calls += 1
    request = build_model_request(candidate, history)
    try:
        raw = model(request)
    except ModelTransportError:
        outcome = CachedModelOutcome(SemanticDecision.pending_model_error, (SemanticReasonCode.TRANSPORT_ERROR,), None, ModelErrorCategory.TRANSPORT_ERROR, ())
    else:
        try:
            if not isinstance(raw, str):
                raise ValueError("non-string model result")
            parsed = parse_model_response(raw)
        except ValueError:
            outcome = CachedModelOutcome(SemanticDecision.pending_model_error, (SemanticReasonCode.MALFORMED_MODEL_OUTPUT,), None, ModelErrorCategory.MALFORMED_OUTPUT, ())
        else:
            if parsed.decision is ModelDecision.MATERIAL_UPDATE and parsed.confidence >= 0.80:
                final = SemanticDecision.material_update
                reasons = (SemanticReasonCode.MODEL_REQUIRED,)
            elif parsed.decision is ModelDecision.REWRITE and parsed.confidence >= 0.95:
                final = SemanticDecision.rewrite
                reasons = (SemanticReasonCode.SAME_FACTS,)
            else:
                final = SemanticDecision.pending_review
                reasons = (SemanticReasonCode.LOW_CONFIDENCE,)
            outcome = CachedModelOutcome(final, reasons, parsed.confidence, None, parsed.facts)
    cache[key] = outcome
    return outcome, calls


def evaluate_semantic_updates(candidates: Sequence[EventCandidate], history: Sequence[HistoryMatch], model: ModelAdjudicatorProtocol, max_model_calls: int):
    if type(max_model_calls) is not int or max_model_calls < 0:
        raise ValueError("max_model_calls must be a non-negative int")
    events = tuple(candidates)
    seen = set()
    for event in events:
        cid = event.candidate.candidate_id
        if cid in seen:
            raise ValueError("duplicate candidate ID")
        seen.add(cid)
    history_rows = tuple(history)
    history_by_article: dict[str, list[HistoryMatch]] = {}
    for row in history_rows:
        history_by_article.setdefault(row.article_id, []).append(row)
    for rows in history_by_article.values():
        rows.sort(key=lambda row: (row.occurred_at, row.observation_id))
    cache: dict[tuple[str, str], CachedModelOutcome] = {}
    calls = 0
    results = []
    terminal = {Phase2DecisionCode.SUPPRESS_BATCH_EXACT, Phase2DecisionCode.SUPPRESS_EXACT_URL, Phase2DecisionCode.SUPPRESS_EXACT_IDENTITY, Phase2DecisionCode.SUPPRESS_RECENT_TITLE, Phase2DecisionCode.DROP_STALE, Phase2DecisionCode.DROP_BLOCKED_SOURCE, Phase2DecisionCode.DROP_NON_ARTICLE_URL}
    passthrough = {Phase2DecisionCode.PENDING_MISSING_EVIDENCE: SemanticReasonCode.PHASE2_MISSING_EVIDENCE, Phase2DecisionCode.PENDING_INVALID_EVIDENCE: SemanticReasonCode.PHASE2_INVALID_EVIDENCE, Phase2DecisionCode.PENDING_HISTORY_UNAVAILABLE: SemanticReasonCode.PHASE2_HISTORY_UNAVAILABLE}
    for event in events:
        candidate = event.candidate
        phase2 = event.filter_result.decision
        if phase2 in terminal:
            results.append(_result(event, SemanticDecision.bypass_phase2_terminal, (SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED,), matched_candidates=(candidate.candidate_id,), matched_history=event.filter_result.matched_article_ids, matched_observations=event.filter_result.matched_observation_ids))
            continue
        if phase2 in passthrough:
            results.append(_result(event, SemanticDecision.pending_review, (passthrough[phase2],), matched_candidates=(candidate.candidate_id,), matched_history=event.filter_result.matched_article_ids, matched_observations=event.filter_result.matched_observation_ids))
            continue
        filter_result = cast(FilterResult, event.filter_result)
        candidate_article = cast(CandidateArticle, candidate)
        if filter_result.audit_only:
            results.append(_result(event, SemanticDecision.pending_review, (SemanticReasonCode.PHASE2_MISSING_EVIDENCE,), matched_candidates=(candidate_article.candidate_id,), matched_history=filter_result.matched_article_ids, matched_observations=filter_result.matched_observation_ids))
            continue
        referenced = tuple(dict.fromkeys(event.filter_result.matched_article_ids))
        if phase2 is Phase2DecisionCode.PENDING_POSSIBLE_UPDATE:
            resolved_rows = [
                row
                for article_id in referenced
                for row in history_by_article.get(article_id, ())
            ]
            resolved_article_ids = tuple(sorted({row.article_id for row in resolved_rows}))
            resolved_observation_ids = tuple(sorted(
                {row.observation_id for row in resolved_rows}
                | set(event.filter_result.matched_observation_ids)
            ))
            if not referenced or any(article_id not in history_by_article for article_id in referenced):
                results.append(_result(
                    event,
                    SemanticDecision.pending_review,
                    (SemanticReasonCode.MISSING_HISTORY_MATCH,),
                    matched_candidates=(candidate.candidate_id,),
                    matched_history=resolved_article_ids,
                    matched_observations=resolved_observation_ids,
                ))
                continue
            selected = [score_pair(candidate, row) for row in resolved_rows]
            selected.sort(key=lambda scored_row: (
                0 if scored_row.exact_url else 1,
                -scored_row.combined_score,
                scored_row.match.occurred_at,
                scored_row.match.article_id,
                scored_row.match.observation_id,
            ))
        else:
            selected = list(select_history_matches(event, history_rows))
        if not selected:
            results.append(_result(event, SemanticDecision.distinct_event, (SemanticReasonCode.DISTINCT_EVENT,)))
            continue
        scored = selected[0]
        verdict = classify_pair(candidate, scored.match, scored)
        matched_history = tuple(sorted({row.match.article_id for row in selected}))
        matched_obs = tuple(sorted({row.match.observation_id for row in selected}))
        cluster = cluster_id_for_url(candidate.canonical_url) if candidate.canonical_url and scored.exact_url else cluster_id_for_topic(candidate.category, scored.match.article_id)
        if verdict.decision is InternalRuleDecision.model_required:
            key = (candidate.candidate_id, scored.match.article_id)
            outcome, calls = _model_outcome(model, candidate, scored.match, calls, max_model_calls, cache, key)
            results.append(_result(event, outcome.final_decision, outcome.reasons, cluster, (candidate.candidate_id,), matched_history, matched_obs, verdict.fact_deltas, outcome.error_category is not None or outcome.confidence is not None, outcome.confidence, outcome.error_category))
        else:
            final = SemanticDecision.material_update if verdict.decision is InternalRuleDecision.material_update else SemanticDecision.rewrite
            results.append(_result(event, final, verdict.reasons, cluster, (candidate.candidate_id,), matched_history, matched_obs, verdict.fact_deltas))
    return tuple(results)
