"""Phase 3 — Slice 5 model parser and Slice 6 batch API tests."""
from __future__ import annotations

import json
import unittest
from datetime import timedelta

from news_pipeline.contracts import CandidateArticle, DecisionCode, FilterResult, HistoryMatch, ReasonCode, TrustTier
from news_pipeline.event_contracts import EventCandidate, ModelErrorCategory, SemanticDecision, SemanticReasonCode
from news_pipeline.models import Category
from news_pipeline.policies import QueryPolicy
from news_pipeline.phase3_api import FIXED_MODEL_INSTRUCTION, ModelTransportError, build_model_request, evaluate_semantic_updates, parse_model_response, render_model_payload


def _policy(category=Category.AI):
    return QueryPolicy(category=category, allowed_query_groups=(category.value,), recency=timedelta(days=7), missing_date_fallback=True, exact_title_lookback=timedelta(days=7), exact_url_lookback=timedelta(days=7), exact_identity_lookback=timedelta(days=7), cross_category_exact_url=True)


def _candidate(cid="c1", title="Product launch", snippet="A product update", category=Category.AI, url=None):
    return CandidateArticle(candidate_id=cid, category=category, query_group=category.value, title=title, snippet=snippet, original_url=url, canonical_url=url, published_at="2026-06-30T00:00:00Z", published_evidence="source", observed_at="2026-07-01T00:00:00Z", evaluated_at="2026-07-01T00:00:00Z")


def _history(aid="h1", title="Product launch", snippet="A product update", category=Category.AI, url=None, occurred="2026-06-01T00:00:00Z", oid=None):
    return HistoryMatch(article_id=aid, observation_id=oid or "o-" + aid, category=category, occurred_at=occurred, title=title, snippet=snippet, canonical_url=url, identity_basis="title_only")


def _event(c, decision=DecisionCode.KEEP, reasons=(ReasonCode.OK_KEEP,), article_ids=(), obs_ids=(), ordinal=0):
    f = FilterResult(candidate=c, decision=decision, reasons=reasons, matched_article_ids=tuple(article_ids), matched_observation_ids=tuple(obs_ids), trust_tier=TrustTier.UNKNOWN, evaluated_publication_time=None, ordinal=ordinal)
    return EventCandidate(candidate=c, filter_result=f, query_policy=_policy(c.category))


class TestModelParser(unittest.TestCase):
    def test_request_field_and_total_bounds(self):
        c = _candidate(title="T" * 600, snippet="C" * 3000, url="https://example.com/" + "u" * 3000)
        h = _history(title="H" * 600, snippet="H" * 3000, url="https://example.org/" + "h" * 3000)
        req = build_model_request(c, h)
        self.assertEqual(req.instruction, FIXED_MODEL_INSTRUCTION)
        self.assertEqual(len(req.candidate_title), 512)
        self.assertEqual(len(req.candidate_snippet), 2048)
        self.assertEqual(len(req.candidate_url), 2048)
        payload = render_model_payload(req)
        obj = json.loads(payload)
        self.assertEqual(set(obj), {"instruction", "candidate", "history"})
        self.assertLessEqual(len(payload), 8192)
        self.assertEqual(set(obj["candidate"]), {"title", "snippet", "canonical_url", "category", "published_at", "evaluated_at"})

    def test_reject_fence_trailing_prose_duplicate_keys(self):
        good = '{"decision":"REWRITE","confidence":0.95,"reason":"same","facts":[]}'
        with self.assertRaises(ValueError): parse_model_response("```json" + good + "```")
        with self.assertRaises(ValueError): parse_model_response(good + " trailing")
        with self.assertRaises(ValueError): parse_model_response('{"decision":"REWRITE","decision":"REWRITE","confidence":0.95,"reason":"same","facts":[]}')

    def test_reject_nan_infinity_bool_confidence(self):
        for conf in ("NaN", "Infinity", "-Infinity", "true"):
            with self.assertRaises(ValueError): parse_model_response('{"decision":"REWRITE","confidence":' + conf + ',"reason":"same","facts":[]}')

    def test_reject_extra_missing_wrong_types_and_duplicate_facts(self):
        with self.assertRaises(ValueError): parse_model_response('{"decision":"REWRITE","confidence":0.95,"reason":"same","facts":[],"x":1}')
        with self.assertRaises(ValueError): parse_model_response('{"decision":"REWRITE","confidence":0.95,"reason":"same"}')
        with self.assertRaises(ValueError): parse_model_response('{"decision":"BAD","confidence":0.95,"reason":"same","facts":[]}')
        with self.assertRaises(ValueError): parse_model_response('{"decision":"REWRITE","confidence":0.95,"reason":"same","facts":["x","x"]}')
        with self.assertRaises(ValueError): parse_model_response('{"decision":"REWRITE","confidence":0.95,"reason":1,"facts":[]}')

    def test_exact_confidence_boundaries(self):
        self.assertEqual(parse_model_response('{"decision":"MATERIAL_UPDATE","confidence":0,"reason":"x","facts":[]}').confidence, 0.0)
        self.assertEqual(parse_model_response('{"decision":"REWRITE","confidence":1,"reason":"x","facts":[]}').confidence, 1.0)

    def test_only_transport_error_is_caught(self):
        c = _candidate(title="Product pricing", snippet="The prices are $20 and $30")
        h = _history(title="Product pricing", snippet="The price is $10")
        class Bad:
            def __call__(self, request): raise RuntimeError("programmer error")
        with self.assertRaises(RuntimeError): evaluate_semantic_updates((_event(c),), (h,), Bad(), 1)
        class Transport:
            def __call__(self, request): raise ModelTransportError("offline")
        result = evaluate_semantic_updates((_event(c),), (h,), Transport(), 1)[0]
        self.assertEqual(result.semantic_decision, SemanticDecision.pending_model_error)
        self.assertEqual(result.model_error_category, ModelErrorCategory.TRANSPORT_ERROR)
        self.assertTrue(result.model_used)


class TestBatchAPI(unittest.TestCase):
    def test_terminal_and_pending_phase2_matrix_no_model_calls(self):
        class Model:
            calls = 0
            def __call__(self, request): self.calls += 1; return '{}'
        model = Model()
        cases = [(DecisionCode.SUPPRESS_BATCH_EXACT, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED), (DecisionCode.SUPPRESS_EXACT_URL, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED), (DecisionCode.SUPPRESS_EXACT_IDENTITY, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED), (DecisionCode.SUPPRESS_RECENT_TITLE, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED), (DecisionCode.DROP_STALE, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED), (DecisionCode.DROP_BLOCKED_SOURCE, SemanticReasonCode.PHASE2_SUPPRESSED_OR_DROPPED), (DecisionCode.PENDING_MISSING_EVIDENCE, SemanticReasonCode.PHASE2_MISSING_EVIDENCE), (DecisionCode.PENDING_INVALID_EVIDENCE, SemanticReasonCode.PHASE2_INVALID_EVIDENCE), (DecisionCode.PENDING_HISTORY_UNAVAILABLE, SemanticReasonCode.PHASE2_HISTORY_UNAVAILABLE)]
        events = tuple(_event(_candidate("c" + str(i)), decision=d, ordinal=i) for i, (d, _) in enumerate(cases))
        out = evaluate_semantic_updates(events, (), model, 0)
        self.assertEqual(model.calls, 0)
        for index, (r, (_, reason)) in enumerate(zip(out, cases)):
            self.assertEqual(r.semantic_decision, SemanticDecision.bypass_phase2_terminal if r.phase2_decision.name.startswith(("SUPPRESS", "DROP")) else SemanticDecision.pending_review)
            self.assertEqual(r.semantic_reasons, (reason,))
            self.assertEqual(r.matched_candidate_ids, ("c" + str(index),))

    def test_keep_without_match_is_distinct(self):
        c = _candidate("c1", title="Unique event", snippet="unrelated")
        result = evaluate_semantic_updates((_event(c),), (_history("h1", title="Other event", snippet="different"),), lambda req: "", 0)[0]
        self.assertEqual(result.semantic_decision, SemanticDecision.distinct_event)
        self.assertEqual(result.semantic_reasons, (SemanticReasonCode.DISTINCT_EVENT,))
        self.assertFalse(result.model_used)

    def test_possible_update_requires_all_history_ids(self):
        c = _candidate("c1", title="Product pricing update", snippet="The prices are $20 and $30")
        e = _event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1", "missing"), ordinal=4)
        r = evaluate_semantic_updates((e,), (_history("h1", title="Product pricing update", snippet="The price is $10"),), lambda req: "", 1)[0]
        self.assertEqual(r.semantic_decision, SemanticDecision.pending_review)
        self.assertEqual(r.semantic_reasons, (SemanticReasonCode.MISSING_HISTORY_MATCH,))
        self.assertEqual(r.matched_history_ids, ("h1",))

    def test_directional_cache_and_reverse_pair_calls(self):
        c1 = _candidate("c1", title="Product pricing update", snippet="The price is $20")
        c2 = _candidate("c2", title="Product pricing update", snippet="The price is $10")
        h1 = _history("h1", title="Product pricing update", snippet="The prices are $10 and $11")
        h2 = _history("h2", title="Product pricing update", snippet="The prices are $20 and $21")
        class Model:
            def __init__(self): self.requests = []
            def __call__(self, request): self.requests.append((request.candidate_title, request.history_title)); return '{"decision":"MATERIAL_UPDATE","confidence":0.8,"reason":"changed","facts":[]}'
        m = Model()
        out = evaluate_semantic_updates((_event(c1, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",)), _event(c2, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h2",))), (h1, h2), m, 4)
        self.assertEqual(len(m.requests), 2)
        self.assertEqual([x.ordinal for x in out], [0, 0])
        self.assertNotEqual(out[0].candidate_id, out[1].candidate_id)

    def test_zero_budget_ordered_pending_results(self):
        cs = tuple(_candidate("c" + str(i), title="Product pricing update", snippet="The prices are $" + str(20 + i) + " and $" + str(30 + i)) for i in range(3))
        hs = (_history("h", title="Product pricing update", snippet="The price is $10"),)
        out = evaluate_semantic_updates(tuple(_event(c, ordinal=i) for i, c in enumerate(cs)), hs, lambda req: (_ for _ in ()).throw(AssertionError()), 0)
        self.assertEqual([r.candidate_id for r in out], ["c0", "c1", "c2"])
        self.assertTrue(all(r.semantic_decision is SemanticDecision.pending_review for r in out))
        self.assertTrue(all(r.semantic_reasons == (SemanticReasonCode.BUDGET_EXHAUSTED,) for r in out))
        self.assertTrue(all(not r.model_used for r in out))

    def test_bool_negative_and_wrong_type_budget_rejected_preloop(self):
        c = _candidate("c1")
        class Model:
            calls = 0
            def __call__(self, request): self.calls += 1; return '{}'
        for budget in (True, -1, 1.0, "1", None):
            m = Model()
            with self.assertRaises(ValueError): evaluate_semantic_updates((_event(c),), (), m, budget)
            self.assertEqual(m.calls, 0)

    def test_input_order_and_ordinal_preserved(self):
        c1 = _candidate("c1", title="Unique one", snippet="one")
        c2 = _candidate("c2", title="Unique two", snippet="two")
        out = evaluate_semantic_updates((_event(c2, ordinal=9), _event(c1, ordinal=3)), (), lambda req: "", 0)
        self.assertEqual([(r.candidate_id, r.ordinal) for r in out], [("c2", 9), ("c1", 3)])

    def test_deterministic_repeat_byte_equivalent(self):
        c = _candidate("c1", title="Product pricing update", snippet="The prices are $20 and $30")
        h = _history("h1", title="Product pricing update", snippet="The price is $10")
        model = lambda req: '{"decision":"MATERIAL_UPDATE","confidence":0.8,"reason":"changed","facts":[]}'
        a = evaluate_semantic_updates((_event(c),), (h,), model, 1)
        b = evaluate_semantic_updates((_event(c),), (h,), model, 1)
        self.assertEqual(a, b)
        self.assertEqual([r.semantic_decision for r in a], [SemanticDecision.material_update])

    def test_programmer_exception_surfaces_and_history_unchanged(self):
        c = _candidate("c1", title="Product pricing update", snippet="The prices are $20 and $30")
        h = _history("h1", title="Product pricing update", snippet="The price is $10")
        before = (h,)
        class Bad:
            def __call__(self, request): raise KeyError("bug")
        e = _event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",))
        with self.assertRaises(KeyError): evaluate_semantic_updates((e,), before, Bad(), 1)
        self.assertEqual(before, (h,))

    # ------------------------------------------------------------------
    # F1 - model-callable exception boundary regression
    # (parent finding 1: only ModelTransportError caught; ValueError/TypeError
    # raised by the model callable surface; parser ValueError still
    # classified as MALFORMED_MODEL_OUTPUT.)
    # ------------------------------------------------------------------

    def test_F1_callable_value_error_surfaces_not_masked(self):
        """A ValueError raised by the model callable must NOT be swallowed
        as MALFORMED_MODEL_OUTPUT."""
        c = _candidate(title="Product pricing", snippet="The prices are $20 and $30")
        h = _history(title="Product pricing", snippet="The prices are $10 and $11")

        class BadValueError:
            def __call__(self, request):
                raise ValueError("programmer bug in model callable")

        with self.assertRaises(ValueError) as ctx:
            evaluate_semantic_updates(
                (_event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",)),),
                (h,),
                BadValueError(),
                1,
            )
        self.assertIn("programmer bug", str(ctx.exception))

    def test_F1_callable_runtime_error_surfaces(self):
        c = _candidate(title="Product pricing", snippet="The prices are $20 and $30")
        h = _history(title="Product pricing", snippet="The prices are $10 and $11")

        class BadRuntime:
            def __call__(self, request):
                raise RuntimeError("kaboom")

        with self.assertRaises(RuntimeError):
            evaluate_semantic_updates(
                (_event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",)),),
                (h,),
                BadRuntime(),
                1,
            )

    def test_F1_callable_key_error_surfaces(self):
        c = _candidate(title="Product pricing", snippet="The prices are $20 and $30")
        h = _history(title="Product pricing", snippet="The prices are $10 and $11")

        class BadKey:
            def __call__(self, request):
                raise KeyError("missing-key")

        with self.assertRaises(KeyError):
            evaluate_semantic_updates(
                (_event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",)),),
                (h,),
                BadKey(),
                1,
            )

    def test_F1_parser_value_error_still_malformed(self):
        """The split try blocks must keep parse_value_error -> MALFORMED_OUTPUT."""
        c = _candidate(title="Product pricing", snippet="The prices are $20 and $30")
        h = _history(title="Product pricing", snippet="The prices are $10 and $11")

        class Malformed:
            def __call__(self, request):
                return '{"decision":"BAD","confidence":0.5,"reason":"x","facts":[]}'

        result = evaluate_semantic_updates(
            (_event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",)),),
            (h,),
            Malformed(),
            1,
        )[0]
        self.assertEqual(result.semantic_decision, SemanticDecision.pending_model_error)
        self.assertEqual(result.model_error_category, ModelErrorCategory.MALFORMED_OUTPUT)

    def test_F1_non_string_returned_still_malformed(self):
        c = _candidate(title="Product pricing", snippet="The prices are $20 and $30")
        h = _history(title="Product pricing", snippet="The prices are $10 and $11")

        class NonString:
            def __call__(self, request):
                return 12345

        result = evaluate_semantic_updates(
            (_event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1",)),),
            (h,),
            NonString(),
            1,
        )[0]
        self.assertEqual(result.semantic_decision, SemanticDecision.pending_model_error)
        self.assertEqual(result.model_error_category, ModelErrorCategory.MALFORMED_OUTPUT)

    # ------------------------------------------------------------------
    # F2 - PENDING_POSSIBLE_UPDATE with zero matched_article_ids returns PENDING_REVIEW
    # ------------------------------------------------------------------

    def test_F2_zero_ids_returns_pending_review_no_model_call(self):
        c = _candidate(title="Product pricing", snippet="The price is $20")
        e = _event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=(), ordinal=2)

        class Model:
            calls = 0

            def __call__(self, request):
                self.calls += 1
                return '{"decision":"MATERIAL_UPDATE","confidence":0.9,"reason":"x","facts":[]}'

        m = Model()
        out = evaluate_semantic_updates((e,), (), m, 5)
        self.assertEqual(m.calls, 0)
        self.assertEqual(out[0].semantic_decision, SemanticDecision.pending_review)
        self.assertEqual(out[0].semantic_reasons, (SemanticReasonCode.MISSING_HISTORY_MATCH,))

    def test_F2_zero_ids_does_not_query_history(self):
        """Zero referenced IDs must NOT trigger any history lookup."""
        c = _candidate(title="Product pricing", snippet="The price is $20")
        e = _event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=(), ordinal=2)

        class Model:
            calls = 0

            def __call__(self, request):
                self.calls += 1
                return '{"decision":"REWRITE","confidence":0.95,"reason":"same","facts":[]}'

        out = evaluate_semantic_updates((e,), (_history("h1"),), Model(), 5)
        self.assertEqual(out[0].semantic_decision, SemanticDecision.pending_review)
        self.assertEqual(out[0].semantic_reasons, (SemanticReasonCode.MISSING_HISTORY_MATCH,))

# ------------------------------------------------------------------
    # F3 - Referenced-history selection scores & sorts resolved rows.
    # Both candidate and history snippets contain TWO conflicting prices so
    # the rule engine is forced to MODEL_REQUIRED, the model is consulted
    # exactly once, and the second referenced ID (h2) wins because it has
    # the exact URL match.
    # ------------------------------------------------------------------

    def test_F3_second_referenced_scores_higher_and_selected(self):
        c = _candidate(
            "c1",
            title="Product pricing update",
            snippet="The prices are $20 and $30",
            url="https://example.com/article",
        )
        h1 = _history(
            "h1",
            title="Product pricing update",
            snippet="The prices are $15 and $25",
            url="https://example.com/old1",
            oid="o-h1",
            occurred="2026-05-01T00:00:00Z",
        )
        h2 = _history(
            "h2",
            title="Product pricing update",
            snippet="The prices are $10 and $11",
            url="https://example.com/article",
            oid="o-h2",
            occurred="2026-04-01T00:00:00Z",
        )

        model_calls = []

        class Model:
            def __call__(self, request):
                model_calls.append(request.history_url)
                if request.history_url == "https://example.com/article":
                    return '{"decision":"MATERIAL_UPDATE","confidence":0.85,"reason":"new","facts":[]}'
                return '{"decision":"REWRITE","confidence":0.95,"reason":"same","facts":[]}'

        e = _event(c, decision=DecisionCode.PENDING_POSSIBLE_UPDATE, article_ids=("h1", "h2"))
        out = evaluate_semantic_updates((e,), (h1, h2), Model(), 4)
        self.assertEqual(len(model_calls), 1)
        self.assertEqual(model_calls[0], "https://example.com/article")
        self.assertEqual(out[0].semantic_decision, SemanticDecision.material_update)

    def test_F3_preserves_all_resolved_observation_ids_in_pending(self):
            c = _candidate("c1", title="Product pricing update", snippet="The prices are $20 and $30")
            h1 = _history("h1", title="Product pricing update", snippet="The prices are $10 and $11", oid="o-h1")
            h2 = _history("h2", title="Product pricing update", snippet="The prices are $12 and $13", oid="o-h2")
            e = _event(
                c,
                decision=DecisionCode.PENDING_POSSIBLE_UPDATE,
                article_ids=("h1", "h2", "missing"),
            )
            out = evaluate_semantic_updates((e,), (h1, h2), lambda req: "{}", 5)
            self.assertEqual(out[0].semantic_decision, SemanticDecision.pending_review)
            self.assertEqual(out[0].semantic_reasons, (SemanticReasonCode.MISSING_HISTORY_MATCH,))
            self.assertEqual(out[0].matched_candidate_ids, ("c1",))
            self.assertEqual(sorted(out[0].matched_history_ids), ["h1", "h2"])
            self.assertEqual(sorted(out[0].matched_observation_ids), ["o-h1", "o-h2"])

# ------------------------------------------------------------------
    # F4 - Missing-ID result explainability. The pending result must
    # preserve the candidate's own ID and the resolved article/observation
    # IDs exactly.
    # ------------------------------------------------------------------

    def test_F4_pending_missing_history_preserves_phase2_ids(self):
        c = _candidate("c1", title="Product pricing update", snippet="The prices are $20 and $30")
        e = _event(
            c,
            decision=DecisionCode.PENDING_POSSIBLE_UPDATE,
            article_ids=("h1", "missing"),
            obs_ids=("o-h1",),
            ordinal=7,
        )
        out = evaluate_semantic_updates((e,), (_history("h1"),), lambda req: "{}", 1)
        self.assertEqual(out[0].semantic_decision, SemanticDecision.pending_review)
        self.assertEqual(out[0].semantic_reasons, (SemanticReasonCode.MISSING_HISTORY_MATCH,))
        self.assertEqual(out[0].matched_candidate_ids, ("c1",))
        self.assertEqual(out[0].matched_history_ids, ("h1",))
        self.assertEqual(out[0].matched_observation_ids, ("o-h1",))

if __name__ == "__main__":
    unittest.main()
