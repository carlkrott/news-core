"""Focused Run 7 subject grouping, editorial QC, and cache tests."""
from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from news_pipeline.briefing_contracts import BriefingInput
from news_pipeline.briefing_engine import BriefingEngine
from news_pipeline.briefing_ledger import CompleteRunResult
from news_pipeline.briefing_renderer import RenderResult, RenderStatus
from news_pipeline.briefing_summarizer import (
    CategorySummaryResult,
    SummarizerErrorCategory,
    SummarizerSession,
    SummaryItem,
    SummarySource,
)
from news_pipeline.contracts import CandidateArticle, DecisionCode, FilterResult, ReasonCode, TrustTier
from news_pipeline.editorial_qc import SubjectEditorialInput, subject_policy
from news_pipeline.event_contracts import (
    AdjudicationResult,
    EventCandidate,
    SemanticDecision,
    SemanticReasonCode,
    event_version_identity,
)
from news_pipeline.models import Category, Subject
from news_pipeline.policies import QueryPolicy


AS_OF = datetime(2026, 7, 1, 9, 0, tzinfo=timezone.utc)


def _policy(category: Category) -> QueryPolicy:
    return QueryPolicy(
        category=category,
        allowed_query_groups=(category.value,),
        recency=timedelta(days=7),
        missing_date_fallback=True,
        exact_title_lookback=timedelta(days=7),
        exact_url_lookback=timedelta(days=7),
        exact_identity_lookback=timedelta(days=7),
        cross_category_exact_url=True,
    )


def _briefing_input(
    category: Category,
    subject: Subject,
    event_id: str,
    candidate_id: str,
) -> BriefingInput:
    timestamp = "2026-07-01T06:00:00Z"
    url = f"https://example.com/{event_id}"
    article = CandidateArticle(
        candidate_id=candidate_id,
        category=category,
        query_group=category.value,
        title=f"{event_id} changed",
        snippet=f"Verified {event_id}",
        original_url=url,
        canonical_url=url,
        published_at=timestamp,
        published_evidence="source",
        observed_at=timestamp,
        evaluated_at=timestamp,
    )
    filtered = FilterResult(
        candidate=article,
        decision=DecisionCode.KEEP,
        reasons=(ReasonCode.OK_KEEP,),
        matched_article_ids=(),
        matched_observation_ids=(),
        trust_tier=TrustTier.UNKNOWN,
        evaluated_publication_time=None,
        ordinal=0,
    )
    event = EventCandidate(candidate=article, filter_result=filtered, query_policy=_policy(category))
    adjudication = AdjudicationResult(
        candidate_id=candidate_id,
        semantic_decision=SemanticDecision.distinct_event,
        phase2_decision=DecisionCode.KEEP,
        phase2_reasons=(ReasonCode.OK_KEEP,),
        semantic_reasons=(SemanticReasonCode.DISTINCT_EVENT,),
        cluster_id=event_id,
        matched_candidate_ids=(candidate_id,),
        matched_history_ids=(),
        matched_observation_ids=(),
        fact_deltas=(),
        model_used=False,
        model_confidence=None,
        model_error_category=None,
        ordinal=0,
        event_version=1,
        subject_id=subject.value,
    )
    return BriefingInput(event_candidate=event, adjudication=adjudication)


class _Ledger:
    def __init__(self) -> None:
        self.completed: list[tuple[str, tuple[Any, ...]]] = []

    def begin_run(self, **_: str) -> None:
        return None

    def seen_candidate_ids(self, ids: tuple[str, ...]) -> tuple[str, ...]:
        return ()

    def complete_run(self, run_id: str, updated: str, events: tuple[Any, ...]) -> CompleteRunResult:
        self.completed.append((run_id, tuple(events)))
        return CompleteRunResult(
            new_ids=tuple(event.candidate_id for event in events),
            already_seen_ids=(),
        )

    def fail_run(self, **_: str) -> None:
        raise AssertionError("subject test should not fail the ledger run")


class _SubjectSummarizer:
    def __init__(self) -> None:
        self.calls: list[tuple[Subject, tuple[str, ...]]] = []
        self._model_calls = 0

    @property
    def model_call_count(self) -> int:
        return self._model_calls

    @property
    def cache_hit_count(self) -> int:
        return 0

    def summarize_subject(self, subject: Subject, raw_inputs: tuple[Any, ...]) -> CategorySummaryResult:
        self.calls.append((subject, tuple(item.event_id for item in raw_inputs)))
        self._model_calls += 1
        items = tuple(
            SummaryItem(
                candidate_id=event_version_identity(subject.value, item.event_id, item.event_version),
                summary=f"{item.event_id} changed and matters.",
                source=SummarySource.MODEL,
                error_category=None,
                subject_id=subject.value,
                event_id=item.event_id,
                event_version=item.event_version,
                source_url=item.source_urls[0],
            )
            for item in raw_inputs
        )
        return CategorySummaryResult(items=items, model_used=True, cache_hit=False)


class _SubjectRenderer:
    def __init__(self) -> None:
        self.calls: list[tuple[str | None, tuple[str | None, ...]]] = []

    def render_briefing(self, records, upper_bound_utc, subject_id=None) -> RenderResult:
        self.calls.append((subject_id, tuple(record.event_id for record in records)))
        return RenderResult(
            status=RenderStatus.RENDERED if records else RenderStatus.NO_DELIVERY,
            chunks=(f"subject:{subject_id}",) if records else (),
        )


class SubjectGroupingTests(unittest.TestCase):
    def test_professional_av_lanes_share_one_subject_call_and_render(self) -> None:
        inputs = (
            _briefing_input(Category.AUDIOVISUAL, Subject.PROFESSIONAL_AV, "av-1", "cand-av-1"),
            _briefing_input(Category.AV_CORPORATE, Subject.PROFESSIONAL_AV, "av-2", "cand-av-2"),
        )
        summarizer = _SubjectSummarizer()
        renderer = _SubjectRenderer()
        result = BriefingEngine(_Ledger(), summarizer, renderer).execute_briefing_run(
            "run-7-av", inputs, AS_OF
        )
        self.assertEqual(result.status.value, "COMPLETED")
        self.assertEqual(summarizer.calls, [(Subject.PROFESSIONAL_AV, ("av-1", "av-2"))])
        self.assertEqual(renderer.calls, [(Subject.PROFESSIONAL_AV.value, ("av-1", "av-2"))])
        self.assertEqual(result.model_call_count, 1)

    def test_audio_engineering_never_enters_professional_av_context(self) -> None:
        inputs = (
            _briefing_input(Category.AUDIO_ENGINEERING, Subject.AUDIO_ENGINEERING, "audio-1", "cand-audio"),
            _briefing_input(Category.AUDIOVISUAL, Subject.PROFESSIONAL_AV, "av-1", "cand-av"),
        )
        summarizer = _SubjectSummarizer()
        renderer = _SubjectRenderer()
        BriefingEngine(_Ledger(), summarizer, renderer).execute_briefing_run(
            "run-7-isolated", inputs, AS_OF
        )
        self.assertEqual(
            summarizer.calls,
            [
                (Subject.AUDIO_ENGINEERING, ("audio-1",)),
                (Subject.PROFESSIONAL_AV, ("av-1",)),
            ],
        )
        self.assertEqual(
            {subject for subject, _ in summarizer.calls},
            {Subject.AUDIO_ENGINEERING, Subject.PROFESSIONAL_AV},
        )


class SubjectSummarizerTests(unittest.TestCase):
    def _input(self, event_id: str = "evt-1") -> SubjectEditorialInput:
        return SubjectEditorialInput(
            subject=Subject.PROFESSIONAL_AV,
            event_id=event_id,
            event_version=1,
            title=f"{event_id} title",
            fact_deltas=(),
            source_urls=(f"https://example.com/{event_id}",),
            policy=subject_policy(Subject.PROFESSIONAL_AV),
        )

    def test_qc_approved_subject_output_is_cached(self) -> None:
        calls: list[bytes] = []

        def transport(request: bytes) -> bytes:
            calls.append(request)
            body = json.loads(request)
            return json.dumps(
                {
                    "items": [
                        {
                            "subject": body["subject"],
                            "event_id": item["event_id"],
                            "event_version": item["event_version"],
                            "what_changed": "The control system shipped.",
                            "why_it_matters": "It expands production coverage.",
                            "source_url": item["source_urls"][0],
                            "fact_deltas": item["fact_deltas"],
                        }
                        for item in body["items"]
                    ]
                }
            ).encode()

        session = SummarizerSession(transport)
        item = self._input()
        first = session.summarize_subject(Subject.PROFESSIONAL_AV, (item,))
        second = session.summarize_subject(Subject.PROFESSIONAL_AV, (item,))
        self.assertEqual(len(calls), 1)
        self.assertTrue(first.model_used)
        self.assertTrue(second.cache_hit)
        self.assertEqual(second.items[0].source, SummarySource.CACHE)

    def test_hallucinated_url_is_fallback_and_not_cached(self) -> None:
        def transport(request: bytes) -> bytes:
            body = json.loads(request)
            item = body["items"][0]
            return json.dumps(
                {
                    "items": [
                        {
                            "subject": body["subject"],
                            "event_id": item["event_id"],
                            "event_version": item["event_version"],
                            "what_changed": "The control system shipped.",
                            "why_it_matters": "It expands production coverage.",
                            "source_url": "https://example.org/not-allowed",
                            "fact_deltas": item["fact_deltas"],
                        }
                    ]
                }
            ).encode()

        cache: dict[bytes, Any] = {}
        session = SummarizerSession(transport, cache)
        result = session.summarize_subject(Subject.PROFESSIONAL_AV, (self._input(),))
        self.assertEqual(result.items[0].source, SummarySource.FALLBACK)
        self.assertEqual(result.items[0].error_category, SummarizerErrorCategory.MALFORMED_OUTPUT)
        self.assertEqual(cache, {})


class NoDeliveryRendererTests(unittest.TestCase):
    def test_zero_story_subject_is_no_delivery(self) -> None:
        result = __import__("news_pipeline.briefing_renderer", fromlist=["render_briefing"]).render_briefing(
            [], AS_OF, subject_id=Subject.AI.value
        )
        self.assertEqual(result.status, RenderStatus.NO_DELIVERY)
        self.assertEqual(result.chunks, ())


if __name__ == "__main__":
    unittest.main()
