"""Hermetic regressions for the opt-in subject model transport."""
from __future__ import annotations

import json
import http.client
import unittest
import urllib.request
from unittest.mock import patch

from news_pipeline import report_builder
from news_pipeline.briefing_summarizer import (
    Category,
    SummarizerErrorCategory,
    SummarizerInput,
    SummarizerSession,
    SummarizerTransportError,
    SummarySource,
)
from news_pipeline.editorial_qc import SubjectEditorialInput, subject_policy
from news_pipeline.models import Subject
from news_pipeline import subject_model_transport
from news_pipeline.subject_model_transport import configured_transport, timeout_from_p95


class _Response:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.read_limit: int | None = None
        self.closed = False

    def read(self, limit: int) -> bytes:
        self.read_limit = limit
        return self.body[:limit]

    def close(self) -> None:
        self.closed = True


class _IncompleteReadResponse(_Response):
    def read(self, limit: int) -> bytes:
        self.read_limit = limit
        raise http.client.IncompleteRead(b"private partial response", 128)


class SubjectModelTransportTests(unittest.TestCase):
    def test_report_builder_uses_configured_route_for_one_model_call(self) -> None:
        calls: list[tuple[urllib.request.Request, float]] = []
        input_item = SubjectEditorialInput(
            subject=Subject.PROFESSIONAL_AV,
            event_id="event-1",
            event_version=1,
            title="A verified event",
            fact_deltas=(),
            source_urls=("https://example.test/a",),
            policy=subject_policy(Subject.PROFESSIONAL_AV),
        )
        model_content = json.dumps(
            {
                "items": [
                    {
                        "subject": Subject.PROFESSIONAL_AV.value,
                        "event_id": input_item.event_id,
                        "event_version": input_item.event_version,
                        "what_changed": "A verified event occurred.",
                        "why_it_matters": "It is relevant to the subject.",
                        "source_url": input_item.source_urls[0],
                        "fact_deltas": [],
                    }
                ]
            }
        )
        response = _Response(
            json.dumps({"choices": [{"message": {"content": model_content}}]}).encode()
        )

        def opener(request: urllib.request.Request, *, timeout: float) -> _Response:
            calls.append((request, timeout))
            return response

        with patch.dict(
            "os.environ",
            {
                "NEWS_SUBJECT_MODEL_BASE_URL": "http://model.example.test/v1",
                "NEWS_SUBJECT_MODEL": "approved-model",
            },
            clear=True,
        ), patch(
            "news_pipeline.subject_model_transport.urllib.request.build_opener",
            return_value=type("FakeOpener", (), {"open": staticmethod(opener)})(),
        ):
            summarizer = report_builder._default_subject_summarizer()
            result = summarizer.summarize_subject(
                Subject.PROFESSIONAL_AV, (input_item,)
            )

        self.assertEqual(len(calls), 1)
        self.assertEqual(summarizer.model_call_count, 1)
        self.assertEqual(len(result.items), 1)
        self.assertIs(result.items[0].source, SummarySource.MODEL)
        self.assertIsNone(result.items[0].error_category)
        self.assertEqual(result.items[0].event_id, input_item.event_id)
        request, timeout = calls[0]
        self.assertEqual(request.full_url, "http://model.example.test/v1/chat/completions")
        self.assertEqual(json.loads(request.data)["model"], "approved-model")
        request_content = json.loads(request.data)["messages"][1]["content"]
        self.assertEqual(json.loads(request_content)["subject"], Subject.PROFESSIONAL_AV.value)
        self.assertEqual(timeout, subject_model_transport._TIMEOUT_SECONDS)
        self.assertTrue(response.closed)

    def test_configured_route_sends_one_bounded_request_and_returns_content(self) -> None:
        calls: list[tuple[object, float]] = []
        response = _Response(
            json.dumps({"choices": [{"message": {"content": '{"items": []}'}}]}).encode()
        )

        def opener(request: urllib.request.Request, *, timeout: float) -> _Response:
            calls.append((request, timeout))
            return response

        transport = configured_transport(
            environ={
                "NEWS_SUBJECT_MODEL_BASE_URL": "http://model.example.test/v1",
                "NEWS_SUBJECT_MODEL": "approved-model",
            },
            opener=opener,
        )
        result = transport(b'{"items":[]}')

        self.assertEqual(result, b'{"items": []}')
        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        self.assertEqual(request.full_url, "http://model.example.test/v1/chat/completions")
        payload = json.loads(request.data)
        self.assertEqual(payload["model"], "approved-model")
        self.assertFalse(payload["stream"])
        self.assertEqual(timeout, subject_model_transport._TIMEOUT_SECONDS)
        self.assertEqual(response.read_limit, 32769)
        self.assertTrue(response.closed)

    @staticmethod
    def _one_input() -> SubjectEditorialInput:
        return SubjectEditorialInput(
            subject=Subject.PROFESSIONAL_AV,
            event_id="event-1",
            event_version=1,
            title="A verified event",
            fact_deltas=(),
            source_urls=("https://example.test/a",),
            policy=subject_policy(Subject.PROFESSIONAL_AV),
        )

    def test_prompt_requires_model_to_write_nonempty_prose_fields(self) -> None:
        """Live replies had valid JSON with null what_changed/why_it_matters."""
        captured: list[urllib.request.Request] = []

        def opener(request: urllib.request.Request, *, timeout: float) -> _Response:
            captured.append(request)
            return _Response(
                json.dumps({"choices": [{"message": {"content": '{"items": []}'}}]}).encode()
            )

        transport = configured_transport(
            environ={
                "NEWS_SUBJECT_MODEL_BASE_URL": "http://model.example.test/v1",
                "NEWS_SUBJECT_MODEL": "approved-model",
            },
            opener=opener,
        )
        transport(b'{"items":[]}')

        system = json.loads(captured[0].data)["messages"][0]["content"]
        for field in ("what_changed", "why_it_matters"):
            self.assertIn(field, system)
        self.assertIn("non-empty string", system)
        self.assertIn("never null", system)
        self.assertIn("256", system)

    def test_null_prose_reply_stays_malformed_output_fallback(self) -> None:
        """Validation is not loosened: null prose is malformed, not accepted."""
        input_item = self._one_input()
        content = json.dumps(
            {
                "items": [
                    {
                        "subject": Subject.PROFESSIONAL_AV.value,
                        "event_id": input_item.event_id,
                        "event_version": input_item.event_version,
                        "what_changed": None,
                        "why_it_matters": None,
                        "source_url": input_item.source_urls[0],
                        "fact_deltas": [],
                    }
                ]
            }
        )
        summarizer = SummarizerSession(lambda _request: content.encode())
        result = summarizer.summarize_subject(Subject.PROFESSIONAL_AV, (input_item,))

        self.assertEqual(len(result.items), 1)
        self.assertIs(result.items[0].source, SummarySource.FALLBACK)
        self.assertIs(result.items[0].error_category, SummarizerErrorCategory.MALFORMED_OUTPUT)

    def test_missing_configuration_keeps_exact_deterministic_fallback_without_io(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            summarizer = report_builder._default_subject_summarizer()
        result = summarizer.summarize_category(
            Category.AI,
            (SummarizerInput("candidate-1", "A title", "A snippet", "https://example.test/a"),),
        )
        self.assertEqual(summarizer.model_call_count, 1)
        self.assertEqual(result.items[0].source, SummarySource.FALLBACK)
        self.assertEqual(
            result.items[0].error_category, SummarizerErrorCategory.TRANSPORT_ERROR
        )

    def test_transport_failure_is_typed_and_sanitized(self) -> None:
        def opener(_request: object, *, timeout: float) -> object:
            raise TimeoutError("secret-value and /private/path")

        transport = configured_transport(
            environ={
                "NEWS_SUBJECT_MODEL_BASE_URL": "http://model.example.test/v1",
                "NEWS_SUBJECT_MODEL": "approved-model",
                "NEWS_SUBJECT_MODEL_API_KEY": "secret-value",
            },
            opener=opener,
        )
        with self.assertRaises(SummarizerTransportError) as raised:
            transport(b'{"items":[]}')
        self.assertEqual(str(raised.exception), "subject model transport failed")
        self.assertNotIn("secret-value", str(raised.exception))
        self.assertNotIn("/private/path", str(raised.exception))

    def test_incomplete_http_read_uses_typed_sanitized_fallback(self) -> None:
        response = _IncompleteReadResponse(b"")
        transport = configured_transport(
            environ={
                "NEWS_SUBJECT_MODEL_BASE_URL": "http://model.example.test/v1",
                "NEWS_SUBJECT_MODEL": "approved-model",
            },
            opener=lambda _request, *, timeout: response,
        )
        with self.assertRaises(SummarizerTransportError) as raised:
            transport(b'{"items":[]}')
        self.assertEqual(str(raised.exception), "subject model transport failed")
        self.assertNotIn("private partial response", str(raised.exception))
        self.assertTrue(response.closed)

        summarizer = SummarizerSession(transport)
        result = summarizer.summarize_category(
            Category.AI,
            (SummarizerInput("candidate-1", "A title", "A snippet", "https://example.test/a"),),
        )
        self.assertEqual(result.items[0].source, SummarySource.FALLBACK)
        self.assertEqual(
            result.items[0].error_category, SummarizerErrorCategory.TRANSPORT_ERROR
        )

    def test_oversized_response_is_rejected_without_leaking_body(self) -> None:
        response = _Response(b"x" * 32769)
        transport = configured_transport(
            environ={
                "NEWS_SUBJECT_MODEL_BASE_URL": "http://model.example.test/v1",
                "NEWS_SUBJECT_MODEL": "approved-model",
            },
            opener=lambda _request, *, timeout: response,
        )
        with self.assertRaisesRegex(
            SummarizerTransportError, "subject model response exceeds configured bounds"
        ):
            transport(b'{"items":[]}')
        self.assertEqual(response.read_limit, 32769)

    def test_route_rejects_userinfo(self) -> None:
        with self.assertRaisesRegex(ValueError, "configuration is invalid"):
            configured_transport(
                environ={
                    "NEWS_SUBJECT_MODEL_BASE_URL": "http://user:secret@model.example.test/v1",
                    "NEWS_SUBJECT_MODEL": "approved-model",
                },
                opener=lambda *_args, **_kwargs: self.fail("must not call transport"),
            )


class TimeoutRuleTests(unittest.TestCase):
    def test_timeout_is_ceil_of_p95_times_margin(self) -> None:
        self.assertEqual(timeout_from_p95(10.0, margin=2.0, minimum=5.0, maximum=60.0), 20.0)
        self.assertEqual(timeout_from_p95(10.1, margin=1.5, minimum=5.0, maximum=60.0), 16.0)

    def test_timeout_is_bounded_by_min_and_max(self) -> None:
        self.assertEqual(timeout_from_p95(0.5, margin=2.0, minimum=5.0, maximum=60.0), 5.0)
        self.assertEqual(timeout_from_p95(100.0, margin=2.0, minimum=5.0, maximum=60.0), 60.0)

    def test_timeout_rejects_invalid_inputs(self) -> None:
        for p95 in (0.0, -1.0, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                timeout_from_p95(p95)

    def test_configured_timeout_follows_documented_rule(self) -> None:
        m = subject_model_transport
        self.assertEqual(
            m._TIMEOUT_SECONDS,
            timeout_from_p95(m._MEASURED_P95_SECONDS, margin=m._TIMEOUT_MARGIN,
                             minimum=m._TIMEOUT_MIN_SECONDS, maximum=m._TIMEOUT_MAX_SECONDS),
        )
        self.assertGreater(m._TIMEOUT_SECONDS, m._MEASURED_P95_SECONDS)


if __name__ == "__main__":
    unittest.main()
