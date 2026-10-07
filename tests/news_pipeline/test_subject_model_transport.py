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
from news_pipeline.subject_model_transport import configured_transport


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
        response = _Response(
            json.dumps({"choices": [{"message": {"content": '{"items": []}'}}]}).encode()
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
            summarizer.summarize_category(
                Category.AI,
                (SummarizerInput("candidate-1", "A title", "A snippet", "https://example.test/a"),),
            )

        self.assertEqual(len(calls), 1)
        request, timeout = calls[0]
        self.assertEqual(request.full_url, "http://model.example.test/v1/chat/completions")
        self.assertEqual(json.loads(request.data)["model"], "approved-model")
        self.assertEqual(timeout, 10.0)
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
        self.assertEqual(timeout, 10.0)
        self.assertEqual(response.read_limit, 32769)
        self.assertTrue(response.closed)

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


if __name__ == "__main__":
    unittest.main()
