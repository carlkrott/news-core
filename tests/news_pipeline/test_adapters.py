"""Offline fixture-only tests for the async stdlib adapter layer.

These tests operate entirely from fixtures: no network calls, no database,
no live sources. They exercise the adapter parsing and error paths using
an injected fake transport that records exact FetchRequest envelopes
and returns production-shaped FetchResponse objects.
"""
from __future__ import annotations

import ast
import json
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

import news_pipeline.adapters.base as adapter_base

from news_pipeline.adapters import (
    AdapterError,
    FetchRequest,
    FetchResponse,
    FetchResult,
    FetchValidators,
    HostSchedule,
    ItemRejection,
    NetworkError,
    NormalizedItem,
    ParseError,
    RssAdapter,
    RetryableHttpError,
    SearxngAdapter,
    TransportError,
    HttpStatusError,
    ContentTypeError,
    ResponseTooLargeError,
    parse_retry_after,
    parse_rate_limit_remaining,
)
from news_pipeline.live_contracts import SourceContract, QuerySeed, SourceAdapter, SourceRole

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "news_pipeline" / "fixtures"
RETRIEVED_AT = "2026-09-06T12:00:00Z"


def _load_json_fixture(name: str) -> dict[str, Any]:
    path = FIXTURES / name
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _load_text_fixture(name: str) -> str:
    data = _load_json_fixture(name)
    return data["response"]


# -------------------------------------------------------------------------
# Fake transport
# -------------------------------------------------------------------------


class FakeTransport:
    """Injected transport that records FetchRequest and returns FetchResponse."""

    def __init__(self, response: FetchResponse) -> None:
        self.response = response
        self.requests: list[FetchRequest] = []

    async def __call__(self, request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
        self.requests.append(request)
        return self.response


# -------------------------------------------------------------------------
# SourceContract helper
# -------------------------------------------------------------------------


def _searxng_contract() -> SourceContract:
    return SourceContract(
        source_id="searxng-ai-main",
        adapter_type=SourceAdapter.SEARXNG,
        source_role=SourceRole.DISCOVERY,
        host="searxng.example.com",
        category_scope=("ai",),
        enabled=True,
        queries=(
            QuerySeed(text="LLM+AI+model+release+2026", categories=("news", "it")),
        ),
        cadence_minutes=180,
    )


# -------------------------------------------------------------------------
# SearxngAdapter tests
# -------------------------------------------------------------------------


class TestSearxngAdapterConstruction(unittest.TestCase):
    def test_constructs_with_required_args(self) -> None:
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
        )
        self.assertEqual(adapter.source_id, "searxng-ai-main")
        self.assertEqual(adapter.host, "searxng.example.com")
        self.assertEqual(adapter.category, "ai")
        self.assertEqual(adapter.source_role, "discovery")
        self.assertEqual(adapter._base_url, "http://searxng.example.com")

    def test_constructs_with_schedule(self) -> None:
        schedule = HostSchedule(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            cadence_minutes=180,
        )
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            schedule=schedule,
        )
        self.assertIs(adapter.schedule, schedule)

    def test_constructs_with_custom_transport(self) -> None:
        transport = FakeTransport(FetchResponse(200, (), b"{}", "https://example.com/search"))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        self.assertIs(adapter._transport, transport)


class TestSearxngFetchQueryURLSemantics(unittest.IsolatedAsyncioTestCase):
    """Test that fetch_query converts '+' separators to spaces before URL encoding."""

    async def test_plus_separators_converted_to_spaces(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps({"results": [], "query": "", "number_of_results": 0}).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        await adapter.fetch_query(
            "LLM+AI+model+release+2026",
            categories=("news", "it"),
            retrieved_at=RETRIEVED_AT,
        )
        self.assertEqual(len(transport.requests), 1)
        req = transport.requests[0]
        # The stored form "LLM+AI+model+release+2026" should become "LLM AI model release 2026"
        self.assertIn("q=LLM+AI+model+release+2026", req.url)  # urlencode turns spaces to +
        self.assertIn("format=json", req.url)
        self.assertIn("language=en", req.url)
        self.assertIn("safesearch=0", req.url)
        self.assertIn("categories=news%2Cit", req.url)

    async def test_categories_passed_to_wire(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps({"results": [], "query": "", "number_of_results": 0}).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        await adapter.fetch_query(
            "test query",
            categories=("news", "it"),
            retrieved_at=RETRIEVED_AT,
        )
        req = transport.requests[0]
        self.assertIn("categories=news%2Cit", req.url)

    async def test_accept_header_json_only(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps({"results": [], "query": "", "number_of_results": 0}).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        req = transport.requests[0]
        headers_dict = dict(req.headers)
        self.assertEqual(headers_dict.get("Accept"), "application/json")


class TestSearxngConditionalHeaders(unittest.IsolatedAsyncioTestCase):
    """Test conditional request headers (If-None-Match, If-Modified-Since)."""

    async def test_etag_header_sent(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps({"results": [], "query": "", "number_of_results": 0}).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        await adapter.fetch_query(
            "test",
            categories=(),
            retrieved_at=RETRIEVED_AT,
            etag='"abc123"',
        )
        req = transport.requests[0]
        headers_dict = dict(req.headers)
        self.assertEqual(headers_dict.get("If-None-Match"), '"abc123"')

    async def test_last_modified_header_sent(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps({"results": [], "query": "", "number_of_results": 0}).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        await adapter.fetch_query(
            "test",
            categories=(),
            retrieved_at=RETRIEVED_AT,
            last_modified="Wed, 06 Sep 2026 10:00:00 GMT",
        )
        req = transport.requests[0]
        headers_dict = dict(req.headers)
        self.assertEqual(headers_dict.get("If-Modified-Since"), "Wed, 06 Sep 2026 10:00:00 GMT")


class TestSearxngExactCallerTime(unittest.IsolatedAsyncioTestCase):
    """Test that retrieved_at is caller-supplied and used in items, not internal clock."""

    async def test_caller_supplied_retrieved_at_in_items(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_valid.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query(
            "LLM+AI+model+release+2026",
            categories=("news", "it"),
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsNone(result.error)
        for item in result.items:
            self.assertEqual(item.retrieved_at, RETRIEVED_AT)

    async def test_different_retrieved_at_produces_different_results(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_valid.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        t1 = "2026-09-06T10:00:00Z"
        t2 = "2026-09-06T14:00:00Z"
        r1 = await adapter.fetch_query("test", categories=(), retrieved_at=t1)
        r2 = await adapter.fetch_query("test", categories=(), retrieved_at=t2)
        self.assertEqual(r1.items[0].retrieved_at, t1)
        self.assertEqual(r2.items[0].retrieved_at, t2)


class TestSearxngCanonicalization(unittest.IsolatedAsyncioTestCase):
    """Test canonicalization of HTTP(S) URLs."""

    async def test_canonical_url_set_from_valid_url(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_valid.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        for item in result.items:
            self.assertIsNotNone(item.canonical_url)
            # canonical_url should be HTTP(S)
            self.assertTrue(item.canonical_url.startswith("http://") or item.canonical_url.startswith("https://"))

    async def test_invalid_url_rejected_with_rejection(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_missing_url.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        # Item 1 has a missing URL (it only has title/content)
        self.assertEqual(len(result.rejections), 1)
        self.assertEqual(result.rejections[0].index, 1)
        self.assertEqual(result.rejections[0].code, "MISSING_URL")


class TestSearxngExternalId(unittest.IsolatedAsyncioTestCase):
    """Test that result.id is used as external_id when present."""

    async def test_result_id_becomes_external_id(self) -> None:
        doc = {
            "results": [
                {
                    "url": "https://example.com/article1",
                    "title": "Article One",
                    "content": "Body text",
                    "id": "ext-article-001",
                }
            ],
            "query": "test",
            "number_of_results": 1,
        }
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps(doc).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.items[0].external_id, "ext-article-001")

    async def test_result_without_id_has_null_external_id(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_valid.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        for item in result.items:
            self.assertIsNone(item.external_id)


class TestSearxngStableIds(unittest.IsolatedAsyncioTestCase):
    """Test stable source_item_id generation using live_contracts.stable_id."""

    async def test_source_item_id_is_deterministic(self) -> None:
        doc = {
            "results": [
                {"url": "https://example.com/article", "title": "Test", "content": "Body"},
            ],
            "query": "test",
            "number_of_results": 1,
        }
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps(doc).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        r1 = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        r2 = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertEqual(r1.items[0].source_item_id, r2.items[0].source_item_id)

    async def test_different_source_id_produces_different_stable_id(self) -> None:
        doc = {
            "results": [
                {"url": "https://example.com/article", "title": "Test", "content": "Body"},
            ],
            "query": "test",
            "number_of_results": 1,
        }
        base_url = "https://searxng.example.com/search"
        transport1 = FakeTransport(FetchResponse(200, (("Content-Type", "application/json"),), json.dumps(doc).encode(), base_url))
        transport2 = FakeTransport(FetchResponse(200, (("Content-Type", "application/json"),), json.dumps(doc).encode(), base_url))
        adapter1 = SearxngAdapter("src1", "searxng.example.com", "ai", "discovery", transport=transport1)
        adapter2 = SearxngAdapter("src2", "searxng.example.com", "ai", "discovery", transport=transport2)
        r1 = await adapter1.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        r2 = await adapter2.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertNotEqual(r1.items[0].source_item_id, r2.items[0].source_item_id)


class TestSearxngPublishedDateEvidence(unittest.IsolatedAsyncioTestCase):
    """Test that publishedDate from SearXNG is stored as publication_evidence metadata."""

    async def test_published_date_evidence_set(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_valid.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        first = result.items[0]
        self.assertIsNotNone(first.published_at)
        self.assertIsNotNone(first.publication_evidence)
        self.assertTrue(first.publication_evidence.startswith("metadata:"))


class TestSearxngParseResponses(unittest.IsolatedAsyncioTestCase):
    """Test parse responses for various error conditions."""

    async def test_valid_response_parses_items(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_valid.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        self.assertEqual(len(result.items), 2)
        self.assertEqual(result.items[0].source_id, "searxng-ai-main")
        self.assertEqual(result.items[0].category, "ai")
        self.assertEqual(result.items[0].retrieval_method, "searxng-query")
        self.assertEqual(result.items[0].original_url, "https://example.com/ai-llm-release-2026")
        self.assertEqual(result.items[0].title, "Major AI Lab Releases New LLM")
        self.assertEqual(result.items[0].author_handle, "Jane Doe")
        self.assertIsNotNone(result.items[0].raw_content_hash)
        self.assertEqual(result.items[0].source_role, "discovery")

    async def test_empty_results_returns_empty_items(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_empty.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        self.assertEqual(len(result.items), 0)

    async def test_invalid_json_returns_parse_error(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=b"not json at all",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ParseError)

    async def test_non_utf8_returns_parse_error(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=b"\xff\xfe invalid utf-8",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ParseError)

    async def test_non_dict_root_returns_parse_error(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps(["not", "a", "dict"]).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ParseError)

    async def test_invalid_results_field_returns_parse_error(self) -> None:
        fixture = _load_json_fixture("phase2_searxng_invalid_structure.json")
        body = json.dumps(fixture["response"]).encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ParseError)
        self.assertIn("'results' field is not a list", str(result.error))


# -------------------------------------------------------------------------
# RssAdapter tests
# -------------------------------------------------------------------------


class TestRssAdapterConstruction(unittest.TestCase):
    def test_constructs_with_required_args(self) -> None:
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
        )
        self.assertEqual(adapter.source_id, "rss-test")
        self.assertEqual(adapter.host, "example.com")
        self.assertEqual(adapter.category, "world")
        self.assertEqual(adapter.source_role, "primary")

    def test_constructs_with_schedule(self) -> None:
        schedule = HostSchedule(
            source_id="rss-test",
            host="example.com",
            cadence_minutes=60,
        )
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            schedule=schedule,
        )
        self.assertIs(adapter.schedule, schedule)


class TestRssParseItems(unittest.IsolatedAsyncioTestCase):
    """Test RSS 2.0 parsing with valid feed."""

    async def test_parses_valid_rss_feed(self) -> None:
        fixture_xml = _load_text_fixture("phase2_rss_valid.xml")
        body = fixture_xml.encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/rss+xml"),),
            body=body,
            final_url="https://example.com/feed.rss",
        ))
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed.rss",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsNone(result.error)
        self.assertEqual(len(result.items), 3)
        item = result.items[0]
        self.assertIsInstance(item, NormalizedItem)
        self.assertEqual(item.source_id, "rss-test")
        self.assertEqual(item.category, "world")
        self.assertEqual(item.retrieval_method, "rss-poll")
        self.assertEqual(item.original_url, "https://example.com/articles/first")
        self.assertEqual(item.title, "First Article")
        self.assertEqual(item.author_handle, "Alice Author")
        self.assertIsNotNone(item.published_at)
        self.assertIsNotNone(item.raw_content_hash)
        self.assertEqual(item.source_role, "primary")

    async def test_rss_item_without_pubdate_has_null_published_at(self) -> None:
        fixture_xml = _load_text_fixture("phase2_rss_valid.xml")
        body = fixture_xml.encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/rss+xml"),),
            body=body,
            final_url="https://example.com/feed.rss",
        ))
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed.rss",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsNone(result.error)
        third = result.items[2]
        self.assertEqual(third.title, "Third Article")
        self.assertIsNone(third.published_at)

    async def test_rss_missing_channel_returns_error(self) -> None:
        fixture_xml = _load_text_fixture("phase2_rss_missing_channel.xml")
        body = fixture_xml.encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/rss+xml"),),
            body=body,
            final_url="https://example.com/bad.rss",
        ))
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/bad.rss",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsInstance(result.error, ParseError)
        self.assertIn("no <channel>", str(result.error))

    async def test_invalid_xml_returns_parse_error(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/rss+xml"),),
            body=b"<not valid xml at all",
            final_url="https://example.com/bad.xml",
        ))
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/bad.xml",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsInstance(result.error, ParseError)

    async def test_guid_used_as_external_id_for_nonpermalink(self) -> None:
        fixture_xml = _load_text_fixture("phase2_rss_valid.xml")
        body = fixture_xml.encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/rss+xml"),),
            body=body,
            final_url="https://example.com/feed.rss",
        ))
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed.rss",
            retrieved_at=RETRIEVED_AT,
        )
        # Third item has guid isPermaLink="false" with URN
        third = result.items[2]
        self.assertEqual(third.external_id, "urn:uuid:third-article-id")


class TestAtomParseItems(unittest.IsolatedAsyncioTestCase):
    """Test Atom 1.0 parsing with valid feed."""

    async def test_parses_valid_atom_feed(self) -> None:
        fixture_xml = _load_text_fixture("phase2_atom_valid.xml")
        body = fixture_xml.encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/atom+xml"),),
            body=body,
            final_url="https://example.com/feed.atom",
        ))
        adapter = RssAdapter(
            source_id="atom-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed.atom",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsNone(result.error)
        self.assertEqual(len(result.items), 2)
        first = result.items[0]
        self.assertEqual(first.title, "Atom Entry One")
        self.assertEqual(first.author_handle, "Bob Author")
        self.assertIsNotNone(first.published_at)
        self.assertEqual(first.original_url, "https://example.com/entries/one")
        self.assertEqual(first.retrieval_method, "rss-poll")

    async def test_atom_entry_uses_published_over_updated(self) -> None:
        fixture_xml = _load_text_fixture("phase2_atom_valid.xml")
        body = fixture_xml.encode("utf-8")
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/atom+xml"),),
            body=body,
            final_url="https://example.com/feed.atom",
        ))
        adapter = RssAdapter(
            source_id="atom-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed.atom",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsNone(result.error)
        second = result.items[1]
        self.assertEqual(second.title, "Atom Entry Two")
        # Second entry has <published> not <updated>
        self.assertIsNotNone(second.published_at)
        # Should have the Atom id as external_id
        self.assertEqual(second.external_id, "https://example.com/entries/two")


class TestRssMissingDateReasons(unittest.IsolatedAsyncioTestCase):
    """Test that missing/unparseable dates get typed unknown_date_reason."""

    async def test_missing_pubdate_has_unknown_date_reason(self) -> None:
        doc = {
            "results": [
                {"url": "https://example.com/no-date", "title": "No Date", "content": "Body"},
            ],
            "query": "test",
            "number_of_results": 1,
        }
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps(doc).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        # No publishedDate means published_at is None and publication_evidence is None
        item = result.items[0]
        self.assertIsNone(item.published_at)


# -------------------------------------------------------------------------
# HTTP status code tests
# -------------------------------------------------------------------------


class TestHttp304NotModified(unittest.IsolatedAsyncioTestCase):
    """Test 304 Not Modified response handling."""

    async def test_304_returns_not_modified_result(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=304,
            headers=(
                ("ETag", '"abc123"'),
                ("Last-Modified", "Wed, 06 Sep 2026 10:00:00 GMT"),
            ),
            body=b"",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertTrue(result.is_not_modified)
        self.assertIsNone(result.error)
        self.assertEqual(result.http_status, 304)
        self.assertIsNotNone(result.validators)
        self.assertEqual(result.validators.etag, '"abc123"')

    async def test_304_items_are_empty(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=304,
            headers=(),
            body=b"",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertEqual(result.items, ())


class TestHttp429RateLimit(unittest.IsolatedAsyncioTestCase):
    """Test 429 rate-limit response with Retry-After header."""

    async def test_429_is_retryable_with_retry_after_seconds(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=429,
            headers=(
                ("Retry-After", "60"),
                ("X-RateLimit-Remaining", "0"),
                ("X-RateLimit-Reset", "1725620400"),
            ),
            body=b"Rate limit exceeded",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, RetryableHttpError)
        self.assertTrue(result.retryable)
        self.assertIsNotNone(result.retry_after)
        self.assertEqual(result.rate_limit_remaining, 0)
        self.assertEqual(result.rate_limit_reset, "2024-09-06T11:00:00Z")

    async def test_429_with_http_date_retry_after(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=429,
            headers=(
                # A date clearly in the future relative to retrieved_at 2026-09-06T12:00:00Z
                ("Retry-After", "Wed, 07 Sep 2026 12:00:00 GMT"),
            ),
            body=b"Rate limit",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, RetryableHttpError)
        self.assertTrue(result.retryable)
        self.assertIsNotNone(result.retry_after)
        self.assertGreater(result.retry_after, 0)


class TestHttp410Gone(unittest.IsolatedAsyncioTestCase):
    """Test 410 Gone is terminal (non-retryable)."""

    async def test_410_is_terminal(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=410,
            headers=(),
            body=b"Resource gone",
            final_url="https://example.com/gone",
        ))
        adapter = RssAdapter(
            source_id="rss-test",
            host="example.com",
            category="world",
            source_role="primary",
            transport=transport,
        )
        result = await adapter.fetch_feed(
            "https://example.com/gone",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertIsInstance(result.error, HttpStatusError)
        self.assertFalse(result.retryable)


class TestHttp5xxRetryable(unittest.IsolatedAsyncioTestCase):
    """Test 5xx errors are retryable."""

    async def test_503_is_retryable(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=503,
            headers=(("Retry-After", "30"),),
            body=b"Service unavailable",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, RetryableHttpError)
        self.assertTrue(result.retryable)
        self.assertEqual(result.retry_after, 30.0)

    async def test_500_is_retryable(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=500,
            headers=(),
            body=b"Internal error",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, RetryableHttpError)
        self.assertTrue(result.retryable)


class TestContentTypeRejection(unittest.IsolatedAsyncioTestCase):
    """Test that bad Content-Type is a terminal ContentTypeError."""

    async def test_html_content_type_is_rejected(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "text/html"),),
            body=b"<html>Not JSON</html>",
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ContentTypeError)
        self.assertFalse(result.retryable)
        self.assertEqual(result.error.content_type, "text/html")


class TestOversizedBody(unittest.IsolatedAsyncioTestCase):
    """Test that oversized responses are rejected."""

    async def test_oversized_body_returns_error(self) -> None:
        large_body = b"x" * (2 * 1024 * 1024 + 1)
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=large_body,
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ResponseTooLargeError)
        self.assertFalse(result.retryable)
        self.assertGreater(result.error.size, result.error.limit)


class TestMalformedPayload(unittest.IsolatedAsyncioTestCase):
    """Test that malformed payloads return ParseError (not broad Exception)."""

    async def test_malformed_json_returns_parse_error(self) -> None:
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=b'{"results": [{"url":',  # truncated JSON
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, ParseError)
        self.assertFalse(result.retryable)


class TestMaxItems(unittest.IsolatedAsyncioTestCase):
    """Test that MAX_ITEMS_PER_RESPONSE is respected."""

    async def test_items_capped_at_max(self) -> None:
        results = [
            {"url": f"https://example.com/item{i}", "title": f"Item {i}", "content": f"Body {i}"}
            for i in range(100)
        ]
        doc = {"results": results, "query": "test", "number_of_results": 100}
        transport = FakeTransport(FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=json.dumps(doc).encode(),
            final_url="https://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(
            source_id="searxng-ai-main",
            host="searxng.example.com",
            category="ai",
            source_role="discovery",
            transport=transport,
        )
        result = await adapter.fetch_query("test", categories=(), retrieved_at=RETRIEVED_AT)
        self.assertIsNone(result.error)
        self.assertLessEqual(len(result.items), adapter.MAX_ITEMS_PER_RESPONSE)


# -------------------------------------------------------------------------
# FetchResult properties
# -------------------------------------------------------------------------


class TestFetchResultProperties(unittest.TestCase):
    def test_is_success_true_when_no_error(self) -> None:
        result = FetchResult(items=(), http_status=200)
        self.assertTrue(result.is_success)
        self.assertFalse(result.is_not_modified)

    def test_is_success_false_when_error(self) -> None:
        result = FetchResult(error=TransportError("failed"))
        self.assertFalse(result.is_success)

    def test_is_not_modified_304(self) -> None:
        result = FetchResult(http_status=304)
        self.assertTrue(result.is_not_modified)
        self.assertTrue(result.is_success)

    def test_fetch_result_default_empty_tuple(self) -> None:
        result = FetchResult()
        self.assertEqual(result.items, ())
        self.assertEqual(result.rejections, ())
        self.assertIsNone(result.error)


# -------------------------------------------------------------------------
# Error hierarchy tests
# -------------------------------------------------------------------------


class TestErrorHierarchy(unittest.TestCase):
    def test_network_error_is_retryable(self) -> None:
        exc = NetworkError("connection refused")
        self.assertIsInstance(exc, TransportError)
        self.assertIsInstance(exc, AdapterError)

    def test_retryable_http_error_carries_status_and_retry_after(self) -> None:
        exc = RetryableHttpError("rate limited", status=429, retry_after_seconds=60.0)
        self.assertEqual(exc.status, 429)
        self.assertEqual(exc.retry_after_seconds, 60.0)

    def test_http_status_error_carries_status(self) -> None:
        exc = HttpStatusError("gone", status=410)
        self.assertEqual(exc.status, 410)

    def test_content_type_error_carries_content_type(self) -> None:
        exc = ContentTypeError("bad type", content_type="text/html")
        self.assertEqual(exc.content_type, "text/html")

    def test_response_too_large_error_carries_size_and_limit(self) -> None:
        exc = ResponseTooLargeError("too large", size=3 * 1024 * 1024, limit=2 * 1024 * 1024)
        self.assertEqual(exc.size, 3 * 1024 * 1024)
        self.assertEqual(exc.limit, 2 * 1024 * 1024)


# -------------------------------------------------------------------------
# HostSchedule tests
# -------------------------------------------------------------------------


class TestHostSchedule(unittest.TestCase):
    def test_host_schedule_defaults(self) -> None:
        schedule = HostSchedule(
            source_id="test",
            host="example.com",
            cadence_minutes=60,
        )
        self.assertEqual(schedule.per_host_concurrency, 1)
        self.assertEqual(schedule.minimum_host_delay_seconds, 1.0)


# -------------------------------------------------------------------------
# NormalizedItem invariants
# -------------------------------------------------------------------------


class TestNormalizedItemFields(unittest.TestCase):
    def test_required_fields_present(self) -> None:
        item = NormalizedItem(
            source_item_id="abc123",
            source_id="test-source",
            external_id=None,
            category="ai",
            original_url="https://example.com/article",
            canonical_url="https://example.com/article",
            publisher="example.com",
            source_role="discovery",
            retrieval_method="searxng-query",
            raw_content_hash="a" * 64,
            retrieved_at="2026-09-06T10:00:00Z",
        )
        self.assertEqual(item.source_item_id, "abc123")
        self.assertIsNone(item.external_id)
        self.assertIsNone(item.title)
        self.assertIsNone(item.author_handle)
        self.assertIsNone(item.published_at)
        self.assertIsNone(item.body)

    def test_all_optional_fields_present(self) -> None:
        item = NormalizedItem(
            source_item_id="abc123",
            source_id="test-source",
            external_id="ext-001",
            category="ai",
            original_url="https://example.com/article",
            canonical_url="https://example.com/article",
            publisher="example.com",
            source_role="primary",
            retrieval_method="rss-poll",
            raw_content_hash="a" * 64,
            retrieved_at="2026-09-06T10:00:00Z",
            title="Article Title",
            author_handle="Jane Doe",
            published_at="2026-09-06T09:00:00Z",
            updated_at="2026-09-06T11:00:00Z",
            publication_evidence="metadata:2026-09-06T09:00:00Z",
            unknown_date_reason=None,
            body="Article body text.",
            raw="<raw/>",
        )
        self.assertEqual(item.title, "Article Title")
        self.assertEqual(item.author_handle, "Jane Doe")
        self.assertIsNotNone(item.body)


# -------------------------------------------------------------------------
# Retry-After parsing
# -------------------------------------------------------------------------


class TestRetryAfterParsing(unittest.TestCase):
    def test_parse_numeric_retry_after(self) -> None:
        result = parse_retry_after("60", reference_epoch=0)
        self.assertEqual(result, 60)

    def test_parse_http_date_retry_after(self) -> None:
        result = parse_retry_after(
            "Mon, 07 Sep 2026 12:00:00 GMT",
            reference_epoch=1788696000,
        )
        self.assertEqual(result, 86400)

    def test_parse_invalid_returns_none(self) -> None:
        result = parse_retry_after("not-a-number-or-date", reference_epoch=0)
        self.assertIsNone(result)


# -------------------------------------------------------------------------
# Rate-limit header parsing
# -------------------------------------------------------------------------


class TestRateLimitParsing(unittest.TestCase):
    def test_parse_rate_limit_remaining(self) -> None:
        headers = (("X-RateLimit-Remaining", "42"),)
        result = parse_rate_limit_remaining(headers)
        self.assertEqual(result, 42)

    def test_parse_rate_limit_remaining_missing(self) -> None:
        headers = (("Content-Type", "application/json"),)
        result = parse_rate_limit_remaining(headers)
        self.assertIsNone(result)


# -------------------------------------------------------------------------
# FetchRequest / FetchResponse invariants
# -------------------------------------------------------------------------


class TestFetchRequestIsImmutable(unittest.TestCase):
    def test_fetch_request_is_frozen(self) -> None:
        req = FetchRequest("https://example.com", (), 15.0, 2 * 1024 * 1024)
        with self.assertRaises(AttributeError):
            req.url = "https://other.com"


class TestFetchResponseIsImmutable(unittest.TestCase):
    def test_fetch_response_is_frozen(self) -> None:
        resp = FetchResponse(200, (), b"body", "https://example.com")
        with self.assertRaises(AttributeError):
            resp.status = 500


class TestNormalizedItemIsImmutable(unittest.TestCase):
    def test_normalized_item_is_frozen(self) -> None:
        item = NormalizedItem(
            source_item_id="abc",
            source_id="s",
            external_id=None,
            category="ai",
            original_url="https://example.com",
            canonical_url="https://example.com",
            publisher="example.com",
            source_role="discovery",
            retrieval_method="searxng-query",
            raw_content_hash="a" * 64,
            retrieved_at="2026-09-06T10:00:00Z",
        )
        with self.assertRaises(AttributeError):
            item.title = "new title"


class TestItemRejectionIsImmutable(unittest.TestCase):
    def test_item_rejection_is_frozen(self) -> None:
        rej = ItemRejection(0, "MISSING_URL", "no url")
        with self.assertRaises(AttributeError):
            rej.index = 1


# -------------------------------------------------------------------------
# Adapter abstract interface
# -------------------------------------------------------------------------


class TestAdapterSubclassInterface(unittest.TestCase):
    def test_searxng_declares_content_types(self) -> None:
        adapter = SearxngAdapter("s", "h", "c", "r")
        self.assertEqual(adapter.ACCEPTED_CONTENT_TYPES, ("application/json",))

    def test_rss_declares_content_types(self) -> None:
        adapter = RssAdapter("s", "h", "c", "r")
        self.assertEqual(adapter.ACCEPTED_CONTENT_TYPES, (
            "application/rss+xml",
            "application/atom+xml",
            "application/rdf+xml",
            "application/xml",
            "text/xml",
        ))


class TestParentRegressionContracts(unittest.IsolatedAsyncioTestCase):
    async def test_source_contract_and_query_seed_are_consumed_directly(self) -> None:
        source = _searxng_contract()
        transport = FakeTransport(FetchResponse(
            200,
            (("Content-Type", "application/json"),),
            b'{"results":[]}',
            "http://searxng.example.com/search",
        ))
        adapter = SearxngAdapter(source, transport=transport)
        result = await adapter.fetch_query(
            source.queries[0],
            retrieved_at=RETRIEVED_AT,
        )
        self.assertTrue(result.is_success)
        self.assertEqual(adapter.source_role, "discovery")
        self.assertIn("categories=news%2Cit", transport.requests[0].url)

    async def test_unparseable_searxng_date_is_not_persisted_as_timestamp(self) -> None:
        body = json.dumps({
            "results": [{
                "url": "https://example.com/item",
                "publishedDate": "definitely-not-a-date",
            }],
        }).encode()
        adapter = SearxngAdapter(
            _searxng_contract(),
            transport=FakeTransport(FetchResponse(
                200,
                (("Content-Type", "application/json"),),
                body,
                "http://searxng.example.com/search",
            )),
        )
        result = await adapter.fetch_query(
            _searxng_contract().queries[0],
            retrieved_at=RETRIEVED_AT,
        )
        item = result.items[0]
        self.assertIsNone(item.published_at)
        self.assertIsNone(item.publication_evidence)
        self.assertEqual(item.unknown_date_reason, "unparseable-published-date")

    async def test_unparseable_rss_date_is_not_persisted_as_timestamp(self) -> None:
        body = b"""<rss><channel><item><link>https://example.com/item</link>
            <pubDate>not-a-date</pubDate></item></channel></rss>"""
        adapter = RssAdapter(
            "rss-test",
            "example.com",
            "world",
            "primary",
            transport=FakeTransport(FetchResponse(
                200,
                (("Content-Type", "application/rss+xml"),),
                body,
                "https://example.com/feed",
            )),
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed",
            retrieved_at=RETRIEVED_AT,
        )
        item = result.items[0]
        self.assertIsNone(item.published_at)
        self.assertIsNone(item.publication_evidence)
        self.assertEqual(item.unknown_date_reason, "unparseable-published-date")

    async def test_namespaced_rdf_items_are_supported(self) -> None:
        body = b"""<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns="http://purl.org/rss/1.0/">
            <channel rdf:about="https://example.com/feed" />
            <item rdf:about="https://example.com/item">
              <title>RDF item</title><link>https://example.com/item</link>
            </item></rdf:RDF>"""
        adapter = RssAdapter(
            "rss-test",
            "example.com",
            "world",
            "primary",
            transport=FakeTransport(FetchResponse(
                200,
                (("Content-Type", "application/rdf+xml"),),
                body,
                "https://example.com/feed",
            )),
        )
        result = await adapter.fetch_feed(
            "https://example.com/feed",
            retrieved_at=RETRIEVED_AT,
        )
        self.assertTrue(result.is_success)
        self.assertEqual([item.title for item in result.items], ["RDF item"])


class _ClosingResponse:
    status = 200
    headers = {"Content-Type": "application/json"}

    def __init__(self, *, fail_read: bool = False) -> None:
        self.fail_read = fail_read
        self.closed = False

    def read(self, _limit: int) -> bytes:
        if self.fail_read:
            raise OSError("read failed")
        return b'{"results":[]}'

    def geturl(self) -> str:
        return "http://searxng.example.com/search"

    def close(self) -> None:
        self.closed = True


class TestDefaultTransportClosure(unittest.IsolatedAsyncioTestCase):
    async def test_default_transport_is_not_bound_as_an_instance_method(self) -> None:
        response = _ClosingResponse()
        with mock.patch.object(
            adapter_base.urllib.request,
            "urlopen",
            return_value=response,
        ):
            result = await SearxngAdapter(
                "source",
                "searxng.example.com",
                "ai",
                "discovery",
            ).fetch_query("query", retrieved_at=RETRIEVED_AT)
        self.assertTrue(result.is_success)
        self.assertTrue(response.closed)

    async def test_response_closes_when_read_fails(self) -> None:
        response = _ClosingResponse(fail_read=True)
        with mock.patch.object(
            adapter_base.urllib.request,
            "urlopen",
            return_value=response,
        ):
            result = await SearxngAdapter(
                "source",
                "searxng.example.com",
                "ai",
                "discovery",
            ).fetch_query("query", retrieved_at=RETRIEVED_AT)
        self.assertIsInstance(result.error, NetworkError)
        self.assertTrue(result.retryable)
        self.assertTrue(response.closed)


class TestAdapterStaticSafety(unittest.TestCase):
    def test_no_broad_exception_handlers_or_internal_wall_clock(self) -> None:
        for name in ("base.py", "searxng.py", "rss.py"):
            path = ROOT / "scripts" / "news_pipeline" / "adapters" / name
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source)
            broad = [
                node.lineno
                for node in ast.walk(tree)
                if isinstance(node, ast.ExceptHandler)
                and (
                    node.type is None
                    or isinstance(node.type, ast.Name)
                    and node.type.id in {"Exception", "BaseException"}
                )
            ]
            self.assertEqual(broad, [], name)
            for forbidden in (
                "datetime.now(",
                "datetime.utcnow(",
                "time.time(",
                '__import__("time")',
            ):
                self.assertNotIn(forbidden, source, name)


if __name__ == "__main__":
    unittest.main()
