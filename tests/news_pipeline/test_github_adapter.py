from __future__ import annotations

import json
import unittest

from news_pipeline.adapters.base import FetchRequest, FetchResponse
from news_pipeline.adapters.github import LATEST_RELEASE_API, LATEST_RELEASE_APIS
from news_pipeline.adapters.github import GitHubReleaseAdapter
from news_pipeline.live_contracts import QuerySeed, SourceAdapter, SourceContract, SourceRole
from news_pipeline.source_registry import _parse_sources


class FakeTransport:
    def __init__(self, response: FetchResponse) -> None:
        self.response = response
        self.requests: list[FetchRequest] = []

    async def __call__(self, request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
        self.requests.append(request)
        return self.response


def _source(endpoint: str = LATEST_RELEASE_API) -> SourceContract:
    return SourceContract(
        source_id="github-llamacpp-release",
        adapter_type=SourceAdapter.GITHUB,
        source_role=SourceRole.DISCOVERY,
        host="api.github.com",
        category_scope=("our_setup",),
        enabled=True,
        queries=(QuerySeed(text=endpoint, categories=("news",)),),
        cadence_minutes=60,
    )


def _claude_source(endpoint: str = "") -> SourceContract:
    endpoint = endpoint or LATEST_RELEASE_APIS["anthropics/claude-code"]
    return SourceContract(
        source_id="github-anthropic-claude-code-release",
        adapter_type=SourceAdapter.GITHUB,
        source_role=SourceRole.DISCOVERY,
        host="api.github.com",
        category_scope=("ai",),
        enabled=True,
        queries=(QuerySeed(text=endpoint, categories=("news",)),),
        cadence_minutes=60,
    )


def _release(**updates: object) -> dict[str, object]:
    value: dict[str, object] = {
        "id": 395048089,
        "html_url": "https://github.com/ggml-org/llama.cpp/releases/tag/v0.5.0",
        "tag_name": "v0.5.0",
        "name": "v0.5.0",
        "draft": False,
        "prerelease": False,
        "published_at": "2026-09-23T20:50:06Z",
        "updated_at": "2026-09-23T20:50:44Z",
        "body": "The release adds support for multiple-address server binding.",
    }
    value.update(updates)
    return value


def _registry_source(
    *,
    endpoint: str = LATEST_RELEASE_API,
    host: str = "api.github.com",
    scope: list[str] | None = None,
) -> dict[str, object]:
    return {
        "source_id": "github-llamacpp-release",
        "adapter_type": "github",
        "source_role": "discovery",
        "host": host,
        "category_scope": scope or ["our_setup"],
        "enabled": True,
        "cadence_minutes": 60,
        "queries": [{"text": endpoint, "categories": ["news"], "pipeline_category": (scope or ["our_setup"])[0]}],
    }


class GitHubSourceRegistryTests(unittest.TestCase):
    def test_registry_accepts_exact_allowlisted_endpoints_and_rejects_variants(self) -> None:
        accepted = _parse_sources({"version": 2, "sources": [_registry_source()]})
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].adapter_type, SourceAdapter.GITHUB)
        claude = _registry_source(
            endpoint=LATEST_RELEASE_APIS["anthropics/claude-code"],
            scope=["ai"],
        )
        claude["source_id"] = "github-anthropic-claude-code-release"
        self.assertEqual(len(_parse_sources({"version": 2, "sources": [claude]})), 1)
        for source in (
            _registry_source(endpoint=LATEST_RELEASE_API.replace("ggml-org/llama.cpp", "other/project")),
            _registry_source(endpoint=LATEST_RELEASE_APIS["anthropics/claude-code"].replace("claude-code", "other"), scope=["ai"]),
            _registry_source(endpoint=LATEST_RELEASE_APIS["anthropics/claude-code"] + ".atom", scope=["ai"]),
            _registry_source(endpoint=LATEST_RELEASE_APIS["anthropics/claude-code"] + "/", scope=["ai"]),
            _registry_source(endpoint=LATEST_RELEASE_APIS["anthropics/claude-code"] + "?x=1", scope=["ai"]),
            _registry_source(host="github.com"),
            _registry_source(scope=["hardware"]),
        ):
            with self.subTest(source=source):
                with self.assertRaises(ValueError):
                    _parse_sources({"version": 2, "sources": [source]})

    def test_registry_rejects_allowlisted_endpoint_with_wrong_source_id(self) -> None:
        source = _registry_source(
            endpoint=LATEST_RELEASE_APIS["anthropics/claude-code"],
            scope=["ai"],
        )
        with self.assertRaises(ValueError):
            _parse_sources({"version": 2, "sources": [source]})


class GitHubReleaseAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_single_release_normalizes_body_identity_and_rate_headers(self) -> None:
        payload = json.dumps(_release()).encode("utf-8")
        transport = FakeTransport(
            FetchResponse(
                status=200,
                headers=(
                    ("Content-Type", "application/json; charset=utf-8"),
                    ("ETag", 'W/"release"'),
                    ("Last-Modified", "Wed, 23 Sep 2026 20:50:44 GMT"),
                    ("X-RateLimit-Remaining", "59"),
                    ("X-RateLimit-Reset", "1790286365"),
                ),
                body=payload,
                final_url=LATEST_RELEASE_API,
            )
        )
        adapter = GitHubReleaseAdapter(_source(), transport=transport)
        result = await adapter.fetch_release(
            LATEST_RELEASE_API,
            retrieved_at="2026-09-24T20:46:05Z",
            etag='W/"prior"',
            last_modified="Tue, 22 Sep 2026 00:00:00 GMT",
        )

        self.assertTrue(result.is_success)
        self.assertEqual(result.http_status, 200)
        self.assertEqual(len(result.items), 1)
        item = result.items[0]
        self.assertEqual(item.category, "our_setup")
        self.assertEqual(item.publisher, "llama.cpp")
        self.assertEqual(item.source_role, "discovery")
        self.assertEqual(item.retrieval_method, "github-release-api")
        self.assertEqual(item.canonical_url, _release()["html_url"])
        self.assertEqual(item.published_at, "2026-09-23T20:50:06Z")
        self.assertEqual(item.publication_evidence, "metadata:" + "2026-09-23T20:50:06Z")
        self.assertEqual(item.body, _release()["body"])
        self.assertEqual(result.validators.etag, 'W/"release"')
        self.assertEqual(result.rate_limit_remaining, 59)
        headers = dict(transport.requests[0].headers)
        self.assertEqual(headers["Accept"], "application/vnd.github+json")
        self.assertEqual(headers["X-GitHub-Api-Version"], "2022-11-28")
        self.assertEqual(headers["If-None-Match"], 'W/"prior"')
        self.assertEqual(headers["If-Modified-Since"], "Tue, 22 Sep 2026 00:00:00 GMT")

    async def test_rejects_other_repository_canonical_url(self) -> None:
        transport = FakeTransport(
            FetchResponse(
                200,
                (("Content-Type", "application/json"),),
                json.dumps(_release(html_url="https://github.com/elsewhere/project/releases/tag/v0.5.0")).encode(),
                LATEST_RELEASE_API,
            )
        )
        result = await GitHubReleaseAdapter(_source(), transport=transport).fetch_release(
            LATEST_RELEASE_API, retrieved_at="2026-09-24T20:46:05Z"
        )
        self.assertFalse(result.is_success)
        self.assertEqual(result.items, ())

    async def test_anthropic_release_normalizes_on_exact_allowlisted_endpoint(self) -> None:
        endpoint = LATEST_RELEASE_APIS["anthropics/claude-code"]
        page_url = endpoint.replace("api.github.com/repos/", "github.com/").replace("/releases/latest", "/releases/tag/v2.0.0")
        payload = _release(
            id=123456789,
            html_url=page_url,
            tag_name="v2.0.0",
            name="Claude Code 2.0.0",
        )
        transport = FakeTransport(FetchResponse(
            200, (("Content-Type", "application/json"),), json.dumps(payload).encode(), endpoint,
        ))
        result = await GitHubReleaseAdapter(_claude_source(), transport=transport).fetch_release(
            endpoint, retrieved_at="2026-10-08T16" + ":00:00Z"
        )
        self.assertTrue(result.is_success)
        self.assertEqual(result.items[0].publisher, "Anthropic")
        self.assertEqual(result.items[0].category, "ai")
        self.assertEqual(result.items[0].canonical_url, page_url)

    async def test_anthropic_release_rejects_canonical_url_for_other_repository(self) -> None:
        endpoint = LATEST_RELEASE_APIS["anthropics/claude-code"]
        page_url = endpoint.replace("api.github.com/repos/", "github.com/").replace(
            "anthropics/claude-code", "anthropics/other"
        ).replace("/releases/latest", "/releases/tag/v0.5.0")
        payload = _release(html_url=page_url)
        transport = FakeTransport(FetchResponse(
            200, (("Content-Type", "application/json"),), json.dumps(payload).encode(), endpoint,
        ))
        result = await GitHubReleaseAdapter(_claude_source(), transport=transport).fetch_release(
            endpoint, retrieved_at="2026-10-08T16" + ":00:00Z"
        )
        self.assertFalse(result.is_success)
        self.assertEqual(result.items, ())

    def test_rejects_unapproved_host_or_endpoint(self) -> None:
        for endpoint in (
            LATEST_RELEASE_APIS["anthropics/claude-code"] + ".atom",
            LATEST_RELEASE_APIS["anthropics/claude-code"] + "/",
            LATEST_RELEASE_APIS["anthropics/claude-code"] + "?x=1",
            LATEST_RELEASE_APIS["anthropics/claude-code"].replace("api.github.com", "github.com").replace("/repos/", "/" ).replace("/releases/latest", "/releases.atom"),
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                GitHubReleaseAdapter(_claude_source(endpoint))
        with self.assertRaises(ValueError):
            GitHubReleaseAdapter(_source("https://api.github.com/repos/other/repo/releases/latest"))
        with self.assertRaises(ValueError):
            GitHubReleaseAdapter(
                SourceContract(
                    source_id="github-llamacpp-release",
                    adapter_type=SourceAdapter.GITHUB,
                    source_role=SourceRole.DISCOVERY,
                    host="github.com",
                    category_scope=("our_setup",),
                    enabled=True,
                    queries=(QuerySeed(text=LATEST_RELEASE_API, categories=("news",)),),
                    cadence_minutes=60,
                )
            )
