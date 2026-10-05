from __future__ import annotations

import json
import unittest

from news_pipeline.adapters.base import FetchRequest, FetchResponse
from news_pipeline.adapters.github import LATEST_RELEASE_API
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
        "queries": [{"text": endpoint, "categories": ["news"], "pipeline_category": "our_setup"}],
    }


class GitHubSourceRegistryTests(unittest.TestCase):
    def test_registry_accepts_only_the_one_official_llama_cpp_endpoint_in_our_setup(self) -> None:
        accepted = _parse_sources({"version": 2, "sources": [_registry_source()]})
        self.assertEqual(len(accepted), 1)
        self.assertEqual(accepted[0].adapter_type, SourceAdapter.GITHUB)
        for source in (
            _registry_source(endpoint="https://api.github.com/repos/other/project/releases/latest"),
            _registry_source(host="github.com"),
            _registry_source(scope=["hardware"]),
        ):
            with self.subTest(source=source):
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
        self.assertEqual(item.publication_evidence, "metadata:2026-09-23T20:50:06Z")
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

    def test_rejects_unapproved_host_or_endpoint(self) -> None:
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
