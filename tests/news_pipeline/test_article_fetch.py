from __future__ import annotations

import hashlib
import unittest

from news_pipeline.adapters.base import FetchResponse, NormalizedItem
from news_pipeline.article_fetch import fetch_publisher_article
from news_pipeline.live_contracts import SourceRole
from news_pipeline.provenance import PublisherRegistry, PublisherRule


class ArticleFetchTests(unittest.IsolatedAsyncioTestCase):
    def item(self) -> NormalizedItem:
        return NormalizedItem(
            source_item_id="feed-item", source_id="arch-feed", external_id="arch-1",
            category="our_setup", original_url="https://archlinux.org/news/example/",
            canonical_url="https://archlinux.org/news/example/", publisher="archlinux.org",
            source_role="primary", retrieval_method="rss-poll", raw_content_hash="a" * 64,
            retrieved_at="2026-10-06T08:00:00Z", published_at="2026-09-22T09:09:27Z",
            title="mkinitcpio advisory", body="RSS snippet only", raw="<item>snippet</item>",
        )

    async def test_fetch_is_bounded_no_auth_html_and_canonical_origin_only(self):
        html = (
            b"<html><script>ignore this</script><article>Starting with package "
            b"version 42-1, the mkinitcpio systemd hook now includes "
            b"systemd-example.service.</article></html>"
        )
        calls = []

        async def transport(request, *, retrieved_at):
            calls.append((request, retrieved_at))
            return FetchResponse(
                200, (("Content-Type", "text/html; charset=utf-8"),), html,
                "https://archlinux.org/news/example/",
            )

        fetched = await fetch_publisher_article(
            self.item(), transport, retrieved_at="2026-10-06T08:00:02Z"
        )
        self.assertIsNotNone(fetched)
        assert fetched is not None
        self.assertEqual(fetched.retrieval_method, "publisher-article-fetch")
        self.assertIn("Starting with package version 42-1", fetched.body)
        self.assertNotIn("ignore this", fetched.body)
        self.assertIsNone(fetched.raw)
        self.assertNotEqual(fetched.source_item_id, "feed-item")
        self.assertEqual(fetched.raw_content_hash, hashlib.sha256(html).hexdigest())
        request, _ = calls[0]
        self.assertEqual(request.url, self.item().canonical_url)
        self.assertLessEqual(request.max_response_bytes, 512 * 1024)
        self.assertLessEqual(request.timeout_seconds, 10)
        self.assertFalse(any(name.casefold() in {"authorization", "cookie"} for name, _ in request.headers))

    async def test_redirect_origin_change_and_non_html_fail_closed(self):
        async def redirected(request, *, retrieved_at):
            return FetchResponse(200, (("Content-Type", "text/html"),), b"body", "https://other.example/story")

        async def non_html(request, *, retrieved_at):
            return FetchResponse(200, (("Content-Type", "text/plain"),), b"body", request.url)

        self.assertIsNone(await fetch_publisher_article(self.item(), redirected, retrieved_at="2026-10-06T08:00:02Z"))
        self.assertIsNone(await fetch_publisher_article(self.item(), non_html, retrieved_at="2026-10-06T08:00:02Z"))

    async def test_http_errors_fail_closed(self):
        async def forbidden(request, *, retrieved_at):
            return FetchResponse(403, (("Content-Type", "text/html"),), b"denied", request.url)

        self.assertIsNone(await fetch_publisher_article(self.item(), forbidden, retrieved_at="2026-10-06T08:00:02Z"))


class ArticleFetchPolicyTests(unittest.TestCase):
    def test_article_fetch_requires_explicit_primary_exact_host_rule(self):
        registry = PublisherRegistry((PublisherRule(
            rule_id="arch-primary", host="archlinux.org", source_role=SourceRole.PRIMARY,
            independence_group="archlinux-own-news", categories=("our_setup",),
            authority_entities=("mkinitcpio",), allow_article_fetch=True, audit_note="approved for public page fetch",
        ),))
        self.assertTrue(registry.article_fetch_allowed(
            "https://archlinux.org/news/example/", category="our_setup"
        ))
        self.assertFalse(registry.article_fetch_allowed(
            "https://sub.archlinux.org/news/example/", category="our_setup"
        ))
        self.assertFalse(registry.article_fetch_allowed(
            "https://archlinux.org/news/example/", category="ai"
        ))
        disabled = PublisherRegistry((PublisherRule(
            rule_id="arch-default", host="archlinux.org", source_role=SourceRole.PRIMARY,
            independence_group="archlinux-own-news", categories=("our_setup",),
            authority_entities=("mkinitcpio",), audit_note="fetch remains disabled by default",
        ),))
        self.assertFalse(disabled.article_fetch_allowed(
            "https://archlinux.org/news/example/", category="our_setup"
        ))
