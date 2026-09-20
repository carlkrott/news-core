"""SearXNG search adapter.

Standard-library only: uses ``urllib.request`` for HTTP and the built-in
``json`` module for parsing. No ``httpx``, ``requests``, or similar.

SearXNG response schema (one-shot JSON):
  {
    "results": [
      {
        "url": "https://example.com/article",
        "title": "Article Title",
        "content": "Snippet text...",
        "engine": "duckduckgo",
        "publishedDate": "2026-09-06T12:00:00Z",
        "author": "Jane Doe",
        "id": "optional-external-id"
      }
    ],
    "query": "...",
    "number_of_results": 42
  }
"""
from __future__ import annotations

import json
import urllib.parse
from typing import Any

from ..canonicalization import canonicalize_url, non_article_url_reason
from ..live_contracts import QuerySeed, SourceAdapter, SourceContract, stable_id
from .base import (
    Adapter,
    AdapterError,
    FetchResult,
    FetchValidators,
    ItemRejection,
    NormalizedItem,
    ParseError,
    Transport,
    normalize_timestamp,
)

_SEARCH_PATH = "/search"


class SearxngAdapter(Adapter):
    """Adapter for the SearXNG metasearch API.

    Accepts only ``application/json`` responses. Parses the flat results
    array and produces :class:`NormalizedItem` objects.
    """

    ACCEPTED_CONTENT_TYPES: tuple[str, ...] = ("application/json",)
    MAX_ITEMS_PER_RESPONSE: int = 50

    def __init__(
        self,
        source_id: str | SourceContract,
        host: str | None = None,
        category: str | None = None,
        source_role: str | None = None,
        *,
        schedule=None,
        transport: Transport | None = None,
    ) -> None:
        if isinstance(source_id, SourceContract) and source_id.adapter_type is not SourceAdapter.SEARXNG:
            raise ValueError("SearxngAdapter requires a searxng SourceContract")
        super().__init__(source_id, host, category, source_role, schedule=schedule, transport=transport)
        self._base_url = f"http://{self.host}"

    # ------------------------------------------------------------------
    # Public async API
    # ------------------------------------------------------------------

    async def fetch_query(
        self,
        query_text: str | QuerySeed,
        categories: tuple[str, ...] = (),
        *,
        retrieved_at: str,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FetchResult:
        """Execute a SearXNG query and return normalised items.

        Converts stored '+' separators to spaces before encoding so the wire
        query semantics match legacy behaviour.
        """
        if isinstance(query_text, QuerySeed):
            categories = query_text.categories
            query_text = query_text.text

        # Convert "LLM+AI+release" stored form to space-separated for encoding
        q = query_text.replace("+", " ")

        params: dict[str, str] = {
            "q": q,
            "format": "json",
            "language": "en",
            "safesearch": "0",
        }
        if categories:
            params["categories"] = ",".join(categories)

        encoded = urllib.parse.urlencode(params)
        url = f"{self._base_url}{_SEARCH_PATH}?{encoded}"

        headers: list[tuple[str, str]] = [
            ("Accept", "application/json"),
            ("User-Agent", "news-pipeline/2.0 (standard-library adapter)"),
        ]

        validators = FetchValidators(etag=etag, last_modified=last_modified)

        return await self.fetch(
            url,
            retrieved_at=retrieved_at,
            headers=tuple(headers),
            validators=validators,
        )

    # ------------------------------------------------------------------
    # Adapter abstract method
    # ------------------------------------------------------------------

    def _parse(
        self, body_bytes: bytes, *, url: str, retrieved_at: str
    ) -> tuple[tuple[NormalizedItem, ...], tuple[ItemRejection, ...], AdapterError | None]:
        try:
            raw_text = body_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            return (), (), ParseError(f"response is not valid UTF-8: {exc}")

        doc: dict[str, Any]
        try:
            doc = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            return (), (), ParseError(f"invalid JSON from {url}: {exc}")

        if not isinstance(doc, dict):
            return (), (), ParseError(f"expected JSON object from {url}, got {type(doc).__name__}")

        raw_results = doc.get("results")
        if not isinstance(raw_results, list):
            return (), (), ParseError(f"'results' field is not a list in {url}")

        items: list[NormalizedItem] = []
        rejections: list[ItemRejection] = []

        for idx, raw in enumerate(raw_results[: self.MAX_ITEMS_PER_RESPONSE]):
            item, rejection = self._parse_result(raw, idx, retrieved_at)
            if rejection is not None:
                rejections.append(rejection)
                continue
            if item is not None:
                items.append(item)

        return tuple(items), tuple(rejections), None

    def _parse_result(
        self, raw: Any, idx: int, retrieved_at: str
    ) -> tuple[NormalizedItem | None, ItemRejection | None]:
        """Parse a single SearXNG result object into a NormalizedItem or ItemRejection."""

        if not isinstance(raw, dict):
            return None, ItemRejection(idx, "INVALID_RESULT", f"result[{idx}] is not a dict")

        original_url = raw.get("url")
        if not isinstance(original_url, str) or not original_url.strip():
            return None, ItemRejection(idx, "MISSING_URL", f"result[{idx}] missing non-empty 'url'")

        # Validate and canonicalize
        try:
            canonical_url = canonicalize_url(original_url)
        except ValueError:
            return None, ItemRejection(idx, "INVALID_URL", f"result[{idx}] has invalid URL {original_url!r}")
        route_reason = non_article_url_reason(canonical_url)
        if route_reason is not None:
            return None, ItemRejection(
                idx,
                "NON_ARTICLE_URL",
                f"result[{idx}] is a {route_reason.replace('_', ' ')} route",
            )

        title: str | None = None
        raw_title = raw.get("title")
        if isinstance(raw_title, str) and raw_title.strip():
            title = raw_title.strip()

        body: str | None = None
        raw_content = raw.get("content")
        if isinstance(raw_content, str) and raw_content.strip():
            body = raw_content.strip()

        author: str | None = None
        raw_author = raw.get("author")
        if isinstance(raw_author, str) and raw_author.strip():
            author = raw_author.strip()

        # External ID from result if present
        external_id: str | None = None
        raw_id = raw.get("id")
        if isinstance(raw_id, str) and raw_id.strip():
            external_id = raw_id.strip()

        # publishedDate — stored as metadata evidence
        published_at: str | None = None
        updated_at: str | None = None
        publication_evidence: str | None = None
        unknown_date_reason: str | None = None
        raw_date = raw.get("publishedDate")
        if isinstance(raw_date, str) and raw_date.strip():
            normalized = normalize_timestamp(raw_date.strip())
            if normalized is not None:
                published_at = normalized
                publication_evidence = f"metadata:{raw_date.strip()}"
            else:
                publication_evidence = "unparseable"
                unknown_date_reason = "unparseable-published-date"
        else:
            publication_evidence = "missing"
            unknown_date_reason = "missing-published-date"

        raw_updated = raw.get("updatedDate")
        if isinstance(raw_updated, str) and raw_updated.strip():
            updated_at = normalize_timestamp(raw_updated.strip())

        # Build raw for hash (bounded per-item)
        raw_bytes = json.dumps(
            raw,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")[: 80 * 1024]
        raw_str = raw_bytes.decode("utf-8", errors="ignore")

        # Stable source item ID
        source_item_id = stable_id("source-item", self.source_id, external_id or canonical_url)

        return NormalizedItem(
            source_item_id=source_item_id,
            source_id=self.source_id,
            external_id=external_id,
            category=self.category,
            original_url=original_url,
            canonical_url=canonical_url,
            publisher=self._publisher_from_url(original_url),
            source_role=self.source_role,
            retrieval_method="searxng-query",
            raw_content_hash=self._raw_hash(raw_str),
            retrieved_at=retrieved_at,
            published_at=published_at,
            updated_at=updated_at,
            publication_evidence=publication_evidence,
            unknown_date_reason=unknown_date_reason,
            title=title,
            body=body,
            raw=raw_str,
            author_handle=author,
        ), None
