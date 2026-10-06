"""Bounded no-auth publisher-page extraction for explicitly approved origins."""
from __future__ import annotations

import hashlib
from dataclasses import replace
from html.parser import HTMLParser
from urllib.parse import urlsplit

from .adapters.base import FetchRequest, NormalizedItem, Transport
from .canonicalization import canonicalize_url
from .live_contracts import stable_id

_MAX_RESPONSE_BYTES = 512 * 1024
_MAX_TEXT_CHARS = 20_000
_TIMEOUT_SECONDS = 10.0


class _VisibleText(HTMLParser):
    """Collect bounded visible text; ignore non-content and executable nodes."""

    _IGNORED = frozenset({"script", "style", "noscript", "svg", "nav", "footer", "header"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._ignored = 0
        self._parts: list[str] = []
        self._size = 0

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.casefold() in self._IGNORED:
            self._ignored += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() in self._IGNORED and self._ignored:
            self._ignored -= 1

    def handle_data(self, data: str) -> None:
        if self._ignored or self._size >= _MAX_TEXT_CHARS:
            return
        text = " ".join(data.split())
        if not text:
            return
        bounded = text[: _MAX_TEXT_CHARS - self._size]
        self._parts.append(bounded)
        self._size += len(bounded)

    def text(self) -> str:
        return " ".join(self._parts)


async def fetch_publisher_article(
    item: NormalizedItem,
    transport: Transport,
    *,
    retrieved_at: str,
) -> NormalizedItem | None:
    """Return a fetched-body version only for a successful exact-URL HTML GET."""
    parsed = urlsplit(item.canonical_url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        return None
    request = FetchRequest(
        url=item.canonical_url,
        headers=(
            ("Accept", "text/html, application/xhtml+xml"),
            ("User-Agent", "news-pipeline/2.0 (publisher article retrieval)"),
        ),
        timeout_seconds=_TIMEOUT_SECONDS,
        max_response_bytes=_MAX_RESPONSE_BYTES,
    )
    try:
        response = await transport(request, retrieved_at=retrieved_at)
    except Exception:
        return None
    if response.status != 200 or len(response.body) > _MAX_RESPONSE_BYTES:
        return None
    content_type = next(
        (value.split(";", 1)[0].strip().casefold()
         for name, value in response.headers if name.casefold() == "content-type"),
        "",
    )
    if content_type not in {"text/html", "application/xhtml+xml"}:
        return None
    requested = canonicalize_url(item.canonical_url)
    final = canonicalize_url(response.final_url)
    if requested is None or final != requested:
        return None
    parser = _VisibleText()
    try:
        parser.feed(response.body.decode("utf-8", errors="replace"))
        parser.close()
    except Exception:
        return None
    body = parser.text()
    if not body:
        return None
    return replace(
        item,
        source_item_id=stable_id(
            "publisher-article", item.source_id, item.canonical_url,
            hashlib.sha256(body.encode("utf-8")).hexdigest(),
        ),
        retrieval_method="publisher-article-fetch",
        raw_content_hash=hashlib.sha256(response.body).hexdigest(),
        retrieved_at=retrieved_at,
        body=body,
        raw=None,
    )
