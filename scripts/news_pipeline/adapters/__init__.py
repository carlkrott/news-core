"""Isolated standard-library async adapters for news ingestion sources.

Each adapter is a pure stdlib implementation: no external HTTP libraries,
no third-party feed parsers. The adapters handle only the transport and
normalization contract — they do not write to the database.

Active adapter: ``SearxngAdapter``
RSS/Atom: implementation and fixtures only; no live source configured.

Standard-library only: ``urllib.request`` for HTTP, ``xml.etree.ElementTree``
for RSS/Atom. No ``requests``, ``httpx``, ``feedparser``, or similar.
"""

from __future__ import annotations

from .base import (
    FetchRequest,
    FetchResponse,
    FetchValidators,
    FetchResult,
    NormalizedItem,
    ItemRejection,
    TransportError,
    NetworkError,
    RetryableHttpError,
    HttpStatusError,
    ContentTypeError,
    ResponseTooLargeError,
    ParseError,
    AdapterError,
    HostSchedule,
    Transport,
    parse_retry_after,
    parse_rate_limit_remaining,
)
from .rss import RssAdapter
from .searxng import SearxngAdapter

__all__ = [
    "FetchRequest",
    "FetchResponse",
    "FetchValidators",
    "FetchResult",
    "NormalizedItem",
    "ItemRejection",
    "TransportError",
    "NetworkError",
    "RetryableHttpError",
    "HttpStatusError",
    "ContentTypeError",
    "ResponseTooLargeError",
    "ParseError",
    "AdapterError",
    "HostSchedule",
    "Transport",
    "SearxngAdapter",
    "RssAdapter",
]
