"""Shared types and abstract base for async source adapters.

All adapters are stdlib-only: no third-party HTTP or feed-parsing libraries.
"""
from __future__ import annotations

import asyncio
import hashlib
import urllib.error
import urllib.request
import urllib.response
from abc import ABC
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import ClassVar, Protocol

from ..live_contracts import SourceContract

# ----------------------------------------------------------------------
# Fetch request / response types
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FetchRequest:
    """Immutable, slotted HTTP request envelope."""

    url: str
    headers: tuple[tuple[str, str], ...]
    timeout_seconds: float
    max_response_bytes: int


@dataclass(frozen=True, slots=True)
class FetchResponse:
    """Immutable, slotted HTTP response envelope."""

    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    final_url: str


@dataclass(frozen=True, slots=True)
class FetchValidators:
    """ETag and Last-Modified response validators for conditional requests."""

    etag: str | None = None
    last_modified: str | None = None


# ----------------------------------------------------------------------
# Item rejection
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ItemRejection:
    """A per-item typed rejection reason."""

    index: int
    code: str
    message: str


# ----------------------------------------------------------------------
# Normalized item
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NormalizedItem:
    """A single normalised source item returned by an adapter fetch.

    ``source_item_id`` is the stable replay identity (live_contracts.stable_id
    of the source_id + external_id / canonical URL combination).
    """

    source_item_id: str
    source_id: str
    external_id: str | None
    category: str
    original_url: str
    canonical_url: str
    publisher: str
    source_role: str
    retrieval_method: str
    raw_content_hash: str
    retrieved_at: str
    published_at: str | None = None
    updated_at: str | None = None
    publication_evidence: str | None = None
    unknown_date_reason: str | None = None
    title: str | None = None
    body: str | None = None
    raw: str | None = None
    author_handle: str | None = None


# ----------------------------------------------------------------------
# Fetch result
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Outcome of a single adapter ``fetch()`` call."""

    items: tuple[NormalizedItem, ...] = field(default_factory=tuple)
    rejections: tuple[ItemRejection, ...] = field(default_factory=tuple)
    http_status: int | None = None
    validators: FetchValidators | None = None
    cursor: str | None = None
    rate_limit_reset: str | None = None
    rate_limit_remaining: int | None = None
    retry_after: int | None = None
    error: AdapterError | None = None
    retryable: bool = False

    @property
    def is_success(self) -> bool:
        return self.error is None

    @property
    def is_not_modified(self) -> bool:
        """304 response received — caller should retain prior state."""
        return self.http_status == 304


# ----------------------------------------------------------------------
# Error hierarchy
# ----------------------------------------------------------------------


class AdapterError(Exception):
    """Base for all adapter errors."""

    pass


class TransportError(AdapterError):
    """Non-retryable network-level failure (DNS, connection refused, etc.)."""

    pass


class NetworkError(TransportError):
    """DNS / connection / timeout failure. Retryable."""

    pass


class RetryableHttpError(TransportError):
    """Retryable HTTP-layer failure (5xx, 429)."""

    status: int
    retry_after_seconds: float | None = None

    def __init__(
        self, message: str, *, status: int, retry_after_seconds: float | None = None
    ) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after_seconds = retry_after_seconds


class HttpStatusError(TransportError):
    """Terminal HTTP response (410 Gone, 404, etc.)."""

    status: int

    def __init__(self, message: str, *, status: int) -> None:
        super().__init__(message)
        self.status = status


class ContentTypeError(TransportError):
    """Response Content-Type is unacceptable."""

    content_type: str

    def __init__(self, message: str, *, content_type: str) -> None:
        super().__init__(message)
        self.content_type = content_type


class ResponseTooLargeError(TransportError):
    """Response body exceeds the max_response_bytes limit."""

    size: int
    limit: int

    def __init__(self, message: str, *, size: int, limit: int) -> None:
        super().__init__(message)
        self.size = size
        self.limit = limit


class ParseError(AdapterError):
    """Non-retryable malformed response (invalid JSON, broken XML, etc.)."""

    pass


# ----------------------------------------------------------------------
# Transport protocol
# ----------------------------------------------------------------------


class Transport(Protocol):
    """Async callable that performs a single HTTP request and returns a response."""

    async def __call__(
        self, request: FetchRequest, *, retrieved_at: str
    ) -> FetchResponse: ...


# ----------------------------------------------------------------------
# Default urllib transport
# ----------------------------------------------------------------------


async def _default_transport(request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
    """Default async transport using asyncio.to_thread around urllib.

    Bound reads to max_response_bytes + 1. Closes response. Never logs
    bodies or secrets. Errors are classified as NetworkError or RetryableHttpError.
    """

    def _sync_fetch() -> FetchResponse:
        headers_dict = {k: v for k, v in request.headers}
        req = urllib.request.Request(
            request.url, headers=headers_dict, method="GET"
        )
        try:
            response = urllib.request.urlopen(
                req, timeout=int(request.timeout_seconds)
            )
        except urllib.error.HTTPError as exc:
            status = exc.code
            resp_headers = (
                tuple((k, v) for k, v in exc.headers.items())
                if exc.headers else ()
            )
            try:
                try:
                    body = exc.read(request.max_response_bytes + 1)
                except (OSError, TimeoutError):
                    body = b""
                final_url = exc.geturl() or request.url
            finally:
                exc.close()
            return FetchResponse(
                status=status,
                headers=resp_headers,
                body=body,
                final_url=final_url,
            )
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", str(exc))
            raise NetworkError(f"network error for {request.url}: {reason}") from exc
        except TimeoutError:
            raise NetworkError(f"timeout fetching {request.url}") from None
        except OSError as exc:
            raise NetworkError(f"network error for {request.url}: {exc}") from exc

        try:
            try:
                body = response.read(request.max_response_bytes + 1)
            except (OSError, TimeoutError) as exc:
                raise NetworkError(f"network read error for {request.url}: {exc}") from exc
            final_url = response.geturl() or request.url
            resp_headers = tuple((k, v) for k, v in response.headers.items())
            status = response.status
        finally:
            response.close()
        return FetchResponse(
            status=status,
            headers=resp_headers,
            body=body,
            final_url=final_url,
        )

    return await asyncio.to_thread(_sync_fetch)


# ----------------------------------------------------------------------
# Retry-After parsing
# ----------------------------------------------------------------------


def parse_retry_after(value: str, *, reference_epoch: float) -> int | None:
    """Parse a Retry-After header value.

    Accepts either an integer number of seconds or an HTTP-date.
    When an HTTP-date is given and reference_epoch is supplied, the
    returned seconds are relative to that reference point.
    Returns None if the value cannot be parsed.
    """
    try:
        seconds = int(value)
    except ValueError:
        seconds = -1
    if seconds >= 0:
        return seconds

    try:
        dt = parsedate_to_datetime(value)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return max(0, int(dt.timestamp() - reference_epoch))
    except (TypeError, ValueError, OverflowError):
        return None


# ----------------------------------------------------------------------
# Rate-limit / cursor helpers
# ----------------------------------------------------------------------


def parse_rate_limit_remaining(headers: tuple[tuple[str, str], ...]) -> int | None:
    """Extract X-RateLimit-Remaining / RateLimit-Remaining from response headers."""
    for key in ("X-RateLimit-Remaining", "RateLimit-Remaining", "Ratelimit-Remaining"):
        for hk, hv in headers:
            if hk.lower() == key.lower():
                try:
                    return int(hv)
                except ValueError:
                    pass
    return None


def parse_rate_limit_reset(headers: tuple[tuple[str, str], ...]) -> str | None:
    """Extract and normalize a rate-limit reset instant as UTC ISO-8601."""
    for key in ("X-RateLimit-Reset", "RateLimit-Reset", "Ratelimit-Reset"):
        for hk, hv in headers:
            if hk.lower() == key.lower():
                try:
                    instant = datetime.fromtimestamp(int(hv), UTC)
                    return instant.strftime("%Y-%m-%dT%H:%M:%SZ")
                except (ValueError, OverflowError, OSError):
                    try:
                        instant = parsedate_to_datetime(hv)
                        if instant is None:
                            continue
                        if instant.tzinfo is None:
                            instant = instant.replace(tzinfo=UTC)
                        return instant.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
                    except (TypeError, ValueError, OverflowError):
                        continue
    return None


# ----------------------------------------------------------------------
# Host schedule
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class HostSchedule:
    """Per-host cadence and concurrency guard, populated from source config."""

    source_id: str
    host: str
    cadence_minutes: int
    per_host_concurrency: int = 1
    minimum_host_delay_seconds: float = 1.0


# ----------------------------------------------------------------------
# Abstract async adapter
# ----------------------------------------------------------------------


class Adapter(ABC):
    """Abstract base for stdlib-only async source adapters.

    Subclasses must implement ``_do_fetch()`` and ``_parse()``.
    """

    GLOBAL_CONCURRENCY: ClassVar[int] = 4
    REQUEST_TIMEOUT_SECONDS: ClassVar[int] = 15
    MAX_RESPONSE_BYTES: ClassVar[int] = 2 * 1024 * 1024
    MAX_ITEMS_PER_RESPONSE: ClassVar[int] = 50
    ACCEPTED_CONTENT_TYPES: ClassVar[tuple[str, ...]] = ()


    def __init__(
        self,
        source_id: str | SourceContract,
        host: str | None = None,
        category: str | None = None,
        source_role: str | None = None,
        *,
        schedule: HostSchedule | None = None,
        transport: Transport | None = None,
    ) -> None:
        self.source: SourceContract | None = (
            source_id if isinstance(source_id, SourceContract) else None
        )
        if self.source is not None:
            if category is None:
                if len(self.source.category_scope) != 1:
                    raise ValueError("category is required for a multi-category source")
                category = self.source.category_scope[0]
            if category not in self.source.category_scope:
                raise ValueError("category is outside source.category_scope")
            self.source_id = self.source.source_id
            self.host = self.source.host
            self.category = category
            self.source_role = self.source.source_role.value
        else:
            if not all(
                isinstance(value, str) and value
                for value in (source_id, host, category, source_role)
            ):
                raise ValueError("source_id, host, category, and source_role are required")
            assert isinstance(source_id, str)
            assert isinstance(host, str)
            assert isinstance(category, str)
            assert isinstance(source_role, str)
            self.source_id = source_id
            self.host = host
            self.category = category
            self.source_role = source_role
        self.schedule = schedule
        self._transport: Transport = transport if transport is not None else _default_transport

    # ------------------------------------------------------------------
    # Public async fetch API
    # ------------------------------------------------------------------

    async def fetch(
        self,
        url: str,
        *,
        retrieved_at: str,
        method: str = "GET",
        headers: tuple[tuple[str, str], ...] = (),
        validators: FetchValidators | None = None,
        max_response_bytes: int | None = None,
        timeout_seconds: float | None = None,
    ) -> FetchResult:
        """Make a single HTTP/HTTPS request and return normalised items.

        ONE attempt only. Retry, sleep, and concurrency belong to the runner.

        Implements:
        - Conditional requests (If-None-Match / If-Modified-Since)
        - Timeout
        - Max response size cap
        - Content-Type validation
        - Typed error classification
        - Retry-After header extraction
        - ETag / Last-Modified passthrough
        """
        if headers is None:
            headers = ()
        if validators is None:
            validators = FetchValidators()
        if max_response_bytes is None:
            max_response_bytes = self.MAX_RESPONSE_BYTES
        if timeout_seconds is None:
            timeout_seconds = self.REQUEST_TIMEOUT_SECONDS

        req_headers = list(headers)
        if validators.etag:
            req_headers.append(("If-None-Match", validators.etag))
        if validators.last_modified:
            req_headers.append(("If-Modified-Since", validators.last_modified))

        request = FetchRequest(
            url=url,
            headers=tuple(req_headers),
            timeout_seconds=timeout_seconds,
            max_response_bytes=max_response_bytes,
        )

        reference_epoch = self._retrieved_at_epoch(retrieved_at)
        try:
            response = await self._transport(request, retrieved_at=retrieved_at)
        except NetworkError as exc:
            return FetchResult(error=exc, retryable=True)

        return self._handle_response(
            response,
            retrieved_at=retrieved_at,
            max_response_bytes=max_response_bytes,
            reference_epoch=reference_epoch,
        )

    def _handle_response(
        self,
        response: FetchResponse,
        *,
        retrieved_at: str,
        max_response_bytes: int,
        reference_epoch: float,
    ) -> FetchResult:
        status = response.status

        # 304 Not Modified
        if status == 304:
            return FetchResult(
                http_status=304,
                validators=FetchValidators(
                    etag=_header(response.headers, "etag"),
                    last_modified=_header(response.headers, "last-modified"),
                ),
            )

        # HTTP error classification (before content-type check)
        if status >= 400:
            return self._http_error_result(response, reference_epoch=reference_epoch)

        # Content-Type check (only for 2xx)
        content_type = _header(response.headers, "content-type") or ""
        if not self._content_type_acceptable(content_type):
            return FetchResult(
                http_status=status,
                error=ContentTypeError(
                    f"unexpected Content-Type {content_type!r} for {response.final_url}",
                    content_type=content_type,
                ),
                validators=FetchValidators(
                    etag=_header(response.headers, "etag"),
                    last_modified=_header(response.headers, "last-modified"),
                ),
                retryable=False,
            )

        body = response.body
        if len(body) > max_response_bytes:
            return FetchResult(
                http_status=status,
                error=ResponseTooLargeError(
                    f"response body {len(body)} bytes exceeds limit {max_response_bytes}",
                    size=len(body),
                    limit=max_response_bytes,
                ),
                validators=FetchValidators(
                    etag=_header(response.headers, "etag"),
                    last_modified=_header(response.headers, "last-modified"),
                ),
                retryable=False,
            )

        # Parse Retry-After and rate-limit headers (for 2xx)
        ra_header = _header(response.headers, "retry-after")
        retry_after: float | None = None
        if ra_header:
            retry_after = parse_retry_after(ra_header, reference_epoch=reference_epoch)

        # Delegate to subclass for actual item parsing
        items: tuple[NormalizedItem, ...]
        rejections: tuple[ItemRejection, ...]
        items, rejections, parse_error = self._parse(
            body, url=response.final_url, retrieved_at=retrieved_at
        )

        return FetchResult(
            items=items,
            rejections=rejections,
            http_status=status,
            validators=FetchValidators(
                etag=_header(response.headers, "etag"),
                last_modified=_header(response.headers, "last-modified"),
            ),
            retry_after=retry_after,
            rate_limit_remaining=parse_rate_limit_remaining(response.headers),
            rate_limit_reset=parse_rate_limit_reset(response.headers),
            error=parse_error,
            retryable=isinstance(parse_error, (NetworkError, RetryableHttpError)),
        )

    def _http_error_result(self, response: FetchResponse, *, reference_epoch: float) -> FetchResult:
        """Classify and return a result for HTTP 4xx/5xx responses."""
        status = response.status

        # Parse Retry-After relative to reference_epoch (caller-supplied UTC moment)
        ra_header = _header(response.headers, "retry-after")
        retry_after: float | None = None
        if ra_header:
            retry_after = parse_retry_after(ra_header, reference_epoch=reference_epoch)

        validators = FetchValidators(
            etag=_header(response.headers, "etag"),
            last_modified=_header(response.headers, "last-modified"),
        )

        if status in (408, 425, 429, 500, 502, 503, 504):
            return FetchResult(
                http_status=status,
                error=RetryableHttpError(
                    f"HTTP {status} for {response.final_url}",
                    status=status,
                    retry_after_seconds=retry_after,
                ),
                retryable=True,
                retry_after=retry_after,
                validators=validators,
                rate_limit_remaining=parse_rate_limit_remaining(response.headers),
                rate_limit_reset=parse_rate_limit_reset(response.headers),
            )
        else:
            # Terminal: 410, 404, etc.
            return FetchResult(
                http_status=status,
                error=HttpStatusError(
                    f"HTTP {status} for {response.final_url}",
                    status=status,
                ),
                retryable=False,
                validators=validators,
            )

    def _content_type_acceptable(self, content_type: str) -> bool:
        if not self.ACCEPTED_CONTENT_TYPES:
            return True
        raw = content_type.split(";")[0].strip().lower()
        if "/" in raw:
            media_type, subtype = raw.split("/", 1)
        else:
            media_type = raw
            subtype = ""
        for accepted in self.ACCEPTED_CONTENT_TYPES:
            if "*" in accepted:
                parts = accepted.lower().split("/")
                if parts[0] == media_type or parts[0] == "*":
                    return True
            elif raw == accepted.lower():
                return True
        return False

    # ------------------------------------------------------------------
    # Abstract methods
    # ------------------------------------------------------------------

    def _parse(
        self, body_bytes: bytes, *, url: str, retrieved_at: str
    ) -> tuple[tuple[NormalizedItem, ...], tuple[ItemRejection, ...], AdapterError | None]:
        """Parse raw response bytes into normalised items.

        Returns (items, rejections, error).
        Rejections are per-item typed reasons for items that could not be parsed.
        The error is a top-level parse failure; if present items/rejections may be empty.
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _raw_hash(self, raw: str | bytes) -> str:
        data = raw if isinstance(raw, bytes) else raw.encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    def _publisher_from_url(self, url: str) -> str:
        """Extract hostname from URL for the publisher field."""
        from urllib.parse import urlsplit
        try:
            parsed = urlsplit(url)
            return parsed.hostname or self.host
        except ValueError:
            return self.host

    @staticmethod
    def _retrieved_at_epoch(retrieved_at: str) -> float:
        """Convert a UTC Z timestamp to a Unix epoch float.

        No internal clock is used — the value is always caller-supplied.
        """
        dt = datetime.strptime(retrieved_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        return dt.timestamp()


def normalize_timestamp(value: str) -> str | None:
    """Normalize ISO-8601 or RFC 2822 input to second-precision UTC."""
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            instant = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if instant is None:
            return None
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    return instant.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _header(headers: tuple[tuple[str, str], ...], key: str) -> str | None:
    """Case-insensitive header lookup."""
    lower = key.lower()
    for hk, hv in headers:
        if hk.lower() == lower:
            return hv
    return None
