"""Bounded, opt-in transport for subject summaries through a configured model route."""
from __future__ import annotations

import http.client
import json
import math
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from .briefing_summarizer import MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, SummarizerTransportError

_BASE_URL_ENV = "NEWS_SUBJECT_MODEL_BASE_URL"
_MODEL_ENV = "NEWS_SUBJECT_MODEL"
_API_KEY_ENV = "NEWS_SUBJECT_MODEL_API_KEY"

# Transport timeout rule: ceil(measured p95 of successful calls * margin), clamped
# to [min, max]. The model is a reasoning model; a fixed 10 s cut off ~half of
# real calls (7 of 12 took >10 s) and each timeout silently became a fallback.
# Measured 2026-10-08 from the production host, 12 sequential calls, one subject
# shape: 11 ok, p50 10.38 s, p95 12.21 s, max 12.21 s, plus one stall that never
# returned within a 90 s probe cap (a stall is not fixable by a longer timeout).
# Margin 2.0 absorbs run-to-run variance (observed ok range 6.6-12.2 s) without
# letting a stalled call hold the cron lock.
#
# Latency grows with request size (the model writes one summary per input item).
# 2026-10-09 replay of the 08:05 report on the production host: 445 B 14.6 s,
# 842 B 13.5 s, 2372 B 31.1 s, 6012 B 49.0 s, all ok with a 170 s cap, but a flat
# 25 s timed out on the two larger calls (31 items fell back as transport_error).
# Slope between the 842 B and 6012 B calls is (49.0 - 13.5) / 5170 B = ~0.007 s/B,
# so the timeout is ceil(margin * (p95 + per_byte * request_bytes)), clamped.
# Ceiling 120 s: at most 8 calls per run (TOTAL_CALL_BUDGET), and the largest
# measured call (6012 B -> 109 s) must fit; a stall is still cut off at 120 s.
_MEASURED_P95_SECONDS = 12.21
_SECONDS_PER_REQUEST_BYTE = 0.007
_TIMEOUT_MARGIN = 2.0
_TIMEOUT_MIN_SECONDS = 10.0
_TIMEOUT_MAX_SECONDS = 120.0


def timeout_from_p95(
    p95_seconds: float,
    *,
    margin: float = _TIMEOUT_MARGIN,
    minimum: float = _TIMEOUT_MIN_SECONDS,
    maximum: float = _TIMEOUT_MAX_SECONDS,
) -> float:
    """Return ceil(p95 * margin) seconds, clamped to [minimum, maximum]."""
    if not math.isfinite(p95_seconds) or p95_seconds <= 0:
        raise ValueError("p95 must be a positive finite number of seconds")
    return float(min(maximum, max(minimum, math.ceil(p95_seconds * margin))))


_TIMEOUT_SECONDS = timeout_from_p95(_MEASURED_P95_SECONDS)


def request_timeout(request_bytes: int) -> float:
    """Return the transport timeout for a request body of ``request_bytes`` bytes."""
    return timeout_from_p95(
        _MEASURED_P95_SECONDS + _SECONDS_PER_REQUEST_BYTE * max(0, request_bytes)
    )


# Per-call observability. While a ``recording_calls()`` block is active every
# HTTP call made by a configured transport appends one record: model, endpoint
# host (no path, query or credentials), sizes, timeout, latency and an outcome
# class. Request/response content is never recorded.
_CALL_SINK: list[dict[str, Any]] | None = None


@contextmanager
def recording_calls() -> Iterator[list[dict[str, Any]]]:
    """Collect one record per subject-model HTTP call made inside the block."""
    global _CALL_SINK
    previous = _CALL_SINK
    sink: list[dict[str, Any]] = []
    _CALL_SINK = sink
    try:
        yield sink
    finally:
        _CALL_SINK = previous


def _is_timeout(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout)):
        return True
    reason = getattr(exc, "reason", None)
    return isinstance(reason, (TimeoutError, socket.timeout))


def _outcome_for(exc: BaseException) -> tuple[str, int | None]:
    if isinstance(exc, urllib.error.HTTPError):
        return "http_error", exc.code if 100 <= exc.code <= 599 else None
    if _is_timeout(exc):
        return "timeout", None
    return "connection", None


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Keep an approved route from redirecting private inputs elsewhere."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def configured_transport(
    *,
    environ: Mapping[str, str] | None = None,
    opener: Callable[..., Any] | None = None,
) -> Callable[[bytes], bytes]:
    """Bind exactly one request transport to the explicitly configured model route.

    The endpoint and model are mandatory deployment configuration. No route or
    model default is embedded in the public source; credentials, when needed,
    are read only from the process environment and never included in errors.
    """
    env = os.environ if environ is None else environ
    base_url = env.get(_BASE_URL_ENV, "").strip()
    model = env.get(_MODEL_ENV, "").strip()
    if not base_url or not model:
        raise ValueError("subject model endpoint and model must both be configured")
    parsed = urllib.parse.urlsplit(base_url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("subject model endpoint configuration is invalid")
    endpoint = base_url.rstrip("/") + "/chat/completions"
    endpoint_host = parsed.hostname if parsed.port is None else f"{parsed.hostname}:{parsed.port}"
    api_key = env.get(_API_KEY_ENV, "")
    if opener is None:
        opener = urllib.request.build_opener(_NoRedirectHandler()).open

    def transport(request_bytes: bytes) -> bytes:
        if not isinstance(request_bytes, bytes) or len(request_bytes) > MAX_REQUEST_BYTES:
            raise SummarizerTransportError("subject model request exceeds configured bounds")
        try:
            request_payload = json.loads(request_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SummarizerTransportError("subject model request is invalid") from exc
        if not isinstance(request_payload, Mapping):
            raise SummarizerTransportError("subject model request is invalid")

        body = json.dumps(
            {
                "model": model,
                "stream": False,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Use only verified facts in the request. Return only a JSON object "
                            "with an items array. Each item must contain exactly subject, "
                            "event_id, event_version, what_changed, why_it_matters, "
                            "source_url, and fact_deltas. Copy subject, event_id, "
                            "event_version, source_url (exactly the first source_urls value), "
                            "and fact_deltas from the matching input; do not add facts or "
                            "URLs. You must write what_changed and why_it_matters yourself: "
                            "each is a non-empty string of 1 to 256 characters of plain text, "
                            "one sentence, based only on the item's title and fact_deltas, "
                            "with no URLs; never null, never empty. Do not use markdown."
                        ),
                    },
                    {"role": "user", "content": request_bytes.decode("utf-8")},
                ],
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(body) > MAX_REQUEST_BYTES:
            raise SummarizerTransportError("subject model request exceeds configured bounds")
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
        timeout = request_timeout(len(body))
        record: dict[str, Any] = {
            "started_at": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "model": model,
            "endpoint_host": endpoint_host,
            "request_bytes": len(body),
            "response_bytes": None,
            "timeout_s": timeout,
            "outcome": "connection",
            "http_status": None,
        }
        started = time.monotonic()
        try:
            try:
                response = opener(request, timeout=timeout)
                status = getattr(response, "status", None)
                record["http_status"] = status if type(status) is int and 100 <= status <= 599 else None
                try:
                    response_bytes = response.read(MAX_RESPONSE_BYTES + 1)
                finally:
                    close = getattr(response, "close", None)
                    if callable(close):
                        close()
            except (
                urllib.error.URLError,
                http.client.HTTPException,
                TimeoutError,
                OSError,
            ) as exc:
                record["outcome"], record["http_status"] = _outcome_for(exc)
                raise SummarizerTransportError("subject model transport failed") from exc
            record["response_bytes"] = len(response_bytes)
            if len(response_bytes) > MAX_RESPONSE_BYTES:
                record["outcome"] = "oversize"
                raise SummarizerTransportError("subject model response exceeds configured bounds")
            record["outcome"] = "invalid_response"
            try:
                decoded = json.loads(response_bytes)
                content = decoded["choices"][0]["message"]["content"]
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
                raise SummarizerTransportError("subject model response is invalid") from exc
            if not isinstance(content, str):
                raise SummarizerTransportError("subject model response is invalid")
            result = content.encode("utf-8")
            if len(result) > MAX_RESPONSE_BYTES:
                record["outcome"] = "oversize"
                raise SummarizerTransportError("subject model response exceeds configured bounds")
            record["outcome"] = "ok"
            return result
        finally:
            record["latency_ms"] = max(0, round((time.monotonic() - started) * 1000))
            sink = _CALL_SINK
            if sink is not None:
                sink.append(record)

    return transport
