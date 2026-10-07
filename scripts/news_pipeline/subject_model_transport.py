"""Bounded, opt-in transport for subject summaries through a configured model route."""
from __future__ import annotations

import http.client
import json
import os
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping
from typing import Any

from .briefing_summarizer import MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, SummarizerTransportError

_BASE_URL_ENV = "NEWS_SUBJECT_MODEL_BASE_URL"
_MODEL_ENV = "NEWS_SUBJECT_MODEL"
_API_KEY_ENV = "NEWS_SUBJECT_MODEL_API_KEY"
_TIMEOUT_SECONDS = 10.0


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
                            "URLs. Do not use markdown."
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
        try:
            response = opener(request, timeout=_TIMEOUT_SECONDS)
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
            raise SummarizerTransportError("subject model transport failed") from exc
        if len(response_bytes) > MAX_RESPONSE_BYTES:
            raise SummarizerTransportError("subject model response exceeds configured bounds")
        try:
            decoded = json.loads(response_bytes)
            content = decoded["choices"][0]["message"]["content"]
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, IndexError, TypeError) as exc:
            raise SummarizerTransportError("subject model response is invalid") from exc
        if not isinstance(content, str):
            raise SummarizerTransportError("subject model response is invalid")
        result = content.encode("utf-8")
        if len(result) > MAX_RESPONSE_BYTES:
            raise SummarizerTransportError("subject model response exceeds configured bounds")
        return result

    return transport
