"""Canonical broker envelope for the search/feed Unix-domain-socket broker.

This module defines the wire contract between the news ingest path and the
out-of-process broker that performs the actual HTTP GETs. The ingest path
runs inside a sealed container with no direct egress; the broker runs in a
narrower host-side daemon and mediates the network calls.

Design constraints (binding for this slice):

* Standard library only. No third-party JSON, no third-party HTTP.
* Canonical JSON: deterministic field order, no whitespace, UTF-8.
* The envelope carries only metadata about the target URL. The broker uses
  the metadata to issue the real HTTP request; the ingest path itself
  never touches the network.
* Routes are explicitly restricted to GET against the two kinds of
  sources this slice supports: ``search`` (SearXNG-style queries) and
  ``feed`` (RSS/Atom-style poll endpoints).
* The envelope is the sole payload format. The broker and ingest path do
  not negotiate alternatives.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Mapping

# Maximum permitted size of the JSON response envelope on the wire. The
# envelope itself is small (headers + a base64 body) and bounded; this
# cap exists to refuse runaway broker processes that try to ship a
# multi-megabyte envelope instead of respecting the per-request byte cap.
RESPONSE_ENVELOPE_MAX_BYTES: int = 4 * 1024 * 1024

# ----------------------------------------------------------------------
# Constants — single source of truth for the broker envelope schema.
# ----------------------------------------------------------------------

PROTOCOL_VERSION = 1

# Routes the broker will accept. Anything else is refused at validation
# time. Delivery / LLM routes are intentionally absent from this slice.
ROUTE_SEARCH = "search"
ROUTE_FEED = "feed"
ALLOWED_ROUTES: frozenset[str] = frozenset({ROUTE_SEARCH, ROUTE_FEED})

# Headers that the ingest path is permitted to forward to the broker.
# Any other header (Authorization, Cookie, X-*, etc.) is rejected by
# the envelope validator — the container must not surface raw secrets
# over the broker seam.
ALLOWED_HEADERS: frozenset[str] = frozenset(
    {
        "Accept",
        "Accept-Encoding",
        "Accept-Language",
        "User-Agent",
        "If-None-Match",
        "If-Modified-Since",
    }
)

# Defaults used when an envelope field is omitted by the caller. They are
# kept here, not in the client, so both sides agree on the exact wire
# shape.
DEFAULT_TIMEOUT_SECONDS: float = 15.0
DEFAULT_MAX_BYTES: int = 2 * 1024 * 1024
MAX_TIMEOUT_SECONDS: float = 120.0
MAX_BYTES_HARD_LIMIT: int = 8 * 1024 * 1024

# Environment variable names used by the client to select the absolute
# socket path for each route. The names are stable across the wire.
ENV_SEARCH_SOCKET = "NEWS_BROKER_SEARCH_SOCKET"
ENV_FEED_SOCKET = "NEWS_BROKER_FEED_SOCKET"
ENV_CONTAINER_MODE = "NEWS_CONTAINER_MODE"


# ----------------------------------------------------------------------
# Errors
# ----------------------------------------------------------------------


class BrokerProtocolError(ValueError):
    """Base for broker envelope validation failures."""


class UnsupportedRouteError(BrokerProtocolError):
    """The envelope's route is outside the search/feed allowlist."""


class DisallowedHeaderError(BrokerProtocolError):
    """The envelope contains a header that is not on the allowlist."""


class InvalidEnvelopeError(BrokerProtocolError):
    """The envelope is missing a required field or has the wrong type."""


class BrokerResponseError(BrokerProtocolError):
    """Base for response-envelope decoding failures.

    Distinguishes envelope-validation failures (mismatched version,
    missing fields, malformed JSON) from per-request broker failures
    encoded inside a well-formed envelope (see :class:`BrokerUpstreamError`).
    """


class BrokerResponseProtocolMismatch(BrokerResponseError):
    """The response envelope failed structural validation."""


class BrokerResponseRequestIdMismatch(BrokerResponseError):
    """The response envelope carries a request_id that does not match the request.

    Carries the offending id on ``offending_request_id`` and the expected
    id on ``expected_request_id``.
    """

    def __init__(
        self,
        message: str,
        *,
        expected_request_id: str,
        offending_request_id: str,
    ) -> None:
        super().__init__(message)
        self.expected_request_id = expected_request_id
        self.offending_request_id = offending_request_id


class BrokerResponseBodyError(BrokerResponseError):
    """The decoded body is malformed (bad base64, length mismatch, oversize)."""


class BrokerResponseOversize(BrokerResponseBodyError):
    """The decoded body is larger than the negotiated :attr:`max_bytes` cap.

    Carries the offending size on ``size`` and the cap on ``limit``.
    """

    def __init__(self, message: str, *, size: int, limit: int) -> None:
        super().__init__(message)
        self.size = size
        self.limit = limit


class BrokerUpstreamError(BrokerResponseError):
    """The broker returned a well-formed envelope that reports an upstream failure.

    The host broker maps every request-side failure
    (``BrokerRequestError``, ``BrokerConfigError``, defensive
    ``Exception``) into a JSON envelope with ``error_code`` set. The
    caller-side :class:`BrokerUpstreamError` mirrors that contract:

    * ``error_code`` is the stable machine identifier
      (e.g. ``"invalid_request"``, ``"broker_config_error"``,
      ``"broker_internal_error"``);
    * ``error_message`` is the human-readable detail;
    * ``status`` is the HTTP-style status the broker reported (typically
      400 / 500); the underlying upstream HTTP status is **not** part of
      this envelope and must not be invented.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        error_message: str,
        status: int,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.error_message = error_message
        self.status = status


# ----------------------------------------------------------------------
# Envelope dataclasses
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BrokerRequest:
    """Canonical broker request envelope.

    Attributes:
        request_id: 32-hex deterministic identifier for this envelope.
        route: One of ``ALLOWED_ROUTES``. The broker only accepts
            ``search`` or ``feed``.
        target_url: The raw upstream URL the broker should fetch.
        headers: Allowlisted request headers as a tuple of
            ``(name, value)`` pairs. Iteration order is preserved.
        timeout_seconds: Per-request socket timeout.
        max_bytes: Hard cap on the response body size. The broker
            enforces this server-side.
        retrieved_at: UTC ISO-8601 timestamp supplied by the caller;
            the broker does not invent one.
    """

    request_id: str
    route: str
    target_url: str
    headers: tuple[tuple[str, str], ...]
    timeout_seconds: float
    max_bytes: int
    retrieved_at: str

    def to_dict(self) -> dict[str, Any]:
        """Render the envelope as a JSON-ready dict with stable ordering."""

        ordered_headers: list[list[str]] = [
            [name, value] for name, value in self.headers
        ]
        return {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": self.request_id,
            "route": self.route,
            "target_url": self.target_url,
            "headers": ordered_headers,
            "timeout_seconds": float(self.timeout_seconds),
            "max_bytes": int(self.max_bytes),
            "retrieved_at": self.retrieved_at,
        }

    def to_canonical_json(self) -> str:
        """Render the envelope as canonical JSON (sorted keys, no spaces)."""

        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    def to_wire_bytes(self) -> bytes:
        """Render the envelope as UTF-8 bytes ready for the socket."""

        return self.to_canonical_json().encode("utf-8")


@dataclass(frozen=True, slots=True)
class BrokerResponse:
    """Canonical broker response envelope returned by the broker."""

    request_id: str
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes
    final_url: str
    retrieved_at: str
    error_code: str | None = None
    error_message: str | None = None

    def to_dict(self) -> dict[str, Any]:
        ordered_headers: list[list[str]] = [
            [name, value] for name, value in self.headers
        ]
        return {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": self.request_id,
            "status": int(self.status),
            "headers": ordered_headers,
            "body_b64": base64.b64encode(self.body).decode("ascii") if self.body else "",
            "final_url": self.final_url,
            "retrieved_at": self.retrieved_at,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "body_length": len(self.body),
        }

    def to_canonical_json(self) -> str:
        """Render the response envelope as canonical JSON (sorted keys, no spaces)."""

        return json.dumps(
            self.to_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )

    def to_wire_bytes(self) -> bytes:
        """Render the response envelope as UTF-8 bytes ready for the socket."""

        return self.to_canonical_json().encode("utf-8")


# ----------------------------------------------------------------------
# Validation helpers
# ----------------------------------------------------------------------


def _require_str(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise InvalidEnvelopeError(
            f"{field_name} must be a string, got {type(value).__name__}"
        )
    if not value:
        raise InvalidEnvelopeError(f"{field_name} must be non-empty")
    return value


def _require_request_id(value: Any) -> str:
    request_id = _require_str(value, "request_id")
    if len(request_id) > 128 or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
        for char in request_id
    ):
        raise InvalidEnvelopeError("request_id must be a bounded token")
    return request_id


def _require_number(value: Any, field_name: str) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise InvalidEnvelopeError(
            f"{field_name} must be a number, got {type(value).__name__}"
        )
    if not math.isfinite(value):
        raise InvalidEnvelopeError(
            f"{field_name} must be a finite number, got {value!r}"
        )
    return value


def validate_request_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a decoded JSON dict against the broker envelope schema.

    Returns the validated dict. Raises :class:`BrokerProtocolError` if any
    field is missing, has the wrong type, or fails the allowlist checks.
    """

    if not isinstance(payload, Mapping):
        raise InvalidEnvelopeError("envelope must be a JSON object")

    version = payload.get("protocol_version", PROTOCOL_VERSION)
    if version != PROTOCOL_VERSION:
        raise InvalidEnvelopeError(
            f"protocol_version must be {PROTOCOL_VERSION}, got {version!r}"
        )

    route = _require_str(payload.get("route"), "route")
    if route not in ALLOWED_ROUTES:
        raise UnsupportedRouteError(
            f"route {route!r} is not allowed (must be one of {sorted(ALLOWED_ROUTES)})"
        )

    target_url = _require_str(payload.get("target_url"), "target_url")
    request_id = _require_request_id(payload.get("request_id"))
    retrieved_at = _require_str(payload.get("retrieved_at"), "retrieved_at")

    timeout = _require_number(payload.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS), "timeout_seconds")
    if timeout <= 0 or timeout > MAX_TIMEOUT_SECONDS:
        raise InvalidEnvelopeError(
            f"timeout_seconds must be in (0, {MAX_TIMEOUT_SECONDS}], got {timeout!r}"
        )

    max_bytes = _require_number(payload.get("max_bytes", DEFAULT_MAX_BYTES), "max_bytes")
    if max_bytes <= 0 or max_bytes > MAX_BYTES_HARD_LIMIT:
        raise InvalidEnvelopeError(
            f"max_bytes must be in (0, {MAX_BYTES_HARD_LIMIT}], got {max_bytes!r}"
        )
    if int(max_bytes) <= 0:
        raise InvalidEnvelopeError("max_bytes must encode at least one byte")

    headers_raw = payload.get("headers", [])
    if not isinstance(headers_raw, list):
        raise InvalidEnvelopeError("headers must be a list of [name, value] pairs")
    cleaned_headers: list[tuple[str, str]] = []
    for entry in headers_raw:
        if (
            not isinstance(entry, list)
            or len(entry) != 2
            or not all(isinstance(part, str) for part in entry)
        ):
            raise InvalidEnvelopeError(
                "headers entries must be two-element lists of strings"
            )
        name, value = entry
        if name not in ALLOWED_HEADERS:
            raise DisallowedHeaderError(
                f"header {name!r} is not on the broker allowlist"
            )
        cleaned_headers.append((name, value))

    return {
        "protocol_version": PROTOCOL_VERSION,
        "request_id": request_id,
        "route": route,
        "target_url": target_url,
        "headers": tuple(cleaned_headers),
        "timeout_seconds": float(timeout),
        "max_bytes": int(max_bytes),
        "retrieved_at": retrieved_at,
    }


# ----------------------------------------------------------------------
# Response envelope decoding
# ----------------------------------------------------------------------


def _coerce_int(
    value: Any, field_name: str, *, minimum: int | None = None
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise BrokerResponseProtocolMismatch(
            f"{field_name} must be an integer, got {type(value).__name__}"
        )
    if minimum is not None and value < minimum:
        raise BrokerResponseProtocolMismatch(
            f"{field_name} must be >= {minimum}, got {value!r}"
        )
    return value


def _coerce_str(value: Any, field_name: str, *, allow_none: bool = False) -> str | None:
    if value is None:
        if allow_none:
            return None
        raise BrokerResponseProtocolMismatch(
            f"{field_name} must be a string, got null"
        )
    if not isinstance(value, str):
        raise BrokerResponseProtocolMismatch(
            f"{field_name} must be a string, got {type(value).__name__}"
        )
    return value


def _coerce_headers(value: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(value, list):
        raise BrokerResponseProtocolMismatch(
            "headers must be a list of [name, value] pairs"
        )
    cleaned: list[tuple[str, str]] = []
    for entry in value:
        if (
            not isinstance(entry, list)
            or len(entry) != 2
            or not all(isinstance(part, str) for part in entry)
        ):
            raise BrokerResponseProtocolMismatch(
                "headers entries must be two-element lists of strings"
            )
        cleaned.append((entry[0], entry[1]))
    return tuple(cleaned)


def decode_response_envelope(
    payload: bytes | str | Mapping[str, Any],
    *,
    expected_request_id: str,
    max_bytes: int,
    expected_protocol_version: int = PROTOCOL_VERSION,
) -> BrokerResponse:
    """Decode a broker response envelope into a :class:`BrokerResponse`.

    The caller-side contract:

    * ``payload`` may be raw JSON bytes / text from the HTTP response
      body, or a decoded mapping (e.g. for in-process unit tests).
    * ``expected_request_id`` is the request_id the *client* sent; the
      envelope must echo it back exactly. A mismatch raises
      :class:`BrokerResponseRequestIdMismatch` with both ids attached.
    * ``max_bytes`` is the negotiated cap from the original request;
      a body that decodes to more than this raises
      :class:`BrokerResponseOversize`. The client must **not**
      silently truncate — the broker enforces the cap server-side.
    * ``expected_protocol_version`` defaults to :data:`PROTOCOL_VERSION`;
      a mismatch raises :class:`BrokerResponseProtocolMismatch`.

    When the envelope carries ``error_code`` set, the function raises
    :class:`BrokerUpstreamError` instead of returning a successful
    :class:`BrokerResponse`. This is how the broker signals per-request
    failures (invalid_request, broker_config_error, broker_internal_error).

    Strict base64 is enforced via :func:`base64.b64decode` with
    ``validate=True`` so the decoder refuses non-canonical encodings.
    The advertised ``body_length`` must equal ``len(decoded)`` exactly;
    a mismatch raises :class:`BrokerResponseBodyError`.

    Raises:

    * :class:`BrokerResponseProtocolMismatch` for structural failures.
    * :class:`BrokerResponseRequestIdMismatch` for id mismatches.
    * :class:`BrokerResponseBodyError` for malformed base64 or length
      mismatches.
    * :class:`BrokerResponseOversize` when the decoded body exceeds
      ``max_bytes``.
    * :class:`BrokerUpstreamError` when the envelope reports a failure.
    """

    if isinstance(max_bytes, bool) or not isinstance(max_bytes, (int, float)) or not math.isfinite(max_bytes) or max_bytes <= 0:
        raise BrokerResponseProtocolMismatch(
            f"max_bytes must be a positive integer, got {max_bytes!r}"
        )
    max_bytes = int(max_bytes)
    if max_bytes <= 0:
        raise BrokerResponseProtocolMismatch(
            "max_bytes must encode at least one byte"
        )
    if isinstance(payload, (bytes, bytearray)):
        raw_bytes = bytes(payload)
        if len(raw_bytes) > RESPONSE_ENVELOPE_MAX_BYTES:
            raise BrokerResponseProtocolMismatch(
                f"response envelope is {len(raw_bytes)} bytes, "
                f"exceeds cap of {RESPONSE_ENVELOPE_MAX_BYTES}"
            )
        try:
            text = raw_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise BrokerResponseProtocolMismatch(
                f"response envelope is not UTF-8: {exc}"
            ) from exc
        try:
            decoded_obj: Any = json.loads(text)
        except json.JSONDecodeError as exc:
            raise BrokerResponseProtocolMismatch(
                f"response envelope is not JSON: {exc}"
            ) from exc
    elif isinstance(payload, str):
        try:
            decoded_obj = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BrokerResponseProtocolMismatch(
                f"response envelope is not JSON: {exc}"
            ) from exc
    elif isinstance(payload, Mapping):
        decoded_obj = dict(payload)
    else:
        raise BrokerResponseProtocolMismatch(
            "response envelope must be bytes, str, or mapping; "
            f"got {type(payload).__name__}"
        )

    if not isinstance(decoded_obj, Mapping):
        raise BrokerResponseProtocolMismatch(
            "response envelope must be a JSON object"
        )

    version = decoded_obj.get("protocol_version", expected_protocol_version)
    if version != expected_protocol_version:
        raise BrokerResponseProtocolMismatch(
            f"response protocol_version must be "
            f"{expected_protocol_version}, got {version!r}"
        )

    request_id = _coerce_str(
        decoded_obj.get("request_id"), "request_id"
    )
    if request_id != expected_request_id:
        raise BrokerResponseRequestIdMismatch(
            f"response request_id {request_id!r} does not match "
            f"expected {expected_request_id!r}",
            expected_request_id=expected_request_id,
            offending_request_id=request_id,
        )

    status = _coerce_int(
        decoded_obj.get("status"), "status", minimum=-1
    )
    headers = _coerce_headers(decoded_obj.get("headers", []))
    final_url_raw = _coerce_str(
        decoded_obj.get("final_url"), "final_url", allow_none=True
    )
    final_url = final_url_raw if final_url_raw is not None else ""
    retrieved_at_raw = _coerce_str(
        decoded_obj.get("retrieved_at"), "retrieved_at"
    )
    assert retrieved_at_raw is not None
    retrieved_at = retrieved_at_raw
    body_b64 = decoded_obj.get("body_b64", "")
    if not isinstance(body_b64, str):
        raise BrokerResponseProtocolMismatch(
            "body_b64 must be a string, got "
            f"{type(body_b64).__name__}"
        )
    body_length = _coerce_int(
        decoded_obj.get("body_length"), "body_length", minimum=0
    )
    error_code = decoded_obj.get("error_code")
    error_message = decoded_obj.get("error_message")

    # Strict base64 decode so non-canonical encodings are refused.
    if body_b64 == "":
        decoded_body = b""
    else:
        try:
            decoded_body = base64.b64decode(body_b64, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise BrokerResponseBodyError(
                f"body_b64 is not strict base64: {exc}"
            ) from exc

    if len(decoded_body) != body_length:
        raise BrokerResponseBodyError(
            f"body_length {body_length} does not match decoded "
            f"base64 length {len(decoded_body)}"
        )

    if len(decoded_body) > max_bytes:
        raise BrokerResponseOversize(
            f"decoded body is {len(decoded_body)} bytes, exceeds "
            f"max_bytes={max_bytes}",
            size=len(decoded_body),
            limit=max_bytes,
        )

    if error_code is not None or error_message is not None:
        # The broker is reporting a per-request failure. Surface it as a
        # typed error; callers can branch on ``error_code``.
        if not isinstance(error_code, str):
            raise BrokerResponseProtocolMismatch(
                "error_code must be a string when set, got "
                f"{type(error_code).__name__}"
            )
        if not isinstance(error_message, str):
            raise BrokerResponseProtocolMismatch(
                "error_message must be a string when set, got "
                f"{type(error_message).__name__}"
            )
        raise BrokerUpstreamError(
            f"broker reported {error_code}: {error_message}",
            error_code=error_code,
            error_message=error_message,
            status=status,
        )

    return BrokerResponse(
        request_id=request_id,
        status=status,
        headers=headers,
        body=decoded_body,
        final_url=final_url,
        retrieved_at=retrieved_at,
        error_code=None,
        error_message=None,
    )


# ----------------------------------------------------------------------
# Deterministic request_id derivation
# ----------------------------------------------------------------------


def derive_request_id(
    *,
    source_id: str,
    route: str,
    target_url: str,
    retrieved_at: str,
    extra: Mapping[str, Any] | None = None,
) -> str:
    """Compute a deterministic 32-hex request_id from the envelope inputs.

    The same inputs always produce the same id; this keeps replays stable
    across runs and makes the broker's audit log replayable.
    """

    seed: dict[str, Any] = {
        "protocol_version": PROTOCOL_VERSION,
        "route": route,
        "target_url": target_url,
        "retrieved_at": retrieved_at,
        "source_id": source_id,
    }
    if extra:
        seed.update({key: value for key, value in extra.items()})
    encoded = json.dumps(
        seed,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# ----------------------------------------------------------------------
# Route-to-socket-path resolution
# ----------------------------------------------------------------------


def socket_path_for_route(
    route: str,
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    """Return the absolute fixed UDS path for ``route``.

    The path comes from the environment variables
    :data:`ENV_SEARCH_SOCKET` / :data:`ENV_FEED_SOCKET`. The function is
    pure with respect to ``env`` (no filesystem access); callers that
    wish to verify presence use :func:`os.path.exists` separately.
    """

    if route not in ALLOWED_ROUTES:
        raise UnsupportedRouteError(
            f"route {route!r} has no broker socket binding"
        )
    mapping = {
        ROUTE_SEARCH: ENV_SEARCH_SOCKET,
        ROUTE_FEED: ENV_FEED_SOCKET,
    }
    var = mapping[route]
    environ = env if env is not None else _os_environ()
    try:
        value = environ[var]
    except KeyError as exc:
        raise BrokerProtocolError(
            f"broker socket path is not configured: {var} is unset"
        ) from exc
    if not value or not value.startswith("/"):
        raise BrokerProtocolError(
            f"broker socket path {var}={value!r} must be an absolute path"
        )
    return value


def _os_environ() -> Mapping[str, str]:
    import os

    return os.environ


__all__ = [
    "PROTOCOL_VERSION",
    "ROUTE_SEARCH",
    "ROUTE_FEED",
    "ALLOWED_ROUTES",
    "ALLOWED_HEADERS",
    "DEFAULT_TIMEOUT_SECONDS",
    "DEFAULT_MAX_BYTES",
    "MAX_TIMEOUT_SECONDS",
    "MAX_BYTES_HARD_LIMIT",
    "RESPONSE_ENVELOPE_MAX_BYTES",
    "ENV_SEARCH_SOCKET",
    "ENV_FEED_SOCKET",
    "ENV_CONTAINER_MODE",
    "BrokerProtocolError",
    "UnsupportedRouteError",
    "DisallowedHeaderError",
    "InvalidEnvelopeError",
    "BrokerResponseError",
    "BrokerResponseProtocolMismatch",
    "BrokerResponseRequestIdMismatch",
    "BrokerResponseBodyError",
    "BrokerResponseOversize",
    "BrokerUpstreamError",
    "BrokerRequest",
    "BrokerResponse",
    "validate_request_payload",
    "decode_response_envelope",
    "derive_request_id",
    "socket_path_for_route",
]
