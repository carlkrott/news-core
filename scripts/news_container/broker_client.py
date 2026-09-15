"""Unix-domain-socket broker client and adapter transport wiring.

This module turns the canonical broker envelope (see
:mod:`news_container.broker_protocol`) into a real
``Transport`` that the existing news adapters can call. The transport:

* speaks HTTP/1.1 over an AF_UNIX socket only,
* carries the canonical broker JSON envelope as the request body,
* never invokes ``urllib``/``http.client`` or otherwise talks to the
  network directly,
* fails closed when ``NEWS_CONTAINER_MODE=1`` is set but the required
  socket is missing.

The adapter-side factory function :func:`broker_transport_factory` is
the single integration point with the existing ingest runner. The
runner already accepts a ``transport_factory`` callable; this module
exposes one that respects the container-mode rules.
"""
from __future__ import annotations

import asyncio
import math
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any, Mapping

from news_pipeline.adapters.base import (
    AdapterError,
    FetchRequest,
    FetchResponse,
    NetworkError,
    Transport,
)
from news_pipeline.live_contracts import SourceAdapter, SourceContract

from .broker_protocol import (
    ALLOWED_HEADERS,
    DEFAULT_MAX_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    ENV_CONTAINER_MODE,
    ENV_FEED_SOCKET,
    ENV_SEARCH_SOCKET,
    PROTOCOL_VERSION,
    RESPONSE_ENVELOPE_MAX_BYTES,
    ROUTE_FEED,
    ROUTE_SEARCH,
    BrokerProtocolError,
    BrokerRequest,
    BrokerResponse,
    BrokerResponseBodyError,
    BrokerResponseError,
    BrokerResponseOversize,
    BrokerResponseProtocolMismatch,
    BrokerResponseRequestIdMismatch,
    BrokerUpstreamError,
    decode_response_envelope,
    derive_request_id,
    socket_path_for_route,
)

# ----------------------------------------------------------------------
# Errors
# ----------------------------------------------------------------------


class BrokerUnavailableError(AdapterError):
    """The broker socket is missing or unreachable. Treated as terminal."""

    def __init__(self, message: str, *, socket_path: str | None = None) -> None:
        super().__init__(message)
        self.socket_path = socket_path


class BrokerProtocolMismatch(AdapterError):
    """The broker replied with an envelope that fails validation."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


# ----------------------------------------------------------------------
# Container-mode gate
# ----------------------------------------------------------------------


def container_mode_enabled(env: Mapping[str, str] | None = None) -> bool:
    """Return True iff the runtime is in sealed-container mode.

    Container mode is enabled by the explicit sentinel
    ``NEWS_CONTAINER_MODE=1``. The runner must fail closed if it is set
    but the broker socket is missing — the container is not allowed to
    bypass the broker and reach the network directly.
    """

    environ = env if env is not None else os.environ
    value = environ.get(ENV_CONTAINER_MODE, "")
    return value.strip() == "1"


def route_for_source(contract: SourceContract) -> str:
    """Map a :class:`SourceContract` to its broker route name."""

    if contract.adapter_type is SourceAdapter.SEARXNG:
        return ROUTE_SEARCH
    if contract.adapter_type is SourceAdapter.RSS:
        return ROUTE_FEED
    raise BrokerProtocolError(
        f"adapter type {contract.adapter_type.value!r} has no broker route"
    )


# ----------------------------------------------------------------------
# HTTP-over-UDS request helper
# ----------------------------------------------------------------------


def _build_http_request(
    *,
    socket_path: str,
    envelope_bytes: bytes,
) -> bytes:
    """Render the HTTP/1.1 request that carries the broker envelope."""

    body = envelope_bytes
    request_line = "POST /broker HTTP/1.1\r\n"
    headers = (
        f"Host: localhost\r\n"
        f"Content-Type: application/json; charset=utf-8\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n"
        f"X-News-Broker-Version: {PROTOCOL_VERSION}\r\n"
        "\r\n"
    )
    return request_line.encode("ascii") + headers.encode("ascii") + body


def _parse_http_response(raw: bytes) -> tuple[int, list[tuple[str, str]], bytes]:
    """Parse a raw HTTP/1.1 response into (status, headers, body)."""

    header_end = raw.find(b"\r\n\r\n")
    if header_end < 0:
        raise BrokerProtocolMismatch("broker response is missing HTTP headers")
    head = raw[:header_end]
    body = raw[header_end + 4 :]
    lines = head.split(b"\r\n")
    if not lines:
        raise BrokerProtocolMismatch("broker response has empty status line")
    status_line = lines[0].decode("ascii", errors="replace")
    parts = status_line.split(" ", 2)
    if len(parts) < 2 or parts[0] != "HTTP/1.1":
        raise BrokerProtocolMismatch(
            f"broker response status line is not HTTP/1.1: {status_line!r}"
        )
    try:
        status = int(parts[1])
    except ValueError as exc:
        raise BrokerProtocolMismatch(
            f"broker response status is not numeric: {parts[1]!r}"
        ) from exc
    headers: list[tuple[str, str]] = []
    for line in lines[1:]:
        if b":" not in line:
            continue
        name, _, value = line.partition(b":")
        headers.append((name.decode("ascii").strip(), value.decode("ascii").strip()))
    return status, headers, body


def _positive_finite_number(value: Any, *, field_name: str, integer: bool = False) -> float | int:
    """Validate resource limits without bool/non-finite coercion."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BrokerProtocolError(f"{field_name} must be a finite positive number")
    if not math.isfinite(value) or value <= 0:
        raise BrokerProtocolError(f"{field_name} must be a finite positive number")
    if integer:
        value = int(value)
        if value <= 0:
            raise BrokerProtocolError(f"{field_name} must be a finite positive number")
    return value


def _remaining_timeout(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise socket.timeout("broker deadline exceeded")
    return remaining


def _sync_send_envelope(
    *,
    socket_path: str,
    envelope_bytes: bytes,
    timeout_seconds: float,
    max_bytes: int,
) -> bytes:
    """Send the envelope over AF_UNIX and return the raw HTTP response."""

    timeout_seconds = float(
        _positive_finite_number(
            timeout_seconds, field_name="timeout_seconds"
        )
    )
    max_bytes = int(
        _positive_finite_number(max_bytes, field_name="max_bytes", integer=True)
    )
    payload = _build_http_request(socket_path=socket_path, envelope_bytes=envelope_bytes)
    deadline = time.monotonic() + timeout_seconds
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(_remaining_timeout(deadline))
            client.connect(socket_path)
            client.settimeout(_remaining_timeout(deadline))
            client.sendall(payload)
            chunks: list[bytes] = []
            received = 0
            while True:
                client.settimeout(_remaining_timeout(deadline))
                chunk = client.recv(65536)
                if not chunk:
                    break
                chunks.append(chunk)
                received += len(chunk)
                if received > max_bytes + 65536:
                    raise BrokerProtocolMismatch("broker response exceeded bounded limit")
            return b"".join(chunks)
    except FileNotFoundError as exc:
        raise BrokerUnavailableError("broker_unavailable", socket_path=socket_path) from exc
    except socket.timeout as exc:
        raise BrokerUnavailableError("broker_timeout", socket_path=socket_path) from exc
    except (ConnectionRefusedError, OSError) as exc:
        raise BrokerUnavailableError("broker_unavailable", socket_path=socket_path) from exc


# ----------------------------------------------------------------------
# Async transport
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _BrokerConfig:
    """Resolved broker configuration for one adapter source."""

    socket_path: str
    route: str
    timeout_seconds: float
    max_bytes: int

    @classmethod
    def for_source(
        cls,
        contract: SourceContract,
        *,
        env: Mapping[str, str] | None = None,
        default_timeout: float = DEFAULT_TIMEOUT_SECONDS,
        default_max_bytes: int = DEFAULT_MAX_BYTES,
    ) -> "_BrokerConfig":
        route = route_for_source(contract)
        socket_path = socket_path_for_route(route, env=env)
        return cls(
            socket_path=socket_path,
            route=route,
            timeout_seconds=float(
                _positive_finite_number(
                    default_timeout, field_name="timeout_seconds"
                )
            ),
            max_bytes=int(
                _positive_finite_number(
                    default_max_bytes, field_name="max_bytes", integer=True
                )
            ),
        )


class BrokerTransport:
    """Async ``Transport`` that funnels requests through the broker."""

    def __init__(
        self,
        *,
        socket_path: str,
        route: str,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        max_bytes: int = DEFAULT_MAX_BYTES,
        source_id: str | None = None,
        env: Mapping[str, str] | None = None,
        sync_send=_sync_send_envelope,
    ) -> None:
        self._socket_path = socket_path
        self._route = route
        self._timeout_seconds = float(
            _positive_finite_number(timeout_seconds, field_name="timeout_seconds")
        )
        self._max_bytes = int(
            _positive_finite_number(max_bytes, field_name="max_bytes", integer=True)
        )
        self._source_id = source_id
        self._env = env
        self._sync_send = sync_send

    @property
    def socket_path(self) -> str:
        return self._socket_path

    @property
    def route(self) -> str:
        return self._route

    async def __call__(
        self, request: FetchRequest, *, retrieved_at: str
    ) -> FetchResponse:
        """Issue a single envelope round-trip and return a FetchResponse.

        The round-trip is:

        1. Build a canonical :class:`BrokerRequest` envelope.
        2. Send it over the AF_UNIX socket.
        3. Parse the HTTP/1.1 response (outer transport layer — separate
           from the upstream status carried inside the envelope).
        4. Decode the JSON :class:`BrokerResponse` envelope from the
           HTTP body, verifying ``protocol_version``, ``request_id``,
           strict base64, ``body_length``, and the negotiated
           ``max_bytes`` cap. Map any envelope failure to a typed error.
        5. Surface a typed :class:`BrokerUpstreamError` when the broker
           reports a per-request failure via ``error_code``.
        6. Return a :class:`FetchResponse` whose ``status`` is the
           upstream status, ``body`` is the decoded base64 bytes,
           ``final_url`` is the broker-reported final URL, and
           ``headers`` are the broker-sanitised response headers.

        The decoded body is **never** truncated — oversize payloads
        raise :class:`BrokerResponseOversize` instead of being silently
        chopped. The outer HTTP layer is bounded to
        :data:`RESPONSE_ENVELOPE_MAX_BYTES` plus a small HTTP framing
        allowance; an envelope that exceeds the negotiated ``max_bytes``
        is rejected up front so we do not pay the cost of decoding it.
        """

        cleaned_headers = _filter_allowed_headers(request.headers)
        request_timeout = float(
            _positive_finite_number(
                request.timeout_seconds, field_name="timeout_seconds"
            )
        )
        request_max_bytes = int(
            _positive_finite_number(
                request.max_response_bytes, field_name="max_bytes", integer=True
            )
        )
        envelope = BrokerRequest(
            request_id=derive_request_id(
                source_id=self._source_id or "unknown",
                route=self._route,
                target_url=request.url,
                retrieved_at=retrieved_at,
            ),
            route=self._route,
            target_url=request.url,
            headers=cleaned_headers,
            timeout_seconds=min(self._timeout_seconds, request_timeout),
            max_bytes=min(self._max_bytes, request_max_bytes),
            retrieved_at=retrieved_at,
        )
        envelope_bytes = envelope.to_wire_bytes()
        loop = asyncio.get_running_loop()
        try:
            raw = await loop.run_in_executor(
                None,
                lambda: self._sync_send(
                    socket_path=self._socket_path,
                    envelope_bytes=envelope_bytes,
                    timeout_seconds=envelope.timeout_seconds,
                    max_bytes=envelope.max_bytes,
                ),
            )
        except BrokerUnavailableError:
            raise
        except NetworkError:
            raise

        status, headers, body = _parse_http_response(raw)
        # The outer HTTP transport status is the broker-to-container
        # transport code. A 200 here means the broker successfully
        # received and answered our envelope; the *upstream* status lives
        # inside the decoded JSON envelope. We surface the upstream
        # status as ``FetchResponse.status`` because that is what the
        # adapter (and the rest of the news pipeline) actually want.
        del headers  # the envelope carries the real response headers.

        # Bound the response envelope. The envelope itself is small;
        # ``max_bytes`` is the negotiated cap on the *decoded body*,
        # not the envelope. We refuse envelopes larger than the
        # RESPONSE_ENVELOPE_MAX_BYTES bound or envelopes whose body
        # would obviously exceed the negotiated cap (when we can tell).
        max_envelope_on_wire = (
            envelope.max_bytes
            + RESPONSE_ENVELOPE_MAX_BYTES
        )
        if len(body) > max_envelope_on_wire:
            raise BrokerProtocolMismatch(
                f"broker response envelope is {len(body)} bytes, "
                f"exceeds bounded wire cap of {max_envelope_on_wire}"
            )

        try:
            decoded = decode_response_envelope(
                body,
                expected_request_id=envelope.request_id,
                max_bytes=envelope.max_bytes,
            )
        except BrokerUpstreamError:
            # Per-request failure surfaced by the broker. Surface as a
            # typed BrokerProtocolMismatch so the runner classifies it
            # consistently; keep the original error chained so the
            # ``error_code`` is still reachable for tests/diagnostics.
            raise
        except BrokerResponseRequestIdMismatch:
            # A wrong request_id is a hard protocol violation: the
            # broker mixed up streams. Re-raise as a typed
            # ``BrokerProtocolMismatch`` so the runner can react.
            raise BrokerProtocolMismatch(
                "broker response carried a request_id that does not "
                "match the request we sent"
            ) from None
        except BrokerResponseOversize as exc:
            raise BrokerProtocolMismatch(
                f"broker response body {exc.size} bytes exceeds "
                f"max_bytes={exc.limit} (no truncation permitted)"
            ) from exc
        except BrokerResponseBodyError as exc:
            raise BrokerProtocolMismatch(
                f"broker response body is malformed: {exc}"
            ) from exc
        except BrokerResponseProtocolMismatch as exc:
            raise BrokerProtocolMismatch(
                f"broker response envelope failed validation: {exc}"
            ) from exc

        return FetchResponse(
            status=decoded.status,
            headers=tuple((name, value) for name, value in decoded.headers),
            body=decoded.body,
            final_url=decoded.final_url or request.url,
        )


def _filter_allowed_headers(
    headers: tuple[tuple[str, str], ...] | list[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Drop any header that is not on the broker allowlist."""

    cleaned: list[tuple[str, str]] = []
    for name, value in headers:
        if name in ALLOWED_HEADERS:
            cleaned.append((name, value))
    return tuple(cleaned)


# ----------------------------------------------------------------------
# Adapter factory — wired into the ingest runner.
# ----------------------------------------------------------------------

# Local fallback constant — when the runtime defaults the broker client's
# per-request max_bytes it uses the standard adapter cap of 2 MiB.
DEFAULT_BROKER_MAX_BYTES = 2 * 1024 * 1024


def broker_transport_factory(
    contract: SourceContract,
    *,
    env: Mapping[str, str] | None = None,
    default_timeout: float = DEFAULT_TIMEOUT_SECONDS,
    default_max_bytes: int = DEFAULT_MAX_BYTES,
) -> Transport:
    """Build a broker :class:`Transport` for ``contract``.

    The factory is the integration point with
    :func:`news_pipeline.ingest_runner.run_ingest`. It picks the route
    from the source's adapter type and resolves the absolute socket path
    via :func:`socket_path_for_route`.

    Behavioural contract:

    * If ``NEWS_CONTAINER_MODE=1`` and the resolved socket path does not
      exist on disk, raise :class:`BrokerUnavailableError` *before* any
      attempt to talk to the broker. The container must not fall back to
      ``urllib`` or any other direct transport.
    * If ``NEWS_CONTAINER_MODE`` is unset/0, behave like the legacy
      path: still build a broker transport, but do not fail closed when
      the socket is missing — the runner's existing transport_factory
      tests rely on that compatibility.
    """

    environ = env if env is not None else os.environ
    try:
        config = _BrokerConfig.for_source(
            contract,
            env=environ,
            default_timeout=default_timeout,
            default_max_bytes=default_max_bytes,
        )
    except BrokerProtocolError as exc:
        # Adapter type without a route mapping is a programming error
        # for both modes. Surface immediately.
        raise BrokerUnavailableError(
            f"no broker route for source {contract.source_id}: {exc}"
        ) from exc

    if container_mode_enabled(environ) and not os.path.exists(config.socket_path):
        raise BrokerUnavailableError(
            (
                f"container mode requires broker socket at {config.socket_path} "
                f"for source {contract.source_id}; refusing to fall back to "
                "direct network egress"
            ),
            socket_path=config.socket_path,
        )

    return BrokerTransport(
        socket_path=config.socket_path,
        route=config.route,
        timeout_seconds=config.timeout_seconds,
        max_bytes=config.max_bytes,
        source_id=contract.source_id,
        env=environ,
    )


__all__ = [
    "BrokerTransport",
    "BrokerUnavailableError",
    "BrokerProtocolMismatch",
    "broker_transport_factory",
    "container_mode_enabled",
    "route_for_source",
]
