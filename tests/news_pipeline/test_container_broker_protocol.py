"""Network-disabled tests for the search/feed Unix-domain-socket broker.

These tests use a real AF_UNIX socket bound by a fake broker thread —
not a stubbed ``socket.socket`` — so the seam is exercised end-to-end.
The fake broker parses the canonical envelope, returns a synthetic
HTTP/1.1 reply, and the test verifies both the wire shape and the
adapter integration.

Coverage:

* Canonical envelope: serialisation, deterministic request_id, header
  allowlist enforcement, route allowlist, timeout / max_bytes bounds.
* BrokerTransport round-trip: SearXNG and RSS adapters successfully
  fetch through the broker; legacy urllib default is never invoked in
  container mode.
* Container-mode gate: ``NEWS_CONTAINER_MODE=1`` with a missing socket
  raises :class:`BrokerUnavailableError` and the runner does not fall
  back to direct egress.
* Default-mode compatibility: ``NEWS_CONTAINER_MODE`` unset means the
  legacy ``urllib``-backed factory still works for the existing tests.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import socket
import tempfile
import threading
import unittest
from pathlib import Path
from typing import Any
from unittest import mock

from news_pipeline.adapters import FetchRequest, FetchResponse, Transport
from news_pipeline.adapters.rss import RssAdapter
from news_pipeline.adapters.searxng import SearxngAdapter
import news_pipeline.jobs as jobs
from news_pipeline.live_contracts import (
    QuerySeed,
    SourceAdapter,
    SourceContract,
    SourceRole,
)

from news_container import broker_protocol
from news_container.broker_client import (
    BrokerProtocolMismatch,
    BrokerTransport,
    BrokerUnavailableError,
    broker_transport_factory,
    container_mode_enabled,
    route_for_source,
)
from news_container.broker_protocol import (
    ALLOWED_HEADERS,
    ALLOWED_ROUTES,
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
    DisallowedHeaderError,
    InvalidEnvelopeError,
    UnsupportedRouteError,
    decode_response_envelope,
    derive_request_id,
    socket_path_for_route,
    validate_request_payload,
)


# ---------------------------------------------------------------------------
# Source contract helpers
# ---------------------------------------------------------------------------


def _searxng_contract() -> SourceContract:
    return SourceContract(
        source_id="searxng-ai-main",
        adapter_type=SourceAdapter.SEARXNG,
        source_role=SourceRole.DISCOVERY,
        host="searxng.example.com",
        category_scope=("ai",),
        enabled=True,
        queries=(QuerySeed(text="LLM+AI+model+release+2026", categories=("news", "it")),),
        cadence_minutes=180,
    )


def _rss_contract() -> SourceContract:
    return SourceContract(
        source_id="rss-ai-feed",
        adapter_type=SourceAdapter.RSS,
        source_role=SourceRole.PRIMARY,
        host="feed.example.com",
        category_scope=("ai",),
        enabled=True,
        queries=(QuerySeed(text="https://feed.example.com/rss.xml", categories=("news",)),),
        cadence_minutes=60,
    )


# ---------------------------------------------------------------------------
# Fake broker
# ---------------------------------------------------------------------------


class _FakeBroker:
    """AF_UNIX HTTP/1.1 server that produces real BrokerResponse envelopes.

    The wire shape mirrors ``host/news_egress_broker.py``:

    * HTTP/1.1 status line, ``Content-Type: application/json; charset=utf-8``,
      ``Content-Length`` of the envelope body, ``X-News-Broker-Request-Id``,
      ``Connection: close``.
    * Body: the canonical JSON envelope from
      :meth:`broker_protocol.BrokerResponse.to_wire_bytes`, which carries
      ``protocol_version``, ``request_id``, ``status``, ``headers``,
      ``body_b64``, ``final_url``, ``retrieved_at``, ``error_code``,
      ``error_message``, ``body_length``.

    The fake honours a per-instance override hook so tests can produce
    ``error_code`` envelopes, mismatched ``request_id`` envelopes, or
    malformed base64 envelopes without forking the harness.
    """

    def __init__(
        self,
        socket_path: str,
        *,
        response_status: int = 200,
        response_headers: tuple[tuple[str, str], ...] = (),
        response_body: bytes = b"",
        response_final_url: str = "",
        retrieved_at: str = "2026-09-14T00:00:00Z",
        response_override=None,
        raw_response_override: bytes | None = None,
    ) -> None:
        self.socket_path = socket_path
        self._response_status = response_status
        self._response_headers = response_headers
        self._response_body = response_body
        self._response_final_url = response_final_url
        self._retrieved_at = retrieved_at
        self._response_override = response_override
        self._raw_response_override = raw_response_override
        self.requests: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(self.socket_path)
        os.chmod(self.socket_path, 0o660)
        self._server.listen(8)
        self._server.settimeout(15.0)
        self._running = True
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        try:
            self._server.close()
        except OSError:
            pass
        if os.path.exists(self.socket_path):
            os.unlink(self.socket_path)
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _serve(self) -> None:
        while self._running:
            try:
                conn, _ = self._server.accept()
            except OSError:
                return
            try:
                self._handle(conn)
            finally:
                try:
                    conn.close()
                except OSError:
                    pass

    def _handle(self, conn: socket.socket) -> None:
        chunks: list[bytes] = []
        while b"\r\n\r\n" not in b"".join(chunks):
            try:
                chunk = conn.recv(65536)
            except OSError:
                return
            if not chunk:
                break
            chunks.append(chunk)
            if len(b"".join(chunks)) > 4 * 1024 * 1024:
                return
        raw = b"".join(chunks)
        head_end = raw.find(b"\r\n\r\n")
        if head_end < 0:
            return
        body = raw[head_end + 4 :]
        envelope = json.loads(body.decode("utf-8"))
        with self._lock:
            self.requests.append(envelope)

        if self._raw_response_override is not None:
            try:
                conn.sendall(self._raw_response_override)
            except OSError:
                pass
            return

        final_url = self._response_final_url or envelope.get("target_url", "")
        request_id = envelope.get("request_id", "")
        if self._response_override is not None:
            envelope_bytes, http_status = self._response_override(envelope)
        else:
            response = BrokerResponse(
                request_id=request_id,
                status=self._response_status,
                headers=self._response_headers,
                body=self._response_body,
                final_url=final_url,
                retrieved_at=self._retrieved_at,
            )
            envelope_bytes = response.to_wire_bytes()
            http_status = 200
        body_bytes = envelope_bytes
        headers_lines = [
            "HTTP/1.1 {status} OK\r\n".format(status=http_status),
            "Content-Type: application/json; charset=utf-8\r\n",
            "Content-Length: {length}\r\n".format(length=len(body_bytes)),
            "X-News-Broker-Request-Id: {rid}\r\n".format(rid=request_id),
            "Connection: close\r\n",
        ]
        response = "".join(headers_lines).encode("ascii") + b"\r\n" + body_bytes
        try:
            conn.sendall(response)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Envelope tests
# ---------------------------------------------------------------------------


class EnvelopeTests(unittest.TestCase):
    def test_route_allowlist_only_search_and_feed(self) -> None:
        self.assertEqual(ALLOWED_ROUTES, frozenset({"search", "feed"}))

    def test_canonical_json_is_deterministic(self) -> None:
        env = BrokerRequest(
            request_id="a" * 64,
            route=ROUTE_SEARCH,
            target_url="https://searxng.example.com/search?q=ai",
            headers=(("Accept", "application/json"),),
            timeout_seconds=15.0,
            max_bytes=1024,
            retrieved_at="2026-09-14T00:00:00Z",
        )
        first = env.to_canonical_json()
        second = env.to_canonical_json()
        self.assertEqual(first, second)
        self.assertIn('"protocol_version":1', first)
        self.assertNotIn(" ", first)
        # Confirm that the canonical ordering is alphabetical by key.
        self.assertLess(first.index('"max_bytes"'), first.index('"protocol_version"'))
        self.assertLess(first.index('"request_id"'), first.index('"route"'))

    def test_request_id_is_deterministic(self) -> None:
        first = derive_request_id(
            source_id="src1",
            route=ROUTE_SEARCH,
            target_url="https://example.com/",
            retrieved_at="2026-09-14T00:00:00Z",
        )
        second = derive_request_id(
            source_id="src1",
            route=ROUTE_SEARCH,
            target_url="https://example.com/",
            retrieved_at="2026-09-14T00:00:00Z",
        )
        self.assertEqual(first, second)
        self.assertEqual(len(first), 64)
        different = derive_request_id(
            source_id="src1",
            route=ROUTE_SEARCH,
            target_url="https://example.com/other",
            retrieved_at="2026-09-14T00:00:00Z",
        )
        self.assertNotEqual(first, different)

    def test_validate_rejects_unsupported_route(self) -> None:
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "a" * 64,
            "route": "delivery",
            "target_url": "https://example.com/",
            "headers": [],
            "timeout_seconds": 1.0,
            "max_bytes": 1024,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        with self.assertRaises(UnsupportedRouteError):
            validate_request_payload(payload)

    def test_validate_rejects_disallowed_header(self) -> None:
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "a" * 64,
            "route": ROUTE_SEARCH,
            "target_url": "https://example.com/",
            "headers": [["Authorization", "Bearer secret"]],
            "timeout_seconds": 1.0,
            "max_bytes": 1024,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        with self.assertRaises(DisallowedHeaderError):
            validate_request_payload(payload)

    def test_validate_enforces_timeout_bounds(self) -> None:
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "a" * 64,
            "route": ROUTE_SEARCH,
            "target_url": "https://example.com/",
            "headers": [],
            "timeout_seconds": 0,
            "max_bytes": 1024,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        with self.assertRaises(InvalidEnvelopeError):
            validate_request_payload(payload)

    def test_validate_enforces_max_bytes_bounds(self) -> None:
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "a" * 64,
            "route": ROUTE_SEARCH,
            "target_url": "https://example.com/",
            "headers": [],
            "timeout_seconds": 1.0,
            "max_bytes": 0,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        with self.assertRaises(InvalidEnvelopeError):
            validate_request_payload(payload)

    def test_socket_path_for_route_requires_absolute_path(self) -> None:
        env = {ENV_SEARCH_SOCKET: "/tmp/search.sock"}
        self.assertEqual(
            socket_path_for_route(ROUTE_SEARCH, env=env), "/tmp/search.sock"
        )
        env = {ENV_FEED_SOCKET: "/tmp/feed.sock"}
        self.assertEqual(socket_path_for_route(ROUTE_FEED, env=env), "/tmp/feed.sock")

    def test_socket_path_for_route_rejects_missing_env(self) -> None:
        with self.assertRaises(BrokerProtocolError):
            socket_path_for_route(ROUTE_SEARCH, env={})

    def test_socket_path_for_route_rejects_relative_path(self) -> None:
        env = {ENV_SEARCH_SOCKET: "search.sock"}
        with self.assertRaises(BrokerProtocolError):
            socket_path_for_route(ROUTE_SEARCH, env=env)

    def test_socket_path_for_route_rejects_unsupported_route(self) -> None:
        env = {ENV_SEARCH_SOCKET: "/tmp/search.sock"}
        with self.assertRaises(UnsupportedRouteError):
            socket_path_for_route("delivery", env=env)


# ---------------------------------------------------------------------------
# Round-trip / adapter seam tests
# ---------------------------------------------------------------------------


class _BrokerHarness:
    def __init__(
        self,
        *,
        route: str,
        response_body: bytes,
        response_status: int = 200,
        response_headers: tuple[tuple[str, str], ...] = (
            ("Content-Type", "application/json"),
        ),
    ) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self.tmp.name, "broker.sock")
        env_var = ENV_SEARCH_SOCKET if route == ROUTE_SEARCH else ENV_FEED_SOCKET
        self.env = {env_var: self.socket_path}
        self.broker = _FakeBroker(
            self.socket_path,
            response_status=response_status,
            response_body=response_body,
            response_headers=response_headers,
        )

    def __enter__(self) -> "_BrokerHarness":
        self.broker.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.broker.stop()
        self.tmp.cleanup()


class RoundTripTests(unittest.IsolatedAsyncioTestCase):
    async def test_searxng_adapter_round_trip_through_broker(self) -> None:
        body = (
            b'{"results":[{"url":"https://example.com/story",'
            b'"title":"A title","content":"Body","publishedDate":"2026-09-14T00:00:00Z",'
            b'"id":"abc","author":"Reporter"}],'
            b'"query":"ai","number_of_results":1}'
        )
        with _BrokerHarness(route=ROUTE_SEARCH, response_body=body) as harness:
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            adapter = SearxngAdapter(
                _searxng_contract(),
                category="ai",
                source_role="discovery",
                transport=transport,
            )
            result = await adapter.fetch_query(
                QuerySeed(text="AI news", categories=("news",)),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            self.assertTrue(result.is_success)
            self.assertEqual(len(result.items), 1)
            self.assertEqual(result.items[0].external_id, "abc")
            self.assertEqual(harness.broker.requests[0]["route"], "search")
            self.assertEqual(
                harness.broker.requests[0]["target_url"],
                "http://searxng.example.com/search?q=AI+news&format=json&language=en&safesearch=0&categories=news",
            )
            self.assertEqual(
                harness.broker.requests[0]["headers"],
                [["Accept", "application/json"], ["User-Agent", "news-pipeline/2.0 (standard-library adapter)"]],
            )

    async def test_rss_adapter_round_trip_through_broker(self) -> None:
        xml = (
            b"<?xml version='1.0'?><rss version='2.0'><channel>"
            b"<item><guid>rss-1</guid><title>Release</title>"
            b"<link>https://feed.example.com/item</link>"
            b"<description>Details</description>"
            b"<pubDate>Wed, 14 Sep 2026 00:00:00 GMT</pubDate>"
            b"</item></channel></rss>"
        )
        with _BrokerHarness(
            route=ROUTE_FEED,
            response_body=xml,
            response_headers=(("Content-Type", "application/rss+xml"),),
        ) as harness:
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_rss_contract(), env=env)
            adapter = RssAdapter(
                _rss_contract(),
                category="ai",
                source_role="primary",
                transport=transport,
            )
            result = await adapter.fetch_feed(
                QuerySeed(text="https://feed.example.com/rss.xml", categories=("news",)),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            self.assertTrue(result.is_success, msg=f"error={result.error}")
            self.assertEqual(len(result.items), 1)
            self.assertEqual(harness.broker.requests[0]["route"], "feed")

    async def test_broker_request_id_is_deterministic(self) -> None:
        body = b'{"results":[]}'
        with _BrokerHarness(route=ROUTE_SEARCH, response_body=body) as harness:
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            adapter = SearxngAdapter(
                _searxng_contract(),
                category="ai",
                source_role="discovery",
                transport=transport,
            )
            await adapter.fetch_query(
                QuerySeed(text="AI news", categories=("news",)),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            request_id = harness.broker.requests[0]["request_id"]
            self.assertEqual(len(request_id), 64)
            # Deterministic on a replay with the same envelope inputs.
            await adapter.fetch_query(
                QuerySeed(text="AI news", categories=("news",)),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            self.assertEqual(harness.broker.requests[1]["request_id"], request_id)

    async def test_broker_forwards_only_allowlisted_headers(self) -> None:
        body = b'{"results":[]}'
        with _BrokerHarness(route=ROUTE_SEARCH, response_body=body) as harness:
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            adapter = SearxngAdapter(
                _searxng_contract(),
                category="ai",
                source_role="discovery",
                transport=transport,
            )

            class _NoisyTransport:
                async def __call__(
                    self, request: FetchRequest, *, retrieved_at: str
                ) -> FetchResponse:
                    return await transport(
                        FetchRequest(
                            url=request.url,
                            headers=(
                                ("Accept", "application/json"),
                                ("Authorization", "Bearer leak"),
                                ("Cookie", "session=leak"),
                                ("User-Agent", "news-pipeline/2.0"),
                                ("X-Custom", "secret"),
                            ),
                            timeout_seconds=request.timeout_seconds,
                            max_response_bytes=request.max_response_bytes,
                        ),
                        retrieved_at=retrieved_at,
                    )

            adapter._transport = _NoisyTransport()  # type: ignore[assignment]
            await adapter.fetch_query(
                QuerySeed(text="AI news", categories=("news",)),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            forwarded = harness.broker.requests[0]["headers"]
            names = {name for name, _ in forwarded}
            self.assertNotIn("Authorization", names)
            self.assertNotIn("Cookie", names)
            self.assertNotIn("X-Custom", names)
            self.assertIn("Accept", names)
            self.assertIn("User-Agent", names)


# ---------------------------------------------------------------------------
# Container-mode gate tests
# ---------------------------------------------------------------------------


class ContainerGateTests(unittest.TestCase):
    def test_jobs_tick_selects_broker_factory_in_container_mode(self) -> None:
        argv = [
            "tick",
            "--db", "/state/news-state.db",
            "--sources", "/app/config/news-sources.toml",
            "--topics", "/app/config/news-topics.toml",
            "--policy", "/app/config/news-policy.toml",
            "--run-started-at", "2026-09-14T00:00:00Z",
            "--enable-network",
        ]
        with mock.patch.dict(os.environ, {ENV_CONTAINER_MODE: "1"}, clear=False), mock.patch(
            "news_pipeline.ingest_runner.run_ingest_sync"
        ) as run_ingest:
            run_ingest.return_value = {"ok": True}
            self.assertEqual(jobs.main(argv), 0)
        self.assertIs(
            run_ingest.call_args.kwargs["transport_factory"],
            broker_transport_factory,
        )

    def test_container_mode_off_when_env_unset(self) -> None:
        self.assertFalse(container_mode_enabled(env={}))

    def test_container_mode_on_only_for_exact_one(self) -> None:
        self.assertTrue(container_mode_enabled(env={ENV_CONTAINER_MODE: "1"}))
        self.assertFalse(container_mode_enabled(env={ENV_CONTAINER_MODE: "0"}))
        self.assertFalse(container_mode_enabled(env={ENV_CONTAINER_MODE: "true"}))

    def test_missing_socket_in_container_mode_fails_closed(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        try:
            missing = os.path.join(tmp.name, "absent.sock")
            env = {ENV_CONTAINER_MODE: "1", ENV_SEARCH_SOCKET: missing}
            with self.assertRaises(BrokerUnavailableError) as ctx:
                broker_transport_factory(_searxng_contract(), env=env)
            self.assertEqual(ctx.exception.socket_path, missing)
        finally:
            tmp.cleanup()

    def test_missing_socket_in_container_mode_fails_closed_when_env_unset(self) -> None:
        """Container mode + unset socket env is also a hard fail-closed.

        This guards the situation where the broker socket path env var
        itself is missing (e.g. the Compose service has not been wired
        up). The container must refuse to operate.
        """
        env = {ENV_CONTAINER_MODE: "1"}
        with self.assertRaises(BrokerUnavailableError):
            broker_transport_factory(_searxng_contract(), env=env)

    def test_missing_socket_outside_container_mode_is_constructed(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        try:
            missing = os.path.join(tmp.name, "absent.sock")
            env = {ENV_SEARCH_SOCKET: missing}
            transport = broker_transport_factory(_searxng_contract(), env=env)
            self.assertIsInstance(transport, BrokerTransport)
        finally:
            tmp.cleanup()

    def test_container_mode_never_invokes_default_urllib_transport(self) -> None:
        """Container mode must refuse to fall back to direct network egress.

        We patch :mod:`urllib.request.urlopen` and the module-level
        :func:`_default_transport` (the only direct-egress path used
        by the legacy adapter default). Either being called would mean
        the container bypassed the broker.
        """
        tmp = tempfile.TemporaryDirectory()
        try:
            missing = os.path.join(tmp.name, "absent.sock")
            env = {ENV_CONTAINER_MODE: "1", ENV_SEARCH_SOCKET: missing}
            with mock.patch(
                "urllib.request.urlopen"
            ) as patched_urlopen, mock.patch(
                "news_pipeline.adapters.base._default_transport"
            ) as patched_default:
                with self.assertRaises(BrokerUnavailableError):
                    broker_transport_factory(_searxng_contract(), env=env)
                patched_urlopen.assert_not_called()
                patched_default.assert_not_called()
        finally:
            tmp.cleanup()


# ---------------------------------------------------------------------------
# Direct-egress / network-disabled integration tests
# ---------------------------------------------------------------------------


class DirectEgressRejectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_broker_transport_never_opens_tcp_socket(self) -> None:
        body = b'{"results":[]}'
        with _BrokerHarness(route=ROUTE_SEARCH, response_body=body) as harness:
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            with mock.patch(
                "socket.socket", wraps=socket.socket
            ) as patched_socket:
                await transport(
                    FetchRequest(
                        url="http://searxng.example.com/search?q=ai",
                        headers=(),
                        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                        max_response_bytes=DEFAULT_MAX_BYTES,
                    ),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
                family_calls = [
                    call.args[0]
                    for call in patched_socket.call_args_list
                    if call.args
                ]
            # AF_UNIX = 1; AF_INET = 2. No TCP sockets should ever be opened.
            self.assertTrue(all(family == socket.AF_UNIX for family in family_calls))
            self.assertNotIn(socket.AF_INET, family_calls)

    async def test_urllib_default_transport_unused_when_broker_factory_chosen(
        self,
    ) -> None:
        body = b'{"results":[]}'
        with _BrokerHarness(route=ROUTE_SEARCH, response_body=body) as harness:
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            adapter = SearxngAdapter(
                _searxng_contract(),
                category="ai",
                source_role="discovery",
                transport=transport,
            )
            with mock.patch(
                "news_pipeline.adapters.base.urllib.request.urlopen"
            ) as patched:
                await adapter.fetch_query(
                    QuerySeed(text="AI news", categories=("news",)),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
                patched.assert_not_called()


# ---------------------------------------------------------------------------
# Legacy/default-mode compatibility tests
# ---------------------------------------------------------------------------


class LegacyCompatibilityTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_mode_routes_through_legacy_factory(self) -> None:
        async def legacy(request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
            return FetchResponse(
                status=200,
                headers=(("Content-Type", "application/json"),),
                body=b'{"results":[]}',
                final_url=request.url,
            )

        class LegacyFactory:
            def __call__(self, contract: SourceContract) -> Transport:
                return legacy

        adapter = SearxngAdapter(
            _searxng_contract(),
            category="ai",
            source_role="discovery",
            transport=LegacyFactory()(_searxng_contract()),
        )
        result = await adapter.fetch_query(
            QuerySeed(text="AI news", categories=("news",)),
            retrieved_at="2026-09-14T00:00:00Z",
        )
        self.assertTrue(result.is_success)


# ---------------------------------------------------------------------------
# Misc coverage
# ---------------------------------------------------------------------------


class RouteMappingTests(unittest.TestCase):
    def test_route_for_source_searxng(self) -> None:
        self.assertEqual(route_for_source(_searxng_contract()), ROUTE_SEARCH)

    def test_route_for_source_rss(self) -> None:
        self.assertEqual(route_for_source(_rss_contract()), ROUTE_FEED)

    def test_route_for_source_unsupported_raises(self) -> None:
        contract = SourceContract(
            source_id="hn",
            adapter_type=SourceAdapter.HACKER_NEWS,
            source_role=SourceRole.DISCOVERY,
            host="hn.example.com",
            category_scope=("ai",),
            enabled=True,
            queries=(QuerySeed(text="hn", categories=("news",)),),
        )
        with self.assertRaises(BrokerProtocolError):
            route_for_source(contract)


class ProtocolStabilityTests(unittest.TestCase):
    def test_allowed_headers_match_documented_set(self) -> None:
        self.assertEqual(
            ALLOWED_HEADERS,
            frozenset(
                {
                    "Accept",
                    "Accept-Encoding",
                    "Accept-Language",
                    "User-Agent",
                    "If-None-Match",
                    "If-Modified-Since",
                }
            ),
        )

    def test_protocol_version_is_one(self) -> None:
        self.assertEqual(PROTOCOL_VERSION, 1)

    def test_envelope_byte_payload_is_canonical(self) -> None:
        env = BrokerRequest(
            request_id="a" * 64,
            route=ROUTE_FEED,
            target_url="https://feed.example.com/rss.xml",
            headers=(
                ("Accept", "application/rss+xml"),
                ("User-Agent", "news-pipeline/2.0"),
            ),
            timeout_seconds=15.0,
            max_bytes=1024,
            retrieved_at="2026-09-14T00:00:00Z",
        )
        payload = env.to_wire_bytes()
        text = payload.decode("utf-8")
        self.assertNotIn(" ", text)
        self.assertTrue(text.startswith("{"))
        # Round-trip through the validator.
        validated = validate_request_payload(json.loads(text))
        self.assertEqual(validated["route"], ROUTE_FEED)
        self.assertEqual(
            validated["headers"],
            (("Accept", "application/rss+xml"), ("User-Agent", "news-pipeline/2.0")),
        )


# ---------------------------------------------------------------------------
# Response envelope decoding — regression coverage for the broker seam
# ---------------------------------------------------------------------------


class ResponseEnvelopeTests(unittest.IsolatedAsyncioTestCase):
    """Real-signature regression coverage for the JSON response seam.

    Every test in this class uses a real AF_UNIX broker whose HTTP body
    is a JSON envelope matching the host serializer's wire shape. The
    goal is to lock down the decode/verify contract so a regression in
    either side is caught immediately.
    """

    async def test_wrong_request_id_raises_typed_error(self) -> None:
        body = b'{"results":[]}'
        good = BrokerResponse(
            request_id="expected-rid",
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url="http://searxng.example.com/search?q=ai",
            retrieved_at="2026-09-14T00:00:00Z",
        )

        def _override(envelope: dict[str, Any]) -> tuple[bytes, int]:
            wrong = BrokerResponse(
                request_id="not-the-same-rid",
                status=200,
                headers=(("Content-Type", "application/json"),),
                body=body,
                final_url=envelope.get("target_url", ""),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            return wrong.to_wire_bytes(), 200

        with _BrokerHarness(route=ROUTE_SEARCH, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            with self.assertRaises(BrokerProtocolMismatch) as ctx:
                await transport(
                    FetchRequest(
                        url="http://searxng.example.com/search?q=ai",
                        headers=(),
                        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                        max_response_bytes=DEFAULT_MAX_BYTES,
                    ),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
            self.assertIn("request_id", str(ctx.exception).lower())
        # Sanity: the good envelope decodes cleanly when the id matches.
        decoded = decode_response_envelope(
            good.to_wire_bytes(),
            expected_request_id="expected-rid",
            max_bytes=DEFAULT_MAX_BYTES,
        )
        self.assertEqual(decoded.body, body)

    async def test_malformed_base64_raises_typed_error(self) -> None:
        def _override(envelope: dict[str, Any]) -> tuple[bytes, int]:
            # Strict base64 rejects non-canonical padding. Use a token
            # whose length is a multiple of 4 but contains an invalid
            # character so b64decode(validate=True) fails.
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": envelope["request_id"],
                "status": 200,
                "headers": [["Content-Type", "application/json"]],
                "body_b64": "!!!!not-base64-at-all",
                "final_url": envelope.get("target_url", ""),
                "retrieved_at": "2026-09-14T00:00:00Z",
                "error_code": None,
                "error_message": None,
                "body_length": 18,
            }
            return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"), 200

        with _BrokerHarness(route=ROUTE_SEARCH, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            with self.assertRaises(BrokerProtocolMismatch) as ctx:
                await transport(
                    FetchRequest(
                        url="http://searxng.example.com/search?q=ai",
                        headers=(),
                        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                        max_response_bytes=DEFAULT_MAX_BYTES,
                    ),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
            self.assertIn("malformed", str(ctx.exception).lower())

    async def test_body_length_mismatch_raises_typed_error(self) -> None:
        def _override(envelope: dict[str, Any]) -> tuple[bytes, int]:
            body_bytes = b'{"results":[]}'
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": envelope["request_id"],
                "status": 200,
                "headers": [["Content-Type", "application/json"]],
                "body_b64": base64.b64encode(body_bytes).decode("ascii"),
                "final_url": envelope.get("target_url", ""),
                "retrieved_at": "2026-09-14T00:00:00Z",
                "error_code": None,
                "error_message": None,
                "body_length": len(body_bytes) + 7,  # lie about the length
            }
            return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"), 200

        with _BrokerHarness(route=ROUTE_SEARCH, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            with self.assertRaises(BrokerProtocolMismatch) as ctx:
                await transport(
                    FetchRequest(
                        url="http://searxng.example.com/search?q=ai",
                        headers=(),
                        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                        max_response_bytes=DEFAULT_MAX_BYTES,
                    ),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
            self.assertIn("body_length", str(ctx.exception).lower())

    async def test_oversize_body_raises_no_truncation(self) -> None:
        """A response that decodes larger than ``max_bytes`` must error, not truncate."""

        def _override(envelope: dict[str, Any]) -> tuple[bytes, int]:
            # Send 32 KiB even though the request was capped at 4 KiB.
            big = b"x" * 32 * 1024
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": envelope["request_id"],
                "status": 200,
                "headers": [],
                "body_b64": base64.b64encode(big).decode("ascii"),
                "final_url": envelope.get("target_url", ""),
                "retrieved_at": "2026-09-14T00:00:00Z",
                "error_code": None,
                "error_message": None,
                "body_length": len(big),
            }
            return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"), 200

        with _BrokerHarness(route=ROUTE_SEARCH, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            cap = 4 * 1024
            with self.assertRaises(BrokerProtocolMismatch) as ctx:
                await transport(
                    FetchRequest(
                        url="http://searxng.example.com/search?q=ai",
                        headers=(),
                        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                        max_response_bytes=cap,
                    ),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
            self.assertIn("exceeds", str(ctx.exception).lower())

    async def test_error_envelope_maps_to_typed_upstream_error(self) -> None:
        def _override(envelope: dict[str, Any]) -> tuple[bytes, int]:
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": envelope["request_id"],
                "status": 400,
                "headers": [],
                "body_b64": "",
                "final_url": "",
                "retrieved_at": "2026-09-14T00:00:00Z",
                "error_code": "invalid_request",
                "error_message": "feed URL is not parseable",
                "body_length": 0,
            }
            return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"), 200

        with _BrokerHarness(route=ROUTE_FEED, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_rss_contract(), env=env)
            with self.assertRaises(BrokerUpstreamError) as ctx:
                await transport(
                    FetchRequest(
                        url="https://feed.example.com/rss.xml",
                        headers=(),
                        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                        max_response_bytes=DEFAULT_MAX_BYTES,
                    ),
                    retrieved_at="2026-09-14T00:00:00Z",
                )
            self.assertEqual(ctx.exception.error_code, "invalid_request")
            self.assertEqual(ctx.exception.status, 400)
            self.assertIn("feed URL", ctx.exception.error_message)

    async def test_outer_http_transport_error_distinguished_from_upstream_status(
        self,
    ) -> None:
        """Outer HTTP 500 from the broker must NOT be confused with an upstream status."""

        def _override(_envelope: dict[str, Any]) -> tuple[bytes, int]:
            # Return a syntactically valid envelope but with HTTP status
            # 500 — the client must still surface the upstream status
            # from the envelope (here: 200), not the outer HTTP code.
            inner_body = b'{"results":[]}'
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": _envelope["request_id"],
                "status": 200,
                "headers": [["Content-Type", "application/json"]],
                "body_b64": base64.b64encode(inner_body).decode("ascii"),
                "final_url": _envelope.get("target_url", ""),
                "retrieved_at": "2026-09-14T00:00:00Z",
                "error_code": None,
                "error_message": None,
                "body_length": len(inner_body),
            }
            return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"), 500

        with _BrokerHarness(route=ROUTE_SEARCH, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_searxng_contract(), env=env)
            response = await transport(
                FetchRequest(
                    url="http://searxng.example.com/search?q=ai",
                    headers=(),
                    timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                    max_response_bytes=DEFAULT_MAX_BYTES,
                ),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            # The outer HTTP 500 was the transport envelope; the upstream
            # status reported inside the envelope is the value the
            # adapter actually wants.
            self.assertEqual(response.status, 200)

    async def test_arbitrary_binary_body_round_trips_byte_for_byte(self) -> None:
        """Body bytes that are not valid UTF-8 must survive the round-trip."""

        # 0xC3 0x28 is an invalid UTF-8 sequence. The broker transports
        # bodies as base64, so the byte sequence must be preserved.
        binary_body = bytes(range(256))

        def _override(envelope: dict[str, Any]) -> tuple[bytes, int]:
            payload = {
                "protocol_version": PROTOCOL_VERSION,
                "request_id": envelope["request_id"],
                "status": 200,
                "headers": [["Content-Type", "application/octet-stream"]],
                "body_b64": base64.b64encode(binary_body).decode("ascii"),
                "final_url": envelope.get("target_url", ""),
                "retrieved_at": "2026-09-14T00:00:00Z",
                "error_code": None,
                "error_message": None,
                "body_length": len(binary_body),
            }
            return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"), 200

        with _BrokerHarness(route=ROUTE_FEED, response_body=b"") as harness:
            harness.broker._response_override = _override  # type: ignore[attr-defined]
            env = harness.env
            env[ENV_CONTAINER_MODE] = "1"
            transport = broker_transport_factory(_rss_contract(), env=env)
            response = await transport(
                FetchRequest(
                    url="https://feed.example.com/rss.xml",
                    headers=(),
                    timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
                    max_response_bytes=DEFAULT_MAX_BYTES,
                ),
                retrieved_at="2026-09-14T00:00:00Z",
            )
            self.assertEqual(response.body, binary_body)
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers, (("Content-Type", "application/octet-stream"),))

    async def test_decode_rejects_mismatched_protocol_version(self) -> None:
        envelope = {
            "protocol_version": PROTOCOL_VERSION + 99,
            "request_id": "a" * 64,
            "status": 200,
            "headers": [],
            "body_b64": "",
            "final_url": "",
            "retrieved_at": "2026-09-14T00:00:00Z",
            "error_code": None,
            "error_message": None,
            "body_length": 0,
        }
        with self.assertRaises(BrokerResponseProtocolMismatch):
            decode_response_envelope(
                envelope, expected_request_id="a" * 64, max_bytes=1024
            )

    async def test_decode_rejects_non_strict_base64_directly(self) -> None:
        envelope = {
            "protocol_version": PROTOCOL_VERSION,
            "request_id": "a" * 64,
            "status": 200,
            "headers": [],
            # "Zm9v" decodes to "foo" (3 bytes); lie about the body
            # length so the decoder raises a body-length mismatch (the
            # body error family). The body_b64 itself is strict.
            "body_b64": "Zm9v",
            "final_url": "",
            "retrieved_at": "2026-09-14T00:00:00Z",
            "error_code": None,
            "error_message": None,
            "body_length": 999,  # wrong on purpose
        }
        with self.assertRaises(BrokerResponseBodyError):
            decode_response_envelope(
                envelope, expected_request_id="a" * 64, max_bytes=1024
            )

    async def test_decode_envelope_in_broker_response_to_wire_is_canonical(
        self,
    ) -> None:
        """BrokerResponse.to_wire_bytes must be deterministic JSON."""

        first = BrokerResponse(
            request_id="a" * 64,
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=b'{"x":1}',
            final_url="http://example.com/",
            retrieved_at="2026-09-14T00:00:00Z",
        )
        second = BrokerResponse(
            request_id="a" * 64,
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=b'{"x":1}',
            final_url="http://example.com/",
            retrieved_at="2026-09-14T00:00:00Z",
        )
        self.assertEqual(first.to_wire_bytes(), second.to_wire_bytes())
        # body_length in the on-wire JSON must equal len(body).
        wire = first.to_wire_bytes().decode("utf-8")
        import json as _json

        self.assertEqual(_json.loads(wire)["body_length"], len(b'{"x":1}'))

    def test_wire_shape_matches_host_serializer(self) -> None:
        """Client and host must emit byte-identical canonical JSON envelopes.

        This is the "real-signature wire shape" lock-in: if either side
        drifts (different separators, different field order, missing
        fields, etc.), the decode side will reject otherwise-valid
        envelopes. We replicate the host's ``_serialise_response`` here
        to keep the test independent of host module import paths.
        """

        def host_serialise(envelope: BrokerResponse) -> bytes:
            payload = envelope.to_dict()
            return json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")

        # A successful envelope: error_code/error_message stay None so the
        # decoder returns a BrokerResponse instead of raising
        # BrokerUpstreamError.
        envelope = BrokerResponse(
            request_id="b" * 64,
            status=503,
            headers=(
                ("Content-Type", "text/html"),
                ("Cache-Control", "no-cache"),
            ),
            body=b"<html>oops</html>",
            final_url="http://upstream.example.com/",
            retrieved_at="2026-09-14T00:00:00Z",
        )
        client_wire = envelope.to_wire_bytes()
        host_wire = host_serialise(envelope)
        self.assertEqual(client_wire, host_wire)
        # The wire must round-trip back to a BrokerResponse when the
        # request_id matches. This is the round-trip that exercises the
        # exact decode path used by the live client.
        decoded = decode_response_envelope(
            client_wire,
            expected_request_id="b" * 64,
            max_bytes=DEFAULT_MAX_BYTES,
        )
        self.assertEqual(decoded.status, 503)
        self.assertEqual(decoded.body, b"<html>oops</html>")
        self.assertEqual(
            decoded.headers,
            (("Content-Type", "text/html"), ("Cache-Control", "no-cache")),
        )
        self.assertEqual(decoded.final_url, "http://upstream.example.com/")
        self.assertEqual(decoded.retrieved_at, "2026-09-14T00:00:00Z")


class GoalB1ProtocolHardeningTests(unittest.TestCase):
    """B1 regressions: finite positive numeric validation rejecting bool/NaN/inf."""

    def test_validate_request_payload_rejects_bool_max_bytes_and_timeout(self) -> None:
        base_payload = {
            "protocol_version": 1,
            "request_id": "0" * 32,
            "route": "feed",
            "target_url": "https://feed.example.com/rss.xml",
            "retrieved_at": "2026-09-14T00:00:00Z",
            "headers": [],
        }
        for bad_val in (True, False):
            payload = dict(base_payload, max_bytes=bad_val)
            with self.assertRaises(InvalidEnvelopeError):
                validate_request_payload(payload)
            payload = dict(base_payload, timeout_seconds=bad_val)
            with self.assertRaises(InvalidEnvelopeError):
                validate_request_payload(payload)

    def test_validate_request_payload_rejects_nan_and_inf(self) -> None:
        base_payload = {
            "protocol_version": 1,
            "request_id": "0" * 32,
            "route": "feed",
            "target_url": "https://feed.example.com/rss.xml",
            "retrieved_at": "2026-09-14T00:00:00Z",
            "headers": [],
        }
        for bad_val in (float("nan"), float("inf"), float("-inf")):
            payload = dict(base_payload, max_bytes=bad_val)
            with self.assertRaises(InvalidEnvelopeError):
                validate_request_payload(payload)
            payload = dict(base_payload, timeout_seconds=bad_val)
            with self.assertRaises(InvalidEnvelopeError):
                validate_request_payload(payload)

    def test_decode_response_envelope_rejects_non_finite_or_bool_max_bytes(self) -> None:
        wire = BrokerResponse(
            request_id="0" * 32,
            status=200,
            headers=(),
            body=b"ok",
            final_url="https://feed.example.com/rss.xml",
            retrieved_at="2026-09-14T00:00:00Z",
        ).to_wire_bytes()
        for bad_val in (True, False, float("nan"), float("inf"), float("-inf"), 0, -1):
            with self.assertRaises(BrokerResponseProtocolMismatch):
                decode_response_envelope(wire, expected_request_id="0" * 32, max_bytes=bad_val)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
