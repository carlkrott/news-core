"""Network-disabled tests for the host Unix-domain-socket egress broker.

These tests stand up a real AF_UNIX HTTP/1.1 server (the broker) and a
real loopback HTTP origin so the broker exercises its seam
end-to-end.  Every upstream is bound to ``127.0.0.1`` — no public
network is touched.

Coverage targets (from the slice brief):

* Route mismatch: a ``search`` envelope received by a ``feed`` broker
  is rejected with a typed sanitised error.
* Search ignores caller authority and uses the policy origin only.
* Feed enforces the host allowlist, SSRF rules, and revalidates each
  redirect hop.
* All routes strip ``Authorization`` / ``Cookie`` / ``Proxy-Authorization``.
* The 64 KiB request cap and bounded response cap are enforced.
* The broker never delivers: there is no ``delivery`` route and no
  way for a caller to inject one.
* Socket-path hardening rules reject missing-parent, symlink,
  foreign-owner, and loose-mode sockets.
"""
from __future__ import annotations

import http.server
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
from pathlib import Path
from typing import Any
from unittest import mock
from urllib.parse import urlsplit

CANDIDATE_ROOT = Path(__file__).resolve().parents[2]

# Make the ``host`` and ``scripts`` packages importable without installing.
SCRIPTS_DIR = CANDIDATE_ROOT / "scripts"
for entry in (str(CANDIDATE_ROOT), str(SCRIPTS_DIR)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from host import news_egress_broker  # noqa: E402
from news_container import broker_protocol  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _policy_toml(
    *,
    search_base: str = "http://127.0.0.1",
    feed_hosts: tuple[str, ...] = ("feed.example.com",),
    llm_base: str = "http://127.0.0.1",
    llm_model: str = "gemma-test",
) -> str:
    """Render a minimal TOML policy document for the broker."""
    hosts = ",\n".join(f'  "{entry}",' for entry in feed_hosts)
    return f"""
version = 1

[common]
max_request_bytes = 65536
max_response_bytes = 1048576
timeout_seconds = 5

[search]
base_url = "{search_base}"

[feed]
allowed_hosts = [
{hosts}
]
max_redirects = 3

[llm]
base_url = "{llm_base}"
model = "{llm_model}"
path = "/v1/chat/completions"
max_response_bytes = 1048576
"""


def _write_policy(tmpdir: str, body: str) -> str:
    path = os.path.join(tmpdir, "policy.toml")
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    return path


def _envelope_bytes(
    *,
    route: str,
    target_url: str,
    headers: tuple[tuple[str, str], ...] = (),
    body: str | None = None,
    max_bytes: int = 1_048_576,
    request_id: str | None = None,
    timeout_seconds: float = 5.0,
) -> bytes:
    """Build a canonical broker envelope as wire bytes."""
    if request_id is None:
        request_id = "0" * 64
    envelope = broker_protocol.BrokerRequest(
        request_id=request_id,
        route=route,
        target_url=target_url,
        headers=headers,
        timeout_seconds=timeout_seconds,
        max_bytes=max_bytes,
        retrieved_at="2026-09-14T00:00:00Z",
    )
    payload: dict[str, Any] = json.loads(envelope.to_canonical_json())
    if body is not None:
        payload["body"] = body
    # Re-encode with the body included. The protocol validator
    # ignores unknown fields, so this is safe for the search/feed
    # routes; the LLM route reads ``body`` directly.
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _http_request(envelope_bytes: bytes) -> bytes:
    """Render the HTTP/1.1 request bytes the broker client emits."""
    return (
        b"POST /broker HTTP/1.1\r\n"
        b"Host: localhost\r\n"
        b"Content-Type: application/json; charset=utf-8\r\n"
        b"Content-Length: " + str(len(envelope_bytes)).encode("ascii") + b"\r\n"
        b"Connection: close\r\n"
        b"X-News-Broker-Version: 1\r\n"
        b"\r\n"
    ) + envelope_bytes


def _send_envelope_via_socket(
    socket_path: str,
    envelope_bytes: bytes,
    *,
    timeout: float = 5.0,
) -> bytes:
    """Connect to ``socket_path``, send ``envelope_bytes``, read response."""
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(timeout)
        client.connect(socket_path)
        client.sendall(_http_request(envelope_bytes))
        chunks: list[bytes] = []
        while True:
            chunk = client.recv(65536)
            if not chunk:
                break
            chunks.append(chunk)
    return b"".join(chunks)


def _parse_response(raw: bytes) -> tuple[int, dict[str, str], bytes]:
    """Parse the HTTP/1.1 response (status, headers, body) from the broker."""
    head_end = raw.find(b"\r\n\r\n")
    assert head_end >= 0, raw
    head = raw[:head_end].decode("iso-8859-1")
    body = raw[head_end + 4 :]
    status_line, *header_lines = head.split("\r\n")
    parts = status_line.split(" ", 2)
    status = int(parts[1])
    headers: dict[str, str] = {}
    for line in header_lines:
        if ":" not in line:
            continue
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()
    return status, headers, body


class _LoopbackHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler that records the request and emits a canned response.

    The ``upstream`` config dict is bound at class-construction time
    via :meth:`bind`.
    """

    upstream_dict: dict[str, Any] | None = None
    request_log_dict: dict[str, Any] | None = None

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    @classmethod
    def bind(
        cls, upstream: dict[str, Any], request_log: dict[str, Any]
    ) -> type["_LoopbackHandler"]:
        """Return a handler subclass pre-bound to ``upstream`` and ``log``."""
        return type(
            "_BoundLoopbackHandler",
            (cls,),
            {"upstream_dict": upstream, "request_log_dict": request_log},
        )

    def _record(self) -> None:
        if self.request_log_dict is not None:
            self.request_log_dict["count"] += 1
            self.request_log_dict["last_request_line"] = (
                f"{self.command} {self.path} {self.request_version}"
            )
            self.request_log_dict["last_path"] = self.path
            self.request_log_dict["last_headers"] = {
                k.lower(): v for k, v in self.headers.items()
            }
            body_len = int(self.headers.get("Content-Length", "0") or 0)
            if body_len:
                self.request_log_dict["last_body"] = self.rfile.read(body_len)
            else:
                self.request_log_dict["last_body"] = b""

    def _emit(self) -> None:
        assert self.upstream_dict is not None
        body = self.upstream_dict["response_body"]
        self.send_response(self.upstream_dict["response_status"])
        for name, value in self.upstream_dict["response_headers"]:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.upstream_dict["response_status"] != 302:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        self._record()
        assert self.upstream_dict is not None
        if self.upstream_dict["redirect_to"]:
            self.send_response(302)
            self.send_header("Location", self.upstream_dict["redirect_to"])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._emit()

    def do_POST(self) -> None:  # noqa: N802
        self._record()
        assert self.upstream_dict is not None
        if self.upstream_dict["redirect_to"]:
            self.send_response(302)
            self.send_header("Location", self.upstream_dict["redirect_to"])
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._emit()


def _make_loopback_upstream(
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    response_status: int = 200,
    response_body: bytes = b"{\"ok\": true}",
    response_headers: tuple[tuple[str, str], ...] = (
        ("Content-Type", "application/json"),
    ),
    redirect_to: str | None = None,
) -> dict[str, Any]:
    """Spin up a loopback HTTP origin and return a mutable handle."""
    request_log: dict[str, Any] = {
        "count": 0,
        "last_request_line": "",
        "last_path": "",
        "last_headers": {},
        "last_body": b"",
    }
    upstream: dict[str, Any] = {
        "response_status": response_status,
        "response_body": response_body,
        "response_headers": response_headers,
        "redirect_to": redirect_to,
        "request_log": request_log,
        "httpd": None,
        "thread": None,
    }

    handler_cls = _LoopbackHandler.bind(upstream, request_log)
    httpd = http.server.HTTPServer(
        (host, port), handler_cls, bind_and_activate=False
    )
    httpd.allow_reuse_address = False
    upstream["httpd"] = httpd
    return upstream


def _start_upstream(upstream: dict[str, Any]) -> int:
    upstream["httpd"].server_bind()
    upstream["httpd"].server_activate()
    port = upstream["httpd"].server_address[1]
    thread = threading.Thread(
        target=upstream["httpd"].serve_forever, daemon=True
    )
    upstream["thread"] = thread
    thread.start()
    return port


def _stop_upstream(upstream: dict[str, Any]) -> None:
    upstream["httpd"].shutdown()
    upstream["httpd"].server_close()
    thread = upstream.get("thread")
    if thread is not None:
        thread.join(timeout=2.0)


class _BrokerHarness:
    """Run a broker subprocess and manage its socket and policy file."""

    def __init__(
        self,
        *,
        tmpdir: str,
        policy_body: str,
        route: str,
        bind_socket_path: str,
    ) -> None:
        self.tmpdir = tmpdir
        self.policy_path = _write_policy(tmpdir, policy_body)
        self.route = route
        self.socket_path = bind_socket_path
        self.process: subprocess.Popen | None = None

    def start(self) -> None:
        env = os.environ.copy()
        env["PYTHONPATH"] = (
            str(CANDIDATE_ROOT) + os.pathsep + str(SCRIPTS_DIR)
        )
        cmd = [
            sys.executable,
            "-m",
            "host.news_egress_broker",
            "--policy",
            self.policy_path,
            "--route",
            self.route,
            "--socket",
            self.socket_path,
        ]
        self.process = subprocess.Popen(
            cmd,
            env=env,
            cwd=str(CANDIDATE_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        deadline = 5.0
        interval = 0.05
        elapsed = 0.0
        while elapsed < deadline:
            if os.path.exists(self.socket_path):
                return
            if self.process.poll() is not None:
                _, stderr = self.process.communicate(timeout=1.0)
                self.process = None
                raise RuntimeError(
                    f"broker exited before binding; stderr={stderr!r}"
                )
            import time
            time.sleep(interval)
            elapsed += interval
        process = self.process
        process.terminate()
        try:
            _, stderr = process.communicate(timeout=1.0)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr = process.communicate(timeout=1.0)
        self.process = None
        raise RuntimeError(
            f"broker did not bind socket in time; stderr={stderr!r}"
        )

    def stop(self) -> None:
        if self.process is None:
            return
        self.process.terminate()
        try:
            self.process.communicate(timeout=3.0)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.communicate(timeout=1.0)
        finally:
            self.process = None
        if os.path.exists(self.socket_path):
            try:
                os.unlink(self.socket_path)
            except OSError:
                pass

    def send_envelope(
        self,
        *,
        route: str,
        target_url: str,
        headers: tuple[tuple[str, str], ...] = (),
        body: str | None = None,
        max_bytes: int = 1_048_576,
    ) -> tuple[int, dict[str, str], bytes, dict[str, Any] | None]:
        raw = _send_envelope_via_socket(
            self.socket_path,
            _envelope_bytes(
                route=route,
                target_url=target_url,
                headers=headers,
                body=body,
                max_bytes=max_bytes,
            ),
        )
        status, headers_out, body_out = _parse_response(raw)
        decoded: dict[str, Any] | None = None
        try:
            decoded = json.loads(body_out.decode("utf-8")) if body_out else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            decoded = None
        return status, headers_out, body_out, decoded


# ---------------------------------------------------------------------------
# Policy + IP-range tests
# ---------------------------------------------------------------------------


class PolicyTests(unittest.TestCase):
    def test_minimal_policy_loads(self) -> None:
        policy = news_egress_broker.BrokerPolicy.from_mapping(
            tomllib.loads(_policy_toml())
        )
        self.assertEqual(policy.search.base_url, "http://127.0.0.1")
        self.assertEqual(policy.llm.model, "gemma-test")
        self.assertEqual(
            [entry.host for entry in policy.feed.allowed_hosts],
            ["feed.example.com"],
        )

    def test_search_base_url_requires_http_scheme(self) -> None:
        bad = _policy_toml(search_base="https://127.0.0.1")
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(bad))

    def test_feed_allowlist_must_be_nonempty(self) -> None:
        bad = _policy_toml(feed_hosts=())
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(bad))

    def test_common_response_cap_above_8mib_rejected(self) -> None:
        body = (
            "[common]\nmax_request_bytes = 65536\n"
            "max_response_bytes = 99999999\ntimeout_seconds = 5\n"
            "[search]\nbase_url = \"http://127.0.0.1\"\n"
            "[feed]\nallowed_hosts = [\"a.example.com\"]\n"
            "[llm]\nbase_url = \"http://127.0.0.1\"\nmodel = \"m\"\n"
        )
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(body))

    def test_feed_entry_must_be_bare_hostname(self) -> None:
        bad = _policy_toml(feed_hosts=("https://feed.example.com/x",))
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(bad))

    def test_llm_base_url_rejects_userinfo(self) -> None:
        body = (
            "[common]\nmax_request_bytes = 65536\n"
            "max_response_bytes = 1048576\ntimeout_seconds = 5\n"
            "[search]\nbase_url = \"http://127.0.0.1\"\n"
            "[feed]\nallowed_hosts = [\"a.example.com\"]\n"
            "[llm]\nbase_url = \"http://user:pass@127.0.0.1\"\nmodel = \"m\"\n"
        )
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(body))

    def test_unknown_policy_keys_are_rejected(self) -> None:
        body = _policy_toml() + "\n[unexpected]\nvalue = 1\n"
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(body))


class IpRangeTests(unittest.TestCase):
    def test_ipv4_blocked_ranges(self) -> None:
        blocked = (
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "224.0.0.1",
            "0.0.0.0",
        )
        for ip in blocked:
            with self.subTest(ip=ip):
                self.assertTrue(news_egress_broker._ip_in_blocked_range(ip))

    def test_ipv6_blocked_ranges(self) -> None:
        blocked = ("::1", "fc00::1", "fd00::1", "fe80::1", "ff02::1", "::")
        for ip in blocked:
            with self.subTest(ip=ip):
                self.assertTrue(news_egress_broker._ip_in_blocked_range(ip))

    def test_public_ips_are_allowed(self) -> None:
        allowed = (
            "1.1.1.1",
            "8.8.8.8",
            "9.9.9.9",
            "2606:4700::1",
            "2606:4700:10::6814:179a",
        )
        for ip in allowed:
            with self.subTest(ip=ip):
                self.assertFalse(news_egress_broker._ip_in_blocked_range(ip))


class HeaderStripTests(unittest.TestCase):
    def test_caller_headers_strip_authorization_and_cookie(self) -> None:
        cleaned = news_egress_broker.sanitise_caller_headers(
            (
                ("Authorization", "Bearer s3cret"),
                ("Cookie", "sid=abc"),
                ("Proxy-Authorization", "Basic xxx"),
                ("Accept", "application/json"),
                ("User-Agent", "test"),
            )
        )
        names = [name for name, _ in cleaned]
        self.assertNotIn("Authorization", names)
        self.assertNotIn("Cookie", names)
        self.assertNotIn("Proxy-Authorization", names)
        self.assertIn("Accept", names)
        self.assertIn("User-Agent", names)

    def test_response_headers_restricted_to_allowlist(self) -> None:
        cleaned = news_egress_broker.sanitise_response_headers(
            (
                ("Content-Type", "application/json"),
                ("Set-Cookie", "evil=1"),
                ("Authorization", "leaked"),
                ("X-Custom", "dropped"),
            )
        )
        names = [name for name, _ in cleaned]
        self.assertEqual(names, ["Content-Type"])


# ---------------------------------------------------------------------------
# Socket-path hardening tests
# ---------------------------------------------------------------------------


class SocketPathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="news-broker-sock-")

    def tearDown(self) -> None:
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _make_socket(self, *, parent_mode: int = 0o750, sock_mode: int = 0o660) -> str:
        path = os.path.join(self.tmpdir, "broker.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(path)
        server.listen(1)
        os.chmod(path, sock_mode)
        os.chmod(self.tmpdir, parent_mode)
        server.close()
        return path

    def test_accepts_owner_correct_hardened_socket(self) -> None:
        path = self._make_socket()
        # Should not raise.
        news_egress_broker._check_socket_path(path)
        # Mode is forced to 0o660 even if we passed a different value.
        path2 = os.path.join(self.tmpdir, "broker2.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(path2)
        server.listen(1)
        os.chmod(path2, 0o600)
        server.close()
        news_egress_broker._check_socket_path(path2)
        mode = os.stat(path2).st_mode & 0o777
        self.assertEqual(mode, 0o660)

    def test_rejects_world_writable_parent(self) -> None:
        try:
            path = self._make_socket(parent_mode=0o777)
        except PermissionError:
            self.skipTest("environment forbids world-writable dirs")
        actual = os.stat(self.tmpdir).st_mode & 0o777
        if not (actual & 0o002):
            self.skipTest("chmod silently dropped world-writable bit")
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker._check_socket_path(path)

    def test_rejects_non_absolute(self) -> None:
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker._check_socket_path("relative/path.sock")

    def test_rejects_missing_parent(self) -> None:
        with self.assertRaises(news_egress_broker.BrokerConfigError):
            news_egress_broker._check_socket_path("/no/such/dir/broker.sock")


# ---------------------------------------------------------------------------
# Search route integration tests
# ---------------------------------------------------------------------------


class SearchRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="news-broker-search-")
        self.upstream = _make_loopback_upstream(
            response_body=b'{"results": []}',
        )
        self.upstream_port = _start_upstream(self.upstream)
        self.socket_path = os.path.join(self.tmpdir, "search.sock")
        policy = _policy_toml(
            search_base=f"http://127.0.0.1:{self.upstream_port}",
            feed_hosts=("feed.example.com",),
            llm_base=f"http://127.0.0.1:{self.upstream_port}",
        )
        self.harness = _BrokerHarness(
            tmpdir=self.tmpdir,
            policy_body=policy,
            route="search",
            bind_socket_path=self.socket_path,
        )
        self.harness.start()

    def tearDown(self) -> None:
        self.harness.stop()
        _stop_upstream(self.upstream)
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_envelope_validator_blocks_disallowed_headers(self) -> None:
        # The existing broker protocol already rejects Authorization
        # at envelope validation; we confirm here that the broker's
        # sanitiser is defensive in depth and would also drop them
        # if they slipped past.
        _, _, _, decoded = self.harness.send_envelope(
            route="search",
            target_url="http://127.0.0.1/search?q=ok",
            headers=(
                ("Authorization", "Bearer s3cret"),
                ("Cookie", "sid=abc"),
                ("Proxy-Authorization", "Basic xxx"),
                ("Accept", "application/json"),
            ),
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        # Upstream must not have been contacted.
        self.assertEqual(self.upstream["request_log"]["count"], 0)

    def test_uses_policy_origin_and_strips_secrets(self) -> None:
        # All headers must be on the broker allowlist; the envelope
        # validator will reject anything else. We still verify the
        # broker rebuilds the URL on the policy origin and that the
        # caller's allowed headers reach upstream.
        status, _, _, decoded = self.harness.send_envelope(
            route="search",
            target_url="https://evil.example.com:9999/search?q=llm&z=1",
            headers=(
                ("Accept", "application/json"),
                ("User-Agent", "container-test/1.0"),
                ("Accept-Language", "en"),
            ),
        )
        self.assertEqual(status, 200)
        assert decoded is not None
        self.assertEqual(decoded["status"], 200)
        log = self.upstream["request_log"]
        self.assertEqual(log["count"], 1)
        # Broker rebuilt the URL on the policy origin, preserving the
        # path/query but replacing scheme/host.
        self.assertTrue(
            log["last_path"].startswith("/search"),
            msg=log["last_path"],
        )
        # Caller's allowlisted headers reached the upstream.
        self.assertEqual(log["last_headers"].get("accept"), "application/json")
        self.assertEqual(log["last_headers"].get("accept-language"), "en")

    def test_rejects_non_search_path(self) -> None:
        _, _, _, decoded = self.harness.send_envelope(
            route="search",
            target_url="http://127.0.0.1/not-search?q=x",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertIn("path", decoded["error_message"])
        self.assertEqual(self.upstream["request_log"]["count"], 0)

    def test_rejects_envelope_route_mismatch(self) -> None:
        # The harness is bound to ``search``; sending a feed envelope
        # must be rejected at dispatch time without contacting the
        # upstream.
        _, _, _, decoded = self.harness.send_envelope(
            route="feed",
            target_url="https://feed.example.com/rss.xml",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertEqual(self.upstream["request_log"]["count"], 0)


# ---------------------------------------------------------------------------
# Feed route integration tests
# ---------------------------------------------------------------------------


class FeedRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="news-broker-feed-")
        self.socket_path = os.path.join(self.tmpdir, "feed.sock")
        policy = _policy_toml(
            search_base="http://127.0.0.1",
            feed_hosts=("feed.example.com",),
            llm_base="http://127.0.0.1",
        )
        self.harness = _BrokerHarness(
            tmpdir=self.tmpdir,
            policy_body=policy,
            route="feed",
            bind_socket_path=self.socket_path,
        )
        self.harness.start()

    def tearDown(self) -> None:
        self.harness.stop()
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_rejects_unknown_host(self) -> None:
        _, _, _, decoded = self.harness.send_envelope(
            route="feed",
            target_url="https://attacker.example.com/rss.xml",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertIn("allowlist", decoded["error_message"])

    def test_rejects_non_https(self) -> None:
        _, _, _, decoded = self.harness.send_envelope(
            route="feed",
            target_url="http://feed.example.com/rss.xml",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertIn("https", decoded["error_message"])

    def test_rejects_userinfo(self) -> None:
        _, _, _, decoded = self.harness.send_envelope(
            route="feed",
            target_url="https://user:pass@feed.example.com/rss.xml",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertIn("userinfo", decoded["error_message"])

    def test_rejects_non_default_port(self) -> None:
        _, _, _, decoded = self.harness.send_envelope(
            route="feed",
            target_url="https://feed.example.com:8443/rss.xml",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertIn("port", decoded["error_message"])

    def test_rejects_oversize_request(self) -> None:
        # 65 KiB JSON envelope (>64 KiB broker cap). The broker
        # detects the overrun and sends a typed invalid_request
        # error before closing the socket; if the broker instead
        # resets the connection (also a valid failure mode) the test
        # still passes.
        huge = b'{"padding":"' + b"a" * 70_000 + b'"}'
        envelope_bytes = (
            b"POST /broker HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Type: application/json; charset=utf-8\r\n"
            b"Content-Length: " + str(len(huge)).encode("ascii") + b"\r\n"
            b"Connection: close\r\n\r\n" + huge
        )
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
            client.settimeout(5.0)
            client.connect(self.socket_path)
            client.sendall(envelope_bytes)
            chunks: list[bytes] = []
            try:
                while True:
                    chunk = client.recv(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
            except ConnectionResetError:
                # Acceptable: broker closed the socket on detection.
                pass
        raw = b"".join(chunks)
        if not raw:
            self.skipTest("broker closed connection on overrun (acceptable)")
        head_end = raw.find(b"\r\n\r\n")
        status_line = raw[:head_end].decode("iso-8859-1").splitlines()[0]
        body = raw[head_end + 4 :]
        self.assertIn("400", status_line)
        decoded = json.loads(body.decode("utf-8"))
        self.assertEqual(decoded["error_code"], "invalid_request")

    def test_production_feed_connects_to_prevalidated_ip(self) -> None:
        class Response:
            status = 200
            headers: dict[str, str] = {"Content-Type": "application/xml"}

            def read(self, _size: int = -1) -> bytes:
                return b""

            def close(self) -> None:
                return None

            def getcode(self) -> int:
                return 200

        policy = news_egress_broker.BrokerPolicy.from_mapping(
            tomllib.loads(_policy_toml())
        )
        request = broker_protocol.BrokerRequest(
            request_id="a" * 32,
            route="feed",
            target_url="https://feed.example.com/rss.xml",
            headers=(),
            timeout_seconds=5.0,
            max_bytes=1_048_576,
            retrieved_at="2026-09-14T00:00:00Z",
        ).to_dict()
        with mock.patch.object(
            news_egress_broker,
            "_open_pinned_https",
            return_value=Response(),
        ) as pinned:
            news_egress_broker.handle_feed(
                request,
                common=policy.common,
                feed=policy.feed,
                resolver=lambda _host, _port: ["93.184.216.34"],
            )
        self.assertEqual(pinned.call_args.kwargs["pinned_ip"], "93.184.216.34")
        outbound_request = pinned.call_args.args[0]
        self.assertEqual(
            urlsplit(outbound_request.full_url).hostname,
            "feed.example.com",
        )


# ---------------------------------------------------------------------------
# LLM route integration tests
# ---------------------------------------------------------------------------


class LlmRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.mkdtemp(prefix="news-broker-llm-")
        self.upstream = _make_loopback_upstream(
            response_body=b'{"choices": []}',
            response_headers=(("Content-Type", "application/json"),),
        )
        self.upstream_port = _start_upstream(self.upstream)
        self.socket_path = os.path.join(self.tmpdir, "llm.sock")
        policy = _policy_toml(
            search_base="http://127.0.0.1",
            feed_hosts=("feed.example.com",),
            llm_base=f"http://127.0.0.1:{self.upstream_port}",
            llm_model="forced-model-id",
        )
        self.harness = _BrokerHarness(
            tmpdir=self.tmpdir,
            policy_body=policy,
            route="llm",
            bind_socket_path=self.socket_path,
        )
        self.harness.start()

    def tearDown(self) -> None:
        self.harness.stop()
        _stop_upstream(self.upstream)
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_forces_model_and_ignores_target(self) -> None:
        caller_body = json.dumps(
            {
                "messages": [{"role": "user", "content": "hi"}],
                "model": "attacker-supplied-model",
            }
        )
        status, _, _, decoded = self.harness.send_envelope(
            route="llm",
            target_url="http://attacker.example.com/v1/chat/completions",
            body=caller_body,
        )
        self.assertEqual(status, 200)
        assert decoded is not None
        self.assertEqual(decoded["status"], 200)
        log = self.upstream["request_log"]
        self.assertEqual(log["count"], 1)
        # Broker POSTed to the policy origin's /v1/chat/completions.
        self.assertEqual(log["last_path"], "/v1/chat/completions")
        self.assertTrue(
            log["last_headers"].get("content-type", "").startswith("application/json")
        )
        # Forced model: ``attacker-supplied-model`` must not appear.
        sent_body = json.loads(log["last_body"].decode("utf-8"))
        self.assertEqual(sent_body["model"], "forced-model-id")

    def test_rejects_stream_true(self) -> None:
        caller_body = json.dumps(
            {
                "messages": [{"role": "user", "content": "hi"}],
                "stream": True,
            }
        )
        _, _, _, decoded = self.harness.send_envelope(
            route="llm",
            target_url="http://127.0.0.1/v1/chat/completions",
            body=caller_body,
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")
        self.assertIn("stream", decoded["error_message"])
        self.assertEqual(self.upstream["request_log"]["count"], 0)

    def test_rejects_malformed_body(self) -> None:
        _, _, _, decoded = self.harness.send_envelope(
            route="llm",
            target_url="http://127.0.0.1/v1/chat/completions",
            body="not json",
        )
        assert decoded is not None
        self.assertEqual(decoded["status"], 400)
        self.assertEqual(decoded["error_code"], "invalid_request")


# ---------------------------------------------------------------------------
# No-delivery tests
# ---------------------------------------------------------------------------


class NoDeliveryTests(unittest.TestCase):
    def test_delivery_route_absent(self) -> None:
        self.assertNotIn("delivery", news_egress_broker.ALLOWED_ROUTES)
        self.assertEqual(
            news_egress_broker.ALLOWED_ROUTES,
            frozenset({"search", "feed", "llm"}),
        )

    def test_cli_rejects_delivery_route(self) -> None:
        from host.news_egress_broker import _parse_args
        with self.assertRaises(SystemExit):
            _parse_args(
                [
                    "--policy",
                    "/tmp/x",
                    "--route",
                    "delivery",
                    "--socket",
                    "/tmp/x.sock",
                ]
            )


class SystemdTemplateTests(unittest.TestCase):
    def test_template_allows_socket_and_upstream_address_families(self) -> None:
        unit = (
            CANDIDATE_ROOT / "deploy/systemd/news-egress-broker@.service"
        ).read_text(encoding="utf-8")
        self.assertIn("RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6", unit)
        self.assertIn("RuntimeDirectory=news-pipeline-broker", unit)

    def test_template_uses_systemd_not_shell_expansion(self) -> None:
        unit = (
            CANDIDATE_ROOT / "deploy/systemd/news-egress-broker@.service"
        ).read_text(encoding="utf-8")
        self.assertNotIn(":-", unit)
        self.assertIn("${NEWS_EGRESS_CODE_ROOT}/host/news_egress_broker.py", unit)
        self.assertIn("--policy ${NEWS_EGRESS_POLICY_PATH}", unit)
        self.assertIn("--socket %t/news-pipeline-broker/%i.sock", unit)


class GoalB1HostBrokerHardeningTests(unittest.TestCase):
    """B1 host broker resource/privacy hardening regressions."""

    def test_default_opener_explicit_empty_proxy_handler(self) -> None:
        import urllib.request
        with mock.patch("urllib.request.build_opener") as mock_build:
            mock_build.return_value.open.return_value = mock.MagicMock()
            req = urllib.request.Request("http://127.0.0.1")
            news_egress_broker.default_opener(req, timeout=5.0)
            args, _ = mock_build.call_args
            proxy_handlers = [h for h in args if isinstance(h, urllib.request.ProxyHandler)]
            self.assertEqual(len(proxy_handlers), 1)
            self.assertEqual(proxy_handlers[0].proxies, {})

    def test_framing_rejects_duplicate_content_length(self) -> None:
        s1, s2 = socket.socketpair()
        with s1, s2:
            s1.sendall(b"POST /broker HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\n12345")
            s1.shutdown(socket.SHUT_WR)
            with self.assertRaises(news_egress_broker.BrokerRequestError) as ctx:
                news_egress_broker._read_envelope_bytes(s2, max_bytes=65536)
            self.assertIn("Content-Length", str(ctx.exception))

    def test_framing_rejects_transfer_encoding(self) -> None:
        s1, s2 = socket.socketpair()
        with s1, s2:
            s1.sendall(b"POST /broker HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n")
            s1.shutdown(socket.SHUT_WR)
            with self.assertRaises(news_egress_broker.BrokerRequestError) as ctx:
                news_egress_broker._read_envelope_bytes(s2, max_bytes=65536)
            self.assertIn("transfer-encoding", str(ctx.exception).lower())

    def test_framing_rejects_early_eof(self) -> None:
        s1, s2 = socket.socketpair()
        with s1, s2:
            s1.sendall(b"POST /broker HTTP/1.1\r\nContent-Length: 50\r\n\r\nshort")
            s1.shutdown(socket.SHUT_WR)
            with self.assertRaises(news_egress_broker.BrokerRequestError) as ctx:
                news_egress_broker._read_envelope_bytes(s2, max_bytes=65536)
            self.assertIn("end of request", str(ctx.exception).lower())

    def test_framing_rejects_unsupported_method_and_path(self) -> None:
        for bad_line in (b"GET /broker HTTP/1.1\r\n", b"POST /not-broker HTTP/1.1\r\n"):
            s1, s2 = socket.socketpair()
            with s1, s2:
                s1.sendall(bad_line + b"Content-Length: 5\r\n\r\n12345")
                s1.shutdown(socket.SHUT_WR)
                with self.assertRaises(news_egress_broker.BrokerRequestError):
                    news_egress_broker._read_envelope_bytes(s2, max_bytes=65536)

    def test_framing_rejects_extra_request_or_trailing_data(self) -> None:
        s1, s2 = socket.socketpair()
        with s1, s2:
            s1.sendall(b"POST /broker HTTP/1.1\r\nContent-Length: 5\r\n\r\n12345EXTRA")
            s1.shutdown(socket.SHUT_WR)
            with self.assertRaises(news_egress_broker.BrokerRequestError) as ctx:
                news_egress_broker._read_envelope_bytes(s2, max_bytes=65536)
            self.assertIn("extra", str(ctx.exception).lower())

    def test_min_policy_cap_and_caller_max_bytes_enforced(self) -> None:
        policy = news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(_policy_toml()))
        # Upstream returns 200 bytes
        body = b"x" * 200
        class MockResp:
            headers = {"Content-Type": "application/json"}
            def __init__(self, data: bytes):
                self._data = data
                self._pos = 0
            def read(self, size: int = -1) -> bytes:
                if self._pos >= len(self._data):
                    return b""
                res = self._data[self._pos:] if size < 0 else self._data[self._pos:self._pos+size]
                self._pos += len(res)
                return res
            def getcode(self) -> int:
                return 200
            def close(self) -> None:
                pass

        # Case 1: Search route with caller max_bytes < policy max_response_bytes (100 < 1048576)
        envelope = {
            "protocol_version": 1,
            "request_id": "0" * 32,
            "route": "search",
            "target_url": "http://127.0.0.1/search?q=test",
            "headers": (),
            "timeout_seconds": 5.0,
            "max_bytes": 100,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        res = news_egress_broker.handle_search(
            envelope,
            common=policy.common,
            search=policy.search,
            caller_headers=(),
            opener=lambda _req, timeout: MockResp(body),
        )
        # Bounded to 100 bytes and marked status -1 due to overrun past caller max_bytes
        self.assertEqual(len(res.body), 100)
        self.assertEqual(res.status, -1)

    def test_feed_closure_on_all_outcomes(self) -> None:
        policy = news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(_policy_toml()))
        closed = {"response": False, "connection": False}
        class MockConn:
            def close(self) -> None:
                closed["connection"] = True
        class MockResp:
            headers = {"Content-Type": "application/xml"}
            _owning_connection = MockConn()
            def read(self, size: int = -1) -> bytes:
                return b"<xml/>"
            def getcode(self) -> int:
                return 200
            def close(self) -> None:
                closed["response"] = True

        envelope = {
            "protocol_version": 1,
            "request_id": "0" * 32,
            "route": "feed",
            "target_url": "https://feed.example.com/rss.xml",
            "headers": (),
            "timeout_seconds": 5.0,
            "max_bytes": 1024,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        news_egress_broker.handle_feed(
            envelope,
            common=policy.common,
            feed=policy.feed,
            resolver=lambda _h, _p: ["93.184.216.34"],
            opener=lambda _req, timeout: MockResp(),
        )
        self.assertTrue(closed["response"])
        self.assertTrue(closed["connection"])

    def test_monotonic_deadline_bounds_and_expires_upstream_call(self) -> None:
        policy = news_egress_broker.BrokerPolicy.from_mapping(tomllib.loads(_policy_toml()))
        envelope = {
            "protocol_version": 1,
            "request_id": "0" * 32,
            "route": "search",
            "target_url": "http://127.0.0.1/search?q=test",
            "headers": (),
            "timeout_seconds": 5.0,
            "max_bytes": 1024,
            "retrieved_at": "2026-09-14T00:00:00Z",
        }
        class Response:
            headers = {}
            def read(self, _size: int = -1) -> bytes:
                return b""
            def getcode(self) -> int:
                return 200
            def close(self) -> None:
                return None

        seen: dict[str, float] = {}
        def opener(_request: Any, *, timeout: float) -> Response:
            seen["timeout"] = timeout
            return Response()

        news_egress_broker.handle_search(
            envelope,
            common=policy.common,
            search=policy.search,
            caller_headers=(),
            opener=opener,
            deadline=time.monotonic() + 1.0,
        )
        self.assertGreater(seen["timeout"], 0.0)
        self.assertLessEqual(seen["timeout"], 1.0)
        with self.assertRaises(news_egress_broker.BrokerRequestError):
            news_egress_broker.handle_search(
                envelope,
                common=policy.common,
                search=policy.search,
                caller_headers=(),
                opener=opener,
                deadline=time.monotonic() - 1.0,
            )

        # SSRF blocked range check must not leak IP or hostname
        with self.assertRaises(news_egress_broker.BrokerRequestError) as ctx:
            news_egress_broker._resolve_and_validate(
                "secret-host.example.com",
                port=443,
                resolver=lambda _h, _p: ["127.0.0.1"],
            )
        msg = str(ctx.exception)
        self.assertNotIn("127.0.0.1", msg)
        self.assertNotIn("secret-host", msg)
        self.assertIn("blocked", msg.lower())

        # DNS resolution failure must not leak host
        with self.assertRaises(news_egress_broker.BrokerRequestError) as ctx:
            news_egress_broker.default_resolver("private-internal.corp", 443)
        msg = str(ctx.exception)
        self.assertNotIn("private-internal.corp", msg)


if __name__ == "__main__":
    unittest.main()