"""Host-side Unix-domain-socket egress broker for the sealed news container.

This broker is the only network-egress path the container is allowed to
use. It speaks HTTP/1.1 over an AF_UNIX socket, decodes the canonical
:class:`BrokerRequest` envelope defined in
:mod:`scripts.news_container.broker_protocol`, and translates it into a
real outbound HTTP request using the policy baked into the TOML file
supplied at start-up.

Design invariants (binding for this slice):

* **Stdlib only.** No third-party HTTP, no third-party JSON, no asyncio
  on the wire loop (the protocol is synchronous HTTP/1.1 over a UNIX
  socket per the existing client contract).
* **No delivery.** ``delivery`` is intentionally absent; the broker
  exists to mediate ``search``, ``feed``, and ``llm`` traffic only.
* **No authority passing.** The broker never forwards ``Authorization``,
  ``Cookie``, or ``Proxy-Authorization`` headers — neither from the
  envelope nor back to the caller. Caller-supplied headers are
  intersected with the envelope allowlist before being sent upstream.
* **Caller-destination overrides for ``search`` and ``llm`` only.**
  Those two routes ignore ``target_url`` and always use the
  policy-configured origin and path / payload; the caller cannot use
  them to probe arbitrary endpoints.
* **``feed`` applies a closed allowlist and SSRF checks on every
  hop.** Userinfo, non-default ports, resolver results in
  loopback / private / link-local / CGNAT / multicast / unspecified
  ranges, and redirects to such addresses are all rejected.
* **Bounded resources.** A 64 KiB request cap, bounded response cap,
  and per-hop and total timeouts are enforced before any upstream
  socket is opened.
* **Socket-path hard check.** The parent directory must exist, must
  not be a symlink, must be owned by the current uid, and must not be
  world-writable; the socket path itself must not be a symlink and
  must be owned by the current uid; the socket mode is forced to
  ``0o660``.

CLI usage:

.. code-block:: text

    python -m host.news_egress_broker \\
        --policy /etc/news-broker/policy.toml \\
        --route search \\
        --socket /run/news-broker/search.sock

The broker is a single-shot process: it ``fork()``-drops privileges to
the configured uid/gid, binds the socket, and runs ``serve_forever()``
until it receives ``SIGINT`` / ``SIGTERM``.

See ``host/broker-policy.example.toml`` for the policy schema and
``deploy/systemd/news-egress-broker@.service`` for a user-service
template.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import errno
import http.client
import json
import logging
import math
import os
import select
import signal
import socket
import socketserver
import ssl
import stat
import sys
import threading
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

# The protocol package lives under scripts/news_container/. We add the
# parent of ``scripts`` to sys.path so this module is importable both as
# ``python -m host.news_egress_broker`` and as a standalone file.
_SCRIPT_PARENT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, "scripts")
)
if _SCRIPT_PARENT not in sys.path:
    sys.path.insert(0, _SCRIPT_PARENT)

from news_container import broker_protocol  # noqa: E402

LOGGER = logging.getLogger("news_egress_broker")

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------

#: Maximum permitted request body (HTTP/1.1 envelope) bytes.
MAX_REQUEST_BYTES = 64 * 1024

#: Maximum number of redirects the feed route will chase.
MAX_FEED_REDIRECTS = 3

#: Maximum accepted concurrent connection handlers per broker instance.
MAX_ACTIVE_CONNECTIONS = 16

#: Maximum HTTP request-head bytes admitted before a body is parsed.
MAX_HTTP_HEADER_BYTES = 8 * 1024

#: Header names that the broker strips unconditionally — the broker
#: never reads, forwards, or echoes them.
_STRIPPED_HEADERS: frozenset[str] = frozenset(
    {"authorization", "cookie", "proxy-authorization"}
)

#: Header names the broker only ever honours from upstream
#: responses. Caller-supplied response headers outside this set are
#: discarded on the wire back to the container.
_RESPONSE_HEADER_ALLOWLIST: frozenset[str] = frozenset(
    {
        "content-type",
        "content-length",
        "content-encoding",
        "cache-control",
        "etag",
        "last-modified",
        "date",
        "expires",
        "vary",
        "x-feedburner-location",
    }
)

#: IPv4 ranges that must never be reached from this host (excluding
#: RFC1918 ranges because ``_ip_in_blocked_range`` matches the prefix
#: directly).
_BLOCKED_IPV4_PREFIXES: tuple[str, ...] = (
    "127.",      # loopback
    "10.",       # RFC1918
    "172.16.",   # RFC1918 (note: a coarse prefix; 172.17-31 also match)
    "172.17.",
    "172.18.",
    "172.19.",
    "172.20.",
    "172.21.",
    "172.22.",
    "172.23.",
    "172.24.",
    "172.25.",
    "172.26.",
    "172.27.",
    "172.28.",
    "172.29.",
    "172.30.",
    "172.31.",
    "192.168.",  # RFC1918
    "169.254.",  # link-local
    "100.64.",   # CGNAT (RFC6598)
    "224.",      # multicast
    "0.",        # unspecified
)

#: Equivalent for IPv6.
_BLOCKED_IPV6_PREFIXES: tuple[str, ...] = (
    "::1",       # loopback
    "fc",        # ULA (fc00::/7)
    "fd",        # ULA
    "fe80:",     # link-local
    "ff",        # multicast
    "::",        # unspecified
)

ALLOWED_ROUTES: frozenset[str] = frozenset({"search", "feed", "llm"})


# ----------------------------------------------------------------------
# Errors
# ----------------------------------------------------------------------


def _safe_error_category(detail: str) -> str:
    """Map diagnostics to a fixed category without exposing request data."""
    text = detail.lower()
    categories = (
        (("timeout", "deadline"), "request_timeout"),
        (("transfer-encoding",), "invalid_request_transfer-encoding"),
        (("end of request",), "invalid_request_end of request"),
        (("content-length",), "invalid_request_Content-Length"),
        (("extra", "trailing"), "invalid_request_extra_data"),
        (("path",), "invalid_request_path"),
        (("allowlist",), "policy_denied_allowlist"),
        (("blocked",), "policy_denied_blocked_address"),
        (("https",), "policy_denied_https"),
        (("userinfo",), "policy_denied_userinfo"),
        (("stream",), "invalid_request_stream"),
        (("port",), "policy_denied_port"),
        (("too large", "exceeded"), "request_too_large"),
        (("upstream",), "upstream_failure"),
        (("json", "utf-8", "envelope"), "invalid_request_envelope"),
        (("socket", "read failed", "io"), "invalid_request_io"),
    )
    for needles, category in categories:
        if any(needle in text for needle in needles):
            return category
    return "invalid_request"


class BrokerConfigError(ValueError):
    """Policy file is missing, unreadable, or fails schema validation."""

    def __init__(self, detail: str) -> None:
        self.detail = str(detail)
        super().__init__(self.detail)

    def __str__(self) -> str:
        return "broker_config_error"


class BrokerRequestError(ValueError):
    """Caller envelope is malformed, exceeds limits, or fails policy.

    The broker always maps these into a typed error response on the
    wire; the field ``error_code`` is the stable, machine-readable
    identifier. Diagnostic text is retained only for local categorisation;
    ``str(error)`` is always a fixed, privacy-safe category.
    """

    def __init__(self, detail: str) -> None:
        self.detail = str(detail)
        self.category = _safe_error_category(self.detail)
        super().__init__(self.detail)

    def __str__(self) -> str:
        return self.category


def _require_exact_keys(
    name: str,
    raw: Mapping[str, Any],
    allowed: frozenset[str],
) -> None:
    unknown = set(raw) - allowed
    if unknown:
        raise BrokerConfigError(
            f"{name} contains unknown keys: {sorted(unknown)!r}"
        )


def _policy_positive_number(
    value: Any,
    *,
    field_name: str,
    maximum: float,
    integer: bool = False,
) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BrokerConfigError(f"{field_name} must be a finite positive number")
    if not math.isfinite(value) or value <= 0 or value > maximum:
        raise BrokerConfigError(f"{field_name} is outside the configured bound")
    if integer:
        result = int(value)
        if result <= 0:
            raise BrokerConfigError(f"{field_name} must encode at least one unit")
        return result
    return float(value)


# ----------------------------------------------------------------------
# Policy schema
# ----------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CommonPolicy:
    """Limits that apply to every route."""

    max_request_bytes: int
    max_response_bytes: int
    timeout_seconds: float
    max_concurrency: int = 16

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "CommonPolicy":
        _require_exact_keys(
            "common",
            raw,
            frozenset({
                "max_request_bytes",
                "max_response_bytes",
                "timeout_seconds",
                "max_concurrency",
                "max_connections",
            }),
        )
        max_request = int(
            _policy_positive_number(
                raw.get("max_request_bytes", MAX_REQUEST_BYTES),
                field_name="common.max_request_bytes",
                maximum=MAX_REQUEST_BYTES,
                integer=True,
            )
        )
        max_response = int(
            _policy_positive_number(
                raw.get("max_response_bytes", 2 * 1024 * 1024),
                field_name="common.max_response_bytes",
                maximum=8 * 1024 * 1024,
                integer=True,
            )
        )
        timeout = float(
            _policy_positive_number(
                raw.get("timeout_seconds", 15.0),
                field_name="common.timeout_seconds",
                maximum=120.0,
            )
        )
        max_concurrency = int(
            _policy_positive_number(
                raw.get("max_concurrency", raw.get("max_connections", 16)),
                field_name="common.max_concurrency",
                maximum=256,
                integer=True,
            )
        )
        if max_request <= 0 or max_request > MAX_REQUEST_BYTES:
            raise BrokerConfigError(
                f"common.max_request_bytes must be in (0, {MAX_REQUEST_BYTES}]"
            )
        if max_response <= 0 or max_response > 8 * 1024 * 1024:
            raise BrokerConfigError(
                "common.max_response_bytes must be in (0, 8 MiB]"
            )
        if timeout <= 0 or timeout > 120.0:
            raise BrokerConfigError(
                "common.timeout_seconds must be in (0, 120]"
            )
        if max_concurrency <= 0 or max_concurrency > 256:
            raise BrokerConfigError(
                "common.max_concurrency must be in (0, 256]"
            )
        return cls(
            max_request_bytes=max_request,
            max_response_bytes=max_response,
            timeout_seconds=timeout,
            max_concurrency=max_concurrency,
        )


@dataclass(frozen=True, slots=True)
class SearchPolicy:
    """Configuration for the search route."""

    base_url: str

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "SearchPolicy":
        _require_exact_keys("search", raw, frozenset({"base_url"}))
        base_url = raw.get("base_url")
        if not isinstance(base_url, str) or not base_url:
            raise BrokerConfigError("search.base_url must be a non-empty string")
        parsed = urllib.parse.urlparse(base_url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise BrokerConfigError(
                "search.base_url must be an http:// URL with a hostname"
            )
        return cls(base_url=base_url)


@dataclass(frozen=True, slots=True)
class FeedHost:
    """A single allowlisted feed origin."""

    host: str

    @classmethod
    def from_raw(cls, raw: Any) -> "FeedHost":
        if not isinstance(raw, str) or not raw:
            raise BrokerConfigError("feed.allowed_hosts entries must be strings")
        host = raw.strip().lower()
        if not host or "/" in host or ":" in host:
            raise BrokerConfigError(
                f"feed.allowed_hosts entry {raw!r} must be a bare hostname"
            )
        return cls(host=host)


@dataclass(frozen=True, slots=True)
class FeedPolicy:
    """Configuration for the feed route."""

    allowed_hosts: tuple[FeedHost, ...]
    max_redirects: int = MAX_FEED_REDIRECTS

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "FeedPolicy":
        _require_exact_keys(
            "feed", raw, frozenset({"allowed_hosts", "max_redirects"})
        )
        hosts_raw = raw.get("allowed_hosts")
        if not isinstance(hosts_raw, list) or not hosts_raw:
            raise BrokerConfigError(
                "feed.allowed_hosts must be a non-empty list of hostnames"
            )
        hosts = tuple(FeedHost.from_raw(entry) for entry in hosts_raw)
        max_redirects = int(raw.get("max_redirects", MAX_FEED_REDIRECTS))
        if max_redirects < 0 or max_redirects > MAX_FEED_REDIRECTS:
            raise BrokerConfigError(
                f"feed.max_redirects must be in [0, {MAX_FEED_REDIRECTS}]"
            )
        return cls(allowed_hosts=hosts, max_redirects=max_redirects)

    def is_allowed(self, host: str) -> bool:
        host = host.lower()
        return any(entry.host == host for entry in self.allowed_hosts)


@dataclass(frozen=True, slots=True)
class LlmPolicy:
    """Configuration for the LLM route."""

    base_url: str
    model: str
    path: str = "/v1/chat/completions"
    max_response_bytes: int = 4 * 1024 * 1024

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "LlmPolicy":
        _require_exact_keys(
            "llm",
            raw,
            frozenset({"base_url", "model", "path", "max_response_bytes"}),
        )
        base_url = raw.get("base_url")
        if not isinstance(base_url, str) or not base_url:
            raise BrokerConfigError("llm.base_url must be a non-empty string")
        parsed = urllib.parse.urlsplit(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise BrokerConfigError(
                "llm.base_url must be an http(s):// URL with a hostname"
            )
        if _netloc_has_userinfo(parsed.netloc):
            raise BrokerConfigError(
                "llm.base_url must not include userinfo"
            )
        model = raw.get("model")
        if not isinstance(model, str) or not model:
            raise BrokerConfigError("llm.model must be a non-empty string")
        path = raw.get("path", "/v1/chat/completions")
        if not isinstance(path, str) or not path.startswith("/"):
            raise BrokerConfigError("llm.path must start with '/'")
        max_response = int(raw.get("max_response_bytes", 4 * 1024 * 1024))
        if max_response <= 0 or max_response > 8 * 1024 * 1024:
            raise BrokerConfigError(
                "llm.max_response_bytes must be in (0, 8 MiB]"
            )
        return cls(
            base_url=base_url,
            model=model,
            path=path,
            max_response_bytes=max_response,
        )


@dataclass(frozen=True, slots=True)
class BrokerPolicy:
    """Top-level policy bundle."""

    common: CommonPolicy
    search: SearchPolicy
    feed: FeedPolicy
    llm: LlmPolicy
    log_level: str = "INFO"

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "BrokerPolicy":
        if not isinstance(raw, Mapping):
            raise BrokerConfigError("policy file must be a TOML table")
        _require_exact_keys(
            "policy",
            raw,
            frozenset({"version", "common", "search", "feed", "llm", "log_level"}),
        )
        if raw.get("version") != 1:
            raise BrokerConfigError("policy.version must be integer 1")
        common_raw = raw.get("common", {})
        if not isinstance(common_raw, Mapping):
            raise BrokerConfigError("policy.common must be a table")
        search_raw = raw.get("search", {})
        feed_raw = raw.get("feed", {})
        llm_raw = raw.get("llm", {})
        for label, section in (
            ("search", search_raw),
            ("feed", feed_raw),
            ("llm", llm_raw),
        ):
            if not isinstance(section, Mapping):
                raise BrokerConfigError(f"policy.{label} must be a table")
        log_level = str(raw.get("log_level", "INFO")).upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise BrokerConfigError("log_level must be DEBUG, INFO, WARNING, or ERROR")
        return cls(
            common=CommonPolicy.from_mapping(common_raw),
            search=SearchPolicy.from_mapping(search_raw),
            feed=FeedPolicy.from_mapping(feed_raw),
            llm=LlmPolicy.from_mapping(llm_raw),
            log_level=log_level,
        )

    @classmethod
    def load(cls, path: str) -> "BrokerPolicy":
        try:
            with open(path, "rb") as handle:
                data = tomllib.load(handle)
        except FileNotFoundError as exc:
            raise BrokerConfigError(f"policy file not found: {path}") from exc
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise BrokerConfigError(
                f"policy file is unreadable / invalid TOML: {exc}"
            ) from exc
        return cls.from_mapping(data)




def _positive_finite_bytes(value: Any, *, field_name: str) -> int:
    """Validate a caller byte cap without bool/non-finite coercion."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BrokerRequestError(f"{field_name} must be a finite positive number")
    if not math.isfinite(value) or value <= 0:
        raise BrokerRequestError(f"{field_name} must be a finite positive number")
    result = int(value)
    if result <= 0:
        raise BrokerRequestError(f"{field_name} must be a finite positive number")
    return result


def _positive_finite_seconds(value: Any, *, field_name: str) -> float:
    """Validate a caller timeout without bool/non-finite coercion."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BrokerRequestError(f"{field_name} must be a finite positive number")
    if not math.isfinite(value) or value <= 0:
        raise BrokerRequestError(f"{field_name} must be a finite positive number")
    return float(value)


def _effective_response_limit(policy_cap: Any, caller_cap: Any) -> int:
    """Return the smaller validated policy/caller response byte cap."""
    policy = _positive_finite_bytes(policy_cap, field_name="policy max_bytes")
    caller = _positive_finite_bytes(caller_cap, field_name="max_bytes")
    return min(policy, caller)


def _remaining_timeout(deadline: float | None, fallback: float) -> float:
    """Return a positive timeout bounded by an end-to-end monotonic deadline."""
    if deadline is None:
        return fallback
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise BrokerRequestError("request deadline exceeded")
    return min(fallback, remaining)


# ----------------------------------------------------------------------
# Header sanitisation
# ----------------------------------------------------------------------


def sanitise_caller_headers(
    headers: Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Return a copy of ``headers`` with stripped names removed.

    The envelope validator already enforces the protocol allowlist, but
    the broker applies a second pass to be defensive: callers cannot
    slip ``Authorization`` / ``Cookie`` / ``Proxy-Authorization``
    through.
    """
    cleaned: list[tuple[str, str]] = []
    for name, value in headers:
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        if name.lower() in _STRIPPED_HEADERS:
            continue
        cleaned.append((name, value))
    return tuple(cleaned)


def sanitise_response_headers(
    headers: Sequence[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Return a copy of ``headers`` restricted to the response allowlist."""
    cleaned: list[tuple[str, str]] = []
    seen: set[str] = set()
    for name, value in headers:
        if not isinstance(name, str) or not isinstance(value, str):
            continue
        key = name.lower()
        if key in _STRIPPED_HEADERS:
            continue
        if key not in _RESPONSE_HEADER_ALLOWLIST:
            continue
        if key in seen:
            continue
        seen.add(key)
        cleaned.append((name, value))
    return tuple(cleaned)


# ----------------------------------------------------------------------
# IP-range enforcement
# ----------------------------------------------------------------------


def _normalise_ip_string(ip: str) -> str:
    """Return a canonical string for an IP literal.

    For IPv6 the result is lowercase and has no ``::`` collapse
    ambiguities; for IPv4 it is the literal text as passed in.
    """
    try:
        import ipaddress

        addr = ipaddress.ip_address(ip.strip())
    except (ValueError, AttributeError):
        return ip.strip()
    return addr.compressed.lower()


def _ip_in_blocked_range(ip: str) -> bool:
    """Return ``True`` when ``ip`` lies in a blocked range.

    Only used for ``feed``. The blocked set covers loopback (v4 + v6),
    RFC1918, link-local, CGNAT, multicast, and unspecified. Public IPs
    are allowed.
    """
    normalised = _normalise_ip_string(ip)
    if not normalised:
        return True
    if ":" in normalised:
        for prefix in _BLOCKED_IPV6_PREFIXES:
            if normalised.startswith(prefix):
                return True
        return False
    for prefix in _BLOCKED_IPV4_PREFIXES:
        if normalised.startswith(prefix):
            return True
    return False


# ----------------------------------------------------------------------
# Resolver and opener injection points
# ----------------------------------------------------------------------

ResolverFn = Callable[[str, int], list[str]]
OpenerFn = Callable[..., Any]


def default_resolver(host: str, port: int) -> list[str]:
    """Resolve ``host`` via :func:`socket.getaddrinfo` and return IPs.

    The default resolver is wrapped so tests can inject deterministic
    results without monkey-patching :mod:`socket`.
    """
    try:
        infos = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise BrokerRequestError("DNS resolution failed") from exc
    ips: list[str] = []
    for info in infos:
        sockaddr = info[4]
        if not sockaddr:
            continue
        ips.append(sockaddr[0])
    if not ips:
        raise BrokerRequestError("DNS resolution returned no addresses")
    return ips


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def default_opener(request: urllib.request.Request, timeout: float):
    """Build a default :class:`urllib.request.OpenerDirector` for an HTTP request.

    The caller controls the timeout, the broker enforces the response
    byte cap. ``https`` requests use the default SSL context.
    """
    context = ssl.create_default_context()
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        _NoRedirectHandler(),
        urllib.request.HTTPSHandler(context=context),
    ).open(request, timeout=timeout)


# ----------------------------------------------------------------------
# URL validation helpers
# ----------------------------------------------------------------------


def _netloc_has_userinfo(netloc: str) -> bool:
    """Return True when ``netloc`` contains ``user[:password]@``."""
    return "@" in netloc


def _validate_feed_target_url(url: str) -> urllib.parse.SplitResult:
    """Parse and validate a feed target URL.

    Enforces ``https``, the absence of userinfo, the default port, and
    the hostname allowlist. Returns the parsed URL so callers can use
    the parsed pieces directly.
    """
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError as exc:
        raise BrokerRequestError(f"feed URL is not parseable: {exc}") from exc
    if parsed.scheme != "https":
        raise BrokerRequestError(
            f"feed URL must use https scheme, got {parsed.scheme!r}"
        )
    if _netloc_has_userinfo(parsed.netloc):
        raise BrokerRequestError("feed URL must not include userinfo")
    if parsed.port is not None and parsed.port != 443:
        raise BrokerRequestError(
            f"feed URL port must be 443, got {parsed.port!r}"
        )
    if not parsed.hostname:
        raise BrokerRequestError("feed URL is missing a hostname")
    return parsed


# ----------------------------------------------------------------------
# Search route
# ----------------------------------------------------------------------


def _build_search_outbound(
    envelope_target_url: str,
    request_id: str,
    policy: SearchPolicy,
    caller_headers: Sequence[tuple[str, str]],
) -> tuple[urllib.request.Request, str]:
    """Build the fixed outbound request for the search route.

    The caller's target URL contributes only ``path`` + ``query``: the
    scheme / netloc are replaced with the policy-configured origin.
    """
    try:
        parsed = urllib.parse.urlsplit(envelope_target_url)
    except ValueError as exc:
        raise BrokerRequestError(f"search URL is not parseable: {exc}") from exc
    if parsed.scheme not in ("http", "https"):
        raise BrokerRequestError(
            f"search URL must be http(s), got {parsed.scheme!r}"
        )
    path = parsed.path or "/search"
    if path != "/search":
        raise BrokerRequestError(
            f"search path must be '/search', got {path!r}"
        )
    query = parsed.query
    if not query:
        raise BrokerRequestError("search URL must include a query string")
    base = urllib.parse.urlsplit(policy.base_url)
    fixed = urllib.parse.urlunsplit(
        (base.scheme, base.netloc, path, query, "")
    )
    request = urllib.request.Request(
        fixed,
        headers={
            "Accept": "application/json",
            "User-Agent": f"news-egress-broker/{broker_protocol.PROTOCOL_VERSION}",
            "X-News-Broker-Request-Id": request_id,
        },
    )
    for name, value in caller_headers:
        if name.lower() in {"accept", "user-agent"}:
            continue
        request.add_header(name, value)
    return request, fixed


def handle_search(
    envelope: Mapping[str, Any],
    *,
    common: CommonPolicy,
    search: SearchPolicy,
    caller_headers: Sequence[tuple[str, str]],
    resolver: ResolverFn = default_resolver,
    opener: OpenerFn = default_opener,
    deadline: float | None = None,
) -> broker_protocol.BrokerResponse:
    """Issue a fixed-origin search call.

    The envelope's ``target_url`` supplies path/query only; the scheme
    and netloc are always the policy-configured SearXNG origin.
    """
    request_id = envelope["request_id"]
    request, fixed_url = _build_search_outbound(
        envelope["target_url"],
        request_id=request_id,
        policy=search,
        caller_headers=caller_headers,
    )
    # The fixed URL lives on the policy origin. We do not re-validate
    # it against the SSRF blocklist: search is loopback-only and the
    # policy is operator-owned. We still hand the URL through the
    # configured opener so the integration is end-to-end.
    try:
        response = opener(
            request,
            timeout=_remaining_timeout(deadline, common.timeout_seconds),
        )
    except urllib.error.URLError as exc:
        raise BrokerRequestError("search upstream error") from exc
    return _read_response_envelope(
        response,
        request_id=request_id,
        fixed_final_url=fixed_url,
        max_response_bytes=_effective_response_limit(
            common.max_response_bytes, envelope.get("max_bytes")
        ),
        deadline=deadline,
    )


# ----------------------------------------------------------------------
# LLM route
# ----------------------------------------------------------------------


def _normalise_llm_payload(
    envelope: Mapping[str, Any],
    *,
    model: str,
    max_bytes: int,
) -> bytes:
    """Build the JSON payload that the LLM broker POSTs upstream.

    The caller controls the JSON object (``{"messages": ...}`` etc.);
    the broker always forces ``model`` to the policy value and refuses
    ``stream: true`` (the broker is request/response only). The encoded
    payload is length-checked against ``max_bytes`` before the request
    is dispatched.
    """
    raw_body = envelope.get("body")
    if not isinstance(raw_body, str):
        raise BrokerRequestError(
            "llm envelope must include a JSON body string"
        )
    try:
        decoded = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise BrokerRequestError(f"llm envelope body is not JSON: {exc}") from exc
    if not isinstance(decoded, Mapping):
        raise BrokerRequestError(
            "llm envelope body must decode to a JSON object"
        )
    if decoded.get("stream") is True:
        raise BrokerRequestError("llm broker does not support stream=true")
    decoded["model"] = model
    encoded = json.dumps(decoded, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if len(encoded) > max_bytes:
        raise BrokerRequestError(
            f"llm request body too large: {len(encoded)} > {max_bytes}"
        )
    return encoded


def handle_llm(
    envelope: Mapping[str, Any],
    *,
    common: CommonPolicy,
    llm: LlmPolicy,
    resolver: ResolverFn = default_resolver,
    opener: OpenerFn = default_opener,
    deadline: float | None = None,
) -> broker_protocol.BrokerResponse:
    """Issue a fixed-origin, forced-model LLM call.

    The envelope carries only a JSON body; ``target_url`` is ignored
    for routing but must still be present and parseable so callers
    cannot accidentally bypass the protocol contract.
    """
    request_id = envelope["request_id"]
    if not isinstance(envelope.get("target_url"), str):
        raise BrokerRequestError("llm envelope must include target_url")

    body_bytes = _normalise_llm_payload(
        envelope,
        model=llm.model,
        max_bytes=_effective_response_limit(
            min(common.max_response_bytes, llm.max_response_bytes),
            envelope.get("max_bytes"),
        ),
    )

    parsed = urllib.parse.urlsplit(llm.base_url)
    if not parsed.hostname:
        raise BrokerConfigError("llm.base_url is missing a hostname")
    if _netloc_has_userinfo(parsed.netloc):
        raise BrokerConfigError("llm.base_url must not include userinfo")
    base_netloc = parsed.netloc

    url = f"{parsed.scheme}://{base_netloc}{llm.path}"
    request = urllib.request.Request(
        url,
        data=body_bytes,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Accept": "application/json",
            "User-Agent": f"news-egress-broker/{broker_protocol.PROTOCOL_VERSION}",
            "X-News-Broker-Request-Id": request_id,
        },
        method="POST",
    )
    try:
        response = opener(
            request,
            timeout=_remaining_timeout(deadline, common.timeout_seconds),
        )
    except urllib.error.URLError as exc:
        raise BrokerRequestError("llm upstream error") from exc
    return _read_response_envelope(
        response,
        request_id=request_id,
        fixed_final_url=url,
        max_response_bytes=_effective_response_limit(
            llm.max_response_bytes, envelope.get("max_bytes")
        ),
        deadline=deadline,
    )


# ----------------------------------------------------------------------
# Feed route
# ----------------------------------------------------------------------


def _open_pinned_https(
    request: urllib.request.Request,
    timeout: float,
    *,
    pinned_ip: str,
) -> Any:
    """Connect to one prevalidated IP while validating TLS for the hostname."""
    parsed = urllib.parse.urlsplit(request.full_url)
    hostname = parsed.hostname
    if hostname is None:
        raise BrokerRequestError("feed URL has no hostname")
    raw_socket = socket.create_connection((pinned_ip, 443), timeout=timeout)
    tls_socket = None
    connection: http.client.HTTPSConnection | None = None
    try:
        context = ssl.create_default_context()
        tls_socket = context.wrap_socket(raw_socket, server_hostname=hostname)
        connection = http.client.HTTPSConnection(
            hostname, 443, timeout=timeout, context=context
        )
        connection.sock = tls_socket
        path = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        headers = dict(request.header_items())
        headers["Host"] = hostname
        connection.request(request.get_method(), path, headers=headers)
        return connection.getresponse()
    except Exception:
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.close()
        elif tls_socket is not None:
            tls_socket.close()
        else:
            raw_socket.close()
        raise


def _fetch_feed_once(
    url: str,
    *,
    timeout: float,
    resolved_ips: Sequence[str],
    opener: OpenerFn,
) -> Any:
    """Issue one GET, pinning production traffic to a validated IP."""
    request = urllib.request.Request(
        url,
        headers={
            "Accept": (
                "application/rss+xml, application/atom+xml, "
                "application/xml;q=0.9, */*;q=0.5"
            ),
            "User-Agent": f"news-egress-broker/{broker_protocol.PROTOCOL_VERSION}",
        },
        method="GET",
    )
    if opener is default_opener:
        last_error: OSError | None = None
        for pinned_ip in resolved_ips:
            try:
                return _open_pinned_https(
                    request, timeout, pinned_ip=pinned_ip
                )
            except OSError as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise BrokerRequestError("feed DNS resolution returned no usable addresses")
    return opener(request, timeout=timeout)


def _resolve_and_validate(
    host: str,
    *,
    port: int,
    resolver: ResolverFn,
) -> list[str]:
    """Resolve ``host`` and reject any blocked address."""
    try:
        ips = resolver(host, port)
    except BrokerRequestError:
        raise
    if not ips:
        raise BrokerRequestError(
            f"DNS resolution returned no addresses for {host!r}"
        )
    for ip in ips:
        if _ip_in_blocked_range(ip):
            raise BrokerRequestError(
                f"feed host {host!r} resolved to blocked address {ip!r}"
            )
    return ips


def handle_feed(
    envelope: Mapping[str, Any],
    *,
    common: CommonPolicy,
    feed: FeedPolicy,
    resolver: ResolverFn = default_resolver,
    opener: OpenerFn = default_opener,
    deadline: float | None = None,
) -> broker_protocol.BrokerResponse:
    """Issue an allowlisted HTTPS GET against ``envelope.target_url``.

    The full request is GET; redirects are followed manually (up to
    ``feed.max_redirects`` hops) and each hop is revalidated against
    the allowlist and SSRF rules.
    """
    request_id = envelope["request_id"]
    parsed = _validate_feed_target_url(envelope["target_url"])
    if not feed.is_allowed(parsed.hostname or ""):
        raise BrokerRequestError(
            f"feed host {parsed.hostname!r} is not on the policy allowlist"
        )

    current_url = urllib.parse.urlunsplit(parsed)
    hops = 0
    last_response: Any = None
    while True:
        current_parsed = urllib.parse.urlsplit(current_url)
        if current_parsed.scheme != "https":
            raise BrokerRequestError("redirect target must remain https")
        if _netloc_has_userinfo(current_parsed.netloc):
            raise BrokerRequestError("redirect target must not include userinfo")
        if current_parsed.port is not None and current_parsed.port != 443:
            raise BrokerRequestError("redirect target port rejected")
        if not current_parsed.hostname:
            raise BrokerRequestError("redirect target is missing a hostname")
        if not feed.is_allowed(current_parsed.hostname):
            raise BrokerRequestError("redirect target is not on the policy allowlist")
        resolved_ips = _resolve_and_validate(
            current_parsed.hostname,
            port=current_parsed.port or 443,
            resolver=resolver,
        )
        response: Any = None
        try:
            response = _fetch_feed_once(
                current_url,
                timeout=_remaining_timeout(deadline, common.timeout_seconds),
                resolved_ips=resolved_ips,
                opener=opener,
            )
            status = getattr(response, "status", None) or response.getcode()
            location = response.headers.get("Location") if hasattr(response, "headers") else None
            if status in (301, 302, 303, 307, 308) and location:
                hops += 1
                if hops > feed.max_redirects:
                    _close_upstream_response(response)
                    response = None
                    raise BrokerRequestError("feed redirect limit exceeded")
                next_url = urllib.parse.urljoin(current_url, location)
                _close_upstream_response(response)
                response = None
                current_url = next_url
                continue
            last_response = response
            break
        except urllib.error.URLError as exc:
            raise BrokerRequestError("feed upstream error") from exc
        except OSError as exc:
            raise BrokerRequestError("feed upstream error") from exc
        finally:
            if response is not None and response is not last_response:
                _close_upstream_response(response)

    assert last_response is not None
    return _read_response_envelope(
        last_response,
        request_id=request_id,
        fixed_final_url=current_url,
        max_response_bytes=_effective_response_limit(
            common.max_response_bytes, envelope.get("max_bytes")
        ),
        deadline=deadline,
    )


# ----------------------------------------------------------------------
# Response envelope assembly
# ----------------------------------------------------------------------


def _close_upstream_response(response: Any) -> None:
    """Close an upstream response and its owning HTTP connection."""
    with contextlib.suppress(Exception):
        response.close()
    for name in ("_owning_connection", "_connection", "connection"):
        owner = getattr(response, name, None)
        if owner is not None and owner is not response:
            with contextlib.suppress(Exception):
                owner.close()


def _read_response_envelope(
    response: Any,
    *,
    request_id: str,
    fixed_final_url: str,
    max_response_bytes: int,
    deadline: float | None = None,
) -> broker_protocol.BrokerResponse:
    """Materialise a bounded response and close all upstream resources."""
    chunks: list[bytes] = []
    received = 0
    status_code = -1
    upstream_headers: list[tuple[str, str]] = []
    try:
        try:
            status_code = int(response.getcode())
        except (AttributeError, TypeError, ValueError):
            status_code = -1
        if hasattr(response, "headers"):
            upstream_headers = list(response.headers.items())
        while True:
            _remaining_timeout(deadline, 120.0)
            chunk = response.read(min(65536, max_response_bytes + 1 - received))
            if not chunk:
                break
            chunks.append(chunk)
            received += len(chunk)
            if received > max_response_bytes:
                status_code = -1
                break
    finally:
        _close_upstream_response(response)
    body = b"".join(chunks)
    if len(body) > max_response_bytes:
        body = body[:max_response_bytes]
        status_code = -1
    return broker_protocol.BrokerResponse(
        request_id=request_id,
        status=status_code,
        headers=sanitise_response_headers(upstream_headers),
        body=body,
        final_url=fixed_final_url,
        retrieved_at=_utcnow_iso(),
    )


def _utcnow_iso() -> str:
    """Return a deterministic UTC ISO-8601 timestamp with second precision.

    The broker stamps its own ``retrieved_at`` so the wire response
    records when the upstream call completed, not when the caller
    dispatched the envelope.
    """
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


# ----------------------------------------------------------------------
# Envelope handling — request side
# ----------------------------------------------------------------------


def _read_envelope_bytes(
    stream: socket.socket,
    *,
    max_bytes: int,
    deadline: float | None = None,
) -> bytes:
    """Read one strictly framed POST /broker request body."""
    raw = bytearray()
    head_end = -1
    while head_end < 0:
        _remaining_timeout(deadline, 120.0)
        try:
            chunk = stream.recv(min(4096, MAX_HTTP_HEADER_BYTES + 4 - len(raw)))
        except socket.timeout as exc:
            raise BrokerRequestError("request deadline exceeded") from exc
        except OSError as exc:
            raise BrokerRequestError("socket read failed") from exc
        if not chunk:
            raise BrokerRequestError("end of request before headers")
        raw.extend(chunk)
        head_end = raw.find(b"\r\n\r\n")
        if head_end < 0 and len(raw) > MAX_HTTP_HEADER_BYTES:
            raise BrokerRequestError("request headers exceed bounded limit")
    head = bytes(raw[:head_end]).decode("iso-8859-1", errors="replace")
    lines = head.split("\r\n")
    if not lines or lines[0] != "POST /broker HTTP/1.1":
        raise BrokerRequestError("unsupported method or path")

    content_lengths: list[int] = []
    has_transfer_encoding = False
    for line in lines[1:]:
        if not line or ":" not in line:
            raise BrokerRequestError("malformed request header")
        name, value = line.split(":", 1)
        name = name.strip().lower()
        value = value.strip()
        if name == "transfer-encoding":
            has_transfer_encoding = True
        elif name == "content-length":
            if not value.isdigit():
                raise BrokerRequestError("invalid Content-Length header")
            content_lengths.append(int(value))
    if has_transfer_encoding:
        raise BrokerRequestError("Transfer-Encoding is not supported")
    if len(content_lengths) != 1:
        raise BrokerRequestError("request requires exactly one Content-Length")
    content_length = content_lengths[0]
    if content_length <= 0 or content_length > max_bytes:
        raise BrokerRequestError("Content-Length exceeds request limit")

    body = bytearray(raw[head_end + 4 :])
    if len(body) > content_length:
        raise BrokerRequestError("extra request data after body")
    while len(body) < content_length:
        _remaining_timeout(deadline, 120.0)
        try:
            chunk = stream.recv(min(4096, content_length - len(body)))
        except socket.timeout as exc:
            raise BrokerRequestError("request deadline exceeded") from exc
        except OSError as exc:
            raise BrokerRequestError("socket read failed") from exc
        if not chunk:
            raise BrokerRequestError("end of request before body")
        body.extend(chunk)
    if len(body) != content_length:
        raise BrokerRequestError("extra request data after body")

    # A second request or trailing bytes already queued on this connection
    # are rejected without waiting for an orderly client close.
    try:
        readable, _, _ = select.select([stream], [], [], 0)
    except (OSError, ValueError) as exc:
        raise BrokerRequestError("socket read failed") from exc
    if readable:
        try:
            extra = stream.recv(1, socket.MSG_PEEK)
        except (BlockingIOError, TimeoutError):
            extra = b""
        except OSError as exc:
            raise BrokerRequestError("socket read failed") from exc
        if extra:
            raise BrokerRequestError("extra request data after body")
    return bytes(body)


def _decode_envelope(body: bytes) -> dict[str, Any]:
    """Decode the envelope body and run it through the protocol validator.

    For ``llm`` envelopes we route through a local validator that
    extends the protocol's route set with ``llm``.  Headers, timeouts,
    and byte caps still go through the canonical validator so the
    surface stays consistent across routes.
    """
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BrokerRequestError(f"envelope is not UTF-8: {exc}") from exc
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError as exc:
        raise BrokerRequestError(f"envelope is not JSON: {exc}") from exc
    try:
        route_value = decoded.get("route") if isinstance(decoded, Mapping) else None
        if route_value == "llm":
            validated = _validate_llm_envelope(decoded)
        else:
            validated = broker_protocol.validate_request_payload(decoded)
    except broker_protocol.BrokerProtocolError as exc:
        raise BrokerRequestError(
            f"envelope validation failed: {exc}"
        ) from exc
    return dict(validated)


def _validate_llm_envelope(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Validate an LLM envelope locally.

    Mirrors :func:`broker_protocol.validate_request_payload` for the
    fields the protocol already enforces (version, headers, timeouts,
    byte caps) but extends the route allowlist with ``llm``.  Lives
    here rather than mutating :mod:`broker_protocol` because the
    canonical protocol deliberately excludes ``llm`` from the
    container-facing route list.
    """
    if not isinstance(payload, Mapping):
        raise broker_protocol.InvalidEnvelopeError("envelope must be a JSON object")
    route = payload.get("route")
    if route not in {"search", "feed", "llm"}:
        raise broker_protocol.UnsupportedRouteError(
            f"route {route!r} is not allowed (must be one of ['feed', 'llm', 'search'])"
        )
    if route != "llm":
        # Defensive: if a non-llm route sneaks through this branch,
        # delegate to the canonical validator.
        return broker_protocol.validate_request_payload(payload)
    version = payload.get("protocol_version", broker_protocol.PROTOCOL_VERSION)
    if version != broker_protocol.PROTOCOL_VERSION:
        raise broker_protocol.InvalidEnvelopeError(
            f"protocol_version must be {broker_protocol.PROTOCOL_VERSION}, got {version!r}"
        )
    target_url = broker_protocol._require_str(payload.get("target_url"), "target_url")
    request_id = broker_protocol._require_request_id(payload.get("request_id"))
    retrieved_at = broker_protocol._require_str(payload.get("retrieved_at"), "retrieved_at")
    timeout = broker_protocol._require_number(
        payload.get("timeout_seconds", broker_protocol.DEFAULT_TIMEOUT_SECONDS),
        "timeout_seconds",
    )
    if timeout <= 0 or timeout > broker_protocol.MAX_TIMEOUT_SECONDS:
        raise broker_protocol.InvalidEnvelopeError(
            f"timeout_seconds must be in (0, {broker_protocol.MAX_TIMEOUT_SECONDS}], got {timeout!r}"
        )
    max_bytes = broker_protocol._require_number(
        payload.get("max_bytes", broker_protocol.DEFAULT_MAX_BYTES),
        "max_bytes",
    )
    if max_bytes <= 0 or max_bytes > broker_protocol.MAX_BYTES_HARD_LIMIT:
        raise broker_protocol.InvalidEnvelopeError(
            f"max_bytes must be in (0, {broker_protocol.MAX_BYTES_HARD_LIMIT}], got {max_bytes!r}"
        )
    if int(max_bytes) <= 0:
        raise broker_protocol.InvalidEnvelopeError("max_bytes must encode at least one byte")
    headers_raw = payload.get("headers", [])
    if not isinstance(headers_raw, list):
        raise broker_protocol.InvalidEnvelopeError(
            "headers must be a list of [name, value] pairs"
        )
    cleaned_headers: list[tuple[str, str]] = []
    for entry in headers_raw:
        if (
            not isinstance(entry, list)
            or len(entry) != 2
            or not all(isinstance(part, str) for part in entry)
        ):
            raise broker_protocol.InvalidEnvelopeError(
                "headers entries must be two-element lists of strings"
            )
        name, value = entry
        if name not in broker_protocol.ALLOWED_HEADERS:
            raise broker_protocol.DisallowedHeaderError(
                f"header {name!r} is not on the broker allowlist"
            )
        cleaned_headers.append((name, value))
    body_raw = payload.get("body", "")
    return {
        "protocol_version": broker_protocol.PROTOCOL_VERSION,
        "request_id": request_id,
        "route": "llm",
        "target_url": target_url,
        "headers": tuple(cleaned_headers),
        "timeout_seconds": float(timeout),
        "max_bytes": int(max_bytes),
        "retrieved_at": retrieved_at,
        "body": body_raw,
    }


# ----------------------------------------------------------------------
# HTTP response serialisation
# ----------------------------------------------------------------------


def _serialise_response(
    envelope: broker_protocol.BrokerResponse | Mapping[str, Any],
) -> bytes:
    """Encode a :class:`BrokerResponse` as a JSON byte string."""
    if isinstance(envelope, broker_protocol.BrokerResponse):
        payload = envelope.to_dict()
    elif isinstance(envelope, Mapping):
        payload = dict(envelope)
    else:
        raise BrokerConfigError(
            "response object must be a BrokerResponse or mapping"
        )
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _build_http_response(
    *,
    status: int,
    body: bytes,
    extra_headers: Sequence[tuple[str, str]] = (),
) -> bytes:
    """Render an HTTP/1.1 response with ``status`` and JSON ``body``."""
    reason = http.client.responses.get(status, "OK")
    headers = [
        "HTTP/1.1 {} {}\r\n".format(status, reason),
        "Content-Type: application/json; charset=utf-8\r\n",
        "Content-Length: {}\r\n".format(len(body)),
        "Connection: close\r\n",
    ]
    for name, value in extra_headers:
        headers.append(f"{name}: {value}\r\n")
    headers.append("\r\n")
    return "".join(headers).encode("ascii") + body


def _build_error_response(
    *,
    request_id: str,
    error_code: str,
    error_message: str,
    status: int = 400,
) -> bytes:
    """Render a sanitised error envelope as an HTTP/1.1 response body."""
    body_obj = {
        "protocol_version": broker_protocol.PROTOCOL_VERSION,
        "request_id": request_id,
        "status": status,
        "headers": [],
        "body_b64": "",
        "final_url": "",
        "retrieved_at": _utcnow_iso(),
        "error_code": error_code,
        "error_message": error_message,
        "body_length": 0,
    }
    body = json.dumps(
        body_obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return _build_http_response(
        status=status,
        body=body,
        extra_headers=(
            ("X-News-Broker-Error-Code", error_code),
        ),
    )


# ----------------------------------------------------------------------
# Dispatch
# ----------------------------------------------------------------------


@dataclass(slots=True)
class _DispatchContext:
    """Per-connection state passed to the handler."""

    policy: BrokerPolicy
    route: str
    resolver: ResolverFn = default_resolver
    opener: OpenerFn = default_opener


def dispatch_envelope(
    envelope: dict[str, Any],
    *,
    ctx: _DispatchContext,
    deadline: float | None = None,
) -> broker_protocol.BrokerResponse:
    """Route a validated envelope to its handler."""
    caller_headers = sanitise_caller_headers(envelope["headers"])
    common = ctx.policy.common
    if ctx.route == "search":
        return handle_search(
            envelope,
            common=common,
            search=ctx.policy.search,
            caller_headers=caller_headers,
            resolver=ctx.resolver,
            opener=ctx.opener,
            deadline=deadline,
        )
    if ctx.route == "feed":
        return handle_feed(
            envelope,
            common=common,
            feed=ctx.policy.feed,
            resolver=ctx.resolver,
            opener=ctx.opener,
            deadline=deadline,
        )
    if ctx.route == "llm":
        return handle_llm(
            envelope,
            common=common,
            llm=ctx.policy.llm,
            resolver=ctx.resolver,
            opener=ctx.opener,
            deadline=deadline,
        )
    raise BrokerRequestError(f"unknown route {ctx.route!r}")


# ----------------------------------------------------------------------
# Socket path checks
# ----------------------------------------------------------------------


def _check_socket_path(path: str) -> None:
    """Verify the socket path passes the host hardening rules.

    Rules:

    * absolute path;
    * parent directory exists, is a real directory (not a symlink),
      is owned by the current uid, and is not world-writable;
    * socket path itself exists, is a socket (not a symlink), and is
      owned by the current uid;
    * socket mode is forced to ``0o660`` afterwards.
    """
    if not os.path.isabs(path):
        raise BrokerConfigError(
            f"socket path must be absolute, got {path!r}"
        )
    parent = os.path.dirname(path)
    if not parent:
        raise BrokerConfigError(
            f"socket path has no parent directory: {path!r}"
        )
    if os.path.islink(parent):
        raise BrokerConfigError(
            f"socket parent directory is a symlink: {parent!r}"
        )
    if not os.path.isdir(parent):
        raise BrokerConfigError(
            f"socket parent directory does not exist: {parent!r}"
        )
    parent_stat = os.stat(parent)
    if parent_stat.st_uid != os.getuid():
        raise BrokerConfigError(
            f"socket parent {parent!r} is not owned by current uid"
        )
    if parent_stat.st_mode & 0o007:
        raise BrokerConfigError(
            f"socket parent {parent!r} must not be world-writable"
        )
    if parent_stat.st_mode & 0o027:
        # Group-writable is allowed only if mode bit 0o020 is unset
        # AND no broader than 0o750; here we reject anything broader
        # than 0750 explicitly.
        allowed = 0o750
        if parent_stat.st_mode & ~0o7777 & ~allowed:  # pragma: no cover
            pass
        if parent_stat.st_mode & ~allowed & 0o7777:
            raise BrokerConfigError(
                f"socket parent {parent!r} mode is broader than 0o750"
            )

    if os.path.islink(path):
        raise BrokerConfigError(
            f"socket path is a symlink: {path!r}"
        )
    if not os.path.exists(path):
        # Caller asked us to verify an existing socket; if it's
        # missing, that's a config error from the caller's
        # perspective.
        raise BrokerConfigError(
            f"socket path does not exist: {path!r}"
        )
    socket_stat = os.stat(path)
    if socket_stat.st_uid != os.getuid():
        raise BrokerConfigError(
            f"socket {path!r} is not owned by current uid"
        )
    if not stat.S_ISSOCK(socket_stat.st_mode):
        raise BrokerConfigError(
            f"socket path is not a socket: {path!r}"
        )
    current_mode = stat.S_IMODE(socket_stat.st_mode)
    if current_mode != 0o660:
        try:
            os.chmod(path, 0o660)
        except OSError as exc:
            raise BrokerConfigError(
                f"could not chmod socket {path!r} to 0o660: {exc}"
            ) from exc


# ----------------------------------------------------------------------
# UDS request handler
# ----------------------------------------------------------------------


class _BrokerRequestHandler(socketserver.BaseRequestHandler):
    """Handle one HTTP/1.1 request over an AF_UNIX connection."""

    def handle(self) -> None:  # type: ignore[override]
        ctx: _DispatchContext = self.server.context  # type: ignore[attr-defined]
        request_id = "<unknown>"
        started = time.monotonic()
        deadline = started + ctx.policy.common.timeout_seconds
        try:
            self.request.settimeout(ctx.policy.common.timeout_seconds)
            body = _read_envelope_bytes(
                self.request,
                max_bytes=ctx.policy.common.max_request_bytes,
                deadline=deadline,
            )
            envelope = _decode_envelope(body)
            request_id = envelope["request_id"]
            deadline = min(
                deadline,
                started + _positive_finite_seconds(
                    envelope.get("timeout_seconds"),
                    field_name="timeout_seconds",
                ),
            )
            if envelope["route"] != ctx.route:
                raise BrokerRequestError("envelope route does not match broker route")
            response = dispatch_envelope(envelope, ctx=ctx, deadline=deadline)
            _remaining_timeout(deadline, ctx.policy.common.timeout_seconds)
            payload = _serialise_response(response)
            self.request.sendall(
                _build_http_response(
                    status=200,
                    body=payload,
                    extra_headers=(("X-News-Broker-Request-Id", request_id),),
                )
            )
        except BrokerRequestError as exc:
            LOGGER.warning("broker request rejected category=%s", exc.category)
            with contextlib.suppress(OSError):
                self.request.sendall(
                    _build_error_response(
                        request_id=request_id,
                        error_code="invalid_request",
                        error_message=exc.category,
                        status=400,
                    )
                )
        except BrokerConfigError:
            LOGGER.error("broker request failed category=broker_config_error")
            with contextlib.suppress(OSError):
                self.request.sendall(
                    _build_error_response(
                        request_id=request_id,
                        error_code="broker_config_error",
                        error_message="broker_config_error",
                        status=500,
                    )
                )
        except Exception:
            LOGGER.error("broker request failed category=broker_internal_error")
            with contextlib.suppress(OSError):
                self.request.sendall(
                    _build_error_response(
                        request_id=request_id,
                        error_code="broker_internal_error",
                        error_message="broker_internal_error",
                        status=500,
                    )
                )


class _UnixStreamServer(socketserver.ThreadingUnixStreamServer):
    """Threaded AF_UNIX server with bounded admission and deadlines."""

    context: _DispatchContext  # type: ignore[assignment]
    daemon_threads = True
    block_on_close = True
    request_queue_size = MAX_ACTIVE_CONNECTIONS

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._admission = threading.BoundedSemaphore(MAX_ACTIVE_CONNECTIONS)

    def set_admission_limit(self, limit: int) -> None:
        """Apply the policy limit without exceeding the hard process cap."""
        self._admission = threading.BoundedSemaphore(
            max(1, min(MAX_ACTIVE_CONNECTIONS, int(limit)))
        )

    def process_request(self, request: socket.socket, client_address: Any) -> None:
        if not self._admission.acquire(blocking=False):
            with contextlib.suppress(OSError):
                request.close()
            return
        try:
            request.settimeout(self.context.policy.common.timeout_seconds)
            super().process_request(request, client_address)
        except Exception:
            self._admission.release()
            with contextlib.suppress(OSError):
                request.close()
            raise

    def process_request_thread(self, request: socket.socket, client_address: Any) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._admission.release()


# ----------------------------------------------------------------------
# Signal handling & lifecycle
# ----------------------------------------------------------------------


@contextlib.contextmanager
def _install_signal_handlers(server: _UnixStreamServer):
    """Install SIGINT/SIGTERM handlers that shut down ``server`` cleanly."""

    stop = {"flag": False}

    def _set_flag(signum, frame):  # noqa: ARG001
        stop["flag"] = True
        with contextlib.suppress(Exception):
            server.shutdown()

    previous_int = signal.getsignal(signal.SIGINT)
    previous_term = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _set_flag)
    signal.signal(signal.SIGTERM, _set_flag)
    try:
        yield stop
    finally:
        signal.signal(signal.SIGINT, previous_int)
        signal.signal(signal.SIGTERM, previous_term)


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="news-egress-broker",
        description=(
            "Host-side Unix-domain-socket egress broker for the sealed "
            "news container."
        ),
    )
    parser.add_argument(
        "--policy",
        required=True,
        help="Path to the broker TOML policy file.",
    )
    parser.add_argument(
        "--route",
        required=True,
        choices=sorted(ALLOWED_ROUTES),
        help="Broker route (one of search, feed, llm).",
    )
    parser.add_argument(
        "--socket",
        required=True,
        help="Absolute path of the AF_UNIX socket to bind.",
    )
    parser.add_argument(
        "--user",
        default=None,
        help="Optional numeric uid to drop privileges to before binding.",
    )
    parser.add_argument(
        "--group",
        default=None,
        help="Optional numeric gid to drop privileges to before binding.",
    )
    return parser.parse_args(argv)


def _maybe_drop_privileges(uid: str | None, gid: str | None) -> None:
    """Drop to ``uid:gid`` if provided. Errors out on invalid input."""
    if uid is None and gid is None:
        return
    try:
        target_uid = int(uid) if uid is not None else os.getuid()
        target_gid = int(gid) if gid is not None else os.getgid()
    except ValueError as exc:
        raise BrokerConfigError(
            f"invalid --user/--group (must be numeric): {exc}"
        ) from exc
    if os.getuid() != 0:
        raise BrokerConfigError(
            "privilege drop requires root; rerun the broker as root"
        )
    try:
        os.setgid(target_gid)
        os.setuid(target_uid)
    except OSError as exc:
        raise BrokerConfigError(
            f"could not drop privileges to {target_uid}:{target_gid}: {exc}"
        ) from exc


def _bind_socket(socket_path: str) -> socket.socket:
    """Bind ``socket_path`` for AF_UNIX and apply the hardening rules."""
    parent = os.path.dirname(socket_path)
    if not parent:
        raise BrokerConfigError(
            f"socket path has no parent directory: {socket_path!r}"
        )
    if os.path.exists(socket_path):
        try:
            os.unlink(socket_path)
        except OSError as exc:
            raise BrokerConfigError(
                f"could not remove stale socket {socket_path!r}: {exc}"
            ) from exc
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(socket_path)
        os.chmod(socket_path, 0o660)
        server.listen(MAX_ACTIVE_CONNECTIONS)
    except OSError as exc:
        server.close()
        raise BrokerConfigError(
            f"could not bind socket {socket_path!r}: {exc}"
        ) from exc
    return server


def serve(args: argparse.Namespace) -> int:
    """Run the broker with ``args``; return 0 on graceful shutdown."""
    policy = BrokerPolicy.load(args.policy)
    _maybe_drop_privileges(args.user, args.group)
    server_sock = _bind_socket(args.socket)
    try:
        server = _UnixStreamServer(
            args.socket,
            _BrokerRequestHandler,
            bind_and_activate=False,
        )
        server.socket = server_sock
        server.allow_reuse_address = False
        server.context = _DispatchContext(policy=policy, route=args.route)
        server.set_admission_limit(policy.common.max_concurrency)
        with _install_signal_handlers(server):
            LOGGER.info(
                "broker ready route=%s socket=%s policy=%s",
                args.route,
                args.socket,
                args.policy,
            )
            server.serve_forever()
    finally:
        with contextlib.suppress(OSError):
            server_sock.close()
        if os.path.exists(args.socket):
            with contextlib.suppress(OSError):
                os.unlink(args.socket)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("NEWS_EGRESS_BROKER_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        return serve(args)
    except BrokerConfigError as exc:
        LOGGER.error("broker refused to start: %s", exc)
        return 2
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())