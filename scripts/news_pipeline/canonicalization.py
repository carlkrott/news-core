from __future__ import annotations

import posixpath
import re
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

_TRACKING_NAMES = {"fbclid", "gclid", "ref", "ref_src", "mc_cid", "mc_eid"}


def retain_original(url: str | None) -> str | None:
    """Return the supplied URL unchanged."""
    return url


def _remove_dot_segments(path: str) -> str:
    if not path:
        return ""
    leading_slash = path.startswith("/")
    trailing_slash = path.endswith("/") or path.endswith("/.") or path.endswith("/..")
    normalized = posixpath.normpath(path)
    if normalized == ".":
        normalized = ""
    if leading_slash and not normalized.startswith("/"):
        normalized = "/" + normalized
    if trailing_slash and normalized and not normalized.endswith("/"):
        normalized += "/"
    return normalized


def canonicalize_url(url: str | None) -> str | None:
    """Canonicalize an HTTP(S) URL.

    Empty input returns ``None``. Invalid or non-HTTP(S) inputs raise ``ValueError``
    so callers can explicitly distinguish malformed data from absent data. The host
    structure is preserved: in particular, a leading ``www.`` is never stripped.
    HTTP is also not upgraded to HTTPS because the two origins need not be equivalent.
    """
    if url is None or url == "":
        return None
    if not isinstance(url, str):
        raise ValueError("URL must be a string or None")
    try:
        parsed = urlsplit(url)
    except ValueError as exc:
        raise ValueError(f"invalid URL: {url!r}") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme!r}")
    if not parsed.hostname:
        raise ValueError("HTTP(S) URL requires a host")
    try:
        host = parsed.hostname.lower().rstrip(".").encode("idna").decode("ascii")
        if not host:
            raise ValueError("HTTP(S) URL requires a non-empty host")
        port = parsed.port
    except (UnicodeError, ValueError) as exc:
        raise ValueError(f"invalid URL host or port: {url!r}") from exc
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        host = f"{host}:{port}"
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("userinfo is not accepted in canonical URLs")

    path = _remove_dot_segments(parsed.path)
    # Retain URL path semantics while normalizing non-ASCII and unsafe characters.
    path = quote(path, safe="/%:@!$&'()*+,;=-._~")
    query_items = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        lowered = key.lower()
        if lowered.startswith("utm_") or lowered in _TRACKING_NAMES:
            continue
        query_items.append((key, value))
    query_items.sort(key=lambda pair: (pair[0], pair[1]))
    query = urlencode(query_items, doseq=True)
    return urlunsplit((scheme, host, path, query, ""))
