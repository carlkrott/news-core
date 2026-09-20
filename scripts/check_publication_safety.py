#!/usr/bin/env python3
"""Publication-safety scanner for the future-public export of the news core.

Scope:
    Walks an intended export root, classifies every entry as either an
    allowlisted export artifact or a forbidden artifact, and rejects the
    export when private evidence, runtime database/WAL/SHM, reports,
    caches, bytecode, the staging manifest, environment files, keys,
    certificates, host identity, or maintainer-identifying endpoints
    are present.

Allowlist contract:
    The allowlist is intentionally narrow.  Only files documented as the
    public-export surface of the news core are accepted.  Everything
    else (including everything else inside the candidate tree) must be
    rejected with a precise reason.

Exit codes:
    0  - allowlist matches and no forbidden artifact is present.
    2  - forbidden artifact, missing required doc, malformed entry, or
         sanitized-config leak.

The scanner is intentionally stdlib-only and dependency-free so it can
run inside the candidate's own unit-test environment as well as in
adversarial publication review.
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# ---------------------------------------------------------------------------
# Allowlist
# ---------------------------------------------------------------------------

# Exact files (relative paths from the export root) that are public-export
# artifacts.  Code and tests are admitted by the closed prefix/suffix rules
# below; unknown paths remain rejected.
PUBLIC_ALLOWLIST: frozenset[str] = frozenset(
    {
        ".gitignore",
        ".dockerignore",
        ".gitattributes",
        "pyproject.toml",
        "README.md",
        "ARCHITECTURE.md",
        "SECURITY.md",
        "CONTRIBUTING.md",
        "BUILD.md",
        "CHANGELOG.md",
        "LICENSE",
        ".github/CODEOWNERS",
        ".github/dependabot.yml",
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/ISSUE_TEMPLATE/config.yml",
        ".github/ISSUE_TEMPLATE/bug_report.yml",
        ".github/ISSUE_TEMPLATE/feature_request.yml",
        ".github/ISSUE_TEMPLATE/security.md",
        ".github/workflows/ci.yml",
        ".github/workflows/security.yml",
        ".github/workflows/release.yml",
        "config/news-policy.example.toml",
        "config/news-provenance.example.toml",
        "config/news-sources.example.toml",
        "config/news-topics.example.toml",
        "config/runtime-schedule.example.toml",
        "Dockerfile",
        "compose.yaml",
        "compose.canary.yaml",
        "bin/container-entrypoint",
        "bin/news-tick",
        "bin/news-process",
        "bin/news-health",
        "bin/news-daily-report",
        "bin/news-daily-close",
        "scripts/check_publication_safety.py",
        "scripts/export_public_tree.py",
        "scripts/render_release_manifest.py",
        "scripts/verify_container_contract.py",
        "host/news_egress_broker.py",
        "host/broker-policy.example.toml",
        "deploy/systemd/news-egress-broker@.service",
        "deploy/systemd/news-egress-broker.env.example",
        "tests/news_pipeline/test_publication_safety.py",
    }
)

PUBLIC_CODE_PREFIXES: tuple[str, ...] = (
    "scripts/news_pipeline/",
    "scripts/news_container/",
)
PUBLIC_TEST_PREFIX = "tests/news_pipeline/"

# Candidate-local material that is deliberately not copied by the export
# helper.  These are explicit, closed exclusions; every other non-public path
# is an error rather than something the exporter silently ignores.
EXPORT_EXCLUDED_EXACT: frozenset[str] = frozenset(
    {
        "config/news-policy.toml",
        "config/news-provenance.toml",
        "config/news-sources.toml",
        "config/news-topics.toml",
        "STAGING_MANIFEST.json",
    }
)
# Git checkout metadata is intentionally ignored by the exporter; direct scans
# still reject it as an unclassified path.
EXPORT_EXCLUDED_COMPONENTS: frozenset[str] = frozenset(
    {".git", "private-evidence", ".release_manifest", "__pycache__", ".pytest_cache"}
)
EXPORT_EXCLUDED_SUFFIXES: tuple[str, ...] = (".pyc", ".pyo")

# Required docs -- the export is rejected if any of these are absent.
REQUIRED_DOCS: tuple[str, ...] = (
    "README.md",
    "ARCHITECTURE.md",
    "SECURITY.md",
    "CONTRIBUTING.md",
    "BUILD.md",
    "pyproject.toml",
    "LICENSE",
)

# Synthetic secret-detector fixtures are allowlisted as fixtures inside the
# unit test.  These names never appear inside the production export.
ALLOWLISTED_FIXTURE_BASENAMES: frozenset[str] = frozenset(
    {
        # Synthetic, generated exclusively for the publication-safety tests.
        # They never appear inside the real export allowlist.
        "phase6_searxng_token_fixture.json",
        "phase6_feed_token_fixture.xml",
        "phase6_telegram_token_fixture.toml",
    }
)

# ---------------------------------------------------------------------------
# Forbidden-artifact detection
# ---------------------------------------------------------------------------

# Paths or basenames that are environment files, keys, certs, or markers.
# When the basename matches any of these the export is rejected.
FORBIDDEN_BASENAMES: frozenset[str] = frozenset(
    {
        ".env",
        ".envrc",
        "id_rsa",
        "id_ed25519",
        "id_ecdsa",
        "id_dsa",
        "authorized_keys",
        "known_hosts",
        "credentials",
        "credentials.json",
        "credentials.toml",
        "cred.json",
        "secret",
        "secret.json",
        "secret.toml",
    }
)

# Path fragments whose presence anywhere in a path causes rejection.
FORBIDDEN_PATH_FRAGMENTS: tuple[str, ...] = (
    "private-evidence",
    "private_evidence",
    "staging_manifest",
    "STAGING_MANIFEST",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "venv",
    "node_modules",
    "target",
    "build",
    "dist",
    ".git",
)

# File suffixes that are runtime, cache, bytecode, or evidence artifacts.
FORBIDDEN_SUFFIXES: tuple[str, ...] = (
    ".pyc",
    ".pyo",
    ".pyd",
    ".db",
    ".db-wal",
    ".db-shm",
    ".sqlite",
    ".sqlite3",
    ".sqlite-wal",
    ".sqlite3-wal",
    ".log",
    ".swp",
    ".swo",
    ".bak",
    ".tmp",
    ".key",
    ".pem",
    ".crt",
    ".cer",
    ".pfx",
    ".p12",
)

# ---------------------------------------------------------------------------
# Secret / endpoint detection
# ---------------------------------------------------------------------------

# Literal patterns that identify maintainer, host, or user names that
# must never appear in the public export.
MAINTAINER_LITERALS: tuple[str, ...] = (
    "/home/korphaus",
    "/home/<user>",
    "/Users/korphaus",
    "/Users/<user>",
    "korphaus",
    "carl",
    ".zeroclaw",
    "zeroclaw",
    "Zeroclaw",
)

# Patterns that look like a credential or token in content.  These are
# intentionally conservative -- the publication surface must not contain
# tokens, API keys, bearer strings, basic-auth userinfo, or chat IDs.
CONTENT_TOKEN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._\-+/=]{16,}"),
    re.compile(r"(?i)\bapi[_-]?key\s*[=:]\s*['\"]?[A-Za-z0-9._\-+/=]{16,}"),
    # ``delivery.py`` assigns ``token_match.group(1)`` to ``token``.  The
    # exact negative lookahead avoids treating that source expression as a
    # credential while retaining literal token assignments.
    re.compile(r"(?i)\btoken\s*[=:]\s*(?!token_match\.group\b)['\"]?[A-Za-z0-9._\-+/=]{16,}"),
    re.compile(r"(?i)\btelegram[_-]?bot[_-]?token\s*[=:]\s*['\"]?[0-9]{6,}:[A-Za-z0-9._\-+/=]{20,}"),
    re.compile(r"(?i)\bchat[_-]?id\s*[=:]\s*['\"]?-?[0-9]{6,}"),
    re.compile(r"(?i)\bghp_[A-Za-z0-9]{20,}"),  # GitHub PAT
    re.compile(r"(?i)\bxox[baprs]-[A-Za-z0-9-]{10,}"),  # Slack tokens
)

# IPv4 literal patterns (RFC 1918, CGNAT, link-local, loopback, multicast,
# unspecified).  These must never appear in the public export.
PRIVATE_IPV4_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b10(?:\.\d{1,3}){3}\b"),
    re.compile(r"\b192\.168(?:\.\d{1,3}){2}\b"),
    re.compile(r"\b172\.(?:1[6-9]|2\d|3[0-1])(?:\.\d{1,3}){2}\b"),
    re.compile(r"\b100\.(?:6[4-9]|[7-9]\d|1[0-1]\d|12[0-7])(?:\.\d{1,3}){2}\b"),  # CGNAT
    re.compile(r"\b169\.254(?:\.\d{1,3}){2}\b"),  # link-local
    re.compile(r"\b127(?:\.\d{1,3}){3}\b"),
    re.compile(r"\b0\.0\.0\.0\b"),
    re.compile(r"\b22[4-9]\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
    re.compile(r"\b23[0-9]\.\d{1,3}\.\d{1,3}\.\d{1,3}\b"),
)

# Tailnet / mesh-VPN literals.
TAILNET_LITERALS: tuple[str, ...] = (
    "ts.net",
    "tailnet",
    "headscale",
    "tailscale",
    "100.100.100.100",  # MagicDNS / coordination
    "fd7a:115c:a1e0:",  # Headscale default IPv6 prefix
)

# Documented example-host placeholders.  Anything else of an endpoint
# shape (host:port) inside the public surface is rejected -- the example
# configs must not carry real endpoints.
EXAMPLE_HOSTS: frozenset[str] = frozenset(
    {
        "example.com",
        "www.example.com",
        "api.example.com",
        "search.example.com",
        "broker.example.com",
        "llm.example.com",
        "feed.example.com",
        "localhost",
    }
)

# Hostnames must start with a letter so numeric fragments such as
# ``time = "08:00"`` or ``port = "8888"`` are not misread as endpoints.
HOST_PORT_PATTERN = re.compile(
    r"\b(?P<host>[A-Za-z][A-Za-z0-9.\-]*[A-Za-z0-9])(?::(?P<port>\d{2,5}))\b"
)

# Content is scanned in every allowlisted file.  These narrow line spans
# cover existing documentation examples and test fixtures that intentionally
# mention blocked addresses or host identities.  Credential matches are
# never covered by this table.
def _line_spans(*lines: int) -> tuple[tuple[int, int], ...]:
    return tuple((line, line) for line in lines)


CONTENT_SPAN_ALLOWANCES: dict[str, dict[str, tuple[tuple[int, int], ...]]] = {
    "README.md": {"MAINTAINER_LITERAL": _line_spans(43, 45)},
    "ARCHITECTURE.md": {"MAINTAINER_LITERAL": _line_spans(150, 155)},
    "CONTRIBUTING.md": {"MAINTAINER_LITERAL": _line_spans(52)},
    "SECURITY.md": {
        "MAINTAINER_LITERAL": _line_spans(46, 47, 48, 49, 116),
        "PRIVATE_IP_LITERAL": _line_spans(46, 47, 48, 49, 52, 53, 54, 55),
        "TAILNET_LITERAL": _line_spans(52, 53, 54, 55),
    },
    "tests/news_pipeline/test_adapters.py": {
        "NON_PLACEHOLDER_HOST": _line_spans(1256),
    },
    "tests/news_pipeline/test_canonicalization.py": {
        "NON_PLACEHOLDER_HOST": _line_spans(11, 15),
    },
    "tests/news_pipeline/test_delivery_phase6.py": {
        "MAINTAINER_LITERAL": _line_spans(307, 323),
    },
    "tests/news_pipeline/test_host_news_egress_broker.py": {
        "PRIVATE_IP_LITERAL": _line_spans(
            5, 59, 61, 262, 440, 448, 461, 463, 477, 479,
            493, 494, 495, 496, 497, 498, 499, 500, 501,
            629, 631, 654, 700, 732, 734, 887, 889,
            941, 953, 1014, 1091, 1150, 1195, 1198,
        ),
        "NON_PLACEHOLDER_HOST": _line_spans(
            675,
        ),
    },
    "tests/news_pipeline/test_process_runner.py": {
        "PRIVATE_IP_LITERAL": _line_spans(47, 74, 352),
        "NON_PLACEHOLDER_HOST": _line_spans(47, 74, 352),
    },
    "tests/news_pipeline/test_query_planner.py": {
        "PRIVATE_IP_LITERAL": _line_spans(92, 102),
        "NON_PLACEHOLDER_HOST": _line_spans(92, 102),
    },
    "tests/news_pipeline/test_schema_v3.py": {
        "PRIVATE_IP_LITERAL": _line_spans(64),
    },
    "tests/news_pipeline/test_source_registry.py": {
        "PRIVATE_IP_LITERAL": _line_spans(121, 150),
    },
    "tests/news_pipeline/test_publication_safety.py": {
        "MAINTAINER_LITERAL": _line_spans(308, 314, 321, 327, 334, 471),
        "PRIVATE_IP_LITERAL": _line_spans(347, 360, 475),
        "TAILNET_LITERAL": _line_spans(353, 360),
    },
    "LICENSE": {"MAINTAINER_LITERAL": _line_spans(3)},
    ".github/CODEOWNERS": {"MAINTAINER_LITERAL": _line_spans(3)},
    ".github/ISSUE_TEMPLATE/config.yml": {"MAINTAINER_LITERAL": _line_spans(10)},
    ".github/ISSUE_TEMPLATE/security.md": {"MAINTAINER_LITERAL": _line_spans(12)},
}

_SCANNER_ALLOWANCE_ASSIGNMENTS = frozenset(
    {"MAINTAINER_LITERALS", "PRIVATE_IPV4_PATTERNS", "TAILNET_LITERALS"}
)


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    """A single violation or information record."""

    path: str
    code: str
    detail: str

    def render(self) -> str:
        return f"[{self.code}] {self.path}: {self.detail}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _iter_files(root: Path) -> Iterable[Path]:
    """Yield every non-root entry without following symlink directories."""

    root = Path(root)
    pending = [root]
    while pending:
        current = pending.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda item: item.as_posix(), reverse=True)
        except OSError:
            continue
        for entry in entries:
            yield entry
            if entry.is_symlink():
                continue
            if entry.is_dir():
                pending.append(entry)


def _classify_path(
    rel: Path,
    source_entry: Path | None = None,
) -> tuple[bool, str]:
    """Return (allowed, reason), inspecting the absolute source entry."""

    posix = rel.as_posix()
    basename = rel.name
    entry = source_entry if source_entry is not None else rel

    if entry.is_symlink():
        return False, "symlink or non-regular file"
    try:
        mode = entry.stat(follow_symlinks=False).st_mode
    except OSError as exc:
        return False, f"source entry cannot be inspected: {exc}"
    if not stat.S_ISREG(mode):
        return False, "symlink or non-regular file"

    if basename in FORBIDDEN_BASENAMES:
        return False, f"forbidden basename {basename!r}"

    for fragment in FORBIDDEN_PATH_FRAGMENTS:
        if fragment in posix.split("/"):
            return False, f"forbidden path fragment {fragment!r}"

    for suffix in FORBIDDEN_SUFFIXES:
        if basename.endswith(suffix):
            return False, f"forbidden suffix {suffix!r}"

    if posix in PUBLIC_ALLOWLIST:
        return True, ""

    if any(posix.startswith(prefix) for prefix in PUBLIC_CODE_PREFIXES):
        if rel.suffix == ".py":
            return True, ""
        return False, "public code prefixes permit Python source only"

    if posix.startswith(PUBLIC_TEST_PREFIX):
        if rel.suffix in {".py", ".json", ".xml", ".md", ".txt"}:
            return True, ""
        return False, "public tests permit source and text fixtures only"

    return False, "not in export allowlist"


def is_export_excluded(rel: Path) -> bool:
    """Return whether a candidate-local path is explicitly non-public."""

    posix = rel.as_posix()
    return (
        posix in EXPORT_EXCLUDED_EXACT
        or bool(set(rel.parts) & EXPORT_EXCLUDED_COMPONENTS)
        or rel.name.endswith(EXPORT_EXCLUDED_SUFFIXES)
    )


def _assignment_spans(text: str) -> dict[str, tuple[tuple[int, int], ...]]:
    """Return byte spans for named assignments in the scanner source."""

    try:
        tree = ast.parse(text)
    except SyntaxError:
        return {}
    offsets = [0]
    for line in text.splitlines(keepends=True):
        offsets.append(offsets[-1] + len(line))
    spans: dict[str, list[tuple[int, int]]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        names: list[str] = []
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        for target in targets:
            if isinstance(target, ast.Name):
                names.append(target.id)
        if not names or node.end_lineno is None or node.end_col_offset is None:
            continue
        start = offsets[node.lineno - 1] + node.col_offset
        end = offsets[node.end_lineno - 1] + node.end_col_offset
        for name in names:
            if name in _SCANNER_ALLOWANCE_ASSIGNMENTS:
                spans.setdefault(name, []).append((start, end))
    return {name: tuple(values) for name, values in spans.items()}


def _span_allowed(rel: str, category: str, text: str, start: int, end: int) -> bool:
    """Allow only a documented, exact structural/span exception."""

    if category == "CREDENTIAL_PATTERN":
        # Credential matches are never exempted by documentation or source
        # type.  Dedicated fixture basenames are handled by fixture_mode.
        return False

    if rel == "scripts/check_publication_safety.py":
        spans = _assignment_spans(text)
        return any(
            start >= allowed_start and end <= allowed_end
            for allowed in spans.values()
            for allowed_start, allowed_end in allowed
        ) and category in {"MAINTAINER_LITERAL", "PRIVATE_IP_LITERAL", "TAILNET_LITERAL"}

    line = text.count("\n", 0, start) + 1
    return any(
        first <= line <= last
        for first, last in CONTENT_SPAN_ALLOWANCES.get(rel, {}).get(category, ())
    )


def _scan_content(
    path: Path,
    text: str,
    fixture_mode: bool,
    documentation_mode: bool = False,
) -> list[Finding]:
    findings: list[Finding] = []
    rel = path.as_posix()

    # ``documentation_mode`` is retained for callers of the old helper API;
    # it no longer disables scanning.  Only exact spans below can be exempt.
    del documentation_mode

    for literal in MAINTAINER_LITERALS:
        start = 0
        while True:
            start = text.find(literal, start)
            if start < 0:
                break
            end = start + len(literal)
            if not _span_allowed(rel, "MAINTAINER_LITERAL", text, start, end):
                findings.append(
                    Finding(rel, "MAINTAINER_LITERAL", "maintainer literal found")
                )
            start = end

    for pattern in CONTENT_TOKEN_PATTERNS:
        for match in pattern.finditer(text):
            if not fixture_mode:
                findings.append(
                    Finding(rel, "CREDENTIAL_PATTERN", f"content matches {pattern.pattern!r}")
                )

    for pattern in PRIVATE_IPV4_PATTERNS:
        for match in pattern.finditer(text):
            if not _span_allowed(rel, "PRIVATE_IP_LITERAL", text, *match.span()):
                findings.append(
                    Finding(rel, "PRIVATE_IP_LITERAL", "private IPv4 literal found")
                )

    for literal in TAILNET_LITERALS:
        start = 0
        while True:
            start = text.find(literal, start)
            if start < 0:
                break
            end = start + len(literal)
            if not _span_allowed(rel, "TAILNET_LITERAL", text, start, end):
                findings.append(Finding(rel, "TAILNET_LITERAL", "blocked mesh literal found"))
            start = end

    for match in HOST_PORT_PATTERN.finditer(text):
        host = match.group("host")
        if host in EXAMPLE_HOSTS:
            continue
        if not _span_allowed(rel, "NON_PLACEHOLDER_HOST", text, *match.span()):
            findings.append(
                Finding(rel, "NON_PLACEHOLDER_HOST", "non-placeholder host found")
            )

    return findings


def _is_fixture(rel: Path) -> bool:
    return rel.name in ALLOWLISTED_FIXTURE_BASENAMES


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def scan(root: Path, *, strict: bool = True) -> tuple[int, list[Finding]]:
    """Scan ``root`` and report findings.

    Returns ``(exit_code, findings)``.
    """

    findings: list[Finding] = []
    root = Path(root)
    if root.is_symlink():
        findings.append(Finding(str(root), "ROOT_SYMLINK", "scan root must not be a symlink"))
        return 2, findings
    root = Path(os.path.abspath(root))

    if not root.exists():
        findings.append(Finding(str(root), "ROOT_MISSING", "export root does not exist"))
        return 2, findings
    if not root.is_dir():
        findings.append(Finding(str(root), "ROOT_NOT_DIRECTORY", "scan root is not a directory"))
        return 2, findings

    # Collect allowlisted paths.
    seen_allowlisted: set[str] = set()
    for entry in _iter_files(root):
        try:
            rel = entry.relative_to(root)
        except ValueError:
            continue
        if entry.is_dir() and not entry.is_symlink():
            continue
        allowed, reason = _classify_path(rel, source_entry=entry)
        if not allowed:
            findings.append(Finding(rel.as_posix(), "FORBIDDEN_ENTRY", reason))
            continue
        seen_allowlisted.add(rel.as_posix())

    # Required docs must all be present.
    for required in REQUIRED_DOCS:
        if required not in seen_allowlisted:
            findings.append(Finding(required, "REQUIRED_DOC_MISSING", "required doc absent"))

    # Content scan only for allowlisted entries.
    for rel_posix in sorted(seen_allowlisted):
        path = root / rel_posix
        rel_path = Path(rel_posix)
        fixture = _is_fixture(rel_path)
        try:
            data = path.read_bytes()
        except OSError as exc:
            findings.append(Finding(rel_posix, "READ_ERROR", str(exc)))
            continue
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            findings.append(
                Finding(rel_posix, "NON_UTF8", "allowlisted file is not UTF-8 text")
            )
            continue
        findings.extend(
            _scan_content(
                rel_path,
                text,
                fixture_mode=fixture,
            )
        )

    exit_code = 0 if not findings else 2
    return exit_code, findings


def _render(findings: Iterable[Finding]) -> str:
    return "\n".join(finding.render() for finding in findings)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Publication-safety scanner for the news core export."
    )
    parser.add_argument(
        "root",
        type=Path,
        help="Export root to scan (e.g. the candidate tree).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit findings as JSON.",
    )
    args = parser.parse_args(argv)

    exit_code, findings = scan(args.root)
    if args.json:
        json.dump(
            {
                "root": str(args.root),
                "exit_code": exit_code,
                "findings": [
                    {"path": f.path, "code": f.code, "detail": f.detail}
                    for f in findings
                ],
            },
            sys.stdout,
            indent=2,
            sort_keys=True,
        )
        sys.stdout.write("\n")
    else:
        if findings:
            sys.stdout.write(_render(findings) + "\n")
        sys.stdout.write(
            f"publication-safety: scanned {args.root} -- {'OK' if exit_code == 0 else 'REJECTED'}\n"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
