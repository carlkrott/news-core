from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .canonicalization import canonicalize_url, retain_original
from .models import ArticleObservation, Category, FileObservation, ParsedArticle, ParseResult

_ARTICLE_START = re.compile(r"^- \*\*(.+?)\*\*(?:\s.*)?$")
_URL_ONLY = re.compile(r"^https?://\S+$", re.IGNORECASE)
_FETCHED = re.compile(r"^_Fetched:\s*(.+?)_$", re.IGNORECASE)
_HEADER_DATE = re.compile(
    r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+"
    r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})\b"
)
_FILENAME_DATE = re.compile(r"(\d{4}-\d{2}-\d{2})(?:-(\d{2}))?")
_HTML_QUERY = re.compile(r"^<!--\s*query\s+(.+?)\s+failed:\s*(.*?)\s*-->$", re.IGNORECASE)
_HTML_QUERY_COMMENT = re.compile(r"^<!--\s*query\s*:?\s*(.*?)\s*-->$", re.IGNORECASE)
_CATEGORY_PREFIXES: tuple[tuple[str, Category], ...] = (
    ("audio-engineering-", Category.AUDIO_ENGINEERING),
    ("fantasy-novel-", Category.FANTASY_NOVEL),
    ("av-corporate-", Category.AV_CORPORATE),
    ("our-setup-", Category.OUR_SETUP),
    ("audiovisual-", Category.AUDIOVISUAL),
    ("hardware-", Category.HARDWARE),
    ("world-", Category.WORLD),
    ("ai-", Category.AI),
)


def _category_for(filename: str) -> Category:
    lowered = filename.lower()
    for prefix, category in _CATEGORY_PREFIXES:
        if lowered.startswith(prefix) and lowered.endswith(".md"):
            return category
    raise ValueError(f"unknown news filename category: {filename}")


def _observed_at(path: Path) -> str:
    match = _FILENAME_DATE.search(path.stem)
    if match:
        hour = int(match.group(2) or 0)
        local = datetime.fromisoformat(match.group(1)).replace(
            hour=hour, tzinfo=ZoneInfo("Europe/London")
        )
        dt = local.astimezone(UTC)
    else:
        dt = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def _strip_indentation(line: str) -> str | None:
    if line.startswith("\t"):
        return line[1:].strip()
    if line.startswith("  "):
        return line[2:].strip()
    return None


def _marker_for_line(markers_by_line: list[tuple[int, str]], idx: int) -> str | None:
    for line_idx, marker in markers_by_line:
        if line_idx >= idx:
            return marker
    return None


def parse_file(file_path: str) -> ParseResult:
    """Parse one legacy news markdown file without discarding later fetch sections.

    File-level HTML query observations that appear before the next ``_Fetched:``
    marker are associated with that following marker so honest fetch-section
    timing survives, while observations that follow no marker at all fall back
    to the filename-derived UTC timestamp.
    """
    path = Path(file_path)
    category = _category_for(path.name)
    observed_at = _observed_at(path)
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    markers_by_line: list[tuple[int, str]] = []
    for line_idx, line in enumerate(lines):
        match = _FETCHED.match(line.strip())
        if match:
            markers_by_line.append((line_idx, match.group(1).strip()))
    articles: list[ParsedArticle] = []
    header_date: str | None = None
    current_marker: str | None = None
    last_marker: str | None = None
    fetch_markers: list[str] = []
    section_start = 0
    current_title: str | None = None
    snippet_lines: list[str] = []
    original_url: str | None = None
    observations: list[ArticleObservation] = []
    file_observations: list[FileObservation] = []

    def flush() -> None:
        nonlocal current_title, snippet_lines, original_url, observations
        if current_title is None:
            return
        canonical_url: str | None = None
        if original_url:
            try:
                canonical_url = canonicalize_url(original_url)
            except ValueError:
                canonical_url = None
        articles.append(
            ParsedArticle(
                title=current_title,
                category=category,
                source_file=path.name,
                observed_at=current_marker or observed_at,
                snippet="\n".join(snippet_lines).strip(),
                original_url=retain_original(original_url),
                canonical_url=canonical_url,
                fetch_marker=current_marker,
                observations=list(observations),
            )
        )
        current_title = None
        snippet_lines = []
        original_url = None
        observations = []

    def assign_marker(marker: str, start: int) -> None:
        for article in articles[start:]:
            article.fetch_marker = marker
            article.observed_at = marker

    for line_idx, line in enumerate(lines):
        if header_date is None and line.startswith("# "):
            match = _HEADER_DATE.search(line)
            if match:
                header_date = match.group(1)
        stripped = line.strip()
        html_failure = _HTML_QUERY.match(stripped)
        if html_failure:
            query, error = html_failure.groups()
            # File-level observations prefer the next marker (so they reflect
            # real section timing), then the previously-seen marker as a
            # fallback, then the filename-derived timestamp.
            file_marker = _marker_for_line(markers_by_line, line_idx) or last_marker
            file_observations.append(
                FileObservation(
                    "query_failure",
                    f"{query}: {error}",
                    file_marker or observed_at,
                    stripped,
                )
            )
            continue
        html_comment = _HTML_QUERY_COMMENT.match(stripped)
        if html_comment:
            file_marker = _marker_for_line(markers_by_line, line_idx) or last_marker
            file_observations.append(
                FileObservation(
                    "query_comment",
                    html_comment.group(1),
                    file_marker or observed_at,
                    stripped,
                )
            )
            continue
        fetched = _FETCHED.match(stripped)
        if fetched:
            flush()
            marker = fetched.group(1).strip()
            fetch_markers.append(marker)
            assign_marker(marker, section_start)
            section_start = len(articles)
            current_marker = marker
            last_marker = marker
            file_observations.append(FileObservation("fetch_marker", marker, marker, stripped))
            continue
        start = _ARTICLE_START.match(line)
        if start:
            flush()
            current_title = start.group(1).strip()
            continue
        if line.startswith("## "):
            flush()
            continue
        if current_title is None:
            continue
        content = _strip_indentation(line)
        if content is None:
            if line.strip():
                flush()
            continue
        if not content:
            continue
        lowered = content.lower()
        if lowered.startswith("> query failed:"):
            observations.append(
                ArticleObservation("query_failure", content.split(":", 1)[1].strip(), raw=line)
            )
        elif lowered.startswith("> query:"):
            observations.append(
                ArticleObservation("query_comment", content.split(":", 1)[1].strip(), raw=line)
            )
        elif _URL_ONLY.fullmatch(content):
            original_url = content
        else:
            snippet_lines.append(content)
    flush()
    if fetch_markers:
        assign_marker(fetch_markers[-1], section_start)
    return ParseResult(
        source_file=path.name,
        category=category,
        observed_at=observed_at,
        articles=articles,
        fetch_marker=fetch_markers[-1] if fetch_markers else None,
        fetch_markers=fetch_markers,
        header_date=header_date,
        observations=file_observations,
    )
