"""Phase 2 — read-only history reader over the Phase 1 ``news-state.db``.

Strict invariants:
- ``sqlite3.connect(..., uri=True, mode=ro)`` — the live DB can never be written to.
- ``PRAGMA query_only=ON`` — even a stray DDL/DML would be rejected by SQLite.
- ``PRAGMA foreign_keys=ON`` — parity with the live DB.
- Bounded ``busy_timeout`` — never block forever waiting for a writer.
- Never call ``init_db`` — the live DB already exists.
- Never create parent directories.

Any ``sqlite3.Error`` (locked/missing/corrupt/schema mismatch) is wrapped in
``HistoryUnavailable`` so the engine can emit ``PENDING_HISTORY_UNAVAILABLE``
without suppressing candidates. We deliberately do **not** catch non-SQLite
errors: those are programmer mistakes and must surface.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from .contracts import HistoryMatch
from .models import Category


BUSY_TIMEOUT_MS = 5000


class HistoryUnavailable(Exception):
    """Raised when the read-only history DB cannot serve queries.

    Catching it explicitly is mandatory in every call site that needs history.
    """


@dataclass(frozen=True, slots=True)
class _HistoryConnection:
    """A read-only wrapper returned by :func:`open_history`."""

    path: str
    con: sqlite3.Connection
    has_event_versions: bool = False

    def __enter__(self) -> "_HistoryConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self.con.close()
        except sqlite3.Error:
            # Closing a closed connection is fine; ignore.
            pass

    def close(self) -> None:
        try:
            self.con.close()
        except sqlite3.Error:
            pass


def _resolve_uri(db_path: str) -> str:
    """Build a ``file:...?mode=ro`` URI from an absolute filesystem path."""
    return Path(db_path).resolve().as_uri() + "?mode=ro"


def open_history(db_path: str, *, busy_timeout_ms: int = BUSY_TIMEOUT_MS) -> _HistoryConnection:
    """Open a read-only connection to ``db_path``.

    The DB must already exist — we refuse to create a new file. ``busy_timeout_ms``
    is bounded by ``BUSY_TIMEOUT_MS`` so a long-running writer cannot stall the
    engine.
    """
    busy_timeout_ms = max(1, min(int(busy_timeout_ms), BUSY_TIMEOUT_MS))
    if not db_path:
        raise HistoryUnavailable("history: db_path is empty")
    p = Path(db_path)
    if not p.exists():
        raise HistoryUnavailable(f"history: database does not exist: {db_path}")
    try:
        uri = _resolve_uri(db_path)
        con = sqlite3.connect(uri, uri=True, timeout=busy_timeout_ms / 1000.0)
    except sqlite3.Error as exc:
        raise HistoryUnavailable(f"history: open failed: {exc}") from exc
    try:
        con.execute("PRAGMA query_only = ON")
        con.execute("PRAGMA foreign_keys = ON")
        con.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        # Detect corrupt/incompatible DBs explicitly. A fresh in-memory DB
        # opens fine against an arbitrary file but will fail any meaningful
        # query, so we probe the Phase 1 schema.
        try:
            con.execute("SELECT 1 FROM schema_migrations LIMIT 1").fetchone()
            con.execute("SELECT 1 FROM articles LIMIT 1").fetchone()
            con.execute("SELECT 1 FROM observations LIMIT 1").fetchone()
        except sqlite3.Error as exc:
            raise HistoryUnavailable(f"history: schema probe failed: {exc}") from exc
    except HistoryUnavailable:
        try:
            con.close()
        except sqlite3.Error:
            pass
        raise
    except sqlite3.Error as exc:
        try:
            con.close()
        except sqlite3.Error:
            pass
        raise HistoryUnavailable(f"history: pragma setup failed: {exc}") from exc
    has_event_versions = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_versions'"
    ).fetchone() is not None
    return _HistoryConnection(path=db_path, con=con, has_event_versions=has_event_versions)


def _parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def _format_upper_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _history_match(history: _HistoryConnection, row: Sequence[Any]) -> HistoryMatch:
    """Build a HistoryMatch while preserving optional durable event identity."""
    event_id = row[8] if len(row) > 8 else None
    event_version = None
    if event_id and history.has_event_versions:
        try:
            version_row = history.con.execute(
                "SELECT MAX(version) FROM event_versions WHERE event_id=?",
                (event_id,),
            ).fetchone()
        except sqlite3.Error as exc:
            raise HistoryUnavailable(f"history: event version lookup failed: {exc}") from exc
        event_version = version_row[0] if version_row and version_row[0] is not None else 1
    return HistoryMatch(
        article_id=row[0],
        observation_id=row[1],
        category=Category(row[2]),
        occurred_at=row[3],
        title=row[4] or "",
        snippet=row[5] or "",
        canonical_url=row[6],
        identity_basis=row[7] or "legacy",
        event_id=event_id,
        event_version=event_version,
    )


# ---------------------------------------------------------------------------
# Exact URL
# ---------------------------------------------------------------------------


def find_exact_url(
    history: _HistoryConnection,
    canonical_url: str,
    evaluated_at: str,
    lookback: timedelta,
    *,
    cross_category: bool = True,
    category: str | None = None,
) -> HistoryMatch | None:
    """Find the most recent parsed_article observation matching ``canonical_url``.

    ``cross_category=True`` (default) searches all categories; the engine only
    sets this to ``False`` when a category-scoped query is desired. The lookback
    window is inclusive of both endpoints ``[evaluated_at - lookback, evaluated_at]``.
    """
    evaluated_dt = _parse_iso(evaluated_at)
    upper_iso = _format_upper_iso(evaluated_dt)
    lower_iso = _format_upper_iso(evaluated_dt - lookback)

    if cross_category:
        sql = (
            "SELECT a.id, o.id, o.category, o.occurred_at, a.title, a.snippet, "
            "a.canonical_url, a.identity_basis, o.event_id "
            "FROM articles a JOIN observations o "
            "ON o.article_id = a.id AND o.kind = 'parsed_article' "
            "WHERE a.canonical_url = ? "
            "AND o.occurred_at >= ? AND o.occurred_at <= ? "
            "ORDER BY o.occurred_at DESC, o.id DESC LIMIT 1"
        )
        params: tuple = (canonical_url, lower_iso, upper_iso)
    else:
        if category is None:
            raise HistoryUnavailable("history: category required for non-cross-category URL lookup")
        sql = (
            "SELECT a.id, o.id, o.category, o.occurred_at, a.title, a.snippet, "
            "a.canonical_url, a.identity_basis, o.event_id "
            "FROM articles a JOIN observations o "
            "ON o.article_id = a.id AND o.kind = 'parsed_article' "
            "WHERE a.canonical_url = ? AND o.category = ? "
            "AND o.occurred_at >= ? AND o.occurred_at <= ? "
            "ORDER BY o.occurred_at DESC, o.id DESC LIMIT 1"
        )
        params = (canonical_url, category, lower_iso, upper_iso)
    try:
        row = history.con.execute(sql, params).fetchone()
    except sqlite3.Error as exc:
        raise HistoryUnavailable(f"history: find_exact_url failed: {exc}") from exc
    if row is None:
        return None
    return _history_match(history, row)


# ---------------------------------------------------------------------------
# Exact URL-less identity
# ---------------------------------------------------------------------------


def find_exact_identity(
    history: _HistoryConnection,
    identity: str,
    category: str,
    evaluated_at: str,
    lookback: timedelta,
) -> HistoryMatch | None:
    """Find the latest parsed_article observation with the same Phase 1 identity.

    Identity is the 32-char ``article_id`` produced by ``news_pipeline.db.article_id``
    and is always category-scoped.
    """
    evaluated_dt = _parse_iso(evaluated_at)
    upper_iso = _format_upper_iso(evaluated_dt)
    lower_iso = _format_upper_iso(evaluated_dt - lookback)
    sql = (
        "SELECT a.id, o.id, o.category, o.occurred_at, a.title, a.snippet, "
        "a.canonical_url, a.identity_basis, o.event_id "
        "FROM articles a JOIN observations o "
        "ON o.article_id = a.id AND o.kind = 'parsed_article' "
        "WHERE a.id = ? AND o.category = ? "
        "AND o.occurred_at >= ? AND o.occurred_at <= ? "
        "ORDER BY o.occurred_at DESC, o.id DESC LIMIT 1"
    )
    try:
        row = history.con.execute(
            sql, (identity, category, lower_iso, upper_iso)
        ).fetchone()
    except sqlite3.Error as exc:
        raise HistoryUnavailable(f"history: find_exact_identity failed: {exc}") from exc
    if row is None:
        return None
    return _history_match(history, row)


# ---------------------------------------------------------------------------
# Exact normalized title (category-scoped, last-write-wins)
# ---------------------------------------------------------------------------


def find_exact_title(
    history: _HistoryConnection,
    normalized_title: str,
    category: str,
    evaluated_at: str,
    lookback: timedelta,
) -> HistoryMatch | None:
    """Find the latest parsed_article observation whose normalized title matches.

    Always category-scoped. Title-only evidence is treated as low confidence
    upstream; the engine still surfaces matches here, but suppresses only when
    other fields (snippet/canonical URL) line up.
    """
    evaluated_dt = _parse_iso(evaluated_at)
    upper_iso = _format_upper_iso(evaluated_dt)
    lower_iso = _format_upper_iso(evaluated_dt - lookback)
    sql = (
        "SELECT a.id, o.id, o.category, o.occurred_at, a.title, a.snippet, "
        "a.canonical_url, a.identity_basis, o.event_id "
        "FROM articles a JOIN observations o "
        "ON o.article_id = a.id AND o.kind = 'parsed_article' "
        "WHERE lower(trim(a.normalized_title)) = ? AND o.category = ? "
        "AND o.occurred_at >= ? AND o.occurred_at <= ? "
        "ORDER BY o.occurred_at DESC, o.id DESC LIMIT 1"
    )
    try:
        row = history.con.execute(
            sql, (normalized_title, category, lower_iso, upper_iso)
        ).fetchone()
    except sqlite3.Error as exc:
        raise HistoryUnavailable(f"history: find_exact_title failed: {exc}") from exc
    if row is None:
        return None
    return _history_match(history, row)


def fetch_history_match(
    history: _HistoryConnection,
    article_id: str,
    observation_id: str,
) -> HistoryMatch | None:
    """Fetch a fully-resolved HistoryMatch by its primary keys.

    Used by the engine to resolve the full title/snippet/canonical_url of any
    candidate it has already learned about. Strict read-only.
    """
    sql = (
        "SELECT a.id, o.id, o.category, o.occurred_at, a.title, a.snippet, "
        "a.canonical_url, a.identity_basis, o.event_id "
        "FROM articles a JOIN observations o "
        "ON o.article_id = a.id "
        "WHERE a.id = ? AND o.id = ? AND o.kind = 'parsed_article' "
        "LIMIT 1"
    )
    try:
        row = history.con.execute(sql, (article_id, observation_id)).fetchone()
    except sqlite3.Error as exc:
        raise HistoryUnavailable(f"history: fetch_history_match failed: {exc}") from exc
    if row is None:
        return None
    return _history_match(history, row)
