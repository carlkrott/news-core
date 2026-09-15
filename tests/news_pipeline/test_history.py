"""Tests for the read-only Phase 1 history reader.

These tests build a fixture DB in a temp dir using the Phase 1 schema (no schema v3).
We never touch the live news-state.db.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from news_pipeline.db import connect, init_db
from news_pipeline.history import (
    HistoryUnavailable,
    find_exact_identity,
    find_exact_title,
    find_exact_url,
    open_history,
)


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


class FixtureDb(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "state.db")
        init_db(self.db_path)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def _seed_run(self, con: sqlite3.Connection, run_id: str = "r1") -> None:
        con.execute(
            "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
            (run_id, "2026-07-01T00:00:00Z", "historical_replay", "observed_historical"),
        )

    def _seed_article(
        self,
        con: sqlite3.Connection,
        *,
        article_id: str,
        run_id: str,
        category: str,
        title: str,
        snippet: str,
        canonical_url: str | None,
        source_file: str = "ai-2026-07-01.md",
        normalized_title: str | None = None,
        identity_basis: str = "canonical_url",
        identity_confidence: float = 1.0,
    ) -> None:
        con.execute(
            "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,"
            "snippet,source_file,observed_at,fetch_marker,provenance,created_at,"
            "normalized_title,identity_confidence,identity_basis) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                article_id,
                run_id,
                category,
                canonical_url,
                canonical_url,
                title,
                snippet,
                source_file,
                "2026-07-01T00:00:00Z",
                None,
                "observed_historical",
                "2026-07-01T00:00:00Z",
                normalized_title if normalized_title is not None else title.lower(),
                identity_confidence,
                identity_basis,
            ),
        )

    def _seed_observation(
        self,
        con: sqlite3.Connection,
        *,
        observation_id: str,
        article_id: str,
        category: str,
        source_file: str,
        occurred_at: str,
        body: str | None = None,
    ) -> None:
        con.execute(
            "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (
                observation_id,
                article_id,
                category,
                source_file,
                "parsed_article",
                body,
                occurred_at,
                occurred_at,
            ),
        )


class OpenHistoryTests(FixtureDb):
    def test_missing_db_raises_history_unavailable(self):
        bogus = str(Path(self.tmp.name) / "missing.db")
        with self.assertRaises(HistoryUnavailable):
            with open_history(bogus):
                pass

    def test_query_only_is_set(self):
        with open_history(self.db_path) as h:
            self.assertEqual(h.con.execute("PRAGMA query_only").fetchone()[0], 1)

    def test_foreign_keys_is_set(self):
        with open_history(self.db_path) as h:
            self.assertEqual(h.con.execute("PRAGMA foreign_keys").fetchone()[0], 1)

    def test_does_not_create_db(self):
        bogus = str(Path(self.tmp.name) / "will_not_be_created.db")
        with self.assertRaises(HistoryUnavailable):
            with open_history(bogus):
                pass
        self.assertFalse(Path(bogus).exists())

    def test_context_manager_closes(self):
        h = open_history(self.db_path)
        with h:
            self.assertEqual(h.con.execute("SELECT 1").fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            h.con.execute("SELECT 1")

    def test_init_db_is_never_called(self):
        # If init_db were called, a missing DB would be created. We rely on
        # HistoryUnavailable being raised to enforce that.
        with self.assertRaises(HistoryUnavailable):
            with open_history("/nonexistent/foo.db"):
                pass


class FindExactUrlTests(FixtureDb):
    def setUp(self) -> None:
        super().setUp()
        with connect(self.db_path) as con:
            self._seed_run(con, "r1")
            self._seed_article(
                con,
                article_id="a1",
                run_id="r1",
                category="ai",
                title="Latest AI news",
                snippet="snippet",
                canonical_url="https://example.com/news/1",
            )
            self._seed_observation(
                con,
                observation_id="o1",
                article_id="a1",
                category="ai",
                source_file="ai-2026-07-01.md",
                occurred_at="2026-07-10T00:00:00Z",
            )

    def test_returns_match_within_lookback(self):
        with open_history(self.db_path) as h:
            m = find_exact_url(h, "https://example.com/news/1", "2026-07-14T00:00:00Z", timedelta(days=7))
        self.assertIsNotNone(m)
        self.assertEqual(m.article_id, "a1")
        self.assertEqual(m.observation_id, "o1")

    def test_returns_none_outside_lookback(self):
        with open_history(self.db_path) as h:
            m = find_exact_url(h, "https://example.com/news/1", "2026-08-01T00:00:00Z", timedelta(days=7))
        self.assertIsNone(m)

    def test_returns_none_for_different_url(self):
        with open_history(self.db_path) as h:
            m = find_exact_url(h, "https://example.com/news/2", "2026-07-14T00:00:00Z", timedelta(days=7))
        self.assertIsNone(m)


class FindExactIdentityTests(FixtureDb):
    def setUp(self) -> None:
        super().setUp()
        with connect(self.db_path) as con:
            self._seed_run(con, "r1")
            self._seed_article(
                con,
                article_id="a1",
                run_id="r1",
                category="ai",
                title="Hello world",
                snippet="specific-snippet",
                canonical_url=None,
                identity_basis="title_snippet",
                identity_confidence=0.75,
                normalized_title="hello world",
                source_file="ai-2026-07-01.md",
            )
            self._seed_observation(
                con,
                observation_id="o1",
                article_id="a1",
                category="ai",
                source_file="ai-2026-07-01.md",
                occurred_at="2026-07-10T00:00:00Z",
            )

    def test_url_less_identity_match(self):
        from news_pipeline.db import article_id

        # The setUp already seeded ``r1`` + ``a1`` + an observation. Rewrite
        # both the observation's article_id and the article's id to match the
        # Phase 1 article_id() formula for our title+snippet.
        ident = article_id(None, "hello world", "specific-snippet", "ai", "ai-2026-07-01.md")
        with connect(self.db_path) as con:
            # Disable FKs temporarily so we can swap article id atomically.
            con.execute("PRAGMA foreign_keys = OFF")
            con.execute("BEGIN")
            con.execute("DELETE FROM observations WHERE article_id = 'a1'")
            con.execute("UPDATE articles SET id=?, normalized_title=?, identity_basis='title_snippet', identity_confidence=0.75 WHERE id='a1'",
                        (ident, "hello world"))
            con.execute(
                "INSERT INTO observations(id,article_id,category,source_file,kind,body,occurred_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    "obs-a1",
                    ident,
                    "ai",
                    "ai-2026-07-01.md",
                    "parsed_article",
                    "specific-snippet",
                    "2026-07-10T00:00:00Z",
                    "2026-07-10T00:00:00Z",
                ),
            )
            con.execute("COMMIT")
            con.execute("PRAGMA foreign_keys = ON")
        with open_history(self.db_path) as h:
            m = find_exact_identity(h, ident, "ai", "2026-07-14T00:00:00Z", timedelta(days=7))
        self.assertIsNotNone(m)
        self.assertEqual(m.article_id, ident)

    def test_url_less_identity_misses_different_category(self):
        from news_pipeline.db import article_id

        ident = article_id(None, "hello world", "specific-snippet", "ai", "ai-2026-07-01.md")
        with open_history(self.db_path) as h:
            m = find_exact_identity(h, ident, "world", "2026-07-14T00:00:00Z", timedelta(days=7))
        self.assertIsNone(m)


class FindExactTitleTests(FixtureDb):
    def setUp(self) -> None:
        super().setUp()
        with connect(self.db_path) as con:
            self._seed_run(con, "r1")
            self._seed_article(
                con,
                article_id="a1",
                run_id="r1",
                category="ai",
                title="Hello world",
                snippet="snippet-1",
                canonical_url="https://example.com/a",
                normalized_title="hello world",
            )
            self._seed_observation(
                con,
                observation_id="o1",
                article_id="a1",
                category="ai",
                source_file="ai-2026-07-01.md",
                occurred_at="2026-07-13T23:00:00Z",
            )
            self._seed_article(
                con,
                article_id="a2",
                run_id="r1",
                category="world",
                title="Hello world",
                snippet="snippet-1",
                canonical_url="https://example.com/b",
                normalized_title="hello world",
            )
            self._seed_observation(
                con,
                observation_id="o2",
                article_id="a2",
                category="world",
                source_file="world-2026-07-01.md",
                occurred_at="2026-07-13T22:00:00Z",
            )

    def test_title_match_category_scoped(self):
        with open_history(self.db_path) as h:
            m = find_exact_title(h, "hello world", "ai", "2026-07-14T00:00:00Z", timedelta(hours=72))
        self.assertIsNotNone(m)
        self.assertEqual(m.article_id, "a1")

    def test_title_match_misses_when_title_changed(self):
        with open_history(self.db_path) as h:
            m = find_exact_title(h, "goodbye world", "ai", "2026-07-14T00:00:00Z", timedelta(hours=72))
        self.assertIsNone(m)


class UnavailableTests(unittest.TestCase):
    def test_corrupt_db_raises_unavailable(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            bad = Path(tmp.name) / "bad.db"
            # Write a clearly bogus SQLite header (magic "SQLite" but invalid format 7
            # pagesize + corrupt schema). SQLite will reject this at open time.
            bad.write_bytes(b"SQLite format 7\x00" + b"\x00" * 4096)
            with self.assertRaises(HistoryUnavailable):
                with open_history(str(bad)):
                    pass
        finally:
            tmp.cleanup()

    def test_path_under_nonexistent_directory_raises_unavailable(self):
        tmp = tempfile.TemporaryDirectory()
        try:
            bogus = str(Path(tmp.name) / "missing" / "nested" / "state.db")
            with self.assertRaises(HistoryUnavailable):
                with open_history(bogus):
                    pass
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
