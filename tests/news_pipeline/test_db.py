from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import TABLES, connect, get_counts, init_db


class DbTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "state.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_migrations_are_idempotent(self) -> None:
        init_db(self.db_path)
        init_db(self.db_path)
        with connect(self.db_path) as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM schema_migrations").fetchone()[0],
                2,
            )
            names = {
                row[0]
                for row in con.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertTrue(set(TABLES).issubset(names))

    def test_connection_pragmas_are_enforced(self) -> None:
        init_db(self.db_path)
        with connect(self.db_path) as con:
            self.assertEqual(con.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertEqual(con.execute("PRAGMA foreign_keys").fetchone()[0], 1)
            self.assertEqual(con.execute("PRAGMA busy_timeout").fetchone()[0], 10000)

    def test_connection_context_manager_closes_connection(self) -> None:
        init_db(self.db_path)
        con = connect(self.db_path)
        with con:
            self.assertEqual(con.execute("SELECT 1").fetchone()[0], 1)
        with self.assertRaises(sqlite3.ProgrammingError):
            con.execute("SELECT 1")

    def test_foreign_keys_and_checks_are_enforced(self) -> None:
        init_db(self.db_path)
        with connect(self.db_path) as con:
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute(
                    "INSERT INTO articles "
                    "(id,run_id,category,title,source_file,provenance,created_at) "
                    "VALUES ('a','missing','ai','t','f','observed_historical','now')"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute(
                    "INSERT INTO runs "
                    "(id,started_at,kind,provenance) "
                    "VALUES ('r','now','invalid','manual')"
                )

    def test_fingerprint_indexes_exist(self) -> None:
        init_db(self.db_path)
        with connect(self.db_path) as con:
            indexes = {
                row[1] for row in con.execute("PRAGMA index_list(fact_fingerprints)")
            }
        self.assertIn("idx_fact_fingerprints_value", indexes)
        self.assertIn("idx_fact_fingerprints_kind", indexes)

    def test_get_counts_has_every_table(self) -> None:
        init_db(self.db_path)
        counts = get_counts(self.db_path)
        self.assertEqual(set(counts), set(TABLES))
        self.assertEqual(counts["schema_migrations"], 2)
        for table, count in counts.items():
            if table != "schema_migrations":
                self.assertEqual(count, 0, table)

    def test_v2_schema_includes_identity_columns_and_indexes_on_fresh_db(self) -> None:
        init_db(self.db_path)
        with connect(self.db_path) as con:
            article_cols = {row[1] for row in con.execute("PRAGMA table_info(articles)")}
            for col in ("normalized_title", "identity_confidence", "identity_basis"):
                self.assertIn(col, article_cols, f"missing articles column: {col}")
            obs_cols = {row[1] for row in con.execute("PRAGMA table_info(observations)")}
            for col in ("event_id", "category", "source_file", "raw"):
                self.assertIn(col, obs_cols, f"missing observations column: {col}")
            obs_indexes = {row[1] for row in con.execute("PRAGMA index_list(observations)")}
            for index in (
                "idx_observations_article",
                "idx_observations_event",
                "idx_observations_kind",
            ):
                self.assertIn(index, obs_indexes, f"missing observations index: {index}")
            article_indexes = {row[1] for row in con.execute("PRAGMA index_list(articles)")}
            self.assertIn("idx_articles_identity", article_indexes)

    def test_fresh_v2_db_enforces_identity_basis_and_confidence_constraints(self) -> None:
        """A fresh v2 DB must reject invalid identity_basis / out-of-range
        identity_confidence values via CHECK constraints."""
        init_db(self.db_path)
        with connect(self.db_path) as con:
            # need a run for the FK
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) "
                "VALUES ('r1','2026-01-01T00:00:00Z','historical_replay','observed_historical')"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute(
                    "INSERT INTO articles "
                    "(id,run_id,category,title,source_file,provenance,created_at,"
                    " normalized_title,identity_confidence,identity_basis) "
                    "VALUES ('a1','r1','ai','t','f','observed_historical','now',"
                    "'t',0.0,'NOPE')"
                )
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute(
                    "INSERT INTO articles "
                    "(id,run_id,category,title,source_file,provenance,created_at,"
                    " normalized_title,identity_confidence,identity_basis) "
                    "VALUES ('a1','r1','ai','t','f','observed_historical','now',"
                    "'t',1.5,'canonical_url')"
                )

    def test_observation_kind_check_rejects_unknown_kind(self) -> None:
        init_db(self.db_path)
        with connect(self.db_path) as con:
            con.execute(
                "INSERT INTO runs(id,started_at,kind,provenance) "
                "VALUES ('r2','2026-01-01T00:00:00Z','historical_replay','observed_historical')"
            )
            con.execute(
                "INSERT INTO articles(id,run_id,category,title,source_file,provenance,created_at) "
                "VALUES ('a2','r2','ai','t','f','observed_historical','now')"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute(
                    "INSERT INTO observations "
                    "(id,article_id,category,source_file,kind,body,created_at) "
                    "VALUES ('o2','a2','ai','f','wrong_kind','x','now')"
                )


class DbMigrationTests(unittest.TestCase):
    """Direct ALTER-based v1 → v2 migration tests.

    The Phase 1 deployment always rebuilds the live DB from scratch, so the
    ALTER migration path is not exercised against production data — but it
    must remain a safe, idempotent upgrade for any pre-existing v1 DB so the
    schema can evolve without manual surgery.
    """

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tempdir.name) / "state.db")

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _seed_v1(self) -> None:
        with connect(self.db_path) as con:
            con.executescript(
                """
                CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
                INSERT INTO schema_migrations(version, applied_at) VALUES (1, '2026-01-01T00:00:00Z');
                CREATE TABLE runs(
                    id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
                    kind TEXT NOT NULL, provenance TEXT NOT NULL,
                    source_dir TEXT, notes TEXT);
                CREATE TABLE articles(
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
                    category TEXT NOT NULL, canonical_url TEXT, original_url TEXT,
                    title TEXT NOT NULL, snippet TEXT, source_file TEXT NOT NULL,
                    observed_at TEXT, fetch_marker TEXT, provenance TEXT NOT NULL,
                    created_at TEXT NOT NULL);
                CREATE TABLE observations(
                    id TEXT PRIMARY KEY,
                    article_id TEXT NOT NULL REFERENCES articles(id),
                    kind TEXT NOT NULL, body TEXT, occurred_at TEXT, created_at TEXT NOT NULL);
                CREATE TABLE events(
                    id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
                    category TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT,
                    article_count INTEGER NOT NULL DEFAULT 0,
                    observation_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL);
                CREATE TABLE event_articles(
                    event_id TEXT NOT NULL REFERENCES events(id),
                    article_id TEXT NOT NULL REFERENCES articles(id),
                    PRIMARY KEY(event_id, article_id));
                INSERT INTO runs VALUES (
                    'r-seed','2026-01-01T00:00:00Z',NULL,'historical_replay',
                    'observed_historical',NULL,NULL);
                INSERT INTO articles(id,run_id,category,title,source_file,provenance,created_at)
                    VALUES ('art-1','r-seed','ai','Title One','ai-2026-01-01.md',
                            'observed_historical','2026-01-01T00:00:00Z');
                INSERT INTO observations(id,article_id,kind,body,occurred_at,created_at)
                    VALUES ('obs-1','art-1','query_failure','timeout',
                            '2026-01-01T00:00:00Z','2026-01-01T00:00:00Z');
                """
            )

    def test_v1_to_v2_preserves_rows_and_is_idempotent(self) -> None:
        self._seed_v1()
        init_db(self.db_path)
        # Second invocation must be a no-op (idempotent).
        init_db(self.db_path)
        with connect(self.db_path) as con:
            versions = sorted(
                row[0] for row in con.execute("SELECT version FROM schema_migrations")
            )
            self.assertEqual(versions, [1, 2])
            runs = con.execute("SELECT id FROM runs").fetchall()
            self.assertEqual([r[0] for r in runs], ["r-seed"])
            articles = con.execute(
                "SELECT id, category, title FROM articles"
            ).fetchall()
            self.assertEqual(articles, [("art-1", "ai", "Title One")])
            # v2 columns present and populated
            self.assertEqual(
                con.execute(
                    "SELECT normalized_title, identity_confidence, identity_basis "
                    "FROM articles WHERE id='art-1'"
                ).fetchone(),
                ("title one", 0.0, "legacy"),
            )
            # v2 observation columns: category, source_file, raw, event_id
            row = con.execute(
                "SELECT category, source_file, raw, event_id FROM observations WHERE id='obs-1'"
            ).fetchone()
            self.assertEqual(row[0], "ai")
            self.assertEqual(row[1], "ai-2026-01-01.md")
            self.assertIsNone(row[2])
            self.assertIsNone(row[3])
            # Foreign key / integrity checks clean
            fk_violations = con.execute("PRAGMA foreign_key_check").fetchall()
            self.assertEqual(fk_violations, [])
            integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
            self.assertEqual(integrity, "ok")
            # v2 indexes exist
            idx_obs = {row[1] for row in con.execute("PRAGMA index_list(observations)")}
            self.assertIn("idx_observations_article", idx_obs)
            self.assertIn("idx_observations_event", idx_obs)
            self.assertIn("idx_observations_kind", idx_obs)
            idx_art = {row[1] for row in con.execute("PRAGMA index_list(articles)")}
            self.assertIn("idx_articles_identity", idx_art)

    def test_v1_to_v2_rejects_bad_observation_kind_after_upgrade(self) -> None:
        self._seed_v1()
        init_db(self.db_path)
        with connect(self.db_path) as con:
            # After upgrade, observations table has CHECK(kind IN ...)
            with self.assertRaises(sqlite3.IntegrityError):
                con.execute(
                    "INSERT INTO observations "
                    "(id,article_id,category,source_file,kind,body,created_at) "
                    "VALUES ('bad','art-1','ai','ai-2026-01-01.md','wrong_kind','x','now')"
                )


if __name__ == "__main__":
    unittest.main()
