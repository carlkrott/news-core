"""Tests for schema_v4 fetch-state migration."""
from __future__ import annotations

import ast
import hashlib
import os
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import SCHEMA_VERSION, V4_COLUMNS, V4_TABLES, migrate_v4

ROOT = Path(__file__).resolve().parents[2]
SEALED_BACKUP = Path(os.environ.get(
    "NEWS_PIPELINE_PHASE0_DB",
    str(ROOT / "tests" / "news_pipeline" / "fixtures" / "sealed-phase0.db"),
))
SEALED_SHA256 = "aee70c657c2958adc578033d3fd353216c77e93e0fb57b711c0bbf2107a32772"
APPLIED_AT_V3 = "2026-09-06T22:00:00Z"
APPLIED_AT_V4 = "2026-09-06T23:00:00Z"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class MigrationFixture(unittest.TestCase):
    def setUp(self) -> None:
        if not SEALED_BACKUP.exists():
            self.skipTest(
                "sealed phase-0 fixture not staged "
                "(set NEWS_PIPELINE_PHASE0_DB or add tests/news_pipeline/fixtures/sealed-phase0.db)"
            )
        self.assertEqual(sha256(SEALED_BACKUP), SEALED_SHA256)
        self.temp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp.name) / "copy.db"
        shutil.copyfile(SEALED_BACKUP, self.db_path)

    def tearDown(self) -> None:
        if not hasattr(self, "temp"):
            return
        self.temp.cleanup()
        if SEALED_BACKUP.exists():
            self.assertEqual(sha256(SEALED_BACKUP), SEALED_SHA256)

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA foreign_keys=ON")
        return connection


class CopiedProductionMigrationTests(MigrationFixture):
    def test_migrates_v4_preserving_v3_state(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            self.assertTrue(migrate_v4(connection, APPLIED_AT_V4))
            # v3 tables still present and untouched
            self.assertEqual(
                connection.execute("SELECT applied_at FROM schema_migrations WHERE version=3").fetchone()[0],
                APPLIED_AT_V3,
            )
            self.assertEqual(
                connection.execute("SELECT applied_at FROM schema_migrations WHERE version=4").fetchone()[0],
                APPLIED_AT_V4,
            )
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchone()[0],
                "ok",
            )
            self.assertEqual(
                connection.execute("PRAGMA foreign_key_check").fetchall(),
                [],
            )

    def test_fetch_state_table_and_exact_columns_exist(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            migrate_v4(connection, APPLIED_AT_V4)
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("fetch_state", tables)
            for table, expected in V4_COLUMNS.items():
                actual = tuple(
                    row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')
                )
                self.assertEqual(actual, expected, table)

    def test_second_migration_is_a_byte_stable_noop(self) -> None:
        connection = self.connect()
        try:
            migrate_v3(connection, APPLIED_AT_V3)
            migrate_v4(connection, APPLIED_AT_V4)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            connection.close()
        first_hash = sha256(self.db_path)
        connection = self.connect()
        try:
            self.assertFalse(migrate_v4(connection, "2026-09-07T00:00:00Z"))
        finally:
            connection.close()
        self.assertEqual(sha256(self.db_path), first_hash)
        with closing(self.connect()) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM schema_migrations WHERE version=?",
                    (SCHEMA_VERSION,),
                ).fetchone()[0],
                1,
            )

    def test_v4_requires_v3_marker_first(self) -> None:
        with closing(self.connect()) as connection:
            with self.assertRaises(ValueError) as ctx:
                migrate_v4(connection, APPLIED_AT_V4)
            self.assertIn("schema v3 must be applied", str(ctx.exception))

    def test_fetch_state_fk_to_source_registry(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            migrate_v4(connection, APPLIED_AT_V4)
            # insert a source first
            connection.execute(
                "INSERT INTO source_registry("
                "source_id,adapter_type,source_role,host,category_scope_json,"
                "enabled,queries_json,title_blocklist_json,content_blocklist_json,"
                "url_blocklist_json,allowlist_domains_json,config_hash,created_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "src1", "searxng", "discovery", "localhost", '["ai"]',
                    1, "[]", "[]", "[]", "[]", "[]", "a" * 64, APPLIED_AT_V4,
                ),
            )
            connection.execute(
                "INSERT INTO fetch_state(source_id,updated_at) VALUES(?,?)",
                ("src1", APPLIED_AT_V4),
            )
            # unknown source must fail FK constraint
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO fetch_state(source_id,updated_at) VALUES(?,?)",
                    ("nonexistent", APPLIED_AT_V4),
                )

    def test_fetch_state_check_constraints(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            migrate_v4(connection, APPLIED_AT_V4)
            connection.execute(
                "INSERT INTO source_registry("
                "source_id,adapter_type,source_role,host,category_scope_json,"
                "enabled,queries_json,title_blocklist_json,content_blocklist_json,"
                "url_blocklist_json,allowlist_domains_json,config_hash,created_at"
                ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "src2", "searxng", "discovery", "localhost", '["ai"]',
                    1, "[]", "[]", "[]", "[]", "[]", "a" * 64, APPLIED_AT_V4,
                ),
            )
            # valid nullables
            connection.execute(
                "INSERT INTO fetch_state(source_id,updated_at) VALUES(?,?)",
                ("src2", APPLIED_AT_V4),
            )
            # rate_limit_remaining must be >= 0
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE fetch_state SET rate_limit_remaining=-1 WHERE source_id=?",
                    ("src2",),
                )
            # retry_after_seconds must be >= 0
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE fetch_state SET retry_after_seconds=-1 WHERE source_id=?",
                    ("src2",),
                )
            # last_http_status out of range
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE fetch_state SET last_http_status=99 WHERE source_id=?",
                    ("src2",),
                )
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute(
                    "UPDATE fetch_state SET last_http_status=600 WHERE source_id=?",
                    ("src2",),
                )

    def test_fetch_state_index_created(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            migrate_v4(connection, APPLIED_AT_V4)
            indexes = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'"
                )
            }
            self.assertIn("idx_fetch_state_reset", indexes)


class MigrationFailureTests(MigrationFixture):
    def test_partial_v4_state_rejected_without_marker(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            connection.execute(
                "CREATE TABLE fetch_state(source_id TEXT PRIMARY KEY, updated_at TEXT NOT NULL)"
            )
            connection.commit()
            with self.assertRaises(ValueError) as ctx:
                migrate_v4(connection, APPLIED_AT_V4)
            self.assertIn("partial schema v4 state", str(ctx.exception))
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
                ).fetchone()
            )

    def test_marker_without_tables_rejected(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            connection.execute(
                "INSERT INTO schema_migrations(version,applied_at) VALUES(?,?)",
                (SCHEMA_VERSION, APPLIED_AT_V4),
            )
            connection.commit()
            with self.assertRaises(ValueError):
                migrate_v4(connection, APPLIED_AT_V4)

    def test_ddl_failure_rolls_back_all_v4_tables_and_marker(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            # create an index that will conflict with v4 DDL
            connection.execute(
                "CREATE INDEX idx_fetch_state_reset ON source_items(source_id)"
            )
            connection.commit()
            with self.assertRaises(sqlite3.OperationalError):
                migrate_v4(connection, APPLIED_AT_V4)
            tables = {
                row[0] for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertNotIn("fetch_state", tables)
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)
                ).fetchone()
            )

    def test_invalid_timestamp_and_active_transaction_rejected(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT_V3)
            with self.assertRaises(ValueError):
                migrate_v4(connection, "")
            connection.execute("BEGIN")
            with self.assertRaises(ValueError):
                migrate_v4(connection, APPLIED_AT_V4)
            connection.rollback()


class MigrationStaticTests(unittest.TestCase):
    def test_schema_module_has_no_path_clock_network_or_subprocess_access(self) -> None:
        tree = ast.parse(
            (ROOT / "scripts/news_pipeline/schema_v4.py").read_text(encoding="utf-8")
        )
        roots = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            (node.module or "").split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        self.assertFalse(
            roots & {
                "pathlib", "socket", "subprocess", "urllib",
                "requests", "aiohttp", "random", "uuid", "time",
            }
        )
        attrs = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        self.assertNotIn("now", attrs)
        self.assertNotIn("utcnow", attrs)


if __name__ == "__main__":
    unittest.main()
