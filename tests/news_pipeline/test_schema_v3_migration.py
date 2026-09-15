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

from news_pipeline import db
from news_pipeline.schema_v3 import SCHEMA_VERSION, V3_COLUMNS, V3_TABLES, migrate_v3

ROOT = Path(__file__).resolve().parents[2]
SEALED_BACKUP = Path(os.environ.get(
    "NEWS_PIPELINE_PHASE0_DB",
    str(ROOT / "tests" / "news_pipeline" / "fixtures" / "sealed-phase0.db"),
))
SEALED_SHA256 = "aee70c657c2958adc578033d3fd353216c77e93e0fb57b711c0bbf2107a32772"
APPLIED_AT = "2026-09-06T22:00:00Z"
HISTORICAL_TABLES = (
    "runs", "articles", "observations", "events", "event_articles",
    "fact_fingerprints", "decisions", "delivery_attempts", "manual_review",
    "query_telemetry",
)
EXPECTED_COUNTS = {
    "articles": 2730,
    "events": 748,
    "observations": 10565,
    "fact_fingerprints": 3346,
    "runs": 373,
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def counts(connection: sqlite3.Connection, tables: tuple[str, ...]) -> dict[str, int]:
    return {table: connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] for table in tables}


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
    def test_migrates_copy_preserving_all_historical_rows(self) -> None:
        with closing(self.connect()) as connection:
            before = counts(connection, HISTORICAL_TABLES)
            self.assertEqual({key: before[key] for key in EXPECTED_COUNTS}, EXPECTED_COUNTS)
            self.assertTrue(migrate_v3(connection, APPLIED_AT))
            after = counts(connection, HISTORICAL_TABLES)
            self.assertEqual(after, before)
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertEqual(connection.execute("SELECT applied_at FROM schema_migrations WHERE version=3").fetchone()[0], APPLIED_AT)

    def test_all_ten_tables_and_exact_columns_exist(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT)
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(set(V3_TABLES).issubset(tables))
            self.assertEqual(len(V3_TABLES), 10)
            for table, expected in V3_COLUMNS.items():
                actual = tuple(row[1] for row in connection.execute(f'PRAGMA table_info("{table}")'))
                self.assertEqual(actual, expected, table)

    def test_second_migration_is_a_byte_stable_noop(self) -> None:
        connection = self.connect()
        try:
            self.assertTrue(migrate_v3(connection, APPLIED_AT))
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            connection.close()
        first_hash = sha256(self.db_path)
        connection = self.connect()
        try:
            self.assertFalse(migrate_v3(connection, "2026-09-07T00:00:00Z"))
        finally:
            connection.close()
        self.assertEqual(sha256(self.db_path), first_hash)
        with closing(self.connect()) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM schema_migrations WHERE version=?", (SCHEMA_VERSION,)).fetchone()[0], 1)

    def test_legacy_initializer_remains_v2_only(self) -> None:
        db.init_db(str(self.db_path))
        with closing(self.connect()) as connection:
            versions = [row[0] for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")]
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertEqual(versions, [1, 2])
        self.assertTrue(set(V3_TABLES).isdisjoint(tables))

    def test_rollback_by_restore_returns_exact_sealed_bytes(self) -> None:
        with closing(self.connect()) as connection:
            self.assertTrue(migrate_v3(connection, APPLIED_AT))
            self.assertIsNotNone(connection.execute("SELECT 1 FROM schema_migrations WHERE version=3").fetchone())
        for suffix in ("-wal", "-shm"):
            Path(str(self.db_path) + suffix).unlink(missing_ok=True)
        shutil.copy2(SEALED_BACKUP, self.db_path)
        self.assertEqual(sha256(self.db_path), SEALED_SHA256)
        with closing(self.connect()) as connection:
            self.assertEqual([row[0] for row in connection.execute("SELECT version FROM schema_migrations ORDER BY version")], [1, 2])
            self.assertTrue(set(V3_TABLES).isdisjoint({row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}))


class MigrationConstraintTests(MigrationFixture):
    def test_foreign_key_and_status_constraints_reject_invalid_rows(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("INSERT INTO source_items(source_item_id,source_id,category,original_url,canonical_url,publisher,source_role,retrieval_method,raw_content_hash,retrieved_at) VALUES(?,?,?,?,?,?,?,?,?,?)", ("item", "missing", "ai", "https://e", "https://e", "p", "discovery", "x", "a" * 64, APPLIED_AT))
            connection.execute("INSERT INTO source_registry(source_id,adapter_type,source_role,host,category_scope_json,enabled,queries_json,title_blocklist_json,content_blocklist_json,url_blocklist_json,allowlist_domains_json,config_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", ("source", "searxng", "discovery", "localhost", '[\"ai\"]', 1, '[]', '[]', '[]', '[]', '[]', "a" * 64, APPLIED_AT))
            connection.execute("INSERT INTO source_items(source_item_id,source_id,category,original_url,canonical_url,publisher,source_role,retrieval_method,raw_content_hash,retrieved_at) VALUES(?,?,?,?,?,?,?,?,?,?)", ("item", "source", "ai", "https://e", "https://e", "p", "discovery", "x", "a" * 64, APPLIED_AT))
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("INSERT INTO claims(claim_id,source_item_id,subject,predicate,object_value,statement_type,extraction_confidence,status,extracted_at) VALUES(?,?,?,?,?,?,?,?,?)", ("claim", "item", "s", "p", "o", "typed", "0.5", "invalid", APPLIED_AT))

    def test_unique_source_item_constraint(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT)
            connection.execute("INSERT INTO source_registry(source_id,adapter_type,source_role,host,category_scope_json,enabled,queries_json,title_blocklist_json,content_blocklist_json,url_blocklist_json,allowlist_domains_json,config_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", ("source", "searxng", "discovery", "localhost", '[\"ai\"]', 1, '[]', '[]', '[]', '[]', '[]', "a" * 64, APPLIED_AT))
            values = ("item1", "source", "ai", "https://original", "https://canonical", "p", "discovery", "x", "a" * 64, APPLIED_AT)
            connection.execute("INSERT INTO source_items(source_item_id,source_id,category,original_url,canonical_url,publisher,source_role,retrieval_method,raw_content_hash,retrieved_at) VALUES(?,?,?,?,?,?,?,?,?,?)", values)
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("INSERT INTO source_items(source_item_id,source_id,category,original_url,canonical_url,publisher,source_role,retrieval_method,raw_content_hash,retrieved_at) VALUES(?,?,?,?,?,?,?,?,?,?)", ("item2",) + values[1:])

    def test_unknown_date_requires_reason_and_unknown_precision(self) -> None:
        with closing(self.connect()) as connection:
            migrate_v3(connection, APPLIED_AT)
            run_id = connection.execute("SELECT id FROM runs LIMIT 1").fetchone()[0]
            connection.execute("INSERT INTO events(id,run_id,category,started_at,status) VALUES(?,?,?,?,?)", ("event-test", run_id, "ai", APPLIED_AT, "complete"))
            connection.execute("INSERT INTO event_versions(event_id,version,material_change_reason,summary,verification_state,valid_from) VALUES(?,?,?,?,?,?)", ("event-test", 1, "initial", "summary", "unverified", APPLIED_AT))
            with self.assertRaises(sqlite3.IntegrityError):
                connection.execute("INSERT INTO event_dates(event_date_id,event_id,event_version,date_type,date_precision) VALUES(?,?,?,?,?)", ("date", "event-test", 1, "scheduled_for", "day"))
            connection.execute("INSERT INTO event_dates(event_date_id,event_id,event_version,date_type,date_precision,unknown_reason) VALUES(?,?,?,?,?,?)", ("date", "event-test", 1, "scheduled_for", "unknown", "not stated"))


class MigrationFailureTests(MigrationFixture):
    def test_empty_or_incompatible_database_rejected(self) -> None:
        empty = Path(self.temp.name) / "empty.db"
        with closing(sqlite3.connect(empty)) as connection:
            with self.assertRaises(ValueError):
                migrate_v3(connection, APPLIED_AT)

    def test_partial_v3_state_rejected_without_marker(self) -> None:
        with closing(self.connect()) as connection:
            connection.execute("CREATE TABLE source_registry(x TEXT)")
            with self.assertRaises(ValueError):
                migrate_v3(connection, APPLIED_AT)
            self.assertIsNone(connection.execute("SELECT 1 FROM schema_migrations WHERE version=3").fetchone())

    def test_marker_without_tables_rejected(self) -> None:
        with closing(self.connect()) as connection:
            connection.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(3,?)", (APPLIED_AT,))
            connection.commit()
            with self.assertRaises(ValueError):
                migrate_v3(connection, APPLIED_AT)

    def test_ddl_failure_rolls_back_all_v3_tables_and_marker(self) -> None:
        with closing(self.connect()) as connection:
            connection.execute("CREATE INDEX idx_source_registry_due ON articles(id)")
            connection.commit()
            with self.assertRaises(sqlite3.OperationalError):
                migrate_v3(connection, APPLIED_AT)
            tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertTrue(set(V3_TABLES).isdisjoint(tables))
            self.assertIsNone(connection.execute("SELECT 1 FROM schema_migrations WHERE version=3").fetchone())

    def test_invalid_timestamp_and_active_transaction_rejected(self) -> None:
        with closing(self.connect()) as connection:
            with self.assertRaises(ValueError):
                migrate_v3(connection, "")
            connection.execute("BEGIN")
            with self.assertRaises(ValueError):
                migrate_v3(connection, APPLIED_AT)
            connection.rollback()


class MigrationStaticTests(unittest.TestCase):
    def test_schema_module_has_no_path_clock_network_or_subprocess_access(self) -> None:
        tree = ast.parse((ROOT / "scripts/news_pipeline/schema_v3.py").read_text(encoding="utf-8"))
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
        self.assertFalse(roots & {"pathlib", "socket", "subprocess", "urllib", "requests", "aiohttp", "random", "uuid", "time"})
        attrs = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        self.assertNotIn("now", attrs)
        self.assertNotIn("utcnow", attrs)


if __name__ == "__main__":
    unittest.main()
