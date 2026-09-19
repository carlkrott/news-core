from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.ingest_runner import _verify_schema
from news_pipeline.provenance import PublisherRule
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import V7_COLUMNS, migrate_v7
from news_pipeline.live_contracts import SourceRole


APPLIED_AT = "2026-09-19T22:00:00Z"


class SchemaV7Tests(unittest.TestCase):
    def _database(self) -> tuple[tempfile.TemporaryDirectory[str], Path, sqlite3.Connection]:
        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / "state.db"
        init_db(str(path))
        connection = sqlite3.connect(path, isolation_level=None)
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, APPLIED_AT)
        migrate_v4(connection, APPLIED_AT)
        migrate_v5(connection, APPLIED_AT)
        migrate_v6(connection, APPLIED_AT)
        connection.execute(
            """INSERT INTO source_registry(
                source_id,adapter_type,source_role,host,category_scope_json,enabled,
                queries_json,title_blocklist_json,content_blocklist_json,url_blocklist_json,
                allowlist_domains_json,config_hash,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "discovery-example", "searxng", "discovery", "search.example.com", '["ai"]', 1,
                '[]', '[]', '[]', '[]', '[]', "a" * 64, APPLIED_AT,
            ),
        )
        connection.execute(
            """INSERT INTO source_items(
                source_item_id,source_id,external_id,category,original_url,canonical_url,
                publisher,source_role,author_handle,retrieval_method,raw_content_hash,
                title,body,raw,retrieved_at,published_at,updated_at,publication_evidence)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "known-item", "discovery-example", "known", "ai",
                "https://manufacturer.example.com/releases/v2",
                "https://manufacturer.example.com/releases/v2",
                "Example Manufacturer", "discovery", None, "searxng", "b" * 64,
                "Release v2", "Release v2", None, APPLIED_AT, APPLIED_AT, None, "published date",
            ),
        )
        connection.execute(
            """INSERT INTO source_items(
                source_item_id,source_id,external_id,category,original_url,canonical_url,
                publisher,source_role,author_handle,retrieval_method,raw_content_hash,
                title,body,raw,retrieved_at,published_at,updated_at,publication_evidence)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                "unknown-item", "discovery-example", "unknown", "ai",
                "https://unknown.example/story",
                "https://unknown.example/story",
                "Unknown Publisher", "discovery", None, "searxng", "c" * 64,
                "Unknown", "Unknown", None, APPLIED_AT, APPLIED_AT, None, "published date",
            ),
        )
        return directory, path, connection

    def test_v7_backfills_known_and_unknown_publishers_conservatively(self) -> None:
        directory, _path, connection = self._database()
        try:
            rules = (
                PublisherRule(
                    rule_id="maker-example",
                    host="manufacturer.example.com",
                    source_role=SourceRole.PRIMARY,
                    independence_group="manufacturer-example",
                    categories=("ai",),
                    authority_entities=("Example Manufacturer",),
                    audit_note="first-party release authority",
                ),
            )
            self.assertTrue(migrate_v7(connection, APPLIED_AT, rules=rules))
            for table, columns in V7_COLUMNS.items():
                actual = tuple(
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM pragma_table_info(?)", (table,)
                    )
                )
                self.assertEqual(actual, columns, table)
            known = connection.execute(
                "SELECT normalized_publisher_host,effective_source_role,independence_group,matched_rule_id,authority_match,classification_reason FROM source_item_provenance WHERE source_item_id='known-item'"
            ).fetchone()
            self.assertEqual(known, ("manufacturer.example.com", "primary", "manufacturer-example", "maker-example", 0, "matched_rule"))
            unknown = connection.execute(
                "SELECT effective_source_role,independence_group,matched_rule_id,authority_match,classification_reason FROM source_item_provenance WHERE source_item_id='unknown-item'"
            ).fetchone()
            self.assertEqual(unknown, ("discovery", "unknown", None, 0, "unknown_publisher"))
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            self.assertFalse(migrate_v7(connection, "2026-09-20T00:00:00Z", rules=rules))
        finally:
            connection.close()
            directory.cleanup()

    def test_partial_v7_state_is_rejected_without_marker(self) -> None:
        directory, _path, connection = self._database()
        try:
            connection.execute("CREATE TABLE publisher_registry(rule_id TEXT PRIMARY KEY)")
            connection.commit()
            with self.assertRaisesRegex(ValueError, "partial schema v7"):
                migrate_v7(connection, APPLIED_AT)
            self.assertIsNone(connection.execute("SELECT 1 FROM schema_migrations WHERE version=7").fetchone())
        finally:
            connection.close()
            directory.cleanup()

    def test_pre_v7_ingest_schema_reader_accepts_v7_tables(self) -> None:
        directory, path, connection = self._database()
        try:
            self.assertTrue(migrate_v7(connection, APPLIED_AT))
            connection.close()
            connection = None
            _verify_schema(path)
        finally:
            if connection is not None:
                connection.close()
            directory.cleanup()


if __name__ == "__main__":
    unittest.main()
