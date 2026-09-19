from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.event_store import process_phase4
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7


APPLIED_AT = "2026-09-19T00:00:00Z"


class V7EventStoreTests(unittest.TestCase):
    def _database(self, *, with_provenance: bool) -> tuple[tempfile.TemporaryDirectory, Path]:
        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / "state.db"
        init_db(str(path))
        connection = sqlite3.connect(path)
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, APPLIED_AT)
        migrate_v4(connection, "2026-09-19T00:00:01Z")
        migrate_v5(connection, "2026-09-19T00:00:02Z")
        migrate_v6(connection, "2026-09-19T00:00:03Z")
        migrate_v7(connection, "2026-09-19T00:00:04Z")
        connection.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
            ("r", APPLIED_AT, None, "historical_replay", "observed_historical", None, None),
        )
        connection.execute(
            "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("s", "rss", "discovery", "publisher.example.com", '["ai"]', 1, "[]", "[]", "[]", "[]", "[]", 60, None, None, None, "a" * 64, APPLIED_AT),
        )
        connection.execute(
            "INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("i", "s", "ext", "ai", "https://publisher.example.com/story", "https://publisher.example.com/story", "Example Publisher", "discovery", None, "rss", "b" * 64, "Widget v2.0", "Widget v2.0 launched on 2026-09-06", None, APPLIED_AT, None, None, None),
        )
        if with_provenance:
            connection.execute(
                "INSERT INTO source_item_provenance VALUES (?,?,?,?,?,?,?,?)",
                ("i", "publisher.example.com", "primary", "publisher-self", None, 1, APPLIED_AT, "reviewed_fixture"),
            )
        connection.execute(
            "INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
            ("d", "r", None, "keep", json.dumps({"source_item_id": "i"}), "2026-09-19T00:00:05Z", "phase3"),
        )
        connection.commit()
        connection.close()
        return directory, path

    def test_explicit_v7_primary_provenance_verifies(self) -> None:
        directory, path = self._database(with_provenance=True)
        try:
            report = process_phase4(path, "2026-09-19T00:01:00Z")
            self.assertEqual(report.processed, 1)
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("SELECT DISTINCT status FROM claims").fetchall(), [("verified",)])
                self.assertEqual(connection.execute("SELECT DISTINCT verification_state FROM event_versions").fetchall(), [("verified",)])
            finally:
                connection.close()
        finally:
            directory.cleanup()

    def test_missing_v7_provenance_fails_closed(self) -> None:
        directory, path = self._database(with_provenance=False)
        try:
            process_phase4(path, "2026-09-19T00:01:00Z")
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("SELECT DISTINCT status FROM claims").fetchall(), [("pending",)])
                self.assertEqual(connection.execute("SELECT DISTINCT verification_state FROM event_versions").fetchall(), [("unverified",)])
            finally:
                connection.close()
        finally:
            directory.cleanup()


if __name__ == "__main__":
    unittest.main()
