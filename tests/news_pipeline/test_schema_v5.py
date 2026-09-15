from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import V5_COLUMNS, _validate_v5, migrate_v5


class SchemaV5Tests(unittest.TestCase):
    def _valid_v5(self):
        directory = tempfile.TemporaryDirectory()
        path = str(Path(directory.name) / "candidate.db")
        init_db(path)
        con = sqlite3.connect(path)
        con.execute("PRAGMA foreign_keys=ON")
        migrate_v3(con, "2026-09-07T00:00:00Z")
        migrate_v4(con, "2026-09-07T00:00:01Z")
        migrate_v5(con, "2026-09-07T00:00:02Z")
        return directory, con

    def test_v5_migrates_and_replays_idempotently(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "candidate.db")
            init_db(path)
            con = sqlite3.connect(path)
            con.execute("PRAGMA foreign_keys=ON")
            migrate_v3(con, "2026-09-07T00:00:00Z")
            migrate_v4(con, "2026-09-07T00:00:01Z")
            self.assertTrue(migrate_v5(con, "2026-09-07T00:00:02Z"))
            self.assertFalse(migrate_v5(con, "2026-09-07T00:00:03Z"))
            self.assertEqual(tuple(row[1] for row in con.execute("PRAGMA table_info(event_claims)")), V5_COLUMNS["event_claims"])
            self.assertEqual(con.execute("SELECT applied_at FROM schema_migrations WHERE version=5").fetchone()[0], "2026-09-07T00:00:02Z")
            self.assertEqual(con.execute("PRAGMA foreign_key_check").fetchall(), [])
            con.close()

    def test_partial_state_rolls_back_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "candidate.db")
            init_db(path)
            con = sqlite3.connect(path)
            con.execute("PRAGMA foreign_keys=ON")
            migrate_v3(con, "2026-09-07T00:00:00Z")
            migrate_v4(con, "2026-09-07T00:00:01Z")
            con.execute("CREATE TABLE event_claims(event_id TEXT, event_version INTEGER, claim_id TEXT)")
            con.commit()
            with self.assertRaises(ValueError):
                migrate_v5(con, "2026-09-07T00:00:02Z")
            self.assertIsNone(con.execute("SELECT 1 FROM schema_migrations WHERE version=5").fetchone())
            con.close()

    def test_preexisting_conflicting_claim_index_rolls_back(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "candidate.db")
            init_db(path)
            con = sqlite3.connect(path)
            con.execute("PRAGMA foreign_keys=ON")
            migrate_v3(con, "2026-09-07T00:00:00Z")
            migrate_v4(con, "2026-09-07T00:00:01Z")
            con.execute("CREATE TABLE unrelated(value TEXT)")
            con.execute("CREATE INDEX idx_event_claims_claim ON unrelated(value)")
            con.commit()
            with self.assertRaises(sqlite3.OperationalError):
                migrate_v5(con, "2026-09-07T00:00:02Z")
            self.assertIsNone(con.execute("SELECT 1 FROM schema_migrations WHERE version=5").fetchone())
            self.assertIsNone(con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_claims'").fetchone())
            con.close()

    def test_marker_present_wrong_primary_key_is_rejected(self):
        directory, con = self._valid_v5()
        try:
            con.execute("DROP TABLE event_claims")
            con.execute("""CREATE TABLE event_claims(
                event_id TEXT NOT NULL, event_version INTEGER NOT NULL, claim_id TEXT NOT NULL,
                PRIMARY KEY(claim_id, event_id, event_version),
                FOREIGN KEY(event_id, event_version) REFERENCES event_versions(event_id, version),
                FOREIGN KEY(claim_id) REFERENCES claims(claim_id))""")
            con.execute("CREATE INDEX idx_event_claims_claim ON event_claims(claim_id, event_id, event_version)")
            con.commit()
            with self.assertRaisesRegex(ValueError, "primary key"):
                _validate_v5(con)
        finally:
            con.close(); directory.cleanup()

    def test_marker_present_wrong_foreign_key_targets_are_rejected(self):
        directory, con = self._valid_v5()
        try:
            con.execute("DROP TABLE event_claims")
            con.execute("""CREATE TABLE event_claims(
                event_id TEXT NOT NULL, event_version INTEGER NOT NULL, claim_id TEXT NOT NULL,
                PRIMARY KEY(event_id, event_version, claim_id),
                FOREIGN KEY(event_id, event_version) REFERENCES events(id, id),
                FOREIGN KEY(claim_id) REFERENCES source_items(source_item_id))""")
            con.execute("CREATE INDEX idx_event_claims_claim ON event_claims(claim_id, event_id, event_version)")
            con.commit()
            with self.assertRaisesRegex(ValueError, "foreign keys"):
                _validate_v5(con)
        finally:
            con.close(); directory.cleanup()

    def test_marker_present_wrong_index_columns_or_order_are_rejected(self):
        for definition in ("event_id, claim_id, event_version", "event_id"):
            directory, con = self._valid_v5()
            try:
                con.execute("DROP INDEX idx_event_claims_claim")
                con.execute("CREATE INDEX idx_event_claims_claim ON event_claims(" + definition + ")")
                con.commit()
                with self.assertRaisesRegex(ValueError, "index"):
                    _validate_v5(con)
            finally:
                con.close(); directory.cleanup()

    def test_marker_present_unique_or_partial_index_is_rejected(self):
        for sql in (
            "CREATE UNIQUE INDEX idx_event_claims_claim ON event_claims(claim_id, event_id, event_version)",
            "CREATE INDEX idx_event_claims_claim ON event_claims(claim_id, event_id, event_version) WHERE claim_id IS NOT NULL",
        ):
            directory, con = self._valid_v5()
            try:
                con.execute("DROP INDEX idx_event_claims_claim")
                con.execute(sql)
                con.commit()
                with self.assertRaisesRegex(ValueError, "index"):
                    _validate_v5(con)
            finally:
                con.close(); directory.cleanup()


if __name__ == "__main__":
    unittest.main()
