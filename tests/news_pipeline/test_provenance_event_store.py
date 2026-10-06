from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.event_store import process_phase4, reverify_source_item_claims
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import backfill_source_item_provenance, migrate_v7
from news_pipeline.provenance import PublisherRegistry, PublisherRule
from news_pipeline.live_contracts import SourceRole


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

    def test_bounded_reverification_promotes_only_exact_fresh_item_and_replays_noop(self) -> None:
        directory, path = self._database(with_provenance=False)
        try:
            connection = sqlite3.connect(path)
            connection.execute(
                "UPDATE source_items SET published_at=?,publication_evidence='source' WHERE source_item_id='i'",
                (APPLIED_AT,),
            )
            connection.commit()
            connection.close()
            first_at = "2026-09-19T00:01:00Z"
            process_phase4(path, first_at)
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA foreign_keys=ON")
            rule = PublisherRule(
                "rule-widget", "publisher.example.com", SourceRole.PRIMARY,
                "widget-official", ("ai",), ("Example Publisher",), audit_note="reviewed fixture",
            )
            backfill_source_item_provenance(
                connection, PublisherRegistry((rule,)), "2026-09-19T00:01:30Z",
                source_item_ids=("i",),
            )
            connection.commit()
            connection.close()

            evaluated_at = "2026-09-19T00:02:00Z"
            result = reverify_source_item_claims(path, "i", evaluated_at)
            self.assertEqual((result.claims_selected, result.claims_verified, result.versions_appended), (2, 2, 1))
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("SELECT DISTINCT status FROM claims WHERE source_item_id='i'").fetchall(), [("verified",)])
                versions = connection.execute(
                    """SELECT version,verification_state,valid_from,superseded_at,verified_at
                         FROM event_versions ORDER BY version"""
                ).fetchall()
                self.assertEqual(versions, [
                    (1, "unverified", first_at, evaluated_at, None),
                    (2, "verified", evaluated_at, None, evaluated_at),
                ])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM report_events").fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
                self.assertEqual(connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            finally:
                connection.close()

            replay = reverify_source_item_claims(path, "i", "2026-09-19T00:03:00Z")
            self.assertEqual((replay.claims_selected, replay.claims_verified, replay.versions_appended), (0, 0, 0))
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM claims WHERE source_item_id='i'").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0], 2)
            finally:
                connection.close()
        finally:
            directory.cleanup()

    def test_bounded_reverification_fails_closed_without_authority_match(self) -> None:
        directory, path = self._database(with_provenance=False)
        try:
            connection = sqlite3.connect(path)
            connection.execute(
                "UPDATE source_items SET published_at=?,publication_evidence='source' WHERE source_item_id='i'",
                (APPLIED_AT,),
            )
            connection.commit()
            connection.close()
            process_phase4(path, "2026-09-19T00:01:00Z")
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA foreign_keys=ON")
            rule = PublisherRule(
                "rule-other", "publisher.example.com", SourceRole.PRIMARY,
                "other-official", ("ai",), ("Unrelated Entity",), audit_note="reviewed fixture",
            )
            backfill_source_item_provenance(
                connection, PublisherRegistry((rule,)), "2026-09-19T00:01:30Z",
                source_item_ids=("i",),
            )
            connection.commit()
            connection.close()
            result = reverify_source_item_claims(path, "i", "2026-09-19T00:02:00Z")
            self.assertEqual((result.claims_selected, result.claims_verified, result.versions_appended), (2, 0, 0))
            with self.assertRaises(ValueError):
                reverify_source_item_claims(path, "missing", "2026-09-19T00:02:00Z")
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("SELECT DISTINCT status FROM claims").fetchall(), [("pending",)])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0], 1)
            finally:
                connection.close()
        finally:
            directory.cleanup()

    def test_bounded_reverification_rejects_aged_out_publication(self) -> None:
        directory, path = self._database(with_provenance=False)
        try:
            process_phase4(path, "2026-09-19T00:01:00Z")
            connection = sqlite3.connect(path)
            connection.execute(
                "UPDATE source_items SET published_at='2026-09-01T00:00:00Z',publication_evidence='source' WHERE source_item_id='i'"
            )
            connection.commit()
            connection.close()
            result = reverify_source_item_claims(path, "i", "2026-09-19T00:02:00Z")
            self.assertEqual((result.claims_selected, result.claims_verified, result.versions_appended), (0, 0, 0))
            connection = sqlite3.connect(path)
            try:
                self.assertEqual(connection.execute("SELECT DISTINCT status FROM claims").fetchall(), [("pending",)])
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0], 1)
            finally:
                connection.close()
        finally:
            directory.cleanup()


if __name__ == "__main__":
    unittest.main()
