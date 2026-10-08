from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from datetime import datetime, timezone
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.event_store import process_phase4
from news_pipeline.provenance import PublisherRule
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9
from news_pipeline.schema_v10 import migrate_v10
from news_pipeline.schema_v11 import migrate_v11, validate_v11
from news_pipeline.live_contracts import SourceRole


APPLIED_AT = "2026-09-23T06:00:00Z"


class SchemaV11Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.db_path = Path(self.directory.name) / "candidate.db"
        init_db(str(self.db_path))
        self.con = sqlite3.connect(self.db_path, isolation_level=None)
        self.con.execute("PRAGMA foreign_keys=ON")
        for migration in (migrate_v3, migrate_v4, migrate_v5, migrate_v6):
            migration(self.con, APPLIED_AT)
        self.con.execute(
            "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
            ("source-run", APPLIED_AT, None, "historical_replay", "observed_historical", None, None),
        )
        self.con.execute(
            "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("rss-discovery", "rss", "discovery", "publisher.example.test", '["ai"]', 1,
             "[]", "[]", "[]", "[]", "[]", 60, None, None, None, "a" * 64, APPLIED_AT),
        )
        self._insert_item(
            "widget-item", "widget-ext", "https://publisher.example.test/widget",
            "Widget", "Version 2.0 released on 2026-09-22", "2026-09-22T08:00:00Z",
        )
        self._insert_item(
            "unrelated-item", "unrelated-ext", "https://publisher.example.test/other",
            "Other Product", "Version 9.0 released on 2026-09-22", "2026-09-22T08:30:00Z",
        )
        for decision_id, item_id, decided_at in (
            ("decision-widget", "widget-item", "2026-09-23T06:01:00Z"),
            ("decision-other", "unrelated-item", "2026-09-23T06:02:00Z"),
        ):
            self.con.execute(
                "INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
                (decision_id, "source-run", None, "keep", json.dumps({"source_item_id": item_id}), decided_at, "phase3"),
            )
        rule = PublisherRule(
            rule_id="publisher-own-widget",
            host="publisher.example.test",
            source_role=SourceRole.PRIMARY,
            independence_group="publisher-origin",
            categories=("ai",),
            authority_entities=("widget",),
            audit_note="first-party authority for Widget only",
        )
        migrate_v7(self.con, APPLIED_AT, rules=(rule,))
        migrate_v8(self.con, APPLIED_AT)
        migrate_v9(self.con, APPLIED_AT)
        migrate_v10(self.con, APPLIED_AT)

    def tearDown(self) -> None:
        self.con.close()
        self.directory.cleanup()

    def _insert_item(self, item_id: str, external_id: str, url: str, title: str, body: str, published_at: str) -> None:
        self.con.execute(
            "INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (item_id, "rss-discovery", external_id, "ai", url, url, "Publisher", "discovery", None,
             "publisher-article", "a" * 64, title, body, None, published_at, published_at, None, "publisher date"),
        )

    def test_v11_migration_cli_applies_and_replays(self) -> None:
        from news_pipeline.jobs import main

        first_output = StringIO()
        with redirect_stdout(first_output):
            first_exit = main([
                "migrate-v11", "--db", str(self.db_path), "--applied-at", APPLIED_AT,
            ])
        self.assertEqual(first_exit, 0)
        self.assertTrue(json.loads(first_output.getvalue())["applied"])

        replay_output = StringIO()
        with redirect_stdout(replay_output):
            replay_exit = main([
                "migrate-v11", "--db", str(self.db_path), "--applied-at", APPLIED_AT,
            ])
        self.assertEqual(replay_exit, 0)
        self.assertFalse(json.loads(replay_output.getvalue())["applied"])
        self.assertIsNotNone(self.con.execute(
            "SELECT 1 FROM schema_migrations WHERE version=11"
        ).fetchone())

    def test_v11_claim_specific_provenance_verification_and_report_membership(self) -> None:
        self.assertTrue(migrate_v11(self.con, APPLIED_AT))
        self.assertFalse(migrate_v11(self.con, "2026-09-23T07:00:00Z"))
        validate_v11(self.con)
        from news_pipeline.ingest_runner import _verify_schema
        _verify_schema(self.db_path)
        self.con.execute(
            "UPDATE source_items SET retrieval_method='rss-poll' WHERE source_item_id='unrelated-item'"
        )

        first = process_phase4(self.db_path, "2026-09-23T07:00:00Z")
        replay = process_phase4(self.db_path, "2026-09-23T07:01:00Z")
        self.assertEqual((first.selected, first.processed), (2, 2))
        self.assertEqual((replay.selected, replay.processed, replay.versions_appended), (0, 0, 0))

        status_by_item = dict(self.con.execute(
            "SELECT source_item_id,MAX(status) FROM claims GROUP BY source_item_id"
        ).fetchall())
        self.assertEqual(status_by_item["widget-item"], "verified")
        self.assertEqual(status_by_item["unrelated-item"], "verified")
        snapshot = self.con.execute(
            """SELECT si.source_item_id,p.normalized_publisher_host,p.effective_source_role,
                      p.independence_group,p.matched_rule_id,p.authority_scope_json,
                      p.authority_entities_json,p.authority_match,p.classification_timestamp
                 FROM claim_evidence_provenance p
                 JOIN claim_evidence ce ON ce.evidence_id=p.evidence_id
                 JOIN source_items si ON si.source_item_id=ce.source_item_id
                WHERE si.source_item_id='widget-item' ORDER BY p.evidence_id LIMIT 1"""
        ).fetchone()
        self.assertEqual(snapshot[:8], (
            "widget-item", "publisher.example.test", "primary", "publisher-origin",
            "publisher-own-widget", '["ai"]', '["widget"]', 1,
        ))
        self.assertEqual(snapshot[8], APPLIED_AT)
        self.assertGreaterEqual(
            self.con.execute("SELECT COUNT(*) FROM event_versions WHERE verification_state='verified'").fetchone()[0],
            1,
        )
        self.assertGreaterEqual(
            self.con.execute("SELECT COUNT(*) FROM event_versions WHERE verification_state='verified'").fetchone()[0],
            2,
        )

        from news_pipeline.report_builder import run_report

        with tempfile.TemporaryDirectory() as artifact_root:
            result = run_report(
                db_path=self.db_path,
                artifacts_root=Path(artifact_root),
                as_of_utc=datetime(2026, 9, 24, 9, 0, tzinfo=timezone.utc),
            )
        self.assertEqual(result.generation_status, "complete")
        self.assertGreaterEqual(result.included_count, 1)
        report_payload = json.loads(result.artifact_result.json_bytes)
        self.assertTrue(any(item.get("verification_basis") == "single outlet" for item in report_payload["items"]))
        self.assertTrue(any(item.get("source_tier") == "primary" for item in report_payload["items"]))
        self.assertIn(b"Verification basis: single outlet", result.artifact_result.markdown_bytes)
        memberships = self.con.execute(
            """SELECT e.category,ev.verification_state,ev.superseded_at,si.canonical_url
                 FROM report_events re
                 JOIN event_versions ev ON ev.event_id=re.event_id AND ev.version=re.event_version
                 JOIN events e ON e.id=ev.event_id
                 JOIN event_claims ec ON ec.event_id=ev.event_id AND ec.event_version=ev.version
                 JOIN claims c ON c.claim_id=ec.claim_id
                 JOIN source_items si ON si.source_item_id=c.source_item_id
                WHERE re.report_id=?""",
            (result.report_id,),
        ).fetchall()
        self.assertTrue(memberships)
        self.assertTrue(all(row[0] == "ai" and row[1] == "verified" and row[2] is None and row[3].startswith("https://") for row in memberships))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
        self.assertEqual(self.con.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        self.assertEqual(self.con.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_v11_backfills_claim_snapshot_from_registry_and_downgrades_transport_leads(self) -> None:
        excerpt = "Version 2.0"
        self.con.execute(
            "INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?)",
            ("legacy-claim", "widget-item", "widget", "has_version", "2.0", "version", "0.8", "pending", APPLIED_AT),
        )
        self.con.execute(
            "INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?)",
            ("legacy-evidence", "legacy-claim", "widget-item", "supports", excerpt,
             "b" * 64, "untrusted-publisher-text", APPLIED_AT),
        )
        self.con.execute(
            "UPDATE source_items SET retrieval_method='rss-poll' WHERE source_item_id='unrelated-item'"
        )
        self.con.execute(
            "INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?)",
            ("lead-claim", "unrelated-item", "Other Product", "has_version", "9.0", "version", "0.8", "pending", APPLIED_AT),
        )
        self.con.execute(
            "INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?)",
            ("lead-evidence", "lead-claim", "unrelated-item", "supports", "Version 9.0",
             "c" * 64, "untrusted-publisher-text", APPLIED_AT),
        )
        migrate_v11(self.con, APPLIED_AT)
        row = self.con.execute(
            """SELECT normalized_publisher_host,effective_source_role,independence_group,
                      matched_rule_id,authority_scope_json,authority_entities_json,
                      authority_match
                 FROM claim_evidence_provenance WHERE evidence_id='legacy-evidence'"""
        ).fetchone()
        self.assertEqual(row, (
            "publisher.example.test", "primary", "publisher-origin",
            "publisher-own-widget", '["ai"]', '["widget"]', 1,
        ))
        lead = self.con.execute(
            """SELECT effective_source_role,independence_group,matched_rule_id,
                      authority_scope_json,authority_entities_json,authority_match,
                      classification_reason
                 FROM claim_evidence_provenance WHERE evidence_id='lead-evidence'"""
        ).fetchone()
        self.assertEqual(lead, (
            "discovery", "unknown", None, "[]", "[]", 0,
            "feed_transport_not_claim_evidence",
        ))
        self.con.execute("UPDATE claims SET status='verified' WHERE claim_id IN ('legacy-claim','lead-claim')")
        from news_pipeline.event_store import EventWrite, _v11_claims_are_verified, append_event_versions

        claim_ids = ("legacy-claim", "lead-claim")
        self.assertFalse(_v11_claims_are_verified(self.con, claim_ids))
        self.con.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?,?,?)",
            ("aggregate-test", "source-run", "ai", APPLIED_AT, None, 0, 0, "complete"),
        )
        with self.assertRaisesRegex(ValueError, "claim-specific v11 provenance"):
            append_event_versions(self.con, [EventWrite(
                "aggregate-test", "combined", "distinct", "verified", APPLIED_AT, claim_ids
            )])
        self.con.execute(
            "INSERT INTO event_versions VALUES (?,?,?,?,?,?,?,?)",
            ("aggregate-test", 1, "distinct", "combined", "verified", APPLIED_AT, None, APPLIED_AT),
        )
        self.con.executemany(
            "INSERT INTO event_claims VALUES (?,?,?)",
            [("aggregate-test", 1, claim_id) for claim_id in claim_ids],
        )
        from news_pipeline.report_builder import _has_verified_claims_and_provenance
        self.assertFalse(_has_verified_claims_and_provenance(self.con, "aggregate-test", 1))
        validate_v11(self.con)


if __name__ == "__main__":
    unittest.main()
