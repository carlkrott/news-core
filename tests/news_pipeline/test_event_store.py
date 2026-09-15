from __future__ import annotations

import json
import hashlib
import multiprocessing
import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.event_store import EventWrite, append_event_versions, process_phase4


class EventStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "candidate.db"
        init_db(str(self.path))
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        migrate_v3(con, "2026-09-07T00:00:00Z")
        migrate_v4(con, "2026-09-07T00:00:01Z")
        migrate_v5(con, "2026-09-07T00:00:02Z")
        con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?)", ("r", "2026-09-07T00:00:00Z", None, "historical_replay", "observed_historical", None, None))
        con.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)", ("e1", "r", "ai", "2026-09-07T00:00:00Z", None, 0, 0, "complete"))
        con.execute("INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("s", "searxng", "primary", "example.test", '["ai"]', 1, "[]", "[]", "[]", "[]", "[]", 60, None, None, None, "a" * 64, "2026-09-07T00:00:00Z"))
        con.execute("INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("i", "s", "ext", "ai", "https://example.test", "https://example.test", "Example", "primary", None, "rss", "a" * 64, "Widget v2.0", "Widget v2.0 launched on 2026-09-06", None, "2026-09-07T00:00:00Z", None, None, None))
        con.execute("INSERT INTO decisions VALUES (?,?,?,?,?,?,?)", ("d", "r", None, "keep", json.dumps({"source_item_id": "i"}), "2026-09-07T00:00:03Z", "phase3"))
        con.commit(); con.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _source(self, source_id, role, adapter="rss"):
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (source_id, adapter, role, "example.test", '["ai"]', 1, "[]", "[]", "[]", "[]", "[]", 60, None, None, None, "a" * 64, "2026-09-07T00:00:00Z"))
        con.commit(); con.close()

    def _item(self, item_id, source_id="s", role="primary", publisher="Example", title: str | None = "Widget v2.0", body: str | None = "Widget v2.0 launched on 2026-09-06"):
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (item_id, source_id, item_id + "-ext", "ai", "https://example.test/" + item_id, "https://example.test/" + item_id, publisher, role, None, "rss", "b" * 64, title, body, None, "2026-09-07T00:00:00Z", None, None, None))
        con.commit(); con.close()

    def _decision(self, decision_id, item_id, kind="keep", decided_at="2026-09-07T00:00:03Z", **extra):
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO decisions VALUES (?,?,?,?,?,?,?)", (decision_id, "r", None, kind, json.dumps({"source_item_id": item_id, **extra}), decided_at, "phase3"))
        con.commit(); con.close()

    def _counts(self):
        con = sqlite3.connect(self.path)
        try:
            return {table: con.execute("SELECT COUNT(*) FROM " + table).fetchone()[0] for table in ("claims", "claim_evidence", "events", "event_versions", "event_claims", "event_dates", "reports", "report_events", "delivery_attempts", "query_plans")}
        finally:
            con.close()

    def test_process_creates_claims_event_and_is_replay_noop(self):
        first = process_phase4(self.path, "2026-09-07T00:01:00Z")
        second = process_phase4(self.path, "2026-09-07T00:02:00Z")
        self.assertEqual((first.selected, first.processed), (1, 1))
        self.assertEqual((second.selected, second.processed), (0, 0))
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        self.assertGreater(con.execute("SELECT COUNT(*) FROM claims").fetchone()[0], 0)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM report_events").fetchone()[0], 0)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
        con.close()

    def test_append_update_closes_only_predecessor(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        claim = "c1"
        con.execute("INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?)", (claim, "i", "s", "p", "v", "statement", "0.8", "pending", "2026-09-07T00:00:00Z"))
        con.commit()
        self.assertEqual(append_event_versions(con, [EventWrite("e1", "first", "distinct", "unverified", "2026-09-07T00:04:00Z", (claim,))]), 1)
        self.assertEqual(append_event_versions(con, [EventWrite("e1", "corrected", "correction", "watchlist", "2026-09-07T00:05:00Z", (claim,))]), 1)
        rows = con.execute("SELECT version,summary,superseded_at FROM event_versions ORDER BY version").fetchall()
        self.assertEqual(rows[0], (1, "first", "2026-09-07T00:05:00Z"))
        self.assertEqual(rows[1][0:2], (2, "corrected"))
        self.assertEqual(append_event_versions(con, [], max_items=0), 0)
        con.close()

    def test_failure_rolls_back_all_rows(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        with self.assertRaises(sqlite3.IntegrityError):
            append_event_versions(con, [EventWrite("e1", "good", "distinct", "unverified", "2026-09-07T00:06:00Z", ("missing",))])
        self.assertEqual(con.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0], 0)
        con.close()

    def test_empty_claim_ids_rejected_without_partial_version(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        with self.assertRaises(ValueError):
            append_event_versions(con, [EventWrite("e1", "missing provenance", "distinct", "unverified", "2026-09-07T00:06:30Z", ())])
        self.assertEqual(con.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0], 0)
        con.close()

    def test_direct_retraction_appends_rejected_version_and_updates_no_outputs(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        con.execute("INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?)", ("direct", "i", "Widget", "has_version", "2.0", "version", "0.8", "verified", "2026-09-07T00:00:00Z"))
        con.commit()
        append_event_versions(con, [EventWrite("e1", "original summary", "distinct", "verified", "2026-09-07T00:06:40Z", ("direct",))])
        before = con.execute("SELECT summary,verification_state,valid_from FROM event_versions WHERE event_id='e1' AND version=1").fetchone()
        self.assertEqual(append_event_versions(con, [EventWrite("e1", "retracted summary", "ignored", "verified", "2026-09-07T00:06:41Z", ("direct",), True)]), 1)
        self.assertEqual(con.execute("SELECT summary,verification_state,valid_from FROM event_versions WHERE event_id='e1' AND version=1").fetchone(), before)
        self.assertEqual(con.execute("SELECT material_change_reason,verification_state FROM event_versions WHERE event_id='e1' AND version=2").fetchone(), ("retraction", "rejected"))
        self.assertIsNotNone(con.execute("SELECT superseded_at FROM event_versions WHERE event_id='e1' AND version=1").fetchone()[0])
        self.assertEqual(con.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
        self.assertEqual(con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
        con.close()

    def test_max_items_zero_is_write_free(self):
        con = sqlite3.connect(self.path)
        try:
            before = {table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in ("runs", "claims", "events", "event_versions")}
        finally:
            con.close()
        report = process_phase4(self.path, "2026-09-07T00:07:00Z", max_items=0)
        self.assertEqual((report.selected, report.processed, report.claims_inserted, report.evidence_inserted), (0, 0, 0, 0))
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(before, {table: con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] for table in before})
        finally:
            con.close()

    def test_primary_source_is_verified_and_has_no_output_rows(self):
        report = process_phase4(self.path, "2026-09-07T00:08:00Z")
        self.assertEqual((report.selected, report.processed, report.claim_status_updates, report.events_created, report.versions_appended), (1, 1, 2, 1, 1))
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT DISTINCT status FROM claims").fetchall(), [("verified",)])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM event_claims").fetchone()[0], 2)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM event_dates").fetchone()[0], 1)
            date_row = con.execute("SELECT event_id,event_version,date_type,date_value,date_precision,evidence_id FROM event_dates").fetchone()
            evidence = con.execute("SELECT evidence_id,exact_excerpt,excerpt_hash FROM claim_evidence WHERE claim_id IN (SELECT claim_id FROM claims WHERE predicate='has_date')").fetchone()
            self.assertEqual(date_row[1:5], (1, "occurred_at", "2026-09-06", "day"))
            self.assertEqual(date_row[5], evidence[0])
            self.assertEqual(evidence[2], hashlib.sha256(evidence[1].encode()).hexdigest())
            self.assertIn('"event_dates_inserted":1', con.execute("SELECT notes FROM runs WHERE notes IS NOT NULL ORDER BY started_at DESC LIMIT 1").fetchone()[0])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM report_events").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
            self.assertEqual(con.execute("PRAGMA foreign_key_check").fetchall(), [])
        finally:
            con.close()

    def test_independent_neutral_specialist_append_verified_and_discovery_stays_unverified(self):
        process_phase4(self.path, "2026-09-07T00:08:00Z")
        for source_id, role, adapter in (("neutral", "neutral", "rss"), ("specialist", "specialist", "rss"), ("discover", "discovery", "searxng")):
            self._source(source_id, role, adapter)
        self._item("n", "neutral", "neutral", "Neutral Press", "Independent fact v3.0", "Independent fact v3.0 launched")
        self._item("sp", "specialist", "specialist", "Specialist Lab", "Independent fact v3.0", "Independent fact v3.0 launched")
        self._item("x", "discover", "discovery", "Discovery Wire", "Discovery note", "Discovery note available")
        for decision_id, item_id, at in (("dn", "n", "2026-09-07T00:08:01Z"), ("dsp", "sp", "2026-09-07T00:08:02Z"), ("dx", "x", "2026-09-07T00:08:03Z")):
            self._decision(decision_id, item_id, decided_at=at)
        report = process_phase4(self.path, "2026-09-07T00:09:00Z")
        self.assertEqual(report.processed, 3)
        con = sqlite3.connect(self.path)
        try:
            dedicated = con.execute("SELECT DISTINCT ec.event_id FROM event_claims ec JOIN claims c ON c.claim_id=ec.claim_id WHERE c.source_item_id IN ('n','sp')").fetchone()[0]
            self.assertEqual(con.execute("SELECT version,verification_state FROM event_versions WHERE event_id=? ORDER BY version", (dedicated,)).fetchall(), [(1, "unverified"), (2, "verified")])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0], 4)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM event_versions WHERE verification_state='verified'").fetchone()[0], 2)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM claims WHERE source_item_id='x' AND status='pending'").fetchone()[0], 1)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM event_versions WHERE verification_state='unverified'").fetchone()[0], 2)
            self.assertLessEqual(con.execute("SELECT COUNT(*) FROM query_plans").fetchone()[0], 2)
            self.assertEqual(con.execute("PRAGMA foreign_key_check").fetchall(), [])
        finally:
            con.close()

    def test_contradiction_forces_watchlist(self):
        process_phase4(self.path, "2026-09-07T00:10:00Z")
        self._source("contradict", "neutral")
        self._item("bad", "contradict", "neutral", "Contrary Press")
        con = sqlite3.connect(self.path)
        claim_id = con.execute("SELECT claim_id FROM claims ORDER BY claim_id LIMIT 1").fetchone()[0]
        excerpt = "Widget v2.0 is false"
        excerpt_hash = hashlib.sha256(excerpt.encode()).hexdigest()
        con.execute("INSERT INTO claim_evidence VALUES (?,?,?,?,?,?,?,?)", ("contra", claim_id, "bad", "contradicts", excerpt, excerpt_hash, "contrary press", "2026-09-07T00:10:01Z"))
        self.assertEqual(con.execute("SELECT excerpt_hash FROM claim_evidence WHERE evidence_id='contra'").fetchone()[0], excerpt_hash)
        con.commit(); con.close()
        self._decision("dbad", "bad", decided_at="2026-09-07T00:10:02Z")
        process_phase4(self.path, "2026-09-07T00:11:00Z")
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT verification_state FROM event_versions ORDER BY version DESC LIMIT 1").fetchone()[0], "watchlist")
        finally:
            con.close()

    def test_promote_one_match_appends_and_closes_predecessor(self):
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?)", ("pc", "i", "Widget", "has_version", "2.0", "version", "0.8", "pending", "2026-09-07T00:00:00Z"))
        con.commit()
        append_event_versions(con, [EventWrite("e1", "old", "old", "unverified", "2026-09-07T00:12:00Z", ("pc",))])
        con.close()
        self._item("prom")
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)", ("obs-prom", None, "e1", "ai", "phase4", "parsed_article", "", "", None, "2026-09-07T00:12:01Z"))
        con.commit(); con.close()
        self._decision("dprom", "prom", "promote", "2026-09-07T00:12:02Z", matched_observation_ids=["obs-prom"])
        process_phase4(self.path, "2026-09-07T00:13:00Z")
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT MAX(version) FROM event_versions WHERE event_id='e1'").fetchone()[0], 2)
            self.assertEqual(con.execute("SELECT superseded_at IS NOT NULL FROM event_versions WHERE event_id='e1' AND version=1").fetchone()[0], 1)
        finally:
            con.close()

    def test_persisted_phase3_retraction_appends_and_rejects_claims(self):
        process_phase4(self.path, "2026-09-07T00:21:00Z")
        con = sqlite3.connect(self.path)
        try:
            event_id = con.execute("SELECT DISTINCT ec.event_id FROM event_claims ec JOIN claims c ON c.claim_id=ec.claim_id WHERE c.source_item_id='i'").fetchone()[0]
            old = con.execute("SELECT summary,verification_state,valid_from FROM event_versions WHERE event_id=? AND version=1", (event_id,)).fetchone()
            old_claims = tuple(row[0] for row in con.execute("SELECT claim_id FROM event_claims WHERE event_id=? AND event_version=1", (event_id,)))
        finally:
            con.close()
        self._item("ret", title="Widget v2.0 retracted", body="Widget v2.0 retracted after an error")
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)", ("obs-ret", None, event_id, "ai", "phase4", "parsed_article", "", "", None, "2026-09-07T00:21:01Z"))
        con.commit(); con.close()
        self._decision("dret", "ret", "promote", "2026-09-07T00:21:02Z", matched_observation_ids=["obs-ret"], semantic_reasons=["CORRECTION_OR_RETRACTION"])
        report = process_phase4(self.path, "2026-09-07T00:22:00Z")
        self.assertGreaterEqual(report.claim_status_updates, 2)
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT summary,verification_state,valid_from FROM event_versions WHERE event_id=? AND version=1", (event_id,)).fetchone(), old)
            self.assertEqual(con.execute("SELECT material_change_reason,verification_state FROM event_versions WHERE event_id=? AND version=2", (event_id,)).fetchone(), ("retraction", "rejected"))
            self.assertEqual(con.execute("SELECT superseded_at FROM event_versions WHERE event_id=? AND version=1", (event_id,)).fetchone()[0], "2026-09-07T00:22:00Z")
            self.assertTrue(all(status == "superseded" for status, in con.execute("SELECT status FROM claims WHERE claim_id IN (%s)" % ",".join("?" for _ in old_claims), old_claims)))
            self.assertTrue(con.execute("SELECT 1 FROM claims WHERE source_item_id='ret' AND status='rejected'").fetchone())
            self.assertEqual(con.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
        finally:
            con.close()

    def test_promote_zero_or_multiple_matches_uses_deterministic_watchlist_reason(self):
        process_phase4(self.path, "2026-09-07T00:13:30Z")
        self._item("zero")
        self._decision("dzero", "zero", "promote", "2026-09-07T00:13:31Z", matched_observation_ids=[])
        process_phase4(self.path, "2026-09-07T00:13:32Z")
        con = sqlite3.connect(self.path)
        try:
            zero_event = con.execute("SELECT ec.event_id FROM event_claims ec JOIN claims c ON c.claim_id=ec.claim_id WHERE c.source_item_id='zero' LIMIT 1").fetchone()[0]
            self.assertEqual(con.execute("SELECT material_change_reason,verification_state FROM event_versions WHERE event_id=? ORDER BY version DESC LIMIT 1", (zero_event,)).fetchone(), ("promote_unresolved_observation_event", "watchlist"))
            claim_id = con.execute("SELECT claim_id FROM claims WHERE source_item_id='i' ORDER BY claim_id LIMIT 1").fetchone()[0]
            con.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)", ("e2", "r", "ai", "2026-09-07T00:13:33Z", None, 0, 0, "complete"))
            con.commit()
            append_event_versions(con, [EventWrite("e2", "second event", "seed", "unverified", "2026-09-07T00:13:33Z", (claim_id,))])
            con.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)", ("obs-a", None, "e1", "ai", "phase4", "parsed_article", "", "", None, "2026-09-07T00:13:34Z"))
            con.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)", ("obs-b", None, "e2", "ai", "phase4", "parsed_article", "", "", None, "2026-09-07T00:13:34Z"))
            con.commit()
        finally:
            con.close()
        self._item("multiple")
        self._decision("dmultiple", "multiple", "promote", "2026-09-07T00:13:35Z", matched_observation_ids=["obs-a", "obs-b"])
        process_phase4(self.path, "2026-09-07T00:13:36Z")
        con = sqlite3.connect(self.path)
        try:
            multiple_event = con.execute("SELECT ec.event_id FROM event_claims ec JOIN claims c ON c.claim_id=ec.claim_id WHERE c.source_item_id='multiple' LIMIT 1").fetchone()[0]
            self.assertEqual(con.execute("SELECT material_change_reason,verification_state FROM event_versions WHERE event_id=? ORDER BY version DESC LIMIT 1", (multiple_event,)).fetchone(), ("promote_ambiguous_observation_events", "watchlist"))
        finally:
            con.close()

    def test_max_items_progresses_and_replay_is_zero(self):
        process_phase4(self.path, "2026-09-07T00:14:00Z")
        self._item("two"); self._item("three")
        self._decision("d2", "two", decided_at="2026-09-07T00:14:01Z")
        self._decision("d3", "three", decided_at="2026-09-07T00:14:02Z")
        self.assertEqual(process_phase4(self.path, "2026-09-07T00:15:00Z", max_items=1).processed, 1)
        self.assertEqual(process_phase4(self.path, "2026-09-07T00:16:00Z", max_items=1).processed, 1)
        self.assertEqual(process_phase4(self.path, "2026-09-07T00:17:00Z", max_items=1).processed, 0)

    def test_late_malformed_source_rolls_back_earlier_phase4_writes(self):
        self._item("valid")
        self._item("malformed", title=None, body=None)
        self._decision("dv", "valid", decided_at="2026-09-07T00:18:01Z")
        self._decision("dm", "malformed", decided_at="2026-09-07T00:18:02Z")
        before = self._counts()
        with self.assertRaises(ValueError):
            process_phase4(self.path, "2026-09-07T00:19:00Z")
        self.assertEqual(self._counts(), before)

    def test_concurrent_callers_serialize_to_one_result_and_one_replay(self):
        def worker(path, queue):
            result = process_phase4(path, "2026-09-07T00:20:00Z")
            queue.put((result.processed, result.selected))
        ctx = multiprocessing.get_context("fork")
        queue = ctx.Queue()
        processes = [ctx.Process(target=worker, args=(str(self.path), queue)) for _ in range(2)]
        for process in processes: process.start()
        results = [queue.get(timeout=10) for _ in processes]
        for process in processes: process.join(10)
        self.assertEqual(sorted(results), [(0, 0), (1, 1)])
    def test_date_traceability_replay_inserts_zero(self):
        first = process_phase4(self.path, "2026-09-07T00:08:00Z")
        self.assertEqual(first.event_dates_inserted, 1)
        second = process_phase4(self.path, "2026-09-07T00:08:01Z")
        self.assertEqual(second.event_dates_inserted, 0)

    def test_persisted_phase3_correction_appends_verified_version_without_rewrite(self):
        process_phase4(self.path, "2026-09-07T00:23:00Z")
        con = sqlite3.connect(self.path)
        event_id = con.execute("SELECT DISTINCT ec.event_id FROM event_claims ec JOIN claims c ON c.claim_id=ec.claim_id WHERE c.source_item_id='i'").fetchone()[0]
        old = con.execute("SELECT event_id,version,material_change_reason,summary,verification_state,valid_from,superseded_at,verified_at FROM event_versions WHERE event_id=? AND version=1", (event_id,)).fetchone()
        con.close()
        self._item("corr", title="Widget v2.0 corrected", body="Widget v2.0 corrected after an error on 2026-09-06")
        con = sqlite3.connect(self.path)
        con.execute("INSERT INTO observations VALUES (?,?,?,?,?,?,?,?,?,?)", ("obs-corr", None, event_id, "ai", "phase4", "parsed_article", "", "", None, "2026-09-07T00:23:01Z"))
        con.commit(); con.close()
        self._decision("dcorr", "corr", "promote", "2026-09-07T00:23:02Z", matched_observation_ids=["obs-corr"], semantic_reasons=["CORRECTION_OR_RETRACTION"])
        report = process_phase4(self.path, "2026-09-07T00:24:00Z")
        self.assertEqual(report.versions_appended, 1)
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT event_id,version,material_change_reason,summary,verification_state,valid_from FROM event_versions WHERE event_id=? AND version=1", (event_id,)).fetchone(), old[:6])
            self.assertEqual(con.execute("SELECT material_change_reason,verification_state FROM event_versions WHERE event_id=? AND version=2", (event_id,)).fetchone(), ("correction", "verified"))
            self.assertEqual(con.execute("SELECT superseded_at FROM event_versions WHERE event_id=? AND version=1", (event_id,)).fetchone()[0], "2026-09-07T00:24:00Z")
            self.assertIsNone(con.execute("SELECT superseded_at FROM event_versions WHERE event_id=? AND version=2", (event_id,)).fetchone()[0])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM reports").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main()
