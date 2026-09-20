from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from decimal import Decimal
from datetime import timedelta
from pathlib import Path

from news_pipeline.adjudication import classify_pair
from news_pipeline.briefing_ledger import (
    BriefingLedger,
    ShadowEvent,
    event_version_identity,
    normalize_canonical_payload,
)
from news_pipeline.clustering import cluster_id_for_url, event_id_for_match, event_version_for_match, score_pair
from news_pipeline.contracts import CandidateArticle, DecisionCode, FilterResult, HistoryMatch, ReasonCode, TrustTier
from news_pipeline.db import connect, init_db
from news_pipeline.event_contracts import EventCandidate, FactDelta, FactKind, SemanticDecision, SemanticReasonCode
from news_pipeline.event_store import EventWrite, append_event_versions, event_version_key
from news_pipeline.history import find_exact_url, open_history
from news_pipeline.models import Category, Subject, SubjectDecision, assign_primary_subject
from news_pipeline.phase3_api import evaluate_semantic_updates
from news_pipeline.policies import QueryPolicy
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5


TS = "2026-09-20T00:00:00Z"


def _candidate(*, cid="candidate-1", category=Category.AI, title="Product launch", snippet="The product is available now", url="https://feed.example.com/story"):
    return CandidateArticle(
        candidate_id=cid,
        category=category,
        query_group="ai",
        title=title,
        snippet=snippet,
        original_url=url,
        canonical_url=url,
        published_at="2026-09-19T00:00:00Z",
        published_evidence="source",
        observed_at=TS,
        evaluated_at=TS,
    )


def _history(*, aid="history-1", title="Product launch announced", snippet="The product was announced", url=None, event_id=None, event_version=None):
    return HistoryMatch(
        article_id=aid,
        observation_id="observation-" + aid,
        category=Category.AI,
        occurred_at="2026-09-18T00:00:00Z",
        title=title,
        snippet=snippet,
        canonical_url=url,
        identity_basis="title_only",
        event_id=event_id,
        event_version=event_version,
    )


def _event(candidate):
    policy = QueryPolicy(
        category=candidate.category,
        allowed_query_groups=(candidate.category.value,),
        recency=timedelta(days=7),
        missing_date_fallback=True,
        exact_title_lookback=timedelta(days=7),
        exact_url_lookback=timedelta(days=7),
        exact_identity_lookback=timedelta(days=7),
        cross_category_exact_url=True,
    )
    filtered = FilterResult(
        candidate=candidate,
        decision=DecisionCode.KEEP,
        reasons=(ReasonCode.OK_KEEP,),
        matched_article_ids=(),
        matched_observation_ids=(),
        trust_tier=TrustTier.UNKNOWN,
        evaluated_publication_time=None,
        ordinal=0,
    )
    return EventCandidate(candidate=candidate, filter_result=filtered, query_policy=policy)


class EventNoveltyContractTests(unittest.TestCase):
    def test_phase3_assigns_durable_identity_and_first_version(self):
        result = evaluate_semantic_updates((_event(_candidate()),), (), lambda _request: "", 0)[0]
        self.assertIs(result.semantic_decision, SemanticDecision.distinct_event)
        self.assertEqual(result.event_id, cluster_id_for_url("https://feed.example.com/story"))
        self.assertEqual(result.event_version, 1)
        self.assertEqual(result.subject_id, Subject.AI.value)

    def test_phase3_suppresses_secondary_subject_for_same_event(self):
        url = "https://feed.example.com/shared-event"
        audio = _candidate(cid="audio", category=Category.AUDIO_ENGINEERING, url=url)
        av = _candidate(cid="av", category=Category.AUDIOVISUAL, url=url)
        results = evaluate_semantic_updates((_event(audio), _event(av)), (), lambda _request: "", 0)
        self.assertEqual(results[0].event_id, results[1].event_id)
        self.assertEqual(results[0].subject_id, Subject.AUDIO_ENGINEERING.value)
        self.assertFalse(results[0].subject_suppressed)
        self.assertEqual(results[1].subject_id, Subject.PROFESSIONAL_AV.value)
        self.assertTrue(results[1].subject_suppressed)
        self.assertIn(SemanticReasonCode.SUBJECT_COLLISION_SUPPRESSED, results[1].semantic_reasons)

    def test_lifecycle_material_update_has_grounded_state_delta(self):
        candidate = _candidate(title="Product launch launched", snippet="The product is available now")
        history = _history()
        verdict = classify_pair(candidate, history, score_pair(candidate, history))
        self.assertEqual(verdict.decision.value, "material_update")
        self.assertEqual(len(verdict.fact_deltas), 1)
        self.assertEqual(verdict.fact_deltas[0].kind.value, "state")
        self.assertEqual((verdict.fact_deltas[0].old_value, verdict.fact_deltas[0].new_value), ("announced", "shipped"))

    def test_persisted_event_identity_wins_over_url_prefilter(self):
        candidate = _candidate()
        history = _history(event_id="event-authority", event_version=4)
        self.assertEqual(event_id_for_match(candidate, history), "event-authority")
        self.assertEqual(event_version_for_match(history), 4)
        identity = event_version_identity("ai", "event-authority", 4)
        self.assertRegex(identity, r"^evv1:[0-9a-f]{64}$")
        self.assertNotEqual(identity, event_version_identity("professional_av", "event-authority", 4))

    def test_subject_collision_has_one_primary_and_one_suppression(self):
        route = assign_primary_subject((Subject.AUDIO_ENGINEERING, Subject.PROFESSIONAL_AV))
        self.assertEqual(route.decision, SubjectDecision.ASSIGNED)
        self.assertEqual(route.primary_subject, Subject.AUDIO_ENGINEERING)
        self.assertEqual(route.suppressed_subjects, (Subject.PROFESSIONAL_AV,))
        self.assertEqual(route.reason, "deterministic_primary_subject")
        preferred = assign_primary_subject(
            (Subject.AUDIO_ENGINEERING, Subject.PROFESSIONAL_AV),
            preferred_subject=Subject.PROFESSIONAL_AV,
        )
        self.assertEqual(preferred.primary_subject, Subject.PROFESSIONAL_AV)
        self.assertEqual(preferred.suppressed_subjects, (Subject.AUDIO_ENGINEERING,))


class EventStoreVersionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "events.db"
        init_db(str(self.path))
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        migrate_v3(con, "2026-09-20T00:00:00Z")
        migrate_v4(con, "2026-09-20T00:00:01Z")
        migrate_v5(con, "2026-09-20T00:00:02Z")
        con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?)", ("run-1", TS, None, "historical_replay", "observed_historical", None, None))
        con.execute("INSERT INTO events VALUES (?,?,?,?,?,?,?,?)", ("event-1", "run-1", "ai", TS, None, 0, 0, "complete"))
        con.execute("INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("source-1", "rss", "primary", "feed.example.com", '["ai"]', 1, "[]", "[]", "[]", "[]", "[]", 60, None, None, None, "a" * 64, TS))
        con.execute("INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", ("item-1", "source-1", "ext-1", "ai", "https://feed.example.com/story", "https://feed.example.com/story", "Publisher", "primary", None, "rss", "b" * 64, "Story", "Story body", None, TS, None, None, None))
        con.execute("INSERT INTO claims VALUES (?,?,?,?,?,?,?,?,?)", ("claim-1", "item-1", "product", "has_version", "1.0", "version", "0.9", "verified", TS))
        con.commit()
        con.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_rewrite_is_suppressed_and_material_update_advances_exact_version(self):
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA foreign_keys=ON")
        delta = FactDelta(FactKind.DATE, "release_date", "2026-10-01", "2026-10-15", Decimal("0.9"))
        self.assertEqual(
            append_event_versions(
                con,
                [EventWrite("event-1", "Initial", "distinct", "verified", TS, ("claim-1",), semantic_decision="distinct_event", event_version=1)],
            ),
            1,
        )
        self.assertEqual(
            append_event_versions(
                con,
                [EventWrite("event-1", "Headline rewrite", "same_facts", "verified", "2026-09-20T00:01:00Z", (), semantic_decision="rewrite")],
            ),
            0,
        )
        self.assertEqual(con.execute("SELECT MAX(version) FROM event_versions WHERE event_id='event-1'").fetchone()[0], 1)
        self.assertEqual(
            append_event_versions(
                con,
                [EventWrite("event-1", "Updated", "release_date_changed", "verified", "2026-09-20T00:02:00Z", ("claim-1",), semantic_decision="material_update", fact_deltas=(delta,), event_version=2)],
            ),
            1,
        )
        with self.assertRaisesRegex(ValueError, "grounded"):
            append_event_versions(
                con,
                [EventWrite("event-1", "Ungrounded", "unknown", "verified", "2026-09-20T00:03:00Z", ("claim-1",), semantic_decision="material_update")],
            )
        with self.assertRaisesRegex(ValueError, "grounded"):
            append_event_versions(
                con,
                [EventWrite("event-1", "Malformed", "unknown", "verified", "2026-09-20T00:03:30Z", ("claim-1",), semantic_decision="material_update", fact_deltas=({"kind": "date", "unit": "release_date", "old_value": "2026-10-01", "new_value": "2026-10-15", "topic_gate": "not-a-decimal"},))],
            )
        with self.assertRaisesRegex(ValueError, "stale"):
            append_event_versions(
                con,
                [EventWrite("event-1", "Stale", "release_date_changed", "verified", "2026-09-20T00:04:00Z", ("claim-1",), semantic_decision="material_update", fact_deltas=(delta,), event_version=2)],
            )
        self.assertEqual(con.execute("SELECT MAX(version) FROM event_versions WHERE event_id='event-1'").fetchone()[0], 2)
        self.assertEqual(
            append_event_versions(
                con,
                [EventWrite("event-1", "Suppressed", "release_date_changed", "verified", "2026-09-20T00:05:00Z", ("claim-1",), semantic_decision="material_update", fact_deltas=(delta,), event_version=3, subject_suppressed=True)],
            ),
            0,
        )
        self.assertEqual(con.execute("SELECT MAX(version) FROM event_versions WHERE event_id='event-1'").fetchone()[0], 2)
        con.close()

    def test_event_version_key_is_subject_scoped(self):
        self.assertEqual(event_version_key("audio_engineering", "event-1", 2), event_version_identity("audio_engineering", "event-1", 2))
        self.assertNotEqual(event_version_key("audio_engineering", "event-1", 2), event_version_key("professional_av", "event-1", 2))


class HistoryEventVersionTests(unittest.TestCase):
    def test_history_enriches_event_id_with_latest_version(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = str(Path(tmp) / "history.db")
            init_db(path)
            with connect(path) as con:
                con.execute(
                    "INSERT INTO runs(id,started_at,kind,provenance) VALUES (?,?,?,?)",
                    ("run-1", "2026-09-01T00:00:00Z", "historical_replay", "observed_historical"),
                )
                con.execute(
                    "INSERT INTO events(id,run_id,category,started_at,status) VALUES (?,?,?,?,?)",
                    ("event-1", "run-1", "ai", "2026-09-01T00:00:00Z", "complete"),
                )
                con.execute(
                    "INSERT INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,source_file,observed_at,provenance,created_at,normalized_title,identity_confidence,identity_basis) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    ("article-1", "run-1", "ai", "https://feed.example.com/story", "https://feed.example.com/story", "Story", "snippet", "ai.md", "2026-09-01T00:00:00Z", "observed_historical", "2026-09-01T00:00:00Z", "story", 1.0, "canonical_url"),
                )
                con.execute(
                    "INSERT INTO observations(id,article_id,event_id,category,source_file,kind,occurred_at,created_at) VALUES (?,?,?,?,?,?,?,?)",
                    ("obs-1", "article-1", "event-1", "ai", "ai.md", "parsed_article", "2026-09-01T00:00:00Z", "2026-09-01T00:00:00Z"),
                )
                con.execute(
                    "CREATE TABLE event_versions(event_id TEXT NOT NULL, version INTEGER NOT NULL, PRIMARY KEY(event_id, version))"
                )
                con.executemany(
                    "INSERT INTO event_versions(event_id,version) VALUES (?,?)",
                    (("event-1", 1), ("event-1", 3)),
                )
            with open_history(path) as history:
                match = find_exact_url(
                    history,
                    "https://feed.example.com/story",
                    "2026-09-02T00:00:00Z",
                    timedelta(days=7),
                )
            self.assertIsNotNone(match)
            self.assertEqual(match.event_id, "event-1")
            self.assertEqual(match.event_version, 3)


class BriefingLedgerEventIdentityTests(unittest.TestCase):
    def test_same_event_version_is_seen_once_but_later_payload_is_audit_only(self):
        con = sqlite3.connect(":memory:", isolation_level=None)
        ledger = BriefingLedger(con)
        ledger.initialize_schema()
        event = ShadowEvent(
            subject_id="audio_engineering",
            event_id="event-1",
            event_version=1,
            category=Category.AUDIO_ENGINEERING,
            decision=SemanticDecision.distinct_event,
            payload_json=normalize_canonical_payload({"event_id": "event-1", "event_version": 1, "subject_id": "audio_engineering", "summary": "first"}),
            recorded_at_utc=TS,
        )
        self.assertEqual(event.candidate_id, event_version_identity("audio_engineering", "event-1", 1))
        ledger.begin_run("run-1", "2026-09-19T00:00:00Z", "2026-09-20T00:00:00Z", TS)
        first = ledger.complete_run("run-1", TS, (event,))
        self.assertEqual(first.new_ids, (event.candidate_id,))
        replay = ShadowEvent(
            subject_id="audio_engineering",
            event_id="event-1",
            event_version=1,
            category=Category.AUDIO_ENGINEERING,
            decision=SemanticDecision.distinct_event,
            payload_json=normalize_canonical_payload({"event_id": "event-1", "event_version": 1, "subject_id": "audio_engineering", "summary": "later copy"}),
            recorded_at_utc="2026-09-20T00:01:00Z",
        )
        ledger.begin_run("run-2", "2026-09-20T00:00:00Z", "2026-09-21T00:00:00Z", "2026-09-20T00:01:00Z")
        second = ledger.complete_run("run-2", "2026-09-20T00:01:00Z", (replay,))
        self.assertEqual(second.already_seen_ids, (event.candidate_id,))
        first_payload = con.execute("SELECT payload_json FROM shadow_briefing_seen WHERE candidate_id=?", (event.candidate_id,)).fetchone()[0]
        later_payload = con.execute("SELECT payload_json FROM shadow_briefing_run_events WHERE run_id='run-2'").fetchone()[0]
        self.assertIn('"summary":"first"', first_payload)
        self.assertIn('"summary":"later copy"', later_payload)
        con.close()


if __name__ == "__main__":
    unittest.main()
