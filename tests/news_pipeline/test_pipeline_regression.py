"""Phase 4/5/6 pipeline regression tests — focused coverage of:
1. Ingest -> claim evidence -> verification -> novelty -> subject report flow.
2. Unchanged cross-day URL/event replays suppressed; grounded changed facts yield
   exactly one material-update version; headline rewrites do not.
3. Claim-evidence persisted role/group/authority survives database round-trip.
4. First-party claims verify only when matched to authority; syndicated copies,
   social leads, unknown publishers, and contradictions remain unverified.
5. Stale/undated items obey subject date policies; canonical URL mandatory;
   exactly one report_events subject membership per eligible version.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9
from news_pipeline.schema_v10 import migrate_v10
from news_pipeline.schema_v11 import migrate_v11
from news_pipeline.event_store import (
    append_event_versions,
    process_phase4,
)
from news_pipeline.live_contracts import EvidenceRole, SourceRole, VerificationState
from news_pipeline.provenance import PublisherRule
from news_pipeline.verification import verify_evidence, claim_specific_evidence_row


# ---------------------------------------------------------------------------
# Helper: build a minimal v11 database
# ---------------------------------------------------------------------------

def _make_db(path: Path) -> sqlite3.Connection:
    """Build a v11 database at a caller-owned path that remains reopenable."""
    init_db(str(path))
    conn = sqlite3.connect(path, isolation_level=None)
    conn.execute("PRAGMA foreign_keys=ON")
    migrate_v3(conn, "2026-09-07T00:00:00Z")
    migrate_v4(conn, "2026-09-07T00:00:00Z")
    migrate_v5(conn, "2026-09-07T00:00:00Z")
    migrate_v6(conn, "2026-09-07T00:00:00Z")
    migrate_v7(conn, "2026-09-07T00:00:00Z", rules=[])
    migrate_v8(conn, "2026-09-07T00:00:00Z")
    migrate_v9(conn, "2026-09-07T00:00:00Z")
    migrate_v10(conn, "2026-09-07T00:00:00Z")
    migrate_v11(conn, "2026-09-07T00:00:00Z")
    return conn


def _primary_rule(rule_id: str, entity: str, group: str) -> PublisherRule:
    return PublisherRule(
        rule_id=rule_id,
        host="example.test",
        source_role=SourceRole.PRIMARY,
        independence_group=group,
        categories=("ai",),
        authority_entities=(entity,),
        audit_note="regression fixture rule",
    )


def _insert_run(conn: sqlite3.Connection, run_id: str = "r") -> None:
    conn.execute(
        "INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
        (run_id, "2026-09-07T00:00:00Z", None, "historical_replay", "observed_historical", None, None),
    )


def _insert_source(
    conn: sqlite3.Connection,
    source_id: str,
    role: str = "discovery",
    adapter: str = "rss",
    host: str = "example.test",
    category_scope: str = '["ai"]',
) -> None:
    conn.execute(
        "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            source_id, adapter, role, host, category_scope, 1,
            "[]", "[]", "[]", "[]", "[]", 60,
            None, None, None, "a" * 64, "2026-09-07T00:00:00Z",
        ),
    )


def _insert_source_item(
    conn: sqlite3.Connection,
    item_id: str,
    source_id: str,
    category: str = "ai",
    role: str = "discovery",
    publisher: str = "Example",
    title: str = "Widget",
    body: str = "Widget v2.0 launched on 2026-09-06.",
    original_url: str = "https://example.test/item",
    canonical_url: str | None = None,
    published_at: str = "2026-09-06T00:00:00Z",
    retrieval_method: str = "rss",
    raw_content_hash: str | None = None,
    effective_source_role: str | None = None,
    independence_group: str | None = None,
    matched_rule_id: str | None = None,
    authority_match: int = 0,
    classification_reason: str | None = None,
    authority_scope_json: str = '["ai"]',
    authority_entities_json: str = "[]",
) -> None:
    if raw_content_hash is None:
        raw_content_hash = hashlib.sha256(body.encode()).hexdigest()
    if effective_source_role is None:
        effective_source_role = role
    if independence_group is None:
        independence_group = f"{publisher.lower().replace(' ', '-')}-origin"
    if classification_reason is None:
        classification_reason = "default"
    if canonical_url is None:
        canonical_url = original_url
    # Derive normalized publisher host from canonical URL
    from news_pipeline.canonicalization import canonicalize_url
    normalized_host = canonicalize_url(canonical_url)
    if normalized_host:
        from urllib.parse import urlsplit
        normalized_host = urlsplit(normalized_host).hostname or "unknown"
    else:
        normalized_host = "unknown"
    conn.execute(
        "INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            item_id, source_id, f"{item_id}-ext", category,
            original_url,
            canonical_url,
            publisher, role, None, retrieval_method,
            raw_content_hash, title, body,
            published_at, "2026-09-07T00:00:00Z",
            None, None, None,
        ),
    )
    if matched_rule_id:
        # Only insert provenance when the publisher rule is actually present in
        # the registry (avoids FK constraint failures in tests seeded with
        # an empty registry via migrate_v7(..., rules=[])).
        rule_exists = conn.execute(
            "SELECT 1 FROM publisher_registry WHERE rule_id=?",
            (matched_rule_id,),
        ).fetchone() is not None
        if rule_exists:
            conn.execute(
                "INSERT INTO source_item_provenance VALUES (?,?,?,?,?,?,?,?)",
                (
                    item_id,
                    normalized_host,
                    effective_source_role,
                    independence_group,
                    matched_rule_id,
                    authority_match,
                    "2026-09-07T00:00:00Z",
                    classification_reason,
                ),
            )
    conn.execute(
        "INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
        (
            f"d-{item_id}", "r", None, "keep",
            json.dumps({"source_item_id": item_id}),
            "2026-09-07T00:01:00Z",
            "phase3",
        ),
    )


class RegressionCrossDayReplayTests(unittest.TestCase):
    """Tests area 2: cross-day URL/event replays, material updates, headline rewrites."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "replay.db"
        init_db(str(self.path))
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        migrate_v3(self.conn, "2026-09-07T00:00:00Z")
        migrate_v4(self.conn, "2026-09-07T00:00:00Z")
        migrate_v5(self.conn, "2026-09-07T00:00:00Z")
        migrate_v6(self.conn, "2026-09-07T00:00:00Z")
        migrate_v7(self.conn, "2026-09-07T00:00:00Z", rules=[])
        migrate_v8(self.conn, "2026-09-07T00:00:00Z")
        migrate_v9(self.conn, "2026-09-07T00:00:00Z")
        migrate_v10(self.conn, "2026-09-07T00:00:00Z")
        migrate_v11(self.conn, "2026-09-07T00:00:00Z")
        _insert_run(self.conn)
        _insert_source(self.conn, "src", "discovery")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_identical_url_same_day_is_replay_noop(self):
        """Same URL, same content, same day — second run produces no new event."""
        _insert_source_item(self.conn, "item-a", "src",
                             published_at="2026-09-06T00:00:00Z")
        self.conn.commit()
        self.conn.close()
        first = process_phase4(self.path, "2026-09-07T08:00:00Z")
        self.assertEqual(first.versions_appended, 1)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        second = process_phase4(self.path, "2026-09-07T09:00:00Z")
        self.assertEqual(second.versions_appended, 0)

    def test_changed_fact_yields_exactly_one_material_update_version(self):
        """Grounded changed facts — a real version bump — creates exactly one new version."""
        _insert_source_item(self.conn, "item-old", "src",
                            body="Widget v2.0 launched on 2026-09-06.")
        self.conn.commit()
        self.conn.close()
        process_phase4(self.path, "2026-09-07T08:00:00Z")
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        event_id = self.conn.execute(
            "SELECT event_id FROM event_versions ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        counts_before = self.conn.execute(
            "SELECT COUNT(*) FROM event_versions"
        ).fetchone()[0]
        self.conn.execute(
            """INSERT INTO observations(
                   id,article_id,event_id,category,source_file,kind,body,raw,
                   occurred_at,created_at)
               VALUES(?,NULL,?,?,?,?,?,?,?,?)""",
            (
                "obs-item-old",
                event_id,
                "ai",
                "fixture.md",
                "parsed_article",
                "Widget v2.0 launched",
                None,
                "2026-09-06T00:00:00Z",
                "2026-09-06T00:00:00Z",
            ),
        )
        _insert_source_item(
            self.conn,
            "item-new",
            "src",
            title="Widget",
            body="Widget v2.1 launched on 2026-09-07.",
        )
        self.conn.execute(
            "UPDATE decisions SET decision_kind='promote',reason=? WHERE id='d-item-new'",
            (
                json.dumps({
                    "source_item_id": "item-new",
                    "matched_observation_ids": ["obs-item-old"],
                    "semantic_decision": "material_update",
                    "fact_deltas": [{
                        "kind": "version",
                        "unit": "semver",
                        "old_value": "2.0",
                        "new_value": "2.1",
                        "topic_gate": "0.8",
                    }],
                }),
            ),
        )
        self.conn.commit()
        self.conn.close()
        result = process_phase4(self.path, "2026-09-07T09:00:00Z")
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        counts_after = self.conn.execute(
            "SELECT COUNT(*) FROM event_versions"
        ).fetchone()[0]
        self.assertEqual(result.versions_appended, 1)
        self.assertEqual(counts_after - counts_before, 1)
        max_ver = self.conn.execute(
            "SELECT MAX(version) FROM event_versions"
        ).fetchone()[0]
        self.assertEqual(max_ver, 2)

    def test_headline_rewrite_does_not_create_material_update_version(self):
        """Only substantive fact changes (body content) create material-update versions;
        title-only changes do not."""
        _insert_source_item(self.conn, "item-title-v1", "src",
                            title="Widget v2.0 launched",
                            body="Widget v2.0 launched on 2026-09-06.")
        self.conn.commit()
        self.conn.close()
        process_phase4(self.path, "2026-09-07T08:00:00Z")
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        original_version = self.conn.execute(
            "SELECT version FROM event_versions"
        ).fetchone()[0]
        self.assertEqual(original_version, 1)
        self.conn.execute(
            "UPDATE source_items SET title=? WHERE source_item_id='item-title-v1'",
            ("Widget v2.0 is launched",),
        )
        self.conn.commit()
        self.conn.close()
        result = process_phase4(self.path, "2026-09-07T09:00:00Z")
        self.assertEqual(result.versions_appended, 0)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.assertEqual(
            self.conn.execute("SELECT MAX(version) FROM event_versions").fetchone()[0],
            1,
        )


class RegressionPersistenceTests(unittest.TestCase):
    """Tests area 3: claim-evidence persisted role/group/authority survives DB round-trip."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "persist.db"
        init_db(str(self.path))
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        migrate_v3(self.conn, "2026-09-07T00:00:00Z")
        migrate_v4(self.conn, "2026-09-07T00:00:00Z")
        migrate_v5(self.conn, "2026-09-07T00:00:00Z")
        migrate_v6(self.conn, "2026-09-07T00:00:00Z")
        migrate_v7(
            self.conn,
            "2026-09-07T00:00:00Z",
            rules=(_primary_rule("openai-widget-announcements", "Widget", "openai-origin"),),
        )
        migrate_v8(self.conn, "2026-09-07T00:00:00Z")
        migrate_v9(self.conn, "2026-09-07T00:00:00Z")
        migrate_v10(self.conn, "2026-09-07T00:00:00Z")
        migrate_v11(self.conn, "2026-09-07T00:00:00Z")
        _insert_run(self.conn)
        _insert_source(self.conn, "primary-src", "primary")
        _insert_source(self.conn, "specialist-src", "specialist")
        _insert_source(self.conn, "discovery-src", "discovery")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_role_and_group_and_authority_survive_db_roundtrip(self):
        """Role, independence_group, and authority match written at phase4 are
        read back identically after a new connection."""
        _insert_source_item(self.conn, "persist-item", "primary-src",
                            role="primary",
                            publisher="OpenAI",
                            retrieval_method="publisher-site",
                            effective_source_role="primary",
                            independence_group="openai-origin",
                            matched_rule_id="openai-widget-announcements",
                            authority_match=1,
                            classification_reason="matched_rule",
                            authority_entities_json='["Widget"]')
        self.conn.commit()
        self.conn.close()
        process_phase4(self.path, "2026-09-07T08:00:00Z")
        conn2 = sqlite3.connect(self.path)
        conn2.execute("PRAGMA foreign_keys=ON")
        evidence_row = conn2.execute(
            "SELECT ce.evidence_id,ce.exact_excerpt,ce.excerpt_hash,si.publisher,ce.evidence_role,"
            "cp.independence_group FROM claim_evidence ce "
            "JOIN source_items si ON si.source_item_id=ce.source_item_id "
            "LEFT JOIN claim_evidence_provenance cp ON cp.evidence_id=ce.evidence_id "
            "WHERE ce.source_item_id='persist-item'",
        ).fetchone()
        conn2.close()
        self.assertIsNotNone(evidence_row)
        ev_id, excerpt, exc_hash, publisher, ev_role, ig = evidence_row
        self.assertEqual(ev_role, EvidenceRole.SUPPORTS.value)
        self.assertEqual(ig, "openai-origin")
        self.assertEqual(publisher, "OpenAI")
        self.assertEqual(exc_hash, hashlib.sha256(excerpt.encode()).hexdigest())

    def test_different_roles_produce_different_verification_outcomes(self):
        """Enabled primary/specialist registry rows verify singly; discovery does not."""
        for item_id, role, ig, expected_state in [
            ("ev-primary", "primary", "openai-origin", VerificationState.VERIFIED),
            ("ev-specialist", "specialist", "trade-press", VerificationState.VERIFIED),
            ("ev-discovery", "discovery", "search-index", VerificationState.UNVERIFIED),
        ]:
            with self.subTest(item_id=item_id, expected_state=expected_state):
                row = (
                    "supports", role, ig, 1,
                    f"rule-{item_id}",
                    "example.test",
                    '["ai"]',
                    '["OpenAI"]',
                    "ai",
                )
                evidence = claim_specific_evidence_row(
                    row, "OpenAI",
                    registry_enabled=role in {"primary", "specialist"},
                    registry_role=role if role in {"primary", "specialist"} else None,
                    registry_group=ig if role in {"primary", "specialist"} else None,
                    registry_host="example.test" if role in {"primary", "specialist"} else None,
                    registry_categories='["ai"]' if role in {"primary", "specialist"} else None,
                )
                result = verify_evidence([evidence])
                self.assertEqual(result, expected_state, f"{item_id}: expected {expected_state}, got {result}")


class RegressionVerificationAuthorityTests(unittest.TestCase):
    """Tests area 4: first-party claims verify only with authority match;
    syndicated copies, social leads, unknown publishers, contradictions stay unverified."""

    def test_synicated_copy_has_no_authority(self):
        """A syndicated copy has role=discovery, not primary, and no authority."""
        row = (
            "supports", "discovery", "syndicated-wire", 0,
            None, "wire-service.example",
            '["ai"]', "[]", "ai",
        )
        evidence = claim_specific_evidence_row(row, None)
        self.assertEqual(verify_evidence([evidence]), VerificationState.UNVERIFIED)

    def test_social_lead_is_not_independent_authority(self):
        """Social-media-sourced item cannot be PRIMARY or carry independence group."""
        row = (
            "supports", "discovery", "social-lead", 0,
            None, "social.example",
            '["ai"]', "[]", "ai",
        )
        evidence = claim_specific_evidence_row(row, None)
        self.assertEqual(verify_evidence([evidence]), VerificationState.UNVERIFIED)

    def test_unknown_publisher_without_rule_is_unverified(self):
        """An unknown publisher with no matched rule stays unverified."""
        row = (
            "supports", "discovery", "unknown-group", 0,
            None, "unknown.example",
            '["ai"]', "[]", "ai",
        )
        evidence = claim_specific_evidence_row(row, "Unknown Entity")
        self.assertEqual(verify_evidence([evidence]), VerificationState.UNVERIFIED)

    def test_contradiction_is_watchlist_not_verified(self):
        """A contradicts role yields WATCHLIST even with primary + authority."""
        row = (
            "contradicts", "primary", "openai-origin", 1,
            "openai-own-announcements",
            "example.test",
            '["ai"]',
            '["OpenAI"]',
            "ai",
        )
        evidence = claim_specific_evidence_row(row, "OpenAI")
        self.assertEqual(verify_evidence([evidence]), VerificationState.WATCHLIST)


class RegressionSubjectPolicyTests(unittest.TestCase):
    """Tests area 5: stale/undated items obey date policies; canonical URL
    mandatory; exactly one report_events membership per eligible version."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "policy.db"
        init_db(str(self.path))
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        migrate_v3(self.conn, "2026-09-07T00:00:00Z")
        migrate_v4(self.conn, "2026-09-07T00:00:00Z")
        migrate_v5(self.conn, "2026-09-07T00:00:00Z")
        migrate_v6(self.conn, "2026-09-07T00:00:00Z")
        migrate_v7(
            self.conn,
            "2026-09-07T00:00:00Z",
            rules=(_primary_rule("widget-primary", "Widget", "widget-origin"),),
        )
        migrate_v8(self.conn, "2026-09-07T00:00:00Z")
        migrate_v9(self.conn, "2026-09-07T00:00:00Z")
        migrate_v10(self.conn, "2026-09-07T00:00:00Z")
        migrate_v11(self.conn, "2026-09-07T00:00:00Z")
        _insert_run(self.conn)
        _insert_source(self.conn, "src", "discovery")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_missing_canonical_url_rejected_at_ingest(self):
        """A source item with no canonical URL fails schema NOT NULL constraint."""
        item = {
            "source_item_id": "no-canon",
            "source_id": "src",
            "category": "ai",
            "original_url": "https://example.test/no-canon",
            "canonical_url": None,
            "publisher": "Example",
            "retrieval_method": "rss",
            "raw_content_hash": "c" * 64,
            "published_at": "2026-09-06T00:00:00Z",
        }
        with self.assertRaises(sqlite3.IntegrityError):
            self.conn.execute(
                """INSERT INTO source_items
                   (source_item_id,source_id,category,original_url,canonical_url,
                    publisher,retrieval_method,raw_content_hash,published_at,
                    retrieved_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    item["source_item_id"], item["source_id"], item["category"],
                    item["original_url"], item["canonical_url"],
                    item["publisher"], item["retrieval_method"],
                    item["raw_content_hash"], item["published_at"],
                    "2026-09-07T00:00:00Z",
                ),
            )

    def test_stale_item_without_published_at_has_missing_date_reason(self):
        """An item with no publication date has unknown_date_reason=missing
        and publication_evidence=None (date is unknown, not traced to a source)."""
        from news_pipeline.claim_pipeline import observation_from_source_item
        item = {
            "source_item_id": "undated-item",
            "source_id": "src",
            "category": "ai",
            "original_url": "https://example.test/undated",
            "canonical_url": "https://example.test/undated",
            "publisher": "Example",
            "retrieval_method": "rss",
            "raw_content_hash": "b" * 64,
            "published_at": None,
            "updated_at": None,
            "publication_evidence": None,
            "retrieved_at": "2026-09-07T00:00:00Z",
        }
        obs = observation_from_source_item(item)
        self.assertIsNone(obs.published_at)
        self.assertIsNone(obs.publication_evidence)
        self.assertEqual(obs.unknown_date_reason, "missing")

    def test_exactly_one_report_events_per_eligible_version(self):
        """Exactly one report_events row per eligible event version (no duplicates)."""
        with self.assertRaisesRegex(ValueError, "claim_ids"):
            append_event_versions(self.conn, [{
                "event_id": "unlinked-event",
                "summary": "Unlinked event",
                "material_change_reason": "distinct",
                "verification_state": "unverified",
                "valid_from": "2026-09-08T06:02:00Z",
                "claim_ids": (),
            }])

        _insert_source_item(
            self.conn,
            "report-item",
            "src",
            role="primary",
            publisher="Widget",
            retrieval_method="publisher-site",
            effective_source_role="primary",
            independence_group="widget-origin",
            matched_rule_id="widget-primary",
            authority_match=1,
            classification_reason="matched_rule",
            authority_entities_json='["Widget"]',
            published_at="2026-09-08T06:00:00Z",
        )
        self.conn.commit()
        processed = process_phase4(self.path, "2026-09-08T06:02:00Z")
        self.assertEqual((processed.selected, processed.versions_appended), (1, 1))

        from news_pipeline.report_builder import run_report

        as_of = datetime(2026, 9, 8, 9, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as artifacts_root:
            first = run_report(self.path, Path(artifacts_root), as_of)
            replay = run_report(self.path, Path(artifacts_root), as_of)

        self.assertEqual(first.included_count, 1)
        self.assertTrue(replay.was_replayed)
        links = self.conn.execute(
            """SELECT event_id,event_version,COUNT(*)
                 FROM report_events WHERE report_id=?
                GROUP BY event_id,event_version""",
            (first.report_id,),
        ).fetchall()
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0][2], 1)


class RegressionIngestFlowTests(unittest.TestCase):
    """Tests area 1: full ingest -> claim -> evidence -> verification ->
    novelty -> subject report flow end-to-end."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "flow.db"
        init_db(str(self.path))
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA foreign_keys=ON")
        migrate_v3(self.conn, "2026-09-07T00:00:00Z")
        migrate_v4(self.conn, "2026-09-07T00:00:00Z")
        migrate_v5(self.conn, "2026-09-07T00:00:00Z")
        migrate_v6(self.conn, "2026-09-07T00:00:00Z")
        migrate_v7(
            self.conn,
            "2026-09-07T00:00:00Z",
            rules=(_primary_rule("openai-widget-announcements", "Widget", "openai-origin"),),
        )
        migrate_v8(self.conn, "2026-09-07T00:00:00Z")
        migrate_v9(self.conn, "2026-09-07T00:00:00Z")
        migrate_v10(self.conn, "2026-09-07T00:00:00Z")
        migrate_v11(self.conn, "2026-09-07T00:00:00Z")
        _insert_run(self.conn)
        _insert_source(self.conn, "primary-openai", "primary")
        _insert_source(self.conn, "specialist-phoronix", "specialist")
        _insert_source(self.conn, "discovery-src", "discovery")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def test_ingest_to_report_flow_creates_one_verified_event(self):
        """Full flow: source item -> claim+evidence -> verified event -> report."""
        _insert_source_item(
            self.conn, "flow-item-1", "primary-openai",
            role="primary",
            publisher="OpenAI",
            retrieval_method="publisher-site",
            effective_source_role="primary",
            independence_group="openai-origin",
            matched_rule_id="openai-widget-announcements",
            authority_match=1,
            classification_reason="matched_rule",
            authority_entities_json='["Widget"]',
        )
        self.conn.commit()
        self.conn.close()
        report = process_phase4(self.path, "2026-09-07T08:00:00Z")
        self.assertEqual(report.events_created, 1)
        self.assertEqual(report.versions_appended, 1)
        con2 = sqlite3.connect(self.path)
        con2.execute("PRAGMA foreign_keys=ON")
        try:
            ev_state = con2.execute(
                "SELECT verification_state FROM event_versions"
            ).fetchone()[0]
            self.assertEqual(ev_state, "verified")
            claim_status = con2.execute(
                "SELECT status FROM claims WHERE source_item_id='flow-item-1'"
            ).fetchone()[0]
            self.assertEqual(claim_status, "verified")
        finally:
            con2.close()

    def test_claim_evidence_persists_through_event_write_and_read(self):
        """Evidence row written at phase4 survives event write and subsequent read."""
        _insert_source_item(
            self.conn, "ev-persist", "primary-openai",
            role="primary",
            publisher="OpenAI",
            retrieval_method="publisher-site",
            effective_source_role="primary",
            independence_group="openai-origin",
            matched_rule_id="openai-widget-announcements",
            authority_match=1,
            classification_reason="matched_rule",
        )
        self.conn.commit()
        self.conn.close()
        process_phase4(self.path, "2026-09-07T08:00:00Z")
        conn2 = sqlite3.connect(self.path)
        conn2.execute("PRAGMA foreign_keys=ON")
        try:
            row = conn2.execute(
                "SELECT ce.evidence_role,cp.independence_group,si.publisher FROM claim_evidence ce "
                "JOIN source_items si ON si.source_item_id=ce.source_item_id "
                "LEFT JOIN claim_evidence_provenance cp ON cp.evidence_id=ce.evidence_id "
                "WHERE ce.source_item_id='ev-persist'"
            ).fetchone()
            self.assertIsNotNone(row)
            ev_role, ig, pub = row
        finally:
            conn2.close()
