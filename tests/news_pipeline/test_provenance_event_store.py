from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.event_store import (
    process_phase4,
    regenerate_release_claim,
    reverify_source_item_claims,
)
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9
from news_pipeline.schema_v10 import migrate_v10
from news_pipeline.schema_v11 import migrate_v11
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


class FirstPartyReleaseEvidenceTests(unittest.TestCase):
    URL = "https://github.com/ggml-org/llama.cpp/releases/tag/v0.6.0"
    RAW_PUBLISHED = "2026-10-05T16:56:22Z"
    EVALUATED = "2026-10-06T12:00:00Z"

    def _build(self, **override) -> tuple[tempfile.TemporaryDirectory, Path]:
        cfg = {
            "source_id": "github-llamacpp-release", "category": "our_setup",
            "publisher": "llama.cpp", "method": "github-release-api",
            "url": self.URL, "published_at": self.RAW_PUBLISHED,
            "evidence": f"metadata:{self.RAW_PUBLISHED}",
            "raw_published": self.RAW_PUBLISHED, "raw_url": self.URL,
            "raw_tag": "v0.6.0", "title": "v0.6.0", "rule_id": "llama-cpp-own-release",
            "entities": ("llama.cpp",), "group": "ggml-org-llama.cpp-origin",
            "scope": ("our_setup",), "enabled": True,
        }
        cfg.update(override)
        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / "state.db"
        init_db(str(path))
        c = sqlite3.connect(path)
        c.execute("PRAGMA foreign_keys=ON")
        for n, fn in enumerate((migrate_v3, migrate_v4, migrate_v5, migrate_v6, migrate_v7)):
            fn(c, f"2026-10-05T00:00:0{n}Z")
        c.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?)",
                  ("r", APPLIED_AT, None, "historical_replay", "observed_historical", None, None))
        c.execute(
            "INSERT INTO source_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (cfg["source_id"], "rss", "primary", "github.com", '["our_setup"]', 1, "[]", "[]", "[]", "[]", "[]", 60, None, None, None, "a" * 64, APPLIED_AT),
        )
        raw = json.dumps({"id": 1, "tag_name": cfg["raw_tag"], "name": "v0.6.0",
                          "html_url": cfg["raw_url"], "published_at": cfg["raw_published"],
                          "updated_at": None, "body": "notes"}, sort_keys=True)
        c.execute(
            "INSERT INTO source_items VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("i", cfg["source_id"], "1", cfg["category"], cfg["url"], cfg["url"], cfg["publisher"],
             "primary", None, cfg["method"], "b" * 64, cfg["title"], "notes", raw,
             "2026-10-06T07:00:00Z", cfg["published_at"], None, cfg["evidence"]),
        )
        c.execute("INSERT INTO decisions VALUES (?,?,?,?,?,?,?)",
                  ("d", "r", None, "keep", json.dumps({"source_item_id": "i"}), "2026-10-06T07:01:00Z", "phase3"))
        c.commit()
        c.close()
        process_phase4(path, "2026-10-06T07:02:00Z")
        c = sqlite3.connect(path)
        c.execute("PRAGMA foreign_keys=ON")
        rule = PublisherRule(
            cfg["rule_id"], "github.com", SourceRole.PRIMARY, cfg["group"],
            cfg["scope"], cfg["entities"], audit_note="reviewed fixture",
        )
        backfill_source_item_provenance(
            c, PublisherRegistry((rule,)), "2026-10-06T07:03:00Z", source_item_ids=("i",)
        )
        if not cfg["enabled"]:
            c.execute("UPDATE publisher_registry SET enabled=0")
        c.commit()
        c.close()
        return directory, path

    def _run(self, **override):
        directory, path = self._build(**override)
        try:
            result = reverify_source_item_claims(path, "i", override.pop("evaluated_at", self.EVALUATED))
            c = sqlite3.connect(path)
            try:
                statuses = c.execute("SELECT DISTINCT status FROM claims").fetchall()
                versions = c.execute("SELECT COUNT(*) FROM event_versions").fetchone()[0]
            finally:
                c.close()
            return result, statuses, versions
        finally:
            directory.cleanup()

    def test_official_release_metadata_date_is_accepted(self) -> None:
        directory, path = self._build()
        try:
            first = reverify_source_item_claims(path, "i", self.EVALUATED)
            self.assertGreater(first.claims_verified, 0)
            self.assertEqual(first.versions_appended, 1)
            replay = reverify_source_item_claims(path, "i", "2026-10-06T12:05:00Z")
            self.assertEqual((replay.claims_selected, replay.claims_verified, replay.versions_appended), (0, 0, 0))
            c = sqlite3.connect(path)
            try:
                self.assertEqual(c.execute("SELECT DISTINCT status FROM claims").fetchall(), [("verified",)])
                self.assertEqual(c.execute("SELECT version,verification_state FROM event_versions ORDER BY version").fetchall(), [(1, "unverified"), (2, "verified")])
                self.assertEqual(c.execute("SELECT publication_evidence FROM source_items").fetchone()[0], f"metadata:{self.RAW_PUBLISHED}")
                self.assertEqual(c.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            finally:
                c.close()
        finally:
            directory.cleanup()

    def _assert_rejected(self, **override) -> None:
        result, statuses, versions = self._run(**override)
        self.assertEqual((result.claims_selected, result.claims_verified, result.versions_appended), (0, 0, 0))
        self.assertEqual(statuses, [("pending",)])
        self.assertEqual(versions, 1)

    def test_unrelated_metadata_source_is_rejected(self) -> None:
        self._assert_rejected(source_id="other-source")

    def test_wrong_retrieval_method_is_rejected(self) -> None:
        self._assert_rejected(method="rss")

    def test_altered_raw_timestamp_is_rejected(self) -> None:
        self._assert_rejected(raw_published="2026-10-05T16:56:23Z")

    def test_altered_stored_date_is_rejected(self) -> None:
        self._assert_rejected(published_at="2026-10-06T11:00:00Z")

    def test_non_release_evidence_is_rejected(self) -> None:
        self._assert_rejected(evidence="unparseable")

    def test_mismatched_release_identity_is_rejected(self) -> None:
        self._assert_rejected(raw_tag="v0.7.0")
        self._assert_rejected(raw_url="https://github.com/ggml-org/llama.cpp/releases/tag/v0.7.0")
        self._assert_rejected(
            url="https://github.com/other/repo/releases/tag/v0.6.0",
            raw_url="https://github.com/other/repo/releases/tag/v0.6.0",
        )

    def test_mismatched_or_disabled_provenance_is_rejected(self) -> None:
        self._assert_rejected(rule_id="other-rule")
        self._assert_rejected(entities=("Other",))
        self._assert_rejected(group="other-group")
        self._assert_rejected(enabled=False)

    def test_stale_and_future_publications_are_rejected(self) -> None:
        stale = "2026-08-01T00:00:00Z"
        self._assert_rejected(published_at=stale, raw_published=stale, evidence=f"metadata:{stale}")
        future = "2026-10-07T00:00:00Z"
        self._assert_rejected(published_at=future, raw_published=future, evidence=f"metadata:{future}")


class ReleaseClaimRegenerationTests(unittest.TestCase):
    """Exact-item regeneration of the grounded release-tag claim (schema v11)."""

    URL = FirstPartyReleaseEvidenceTests.URL
    RAW_PUBLISHED = FirstPartyReleaseEvidenceTests.RAW_PUBLISHED
    EVALUATED = FirstPartyReleaseEvidenceTests.EVALUATED

    def _build(self, **override):
        directory, path = FirstPartyReleaseEvidenceTests._build(self, **override)
        c = sqlite3.connect(path)
        try:
            for n, fn in enumerate((migrate_v8, migrate_v9, migrate_v10, migrate_v11)):
                fn(c, f"2026-10-06T07:04:0{n}Z")
            c.commit()
        finally:
            c.close()
        return directory, path

    def _counts(self, path: Path) -> tuple[int, ...]:
        c = sqlite3.connect(path)
        try:
            return tuple(
                c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("claims", "claim_evidence", "claim_evidence_provenance",
                              "events", "event_versions", "event_claims")
            )
        finally:
            c.close()

    def _assert_no_write(self, **override) -> None:
        directory, path = self._build(**override)
        try:
            before = self._counts(path)
            try:
                report = regenerate_release_claim(path, "i", self.EVALUATED)
            except ValueError:
                pass
            else:
                self.assertEqual((report.claims_inserted, report.versions_appended), (0, 0))
            self.assertEqual(self._counts(path), before)
        finally:
            directory.cleanup()

    def test_regeneration_is_exact_additive_and_idempotent(self) -> None:
        directory, path = self._build()
        try:
            before = self._counts(path)
            c = sqlite3.connect(path)
            old_claims = c.execute("SELECT * FROM claims ORDER BY claim_id").fetchall()
            old_versions = c.execute("SELECT * FROM event_versions ORDER BY event_id,version").fetchall()
            old_snapshots = c.execute("SELECT * FROM claim_evidence_provenance ORDER BY evidence_id").fetchall()
            c.close()
            report = regenerate_release_claim(path, "i", self.EVALUATED)
            self.assertEqual(
                (report.claims_inserted, report.evidence_inserted, report.claims_verified,
                 report.events_created, report.versions_appended, report.event_links_inserted),
                (1, 1, 1, 1, 1, 1),
            )
            self.assertEqual(
                self._counts(path), tuple(n + d for n, d in zip(before, (1, 1, 1, 1, 1, 1)))
            )
            c = sqlite3.connect(path)
            try:
                self.assertEqual(
                    c.execute("SELECT * FROM claims WHERE claim_id!=? ORDER BY claim_id", (report.claim_id,)).fetchall(),
                    old_claims,
                )
                self.assertEqual(
                    c.execute("SELECT * FROM event_versions WHERE event_id!=? ORDER BY event_id,version", (report.event_id,)).fetchall(),
                    old_versions,
                )
                self.assertEqual(
                    c.execute("SELECT * FROM claim_evidence_provenance WHERE evidence_id NOT IN (SELECT evidence_id FROM claim_evidence WHERE claim_id=?) ORDER BY evidence_id", (report.claim_id,)).fetchall(),
                    old_snapshots,
                )
                self.assertEqual(
                    c.execute("SELECT subject,predicate,object_value,status FROM claims WHERE claim_id=?", (report.claim_id,)).fetchone(),
                    ("llama.cpp", "has_version", "0.6.0", "verified"),
                )
                self.assertEqual(
                    c.execute(
                        "SELECT p.authority_match,p.matched_rule_id,e.exact_excerpt FROM claim_evidence e "
                        "JOIN claim_evidence_provenance p USING(evidence_id) WHERE e.claim_id=?",
                        (report.claim_id,),
                    ).fetchone(),
                    (1, "llama-cpp-own-release", "v0.6.0"),
                )
                self.assertEqual(
                    c.execute("SELECT version,verification_state,superseded_at FROM event_versions WHERE event_id=?", (report.event_id,)).fetchall(),
                    [(1, "verified", None)],
                )
                self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
            finally:
                c.close()
            after = self._counts(path)
            replay = regenerate_release_claim(path, "i", "2026-10-06T12:05:00Z")
            self.assertEqual(
                (replay.claims_inserted, replay.events_created, replay.versions_appended), (0, 0, 0)
            )
            self.assertEqual(self._counts(path), after)
        finally:
            directory.cleanup()

    def test_unrelated_mismatched_or_disabled_items_write_nothing(self) -> None:
        self._assert_no_write(source_id="other-source")
        self._assert_no_write(method="rss")
        self._assert_no_write(raw_tag="v0.7.0")
        self._assert_no_write(raw_published="2026-10-05T16:56:23Z")
        self._assert_no_write(rule_id="other-rule")
        self._assert_no_write(entities=("Other",))
        self._assert_no_write(group="other-group")
        self._assert_no_write(enabled=False)
        self._assert_no_write(
            url="https://github.com/other/repo/releases/tag/v0.6.0",
            raw_url="https://github.com/other/repo/releases/tag/v0.6.0",
        )

    def test_stale_and_future_items_write_nothing(self) -> None:
        for stamp in ("2026-08-01T00:00:00Z", "2026-10-07T00:00:00Z"):
            self._assert_no_write(
                published_at=stamp, raw_published=stamp, evidence=f"metadata:{stamp}"
            )

    def test_numeric_only_or_ungrounded_title_writes_nothing(self) -> None:
        self._assert_no_write(title="Bump ggml to 0.26.0 and 0.5.0")
        self._assert_no_write(title="v0.7.0")

    def test_unknown_source_item_is_rejected(self) -> None:
        directory, path = self._build()
        try:
            with self.assertRaises(ValueError):
                regenerate_release_claim(path, "missing", self.EVALUATED)
        finally:
            directory.cleanup()
