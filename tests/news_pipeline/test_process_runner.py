from __future__ import annotations

import ast
import hashlib
import json
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from news_pipeline.db import init_db
from news_pipeline.live_contracts import QueryPlanContract, stable_id
from news_pipeline.process_runner import candidate_from_source_item, process_news
from news_pipeline.query_planner import EXPANSION_REASON
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4

MIGRATED_AT = "2026-09-06T09:00:00Z"
EVALUATED_AT = "2026-09-06T12:00:00Z"


def _create_v4(path: Path) -> None:
    init_db(str(path))
    connection = sqlite3.connect(path, isolation_level=None)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(connection, MIGRATED_AT)
        migrate_v4(connection, MIGRATED_AT)
    finally:
        connection.close()


def _seed_source(connection: sqlite3.Connection, source_id: str = "searx") -> None:
    connection.execute(
        """INSERT INTO source_registry(
               source_id,adapter_type,source_role,host,category_scope_json,enabled,
               queries_json,title_blocklist_json,content_blocklist_json,
               url_blocklist_json,allowlist_domains_json,cadence_minutes,terms_notes,
               rate_limit_notes,next_due_at,config_hash,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            source_id,
            "searxng",
            "discovery",
            "127.0.0.1",
            '["ai"]',
            1,
            "[]",
            "[]",
            "[]",
            "[]",
            "[]",
            15,
            "",
            "",
            None,
            "0" * 64,
            MIGRATED_AT,
        ),
    )


def _seed_item(
    connection: sqlite3.Connection,
    item_id: str,
    *,
    title: str = "Unique product announcement",
    body: str = "A genuinely new product was announced.",
    original_url: str | None = None,
    canonical_url: str | None = None,
    published_at: str | None = "2026-09-06T11:00:00Z",
    publication_evidence: str | None = "metadata:2026-09-06T11:00:00Z",
) -> None:
    canonical = canonical_url or original_url or f"https://publisher.test/{item_id}"
    original = original_url or canonical
    connection.execute(
        """INSERT INTO source_items(
               source_item_id,source_id,external_id,category,original_url,
               canonical_url,publisher,source_role,author_handle,retrieval_method,
               raw_content_hash,title,body,raw,retrieved_at,published_at,updated_at,
               publication_evidence)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            item_id,
            "searx",
            item_id,
            "ai",
            original,
            canonical,
            "publisher.test",
            "discovery",
            None,
            "searxng",
            hashlib.sha256(item_id.encode()).hexdigest(),
            title,
            body,
            "{}",
            "2026-09-06T11:30:00Z",
            published_at,
            None,
            publication_evidence,
        ),
    )


def _seed_history(
    connection: sqlite3.Connection,
    article_id: str,
    *,
    title: str,
    body: str,
    canonical_url: str,
) -> None:
    run = f"history-{article_id}"
    observation = f"observation-{article_id}"
    connection.execute(
        "INSERT INTO runs(id,started_at,finished_at,kind,provenance,source_dir,notes) VALUES(?,?,?,?,?,?,?)",
        (run, "2026-09-05T10:00:00Z", "2026-09-05T10:01:00Z", "historical_replay", "observed_historical", "fixture", None),
    )
    connection.execute(
        """INSERT INTO articles(
               id,run_id,category,canonical_url,original_url,title,snippet,source_file,
               observed_at,fetch_marker,provenance,created_at,normalized_title,
               identity_confidence,identity_basis)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            article_id,
            run,
            "ai",
            canonical_url,
            canonical_url,
            title,
            body,
            "fixture.md",
            "2026-09-05T10:00:00Z",
            None,
            "observed_historical",
            "2026-09-05T10:00:00Z",
            title.casefold(),
            1.0,
            "canonical_url",
        ),
    )
    connection.execute(
        """INSERT INTO observations(
               id,article_id,event_id,category,source_file,kind,body,raw,occurred_at,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (
            observation,
            article_id,
            None,
            "ai",
            "fixture.md",
            "parsed_article",
            body,
            None,
            "2026-09-05T10:00:00Z",
            "2026-09-05T10:00:00Z",
        ),
    )


def _seed_attempt(
    connection: sqlite3.Connection,
    attempt_id: str,
    *,
    status: str = "success",
    returned: int = 0,
    duplicates: int = 0,
    errors: int = 0,
    started_at: str = "2026-09-06T11:45:00Z",
) -> None:
    plan_id = f"configured-{attempt_id}"
    connection.execute(
        """INSERT INTO query_plans(
               query_plan_id,source_id,query_text,category,topic,entity,
               reason_selected,cooldown_seconds,max_rounds,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (
            plan_id,
            "searx",
            f"configured query {attempt_id}",
            "ai",
            None,
            None,
            "configured",
            900,
            1,
            MIGRATED_AT,
        ),
    )
    connection.execute(
        """INSERT INTO query_attempts(
               attempt_id,query_plan_id,status,started_at,finished_at,returned_count,
               novel_count,verified_count,duplicate_count,stale_count,error_count,error,
               rate_limit_reset_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            attempt_id,
            plan_id,
            status,
            started_at,
            started_at,
            returned,
            0,
            0,
            duplicates,
            0,
            errors,
            "transport failure" if status in {"failed", "rate_limited"} else None,
            None,
        ),
    )


class ProcessRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "state.db"
        _create_v4(self.db)
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            _seed_source(connection)
            connection.commit()
        finally:
            connection.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _write(self, operation) -> None:
        connection = sqlite3.connect(self.db)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            operation(connection)
            connection.commit()
        finally:
            connection.close()

    def _rows(self, sql: str) -> list[tuple]:
        connection = sqlite3.connect(self.db)
        try:
            return connection.execute(sql).fetchall()
        finally:
            connection.close()

    def test_publication_evidence_adaptation_fails_closed(self) -> None:
        base = {
            "source_item_id": "x",
            "category": "ai",
            "title": "T",
            "body": "B",
            "original_url": "https://example.test/a",
            "canonical_url": "https://example.test/a",
            "retrieved_at": "2026-09-06T11:30:00Z",
        }
        metadata = candidate_from_source_item(
            base
            | {
                "published_at": "2026-09-06T11:00:00Z",
                "publication_evidence": "metadata:raw",
            },
            EVALUATED_AT,
        )
        missing = candidate_from_source_item(
            base | {"published_at": None, "publication_evidence": None}, EVALUATED_AT
        )
        invalid = candidate_from_source_item(
            base
            | {
                "published_at": "not-a-time",
                "publication_evidence": "metadata:raw",
            },
            EVALUATED_AT,
        )
        self.assertEqual(metadata.published_evidence, "metadata")
        self.assertEqual(missing.published_evidence, "missing")
        self.assertEqual(invalid.published_evidence, "unparseable")
        self.assertIsNone(invalid.published_at)

    def test_exact_and_syndicated_urls_are_suppressed(self) -> None:
        def seed(connection: sqlite3.Connection) -> None:
            _seed_history(
                connection,
                "history",
                title="Product launch",
                body="The product launched today.",
                canonical_url="https://publisher.test/story",
            )
            _seed_item(
                connection,
                "direct",
                title="Product launch",
                body="The product launched today.",
                canonical_url="https://publisher.test/story",
            )
            _seed_item(
                connection,
                "syndicated",
                title="Product launch",
                body="The product launched today.",
                original_url="https://publisher.test/story?utm_source=google-news",
                canonical_url="https://publisher.test/story",
            )

        self._write(seed)
        report = process_news(self.db, EVALUATED_AT)
        self.assertEqual(report.decisions_persisted, 2)
        decisions = self._rows("SELECT decision_kind,reason FROM decisions WHERE decided_by='phase3' ORDER BY id")
        self.assertEqual([row[0] for row in decisions], ["suppress", "suppress"])
        self.assertTrue(
            all(json.loads(reason)["phase2_decision"].startswith("suppress_") for _, reason in decisions)
        )

    def test_rewrite_material_update_stale_and_zero_model_budget(self) -> None:
        def seed(connection: sqlite3.Connection) -> None:
            _seed_history(
                connection,
                "price-history",
                title="Product pricing update",
                body="The price is $10",
                canonical_url="https://publisher.test/price",
            )
            _seed_item(
                connection,
                "material",
                title="Product pricing update",
                body="The price is $20",
                canonical_url="https://publisher.test/price",
            )
            _seed_history(
                connection,
                "spec-history",
                title="Product specifications",
                body="The price is $20 and 10 users",
                canonical_url="https://publisher.test/spec",
            )
            _seed_item(
                connection,
                "rewrite",
                title="Product specifications",
                body="The price is $20 for 10 users",
                canonical_url="https://publisher.test/spec",
            )
            _seed_item(
                connection,
                "stale",
                published_at="2026-01-01T00:00:00Z",
                publication_evidence="metadata:2026-01-01T00:00:00Z",
            )
            _seed_history(
                connection,
                "ambiguous-history",
                title="Product pricing comparison",
                body="The price is $10",
                canonical_url="https://publisher.test/ambiguous",
            )
            _seed_item(
                connection,
                "ambiguous",
                title="Product pricing comparison",
                body="The prices are $20 and $30",
                canonical_url="https://publisher.test/ambiguous",
            )

        self._write(seed)
        report = process_news(self.db, EVALUATED_AT)
        reasons = {
            json.loads(reason)["source_item_id"]: (kind, json.loads(reason)["semantic_decision"])
            for kind, reason in self._rows(
                "SELECT decision_kind,reason FROM decisions WHERE decided_by='phase3'"
            )
        }
        self.assertEqual(reasons["material"], ("promote", "material_update"))
        self.assertEqual(reasons["rewrite"], ("suppress", "rewrite"))
        self.assertEqual(reasons["stale"], ("suppress", "bypass_phase2_terminal"))
        self.assertEqual(reasons["ambiguous"], ("manual_review", "pending_review"))
        self.assertEqual(report.eligible_count, 1)
        self.assertTrue(all(row[0] is None for row in self._rows("SELECT article_id FROM decisions WHERE decided_by='phase3'")))

    def test_history_unavailable_is_manual_review(self) -> None:
        self._write(lambda connection: _seed_item(connection, "candidate"))
        report = process_news(
            self.db,
            EVALUATED_AT,
            history_db_path=Path(self.temp.name) / "missing.db",
        )
        self.assertEqual(report.decisions_persisted, 1)
        kind, reason = self._rows(
            "SELECT decision_kind,reason FROM decisions WHERE decided_by='phase3'"
        )[0]
        self.assertEqual(kind, "manual_review")
        self.assertEqual(json.loads(reason)["phase2_decision"], "pending_history_unavailable")

    def test_replay_and_max_items_progress_without_starvation(self) -> None:
        self._write(
            lambda connection: [
                _seed_item(connection, f"item-{index}") for index in range(3)
            ]
        )
        first = process_news(self.db, EVALUATED_AT, max_items=1)
        second = process_news(self.db, "2026-09-06T12:01:00Z", max_items=1)
        third = process_news(self.db, "2026-09-06T12:02:00Z", max_items=1)
        replay = process_news(self.db, "2026-09-06T12:03:00Z", max_items=1)
        self.assertEqual([first.source_items_processed, second.source_items_processed, third.source_items_processed], [1, 1, 1])
        self.assertEqual(replay.source_items_pending, 0)
        self.assertEqual(replay.decisions_persisted, 0)
        self.assertEqual(self._rows("SELECT COUNT(*) FROM decisions WHERE decided_by='phase3'")[0][0], 3)

    def test_low_yield_persists_truthful_telemetry_and_two_plans(self) -> None:
        def seed(connection: sqlite3.Connection) -> None:
            _seed_item(
                connection,
                "stale-one",
                title="First stale launch story",
                published_at="2026-01-01T00:00:00Z",
                publication_evidence="metadata:old",
            )
            _seed_item(
                connection,
                "stale-two",
                title="Second stale pricing story",
                published_at="2026-01-01T00:00:00Z",
                publication_evidence="metadata:old",
            )
            _seed_attempt(connection, "attempt", returned=4, duplicates=2)

        self._write(seed)
        report = process_news(self.db, EVALUATED_AT)
        self.assertEqual(report.query_telemetry_persisted, 1)
        self.assertEqual(report.expansion_plans_persisted, 2)
        self.assertEqual(len(report.expansion_plans), 2)
        summary = report.category_yields[0]
        self.assertEqual(summary.returned_count, 4)
        self.assertEqual(summary.ingest_duplicate_count, 2)
        self.assertEqual(summary.stale_count, 2)
        telemetry = self._rows("SELECT returned_count,error_count FROM query_telemetry")
        self.assertEqual(telemetry, [(4, 0)])
        plans = self._rows(
            "SELECT reason_selected,max_rounds,cooldown_seconds FROM query_plans WHERE reason_selected='low_novelty_expansion'"
        )
        self.assertEqual(plans, [(EXPANSION_REASON, 2, 900), (EXPANSION_REASON, 2, 900)])

    def test_transport_failure_does_not_create_expansion(self) -> None:
        self._write(
            lambda connection: _seed_attempt(
                connection, "failed", status="failed", errors=1
            )
        )
        report = process_news(self.db, EVALUATED_AT)
        self.assertEqual(report.query_telemetry_persisted, 1)
        self.assertEqual(report.expansion_plans, ())
        self.assertEqual(report.category_yields[0].transport_failure_count, 1)

    def test_historical_failure_does_not_poison_new_success_or_cooldown(self) -> None:
        self._write(
            lambda connection: _seed_attempt(
                connection, "failed", status="failed", errors=1
            )
        )
        first = process_news(self.db, EVALUATED_AT)
        self.assertEqual(first.expansion_plans, ())
        self._write(
            lambda connection: _seed_attempt(
                connection,
                "success",
                status="success",
                returned=2,
                duplicates=2,
                started_at="2026-09-06T12:04:00Z",
            )
        )
        second = process_news(self.db, "2026-09-06T12:05:00Z")
        self.assertEqual(second.category_yields[0].transport_failure_count, 0)
        self.assertEqual(second.expansion_plans_persisted, 1)
        self._write(
            lambda connection: _seed_attempt(
                connection,
                "success-two",
                status="success",
                returned=1,
                duplicates=1,
                started_at="2026-09-06T12:09:00Z",
            )
        )
        third = process_news(self.db, "2026-09-06T12:10:00Z")
        self.assertEqual(third.expansion_plans, ())

    def test_transaction_rolls_back_all_phase3_rows(self) -> None:
        def seed(connection: sqlite3.Connection) -> None:
            _seed_item(connection, "candidate")
            _seed_attempt(connection, "attempt", status="success", returned=0)

        self._write(seed)
        invalid = QueryPlanContract(
            query_plan_id=stable_id("invalid-plan"),
            source_id="missing-source",
            query_text="safe official update",
            category="ai",
            reason_selected=EXPANSION_REASON,
            cooldown_seconds=900,
            max_rounds=2,
            created_at=EVALUATED_AT,
        )
        with patch("news_pipeline.process_runner._plan_expansions", return_value=(invalid,)):
            with self.assertRaises(sqlite3.IntegrityError):
                process_news(self.db, EVALUATED_AT)
        self.assertEqual(self._rows("SELECT COUNT(*) FROM decisions WHERE decided_by='phase3'")[0][0], 0)
        self.assertEqual(self._rows("SELECT COUNT(*) FROM query_telemetry")[0][0], 0)
        self.assertEqual(self._rows("SELECT COUNT(*) FROM runs WHERE notes LIKE '%phase3%'")[0][0], 0)

    def test_v2_database_fails_without_migration(self) -> None:
        old_db = Path(self.temp.name) / "v2.db"
        init_db(str(old_db))
        with self.assertRaisesRegex(ValueError, "schema v4"):
            process_news(old_db, EVALUATED_AT)
        connection = sqlite3.connect(old_db)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                ).fetchall(),
                [(1,), (2,)],
            )
        finally:
            connection.close()

    def test_static_no_network_delivery_model_or_phase4_write_boundary(self) -> None:
        root = Path(__file__).parents[2] / "scripts" / "news_pipeline"
        forbidden_imports = {"aiohttp", "requests", "socket", "subprocess", "urllib"}
        forbidden_tables = {
            "claims",
            "claim_evidence",
            "event_versions",
            "event_dates",
            "events",
            "reports",
            "report_events",
            "delivery_attempts",
        }
        for name in ("query_planner.py", "novelty.py", "process_runner.py"):
            source = (root / name).read_text(encoding="utf-8")
            tree = ast.parse(source)
            imported = {
                node.names[0].name.split(".")[0]
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
            } | {
                (node.module or "").split(".")[0]
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom)
            }
            self.assertFalse(imported & forbidden_imports, name)
            for table in forbidden_tables:
                self.assertIsNone(
                    re.search(
                        rf"INSERT\s+(?:OR\s+\w+\s+)?INTO\s+{table}\b",
                        source,
                        re.IGNORECASE,
                    ),
                    f"{name} writes forbidden table {table}",
                )
        process_source = (root / "process_runner.py").read_text(encoding="utf-8")
        self.assertIn("_forbidden_model", process_source)
        self.assertRegex(process_source, r"_forbidden_model,\s*\n\s*0,")


if __name__ == "__main__":
    unittest.main()
