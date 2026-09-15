from __future__ import annotations

import ast
import hashlib
import unittest
from dataclasses import FrozenInstanceError
from decimal import Decimal
from pathlib import Path

from news_pipeline.live_contracts import (
    ArtifactDigest,
    ClaimContract,
    ClaimStatus,
    DatePrecision,
    DateType,
    DeliveryState,
    EventDateContract,
    EventVersionContract,
    EvidenceContract,
    EvidenceRole,
    ObservationContract,
    ObservationKind,
    QueryAttemptContract,
    QueryPlanContract,
    QuerySeed,
    QueryStatus,
    ReportContract,
    ReportStatus,
    SourceAdapter,
    SourceContract,
    SourceRole,
    VerificationState,
    claim_from_row,
    claim_to_row,
    event_date_from_row,
    event_date_to_row,
    event_version_from_row,
    event_version_to_row,
    evidence_from_row,
    evidence_to_row,
    observation_from_row,
    observation_to_row,
    query_attempt_from_row,
    query_attempt_to_row,
    query_plan_from_row,
    query_plan_to_row,
    report_from_row,
    report_to_row,
    source_from_row,
    source_to_row,
    stable_id,
)

ROOT = Path(__file__).resolve().parents[2]
TS = "2026-09-06T20:00:00Z"
DIGEST = hashlib.sha256(b"body").hexdigest()


def make_source() -> SourceContract:
    return SourceContract(
        source_id="source|unicode-µ",
        adapter_type=SourceAdapter.SEARXNG,
        source_role=SourceRole.DISCOVERY,
        host="127.0.0.1:8888",
        category_scope=("ai",),
        enabled=True,
        queries=(QuerySeed("literal|pipe\nline", ("news", "general")),),
        title_blocklist=("a|b", "line\nbreak", "雪"),
        content_blocklist=("[]",),
        url_blocklist=("reddit.com/r/",),
        allowlist_domains=("example.com",),
        cadence_minutes=None,
        next_due_at=None,
    )


def make_observation() -> ObservationContract:
    return ObservationContract(
        observation_id="obs-1",
        source_id="source-1",
        category="ai",
        kind=ObservationKind.PARSED_ARTICLE,
        original_url="https://example.com/item?a=1|2",
        canonical_url="https://example.com/item",
        publisher="Example",
        retrieval_method="searxng",
        raw_content_hash=DIGEST,
        observed_at=TS,
        external_id="external-1",
        author_handle="author",
        title="A | title\nwith lines",
        body="body",
        raw="{\"raw\": true}",
        published_at="2026-09-06T19:00:00Z",
        publication_evidence="source_metadata",
        unknown_date_reason="scheduled date not stated",
    )


class ContractValidationTests(unittest.TestCase):
    def test_source_is_frozen_and_deep_values_are_immutable(self) -> None:
        value = make_source()
        with self.assertRaises(FrozenInstanceError):
            value.source_id = "changed"  # type: ignore[misc]
        self.assertIsInstance(value.queries, tuple)
        self.assertIsInstance(value.title_blocklist, tuple)

    def test_strict_bool_and_integer_validation(self) -> None:
        with self.assertRaises(ValueError):
            SourceContract(
                source_id="x", adapter_type=SourceAdapter.SEARXNG,
                source_role=SourceRole.DISCOVERY, host="localhost",
                category_scope=("ai",), enabled=1,  # type: ignore[arg-type]
                queries=(QuerySeed("q", ("news",)),),
            )
        with self.assertRaises(ValueError):
            QueryPlanContract("q", "s", "text", "ai", "reason", True, 1, TS)  # type: ignore[arg-type]

    def test_invalid_enums_and_timestamps_rejected(self) -> None:
        with self.assertRaises(ValueError):
            ObservationContract(
                observation_id="o", source_id="s", category="ai",
                kind="parsed_article",  # type: ignore[arg-type]
                original_url="https://e", canonical_url="https://e",
                publisher="e", retrieval_method="x", raw_content_hash=DIGEST,
                observed_at="2026-09-06T20:00:00+00:00",
            )
        with self.assertRaises(ValueError):
            stable_id("x", length=0)

    def test_evidence_hash_is_verified(self) -> None:
        with self.assertRaises(ValueError):
            EvidenceContract("e", "c", "o", EvidenceRole.SUPPORTS, "excerpt", DIGEST, "group", TS)

    def test_verified_event_requires_timestamp(self) -> None:
        with self.assertRaises(ValueError):
            EventVersionContract("e", 1, "initial", "summary", VerificationState.VERIFIED, TS)

    def test_unknown_dates_remain_explicit(self) -> None:
        unknown = EventDateContract("d", "e", 1, DateType.SCHEDULED_FOR, None, DatePrecision.UNKNOWN, unknown_reason="vague future date")
        self.assertEqual(event_date_from_row(event_date_to_row(unknown)), unknown)
        with self.assertRaises(ValueError):
            EventDateContract("d", "e", 1, DateType.SCHEDULED_FOR, None, DatePrecision.DAY)

    def test_query_terminal_state_constraints(self) -> None:
        with self.assertRaises(ValueError):
            QueryAttemptContract("a", "p", QueryStatus.FAILED, TS)
        with self.assertRaises(ValueError):
            QueryAttemptContract("a", "p", QueryStatus.SUCCESS, TS)

    def test_stable_id_is_deterministic_and_null_sensitive(self) -> None:
        self.assertEqual(stable_id("a", "b"), stable_id("a", "b"))
        self.assertNotEqual(stable_id("a", None, "b"), stable_id("a", "", "b"))
        self.assertEqual(len(stable_id("a", length=None)), 64)


class LosslessRowCodecTests(unittest.TestCase):
    def assert_round_trip(self, value: object, to_row: object, from_row: object) -> None:
        row1 = to_row(value)  # type: ignore[operator]
        row2 = to_row(value)  # type: ignore[operator]
        self.assertEqual(row1, row2)
        self.assertEqual(from_row(row1), value)  # type: ignore[operator]

    def test_source_json_codec_preserves_delimiters_unicode_and_newlines(self) -> None:
        value = make_source()
        row = source_to_row(value)
        self.assertTrue(row["queries_json"].startswith("["))
        self.assert_round_trip(value, source_to_row, source_from_row)

    def test_observation_round_trip(self) -> None:
        self.assert_round_trip(make_observation(), observation_to_row, observation_from_row)

    def test_claim_round_trip_preserves_decimal(self) -> None:
        value = ClaimContract("c", "o", "subject", "predicate", "value|x", "typed", Decimal("0.7500"), ClaimStatus.PENDING, TS)
        self.assert_round_trip(value, claim_to_row, claim_from_row)

    def test_evidence_round_trip(self) -> None:
        excerpt = "exact | excerpt\n雪"
        value = EvidenceContract("e", "c", "o", EvidenceRole.CONTRADICTS, excerpt, hashlib.sha256(excerpt.encode()).hexdigest(), "publisher-group", TS)
        self.assert_round_trip(value, evidence_to_row, evidence_from_row)

    def test_event_version_round_trip(self) -> None:
        value = EventVersionContract("event", 2, "material update", "summary", VerificationState.VERIFIED, TS, verified_at=TS)
        self.assert_round_trip(value, event_version_to_row, event_version_from_row)

    def test_query_plan_and_attempt_round_trip(self) -> None:
        plan = QueryPlanContract("plan", "source", "q|x", "world", "seed", 60, 2, TS, topic="topic", entity="entity")
        attempt = QueryAttemptContract("attempt", "plan", QueryStatus.PARTIAL, TS, returned_count=3, novel_count=2, duplicate_count=1, error_count=1, finished_at=TS, error="one backend failed")
        self.assert_round_trip(plan, query_plan_to_row, query_plan_from_row)
        self.assert_round_trip(attempt, query_attempt_to_row, query_attempt_from_row)

    def test_report_artifacts_use_lossless_json(self) -> None:
        artifact = ArtifactDigest("news|report.json", DIGEST, 12)
        value = ReportContract("report", TS, "2026-09-07T20:00:00Z", ReportStatus.COMPLETE, DeliveryState.DRY_RUN, TS, artifacts=(artifact,))
        row = report_to_row(value)
        self.assertIn("news|report.json", row["artifacts_json"])
        self.assert_round_trip(value, report_to_row, report_from_row)

    def test_missing_or_corrupt_row_data_rejected(self) -> None:
        row = source_to_row(make_source())
        del row["host"]
        with self.assertRaises(ValueError):
            source_from_row(row)
        row = source_to_row(make_source())
        row["queries_json"] = "not json"
        with self.assertRaises(ValueError):
            source_from_row(row)


class StaticPurityTests(unittest.TestCase):
    def test_live_contracts_has_no_effectful_imports_or_calls(self) -> None:
        path = ROOT / "scripts/news_pipeline/live_contracts.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        forbidden_import_roots = {"pathlib", "sqlite3", "socket", "subprocess", "urllib", "requests", "aiohttp", "random", "uuid", "time"}
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            (node.module or "").split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        }
        self.assertFalse(imported & forbidden_import_roots)
        forbidden_calls = {"open", "exec", "eval"}
        called = {node.func.id for node in ast.walk(tree) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
        self.assertFalse(called & forbidden_calls)
        attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
        self.assertNotIn("now", attributes)
        self.assertNotIn("utcnow", attributes)


if __name__ == "__main__":
    unittest.main()
