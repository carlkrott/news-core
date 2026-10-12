"""Schema v12 observability: job runs, model calls, funnel, migration safety."""
from __future__ import annotations

import io
import json
import sqlite3
import unittest
import urllib.error
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from news_pipeline import observability
from news_pipeline.briefing_summarizer import SummarizerTransportError
from news_pipeline.delivery_schema_v6 import validate_v6
from news_pipeline.schema_v10 import validate_v10
from news_pipeline.schema_v11 import migrate_v11, validate_v11
from news_pipeline.schema_v12 import V12_COLUMNS, V12_TABLES, migrate_v12, validate_v12
from news_pipeline.subject_model_transport import configured_transport, recording_calls

from tests.news_pipeline import test_schema_v11 as _v11

APPLIED_AT = _v11.APPLIED_AT


V12_AT = "2026-10-12T10:00:00Z"
SECRET = "sk-test-not-a-real-key-0123456789"


class _V11Fixture(unittest.TestCase):
    """A fully migrated v11 database (the live production schema level)."""

    _insert_item = _v11.SchemaV11Tests._insert_item

    def setUp(self) -> None:
        _v11.SchemaV11Tests.setUp(self)
        migrate_v11(self.con, APPLIED_AT)

    def tearDown(self) -> None:
        _v11.SchemaV11Tests.tearDown(self)

    def run_job(self, *argv: str) -> tuple[int, dict]:
        from news_pipeline.jobs import main

        output = io.StringIO()
        with redirect_stdout(output):
            code = main(list(argv))
        return code, json.loads(output.getvalue().strip().splitlines()[-1])


class SchemaV12MigrationTests(_V11Fixture):
    def test_v12_on_v11_fixture_keeps_every_earlier_validator_green(self) -> None:
        self.assertTrue(migrate_v12(self.con, V12_AT))
        validate_v6(self.con)
        validate_v10(self.con)
        validate_v11(self.con)
        validate_v12(self.con)
        self.assertFalse(migrate_v12(self.con, V12_AT))
        for table in V12_TABLES:
            columns = tuple(row[1] for row in self.con.execute(f"PRAGMA table_info({table})"))
            self.assertEqual(columns, V12_COLUMNS[table])

    def test_partial_v12_state_is_rejected(self) -> None:
        self.con.execute("CREATE TABLE job_runs(x)")
        with self.assertRaisesRegex(ValueError, "partial schema v12"):
            migrate_v12(self.con, V12_AT)

    def test_claims_status_check_is_untouched(self) -> None:
        migrate_v12(self.con, V12_AT)
        sql = self.con.execute("SELECT sql FROM sqlite_master WHERE name='claims'").fetchone()[0]
        self.assertNotIn("expired", sql)

    def test_migrate_cli_applies_replays_and_records_job_runs(self) -> None:
        code, first = self.run_job("migrate-v12", "--db", str(self.db_path), "--applied-at", V12_AT)
        self.assertEqual(code, 0)
        self.assertTrue(first["applied"])
        for key in ("started_at", "finished_at", "duration_ms"):
            self.assertIn(key, first)
        self.assertTrue(first["started_at"].endswith("Z"))
        code, replay = self.run_job("migrate-v12", "--db", str(self.db_path), "--applied-at", V12_AT)
        self.assertEqual(code, 0)
        self.assertFalse(replay["applied"])
        rows = self.con.execute(
            "SELECT job,state,exit_code,duration_ms,summary_json FROM job_runs ORDER BY job_run_id"
        ).fetchall()
        self.assertEqual([row[:3] for row in rows], [("migrate-v12", "completed", 0)] * 2)
        self.assertGreaterEqual(rows[0][3], 0)
        self.assertEqual(json.loads(rows[1][4])["applied"], False)


class JobRunRecordingTests(_V11Fixture):
    def test_pre_v12_database_runs_jobs_without_recording(self) -> None:
        code, out = self.run_job("migrate-v11", "--db", str(self.db_path), "--applied-at", APPLIED_AT)
        self.assertEqual(code, 0)
        self.assertIn("duration_ms", out)
        tables = {row[0] for row in self.con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("job_runs", tables)

    def test_failed_job_records_state_and_error_class(self) -> None:
        migrate_v12(self.con, V12_AT)
        with patch("news_pipeline.process_runner.process_news", side_effect=TimeoutError("slow")):
            code, out = self.run_job(
                "process", "--db", str(self.db_path), "--evaluated-at", V12_AT, "--enable-network",
            )
        self.assertEqual(code, 1)
        self.assertEqual(out["error_class"], "TimeoutError")
        row = self.con.execute(
            "SELECT job,slot_utc,state,exit_code,error_class FROM job_runs"
        ).fetchone()
        self.assertEqual(row, ("process", V12_AT, "failed", 1, "TimeoutError"))

    def test_disabled_job_is_recorded_as_disabled(self) -> None:
        migrate_v12(self.con, V12_AT)
        code, out = self.run_job("process", "--db", str(self.db_path))
        self.assertEqual((code, out["state"]), (0, "disabled"))
        self.assertEqual(self.con.execute("SELECT state FROM job_runs").fetchone(), ("disabled",))

    def test_health_is_not_recorded(self) -> None:
        migrate_v12(self.con, V12_AT)
        self.run_job("health", "--db", str(self.db_path))
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM job_runs").fetchone(), (0,))

    def test_record_failure_never_breaks_the_job(self) -> None:
        migrate_v12(self.con, V12_AT)
        with patch("news_pipeline.observability.summary_json", side_effect=RuntimeError("boom")):
            code, out = self.run_job("migrate-v12", "--db", str(self.db_path), "--applied-at", V12_AT)
        self.assertEqual(code, 0)
        self.assertFalse(out["applied"])

    def test_process_output_has_counts_and_funnel(self) -> None:
        migrate_v12(self.con, V12_AT)
        code, out = self.run_job(
            "process", "--db", str(self.db_path), "--evaluated-at", APPLIED_AT, "--enable-network",
        )
        self.assertEqual(code, 0, out)
        self.assertIn("source_items_seen", out["counts"])
        self.assertIn("events_created", out["event_counts"])
        funnel = out["funnel_24h"]
        self.assertEqual(funnel["window_hours"], 24)
        self.assertEqual(funnel["categories"]["ai"]["items"], 2)
        ai = funnel["categories"]["ai"]
        self.assertEqual((ai["verified"], ai["event_versions"], ai["eligible"]), (2, 2, 2))
        summary = json.loads(self.con.execute("SELECT summary_json FROM job_runs").fetchone()[0])
        self.assertEqual(summary["funnel_24h"]["categories"]["ai"]["items"], 2)
        self.assertEqual(out["claims_pending_age"]["expiry_days"], 14)
        self.assertEqual(summary["claims_pending_age"]["expired"], 0)


class FunnelTests(_V11Fixture):
    def test_window_excludes_older_items_and_counts_claim_statuses(self) -> None:
        funnel = observability.category_funnel(self.db_path, "2026-09-23T06:00:00Z")
        self.assertEqual(funnel["categories"]["ai"]["items"], 2)
        later = observability.category_funnel(self.db_path, "2026-09-25T06:00:00Z")
        self.assertNotIn("ai", later["categories"])
        self.assertEqual(later["stories"], {})


class ClaimsPendingAgeTests(_V11Fixture):
    def test_pending_claims_bucket_by_age_and_expire_after_14_days(self) -> None:
        code, out = self.run_job(
            "process", "--db", str(self.db_path), "--evaluated-at", APPLIED_AT, "--enable-network",
        )
        self.assertEqual(code, 0, out)
        ids = [row[0] for row in self.con.execute("SELECT claim_id FROM claims ORDER BY claim_id")]
        self.assertGreaterEqual(len(ids), 2)
        ages = {ids[0]: "2026-10-12T06:00:00Z", ids[1]: "2026-09-20T00:00:00Z"}
        for claim_id, extracted_at in ages.items():
            self.con.execute(
                "UPDATE claims SET status='pending',extracted_at=? WHERE claim_id=?",
                (extracted_at, claim_id),
            )
        self.con.commit()
        age = observability.claims_pending_age(self.db_path, "2026-10-12T08:00:00Z")
        self.assertEqual(
            age["buckets"], {"lt_1d": 1, "1_3d": 0, "3_7d": 0, "7_14d": 0, "expired": 1}
        )
        self.assertEqual((age["pending"], age["expired"]), (1, 1))
        self.assertEqual(age["oldest_pending_at"], "2026-10-12T06:00:00Z")
        self.assertEqual(age["oldest_at"], "2026-09-20T00:00:00Z")
        before = observability.claims_pending_age(self.db_path, "2026-10-01T00:00:00Z")
        self.assertEqual(before["buckets"]["7_14d"], 1)  # 11 days old then, not yet expired
        self.assertEqual(before["pending"], 1)  # the later claim did not exist yet


class _Response:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body = body
        self.status = status

    def read(self, limit: int) -> bytes:
        return self.body[:limit]

    def close(self) -> None:
        pass


def _ok_body(content: str = '{"items":[]}') -> bytes:
    return json.dumps({"choices": [{"message": {"content": content}}]}).encode()


class ModelCallRecordingTests(unittest.TestCase):
    ENV = {
        "NEWS_SUBJECT_MODEL_BASE_URL": "https://api.example.com:8443/private/path/v1",
        "NEWS_SUBJECT_MODEL": "approved-model",
        "NEWS_SUBJECT_MODEL_API_KEY": SECRET,
    }

    def call(self, opener) -> list[dict]:
        transport = configured_transport(environ=self.ENV, opener=opener)
        with recording_calls() as calls:
            try:
                transport(b'{"items":[]}')
            except SummarizerTransportError:
                pass
        return calls

    def test_ok_call_records_host_model_sizes_and_no_secret_or_path(self) -> None:
        calls = self.call(lambda request, *, timeout: _Response(_ok_body()))
        self.assertEqual(len(calls), 1)
        record = calls[0]
        self.assertEqual(record["outcome"], "ok")
        self.assertEqual(record["http_status"], 200)
        self.assertEqual(record["model"], "approved-model")
        self.assertEqual(record["endpoint_host"], "api.example.com:8443")
        self.assertGreater(record["request_bytes"], 0)
        self.assertGreater(record["timeout_s"], 0)
        self.assertGreaterEqual(record["latency_ms"], 0)
        text = json.dumps(calls)
        self.assertNotIn(SECRET, text)
        self.assertNotIn("private", text)
        self.assertNotIn("items", text)

    def test_outcome_classes(self) -> None:
        def raiser(exc):
            def opener(request, *, timeout):
                raise exc
            return opener

        cases = {
            "http_error": raiser(urllib.error.HTTPError("u", 503, "busy", {}, None)),
            "timeout": raiser(urllib.error.URLError(TimeoutError("timed out"))),
            "connection": raiser(urllib.error.URLError(ConnectionRefusedError())),
            "invalid_response": lambda request, *, timeout: _Response(b"not json"),
            "oversize": lambda request, *, timeout: _Response(b"x" * 40000),
        }
        for outcome, opener in cases.items():
            with self.subTest(outcome=outcome):
                calls = self.call(opener)
                self.assertEqual([call["outcome"] for call in calls], [outcome])
        self.assertEqual(self.call(cases["http_error"])[0]["http_status"], 503)
        self.assertEqual(self.call(raiser(TimeoutError()))[0]["outcome"], "timeout")

    def test_no_recording_outside_a_block_and_bad_requests_are_not_calls(self) -> None:
        transport = configured_transport(
            environ=self.ENV, opener=lambda request, *, timeout: _Response(_ok_body())
        )
        self.assertEqual(transport(b'{"items":[]}'), b'{"items":[]}')
        with recording_calls() as calls:
            with self.assertRaises(SummarizerTransportError):
                transport(b"not json")
        self.assertEqual(calls, [])


class ModelCallPersistenceTests(_V11Fixture):
    def test_rows_written_only_after_v12(self) -> None:
        record = {
            "started_at": V12_AT, "model": "approved-model", "endpoint_host": "model.example.test",
            "request_bytes": 10, "response_bytes": 20, "timeout_s": 25.0, "latency_ms": 1234,
            "outcome": "timeout", "http_status": None,
        }
        self.assertEqual(observability.record_model_calls(self.db_path, job="daily-report", records=[record]), 0)
        migrate_v12(self.con, V12_AT)
        self.assertEqual(
            observability.record_model_calls(
                self.db_path, job="daily-report", records=[record, {**record, "outcome": "bogus"}]
            ),
            1,
        )
        self.assertEqual(
            self.con.execute("SELECT job,outcome,latency_ms FROM model_calls").fetchall(),
            [("daily-report", "timeout", 1234)],
        )


class ShadowReportCliTests(_V11Fixture):
    """news_cli is the 08:05 shadow run: the only path that makes real model calls."""

    ENV = ModelCallRecordingTests.ENV

    def run_cli(self, fake_run_report) -> tuple[int, str, str]:
        from news_pipeline import news_cli

        artifacts = self.db_path.parent / "artifacts"
        artifacts.mkdir(exist_ok=True)
        out, err = io.StringIO(), io.StringIO()
        with patch("news_pipeline.report_builder.run_report", side_effect=fake_run_report), \
                redirect_stdout(out), patch("sys.stderr", err):
            code = news_cli.main([
                "--db", str(self.db_path), "--artifact-root", str(artifacts),
                "--as-of-utc", V12_AT,
            ])
        return code, out.getvalue(), err.getvalue()

    def _model_call(self) -> None:
        transport = configured_transport(
            environ=self.ENV, opener=lambda request, *, timeout: _Response(_ok_body())
        )
        transport(b'{"items":[]}')

    def test_success_records_model_call_and_job_run(self) -> None:
        from types import SimpleNamespace

        migrate_v12(self.con, V12_AT)

        def fake_run_report(*_args, **_kwargs):
            self._model_call()
            return SimpleNamespace(
                report_id="r1", window_start=APPLIED_AT, window_end=V12_AT,
                generation_status="completed", was_replayed=False,
                included_count=1, excluded_count=0, artifact_result=None,
            )

        code, out, _err = self.run_cli(fake_run_report)
        self.assertEqual(code, 0)
        output = json.loads(out)
        self.assertEqual(output["model_calls"], {"ok": 1})
        self.assertIn("duration_ms", output)
        self.assertEqual(
            self.con.execute("SELECT job,outcome,model FROM model_calls").fetchall(),
            [("shadow-report", "ok", "approved-model")],
        )
        self.assertEqual(
            self.con.execute("SELECT job,slot_utc,state,exit_code FROM job_runs").fetchall(),
            [("shadow-report", V12_AT, "completed", 0)],
        )

    def test_failure_still_records_calls_and_failed_run(self) -> None:
        migrate_v12(self.con, V12_AT)

        def fake_run_report(*_args, **_kwargs):
            self._model_call()
            raise RuntimeError("report exploded")

        code, _out, err = self.run_cli(fake_run_report)
        self.assertEqual(code, 1)
        self.assertIn("runtime", err)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM model_calls").fetchone(), (1,))
        self.assertEqual(
            self.con.execute("SELECT job,state,exit_code,error_class FROM job_runs").fetchall(),
            [("shadow-report", "failed", 1, "RuntimeError")],
        )


class IngestSchemaGateTests(_V11Fixture):
    """Every cron job's schema gate must admit v12 (tick refused it on 2026-10-12)."""

    def test_ingest_gate_admits_v12_and_checks_its_tables(self) -> None:
        from news_pipeline.ingest_runner import _verify_schema

        _verify_schema(self.db_path)
        migrate_v12(self.con, V12_AT)
        _verify_schema(self.db_path)
        self.con.execute("ALTER TABLE job_runs ADD COLUMN extra TEXT")
        with self.assertRaisesRegex(ValueError, "job_runs has incompatible columns"):
            _verify_schema(self.db_path)

    def test_ingest_gate_names_the_newest_schema_module(self) -> None:
        import news_pipeline
        import news_pipeline.ingest_runner as ingest_runner

        package = Path(news_pipeline.__file__).parent
        newest = max(int(path.stem.removeprefix("schema_v")) for path in package.glob("schema_v*.py"))
        source = Path(ingest_runner.__file__).read_text(encoding="utf-8")
        self.assertIn(f"v4-v{newest} prefix", source, "a new schema module needs a matching ingest gate")


class CronLineFormatTests(_V11Fixture):
    """Batch 2: every cron line is one JSON object with real fields, failures included."""

    def tick(self) -> tuple[int, dict]:
        return self.run_job(
            "tick", "--db", str(self.db_path), "--enable-network", "--sources", "s.toml",
            "--topics", "t.toml", "--policy", "p.toml", "--run-started-at", V12_AT,
        )

    def test_plain_turns_slotted_reports_into_json_objects(self) -> None:
        import dataclasses
        import enum

        class Kind(enum.Enum):
            FEED = "feed"

        @dataclasses.dataclass(frozen=True, slots=True)
        class Lane:
            kind: Kind
            ids: tuple[str, ...]

        @dataclasses.dataclass(frozen=True, slots=True)
        class Report:
            run_started_at: str
            lanes: tuple[Lane, ...]
            by_source: dict[str, int]

        report = Report(V12_AT, (Lane(Kind.FEED, ("a", "b")),), {"src": 3})
        expected = {
            "run_started_at": V12_AT,
            "lanes": [{"kind": "feed", "ids": ["a", "b"]}],
            "by_source": {"src": 3},
        }
        self.assertEqual(observability.plain(report), expected)
        self.assertEqual(json.loads(json.dumps(observability.plain(report))), expected)

    def test_tick_report_is_a_json_object_not_a_repr(self) -> None:
        import dataclasses

        @dataclasses.dataclass(frozen=True, slots=True)
        class Report:
            run_started_at: str
            queries_attempted: int
            failures: tuple[str, ...]

        with patch("news_pipeline.ingest_runner.run_ingest_sync", return_value=Report(V12_AT, 4, ("x",))):
            code, out = self.tick()
        self.assertEqual(code, 0, out)
        self.assertEqual(out["report"], {"run_started_at": V12_AT, "queries_attempted": 4, "failures": ["x"]})
        self.assertEqual(out["counts"]["queries_attempted"], 4)

    def test_failed_lines_name_their_job_and_state(self) -> None:
        with patch("news_pipeline.ingest_runner.run_ingest_sync", side_effect=ValueError("bad schema")):
            code, out = self.tick()
        self.assertEqual(code, 1)
        self.assertEqual((out["job"], out["state"], out["error_class"]), ("tick", "failed", "ValueError"))
        self.assertEqual(out["error"]["message"], "bad schema")
        code, out = self.run_job("tick", "--db", str(self.db_path), "--enable-network")
        self.assertEqual((code, out["job"], out["state"]), (2, "tick", "failed"))


if __name__ == "__main__":
    unittest.main()
