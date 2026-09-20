from __future__ import annotations

import hashlib
from contextlib import redirect_stdout
from io import StringIO
import json
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from news_container.control_store import (
    claim,
    complete,
    enqueue,
    open as open_control,
    retry_investigation,
    schema_version,
)
from news_pipeline.adapters.base import FetchResponse, FetchResult, NormalizedItem
from news_pipeline.db import init_db
from news_pipeline.ingest_runner import _Job, _NetworkOutcome, _open_writer, _persist_outcome
from news_pipeline.jobs import _parser, _run_investigate
from news_pipeline.investigation import (
    investigation_id,
    investigation_payload,
    enqueue_candidate_investigations,
    make_investigation_job,
    persist_investigation,
    run_investigation,
    validate_investigation_payload,
)
from news_pipeline.live_contracts import QuerySeed, SourceAdapter, SourceContract, SourceRole, stable_feed_lane_id
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.delivery_schema_v6 import migrate_v6


TS = "2026-09-20T00:00:00Z"
LEASE = "2026-09-20T00:05:00Z"


def _database() -> tuple[tempfile.TemporaryDirectory[str], Path, sqlite3.Connection]:
    directory = tempfile.TemporaryDirectory()
    path = Path(directory.name) / "state.db"
    init_db(str(path))
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("PRAGMA foreign_keys=ON")
    migrate_v3(connection, TS)
    migrate_v4(connection, TS)
    migrate_v5(connection, TS)
    migrate_v6(connection, TS)
    migrate_v7(connection, TS)
    migrate_v8(connection, TS)
    configured_text = "configured query"
    configured_lane = stable_feed_lane_id("source-a", "ai", configured_text, ("general",))
    connection.execute(
        """INSERT INTO source_registry(
               source_id,adapter_type,source_role,host,category_scope_json,enabled,queries_json,
               title_blocklist_json,content_blocklist_json,url_blocklist_json,allowlist_domains_json,
               cadence_minutes,terms_notes,rate_limit_notes,next_due_at,config_hash,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            "source-a", "searxng", "discovery", "example.com", '["ai"]', 1,
            json.dumps([{
                "text": configured_text,
                "categories": ["general"],
                "pipeline_category": "ai",
                "feed_lane_id": configured_lane,
            }]),
            "[]", "[]", "[]", "[]", 15, None, None, LEASE, "a" * 64, TS,
        ),
    )
    return directory, path, connection


def _plan_attempt(connection: sqlite3.Connection, plan_id: str, attempt_id: str) -> None:
    connection.execute(
        """INSERT INTO query_plans(
               query_plan_id,source_id,query_text,category,topic,entity,reason_selected,
               cooldown_seconds,max_rounds,created_at)
           VALUES(?,?,?,?,?,?,?,?,?,?)""",
        (plan_id, "source-a", plan_id, "ai", None, None, "source-config", 0, 1, TS),
    )
    connection.execute(
        """INSERT INTO query_attempts(
               attempt_id,query_plan_id,status,started_at,finished_at,returned_count,novel_count,
               verified_count,duplicate_count,stale_count,error_count,error,rate_limit_reset_at)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (attempt_id, plan_id, "running", TS, None, 0, 0, 0, 0, 0, 0, None, None),
    )


def _item(item_id: str, title: str) -> NormalizedItem:
    raw = json.dumps({"id": item_id, "title": title}, sort_keys=True)
    return NormalizedItem(
        source_item_id=item_id,
        source_id="source-a",
        external_id=item_id,
        category="ai",
        original_url=f"https://example.com/{item_id}",
        canonical_url=f"https://example.com/{item_id}",
        publisher="Publisher",
        source_role="discovery",
        retrieval_method="searxng-query",
        raw_content_hash=hashlib.sha256(raw.encode()).hexdigest(),
        retrieved_at=TS,
        published_at=TS,
        publication_evidence="metadata",
        title=title,
        body=title,
        raw=raw,
    )


class _BrokerSearchTransport:
    def __init__(self) -> None:
        self.requests = []

    async def __call__(self, request, *, retrieved_at: str) -> FetchResponse:
        self.requests.append(request)
        body = json.dumps({
            "results": [{
                "url": "https://example.com/evidence",
                "title": "Independent evidence",
                "content": "A bounded broker result.",
                "publishedDate": retrieved_at,
            }],
        }).encode()
        return FetchResponse(
            status=200,
            headers=(("Content-Type", "application/json"),),
            body=body,
            final_url=request.url,
        )


class SchemaV8Tests(unittest.TestCase):
    def test_replay_is_noop_and_partial_state_is_rejected(self) -> None:
        directory, _path, connection = _database()
        try:
            self.assertFalse(migrate_v8(connection, TS))
        finally:
            connection.close()
            directory.cleanup()

        directory = tempfile.TemporaryDirectory()
        path = Path(directory.name) / "partial.db"
        init_db(str(path))
        connection = sqlite3.connect(path, isolation_level=None)
        connection.execute("PRAGMA foreign_keys=ON")
        try:
            migrate_v3(connection, TS)
            migrate_v4(connection, TS)
            migrate_v5(connection, TS)
            migrate_v6(connection, TS)
            migrate_v7(connection, TS)
            connection.execute("CREATE TABLE feed_lane_receipts(sentinel INTEGER)")
            with self.assertRaises(ValueError):
                migrate_v8(connection, TS)
        finally:
            connection.close()
            directory.cleanup()


class FeedLanePersistenceTests(unittest.TestCase):
    def test_query_seeds_receive_one_category_and_distinct_stable_lanes(self) -> None:
        source = SourceContract(
            source_id="source-a",
            adapter_type=SourceAdapter.SEARXNG,
            source_role=SourceRole.DISCOVERY,
            host="example.com",
            category_scope=("ai",),
            enabled=True,
            queries=(
                QuerySeed("first query", ("general",)),
                QuerySeed("second query", ("general",)),
            ),
        )
        self.assertEqual({query.pipeline_category for query in source.queries}, {"ai"})
        self.assertEqual(len({query.feed_lane_id for query in source.queries}), 2)
        with self.assertRaises(ValueError):
            SourceContract(
                source_id="bad-source", adapter_type=SourceAdapter.SEARXNG,
                source_role=SourceRole.DISCOVERY, host="example.com",
                category_scope=("ai",), enabled=True,
                queries=(QuerySeed("bad", ("general",), pipeline_category="world"),),
            )

    def test_two_lanes_persist_independent_receipts(self) -> None:
        directory, path, connection = _database()
        try:
            _plan_attempt(connection, "plan-1", "attempt-1")
            _plan_attempt(connection, "plan-2", "attempt-2")
            source = SourceContract(
                source_id="source-a", adapter_type=SourceAdapter.SEARXNG,
                source_role=SourceRole.DISCOVERY, host="example.com",
                category_scope=("ai",), enabled=True,
                queries=(QuerySeed("one", ("general",)), QuerySeed("two", ("general",))),
            )
            jobs = []
            for query, plan_id, attempt_id, item_id in zip(
                source.queries, ("plan-1", "plan-2"), ("attempt-1", "attempt-2"), ("candidate-1", "candidate-2")
            ):
                job = _Job(
                    source=source, query=query, category="ai",
                    feed_lane_id=query.feed_lane_id or stable_feed_lane_id("source-a", "ai", query.text, query.categories),
                    query_plan_id=plan_id, attempt_id=attempt_id,
                    lease_expires_at=LEASE, etag=None, last_modified=None,
                )
                result = _persist_outcome(
                    connection,
                    _NetworkOutcome(job, FetchResult(items=(_item(item_id, query.text),), http_status=200), TS, 0),
                )
                jobs.append((job, result))
            self.assertNotEqual(jobs[0][1].feed_lane_id, jobs[1][1].feed_lane_id)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM feed_lane_receipts").fetchone()[0], 2)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM candidate_feed_lanes").fetchone()[0], 2)
        finally:
            connection.close()
            directory.cleanup()


class InvestigationTests(unittest.TestCase):
    def test_one_candidate_job_is_retry_stable_and_terminal_receipt_is_idempotent(self) -> None:
        directory, path, connection = _database()
        try:
            _plan_attempt(connection, "plan-candidate", "attempt-candidate")
            feed_lane = stable_feed_lane_id("source-a", "ai", "candidate query", ("general",))
            candidate = _item("candidate", "A new model release")
            connection.execute(
                """INSERT INTO source_items(
                       source_item_id,source_id,external_id,category,original_url,canonical_url,publisher,
                       source_role,author_handle,retrieval_method,raw_content_hash,title,body,raw,retrieved_at,
                       published_at,updated_at,publication_evidence)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    candidate.source_item_id, candidate.source_id, candidate.external_id, candidate.category,
                    candidate.original_url, candidate.canonical_url, candidate.publisher, candidate.source_role,
                    None, candidate.retrieval_method, candidate.raw_content_hash, candidate.title, candidate.body,
                    candidate.raw, candidate.retrieved_at, candidate.published_at, None, candidate.publication_evidence,
                ),
            )
            connection.execute(
                """INSERT INTO candidate_feed_lanes(
                       source_item_id,feed_lane_id,query_plan_id,attempt_id,category,first_seen_at)
                   VALUES(?,?,?,?,?,?)""",
                ("candidate", feed_lane, "plan-candidate", "attempt-candidate", "ai", TS),
            )
            job = make_investigation_job(
                candidate_id="candidate", feed_lane_id=feed_lane, query_plan_id="plan-candidate",
                category="ai", evaluated_at=TS,
            )
            payload = investigation_payload(job)
            self.assertEqual(payload["investigation_id"], investigation_id("candidate", feed_lane, "plan-candidate"))
            validate_investigation_payload(payload)
            for key, value in (
                ("candidate_ids", ["candidate", "other"]),
                ("other_candidate_id", "other"),
                ("candidates", ["candidate"]),
            ):
                with self.assertRaises(ValueError):
                    validate_investigation_payload({**payload, key: value})
            transport = _BrokerSearchTransport()
            completed = run_investigation(
                str(path), candidate_id="candidate", feed_lane_id=feed_lane,
                query_plan_id="plan-candidate", category="ai", evaluated_at=TS,
                transport_factory=lambda source: transport,
            )
            self.assertEqual(completed.job.terminal_state, "complete")
            self.assertTrue(completed.network_used)
            self.assertTrue(all(item.status == "success" for item in completed.query_results))
            self.assertEqual(len(transport.requests), len(completed.targeted_queries))
            persist_investigation(connection, completed.job)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM investigations").fetchone()[0], 1)
            replay = run_investigation(
                str(path), candidate_id="candidate", feed_lane_id=feed_lane,
                query_plan_id="plan-candidate", category="ai", evaluated_at=TS,
                transport_factory=lambda source: transport,
            )
            self.assertFalse(replay.network_used)
            self.assertEqual(replay.query_results, ())
            self.assertEqual(len(transport.requests), len(completed.targeted_queries))
            with self.assertRaises(ValueError):
                persist_investigation(
                    connection,
                    make_investigation_job(
                        candidate_id="candidate", feed_lane_id=feed_lane,
                        query_plan_id="plan-candidate", category="ai", evaluated_at=TS,
                        state="running",
                    ),
                )

            fenced_candidate = _item("fenced-candidate", "A fenced candidate")
            connection.execute(
                """INSERT INTO source_items(
                       source_item_id,source_id,external_id,category,original_url,canonical_url,publisher,
                       source_role,author_handle,retrieval_method,raw_content_hash,title,body,raw,retrieved_at,
                       published_at,updated_at,publication_evidence)
                   VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    fenced_candidate.source_item_id, fenced_candidate.source_id, fenced_candidate.external_id,
                    fenced_candidate.category, fenced_candidate.original_url, fenced_candidate.canonical_url,
                    fenced_candidate.publisher, fenced_candidate.source_role, None,
                    fenced_candidate.retrieval_method, fenced_candidate.raw_content_hash, fenced_candidate.title,
                    fenced_candidate.body, fenced_candidate.raw, fenced_candidate.retrieved_at,
                    fenced_candidate.published_at, None, fenced_candidate.publication_evidence,
                ),
            )
            _plan_attempt(connection, "plan-fenced", "attempt-fenced")
            connection.execute(
                """INSERT INTO candidate_feed_lanes(
                       source_item_id,feed_lane_id,query_plan_id,attempt_id,category,first_seen_at)
                   VALUES(?,?,?,?,?,?)""",
                (
                    "fenced-candidate", "fenced-lane", "plan-fenced", "attempt-fenced", "ai", TS,
                ),
            )
            fence_calls = 0

            def lose_fence() -> None:
                nonlocal fence_calls
                fence_calls += 1
                if fence_calls == 3:
                    raise RuntimeError("lease fence lost")

            with self.assertRaises(RuntimeError):
                persist_investigation(
                    connection,
                    make_investigation_job(
                        candidate_id="fenced-candidate", feed_lane_id="fenced-lane",
                        query_plan_id="plan-fenced", category="ai", evaluated_at=TS,
                        state="running",
                    ),
                    fence_check=lose_fence,
                )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM investigations WHERE candidate_id='fenced-candidate'"
                ).fetchone()[0],
                0,
            )
        finally:
            connection.close()
            directory.cleanup()


class InvestigationQueueTests(unittest.TestCase):
    def test_direct_investigate_job_requires_a_control_store_fence(self) -> None:
        investigation = investigation_id("candidate", "lane", "plan")
        args = _parser().parse_args([
            "investigate",
            "--db", "/tmp/state.db",
            "--candidate-id", "candidate",
            "--feed-lane-id", "lane",
            "--query-plan-id", "plan",
            "--investigation-id", investigation,
            "--category", "ai",
            "--evaluated-at", TS,
            "--enable-network",
        ])
        output = StringIO()
        with redirect_stdout(output):
            result = _run_investigate(args)
        self.assertEqual(result, 1)
        self.assertIn("requires --control-db", output.getvalue())

    def test_persisted_candidates_enqueue_one_idempotent_investigation_each(self) -> None:
        directory, path, connection = _database()
        control = open_control(Path(directory.name) / "control.db")
        try:
            for candidate_id in ("candidate-a", "candidate-b"):
                candidate = _item(candidate_id, candidate_id)
                connection.execute(
                    """INSERT INTO source_items(
                           source_item_id,source_id,external_id,category,original_url,canonical_url,publisher,
                           source_role,author_handle,retrieval_method,raw_content_hash,title,body,raw,retrieved_at,
                           published_at,updated_at,publication_evidence)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        candidate.source_item_id, candidate.source_id, candidate.external_id, candidate.category,
                        candidate.original_url, candidate.canonical_url, candidate.publisher, candidate.source_role,
                        None, candidate.retrieval_method, candidate.raw_content_hash, candidate.title, candidate.body,
                        candidate.raw, candidate.retrieved_at, candidate.published_at, None, candidate.publication_evidence,
                    ),
                )
                _plan_attempt(
                    connection,
                    f"plan-{candidate_id}",
                    f"attempt-{candidate_id}",
                )
                connection.execute(
                    """INSERT INTO candidate_feed_lanes(
                           source_item_id,feed_lane_id,query_plan_id,attempt_id,category,first_seen_at)
                       VALUES(?,?,?,?,?,?)""",
                    (
                        candidate_id,
                        stable_feed_lane_id("source-a", "ai", candidate_id, ("general",)),
                        f"plan-{candidate_id}", f"attempt-{candidate_id}", "ai", TS,
                    ),
                )
            connection.commit()
            first = enqueue_candidate_investigations(path, control, due_slot_utc=TS)
            second = enqueue_candidate_investigations(path, control, due_slot_utc=TS)
            self.assertEqual(len(first), 2)
            self.assertTrue(all(created for _task_id, created in first))
            self.assertTrue(all(not created for _task_id, created in second))
            self.assertEqual(control.execute("SELECT COUNT(*) FROM tasks WHERE kind='investigate'").fetchone()[0], 2)
        finally:
            control.close()
            connection.close()
            directory.cleanup()

    def test_failed_investigation_retry_advances_round_without_changing_task_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            control = open_control(Path(directory) / "control.db")
            try:
                first_job = make_investigation_job(
                    candidate_id="candidate", feed_lane_id="lane", query_plan_id="plan",
                    category="ai", evaluated_at=TS,
                )
                task_id, created = enqueue(
                    control, kind="investigate", due_slot_utc=TS,
                    payload=investigation_payload(first_job),
                )
                self.assertTrue(created)
                generation, _attempt, _expiry = claim(
                    control, task_id=task_id, owner="investigate-worker", ttl_seconds=60,
                )
                complete(
                    control, task_id=task_id, owner="investigate-worker", generation=generation,
                    status="failed", exit_code=1, stdout_hash="a" * 64,
                    error_class="RuntimeError", error_message="broker failed",
                )
                second_job = make_investigation_job(
                    candidate_id="candidate", feed_lane_id="lane", query_plan_id="plan",
                    category="ai", evaluated_at=TS, round_number=1,
                )
                retry_investigation(
                    control, task_id=task_id, payload=investigation_payload(second_job),
                )
                row = control.execute(
                    "SELECT task_id,state,payload_json FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()
                self.assertEqual(row[0], task_id)
                self.assertEqual(row[1], "pending")
                self.assertEqual(json.loads(row[2])["round_number"], 1)
            finally:
                control.close()

    def test_legacy_control_store_upgrades_without_losing_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy-control.db"
            connection = sqlite3.connect(path, isolation_level=None)
            connection.executescript(
                """
                CREATE TABLE schema_meta(version INTEGER PRIMARY KEY, created_at TEXT NOT NULL);
                INSERT INTO schema_meta(version, created_at) VALUES (1, '2026-09-20T00:00:00Z');
                CREATE TABLE tasks(
                    task_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    due_slot_utc TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(kind, due_slot_utc)
                );
                CREATE TABLE claims(
                    task_id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    expires_at_utc TEXT NOT NULL,
                    claimed_at_utc TEXT NOT NULL,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );
                CREATE TABLE runs(
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    owner TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    started_at_utc TEXT NOT NULL,
                    finished_at_utc TEXT,
                    status TEXT NOT NULL,
                    exit_code INTEGER,
                    stdout_hash TEXT,
                    error_class TEXT,
                    error_message TEXT,
                    canary_root TEXT,
                    FOREIGN KEY(task_id) REFERENCES tasks(task_id)
                );
                INSERT INTO tasks(task_id,kind,due_slot_utc,payload_json,state,generation,created_at)
                VALUES('legacy-task','ingest','2026-09-20T00:00:00Z','{}','pending',0,'2026-09-20T00:00:00Z');
                """
            )
            connection.close()
            upgraded = open_control(path)
            try:
                self.assertEqual(schema_version(upgraded), 2)
                self.assertEqual(upgraded.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 1)
                self.assertEqual(upgraded.execute("SELECT job_key FROM tasks").fetchone()[0], "")
            finally:
                upgraded.close()

    def test_same_due_slot_supports_two_candidates_and_reclaim_does_not_duplicate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            control = open_control(Path(directory) / "control.db")
            try:
                jobs = [
                    make_investigation_job(
                        candidate_id=candidate, feed_lane_id="lane", query_plan_id="plan",
                        category="ai", evaluated_at=TS,
                    )
                    for candidate in ("candidate-a", "candidate-b")
                ]
                task_a, created_a = enqueue(
                    control, kind="investigate", due_slot_utc=TS,
                    payload=investigation_payload(jobs[0]),
                )
                task_b, created_b = enqueue(
                    control, kind="investigate", due_slot_utc=TS,
                    payload=investigation_payload(jobs[1]),
                )
                self.assertTrue(created_a and created_b)
                self.assertNotEqual(task_a, task_b)
                self.assertEqual(enqueue(control, kind="investigate", due_slot_utc=TS, payload=investigation_payload(jobs[0]))[1], False)
                generation, _, expiry = claim(control, task_id=task_a, owner="worker-a", ttl_seconds=1)
                claim(
                    control,
                    task_id=task_a,
                    owner="worker-b",
                    ttl_seconds=1,
                    now=datetime.now(UTC) + timedelta(seconds=2),
                )
                self.assertGreater(generation, 0)
                self.assertTrue(expiry.endswith("Z"))
                self.assertEqual(control.execute("SELECT COUNT(*) FROM tasks WHERE kind='investigate'").fetchone()[0], 2)
            finally:
                control.close()


if __name__ == "__main__":
    unittest.main()
