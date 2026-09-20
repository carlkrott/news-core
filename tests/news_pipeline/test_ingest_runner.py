"""Network-disabled integration tests for the bounded schema-v4 ingest runner."""
from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import sqlite3
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from news_pipeline import db as news_db
from news_pipeline.adapters.base import FetchRequest, FetchResponse, NetworkError
from news_pipeline.ingest_runner import (
    IngestReport,
    _claim_queries,
    _filter_reason,
    _open_writer,
    _sync_sources,
    run_ingest,
)
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.source_registry import load_registry

ROOT = Path(__file__).resolve().parents[2]
TOPICS = ROOT / "config/news-topics.example.toml"
POLICY = ROOT / "config/news-policy.example.toml"
RUN_AT = "2026-01-01T00:00:00Z"
FINISHED_AT = "2026-01-01T00:00:10Z"


def create_v4_db(path: Path) -> None:
    news_db.init_db(str(path))
    connection = sqlite3.connect(path, isolation_level=None)
    connection.execute("PRAGMA foreign_keys=ON")
    try:
        migrate_v3(connection, "2026-01-01T00:00:01Z")
        migrate_v4(connection, "2026-01-01T00:00:02Z")
    finally:
        connection.close()


def write_sources(path: Path, sources: list[dict]) -> Path:
    required_categories = {
        "ai",
        "world",
        "audio_engineering",
        "hardware",
        "fantasy_novel",
        "audiovisual",
        "av_corporate",
        "our_setup",
    }
    present_categories = {
        category
        for configured_source in sources
        for category in configured_source.get("category_scope", ["ai"])
    }
    for category in sorted(required_categories - present_categories):
        sources.append(
            {
                "source_id": f"zz-{category}",
                "host": f"disabled-{category}.test",
                "adapter_type": "searxng",
                "source_role": "discovery",
                "category_scope": [category],
                "enabled": False,
                "queries": [{"text": f"disabled {category}", "categories": ["general"]}],
            }
        )
    sources.sort(key=lambda configured_source: configured_source["source_id"])
    lines = ["version = 2", ""]
    for source in sources:
        lines.extend(
            [
                "[[sources]]",
                f"source_id = {json.dumps(source['source_id'])}",
                f"adapter_type = {json.dumps(source.get('adapter_type', 'searxng'))}",
                f"source_role = {json.dumps(source.get('source_role', 'discovery'))}",
                f"host = {json.dumps(source['host'])}",
                f"category_scope = {json.dumps(source.get('category_scope', ['ai']))}",
                f"enabled = {'true' if source.get('enabled', True) else 'false'}",
                f"cadence_minutes = {source.get('cadence_minutes', 30)}",
            ]
        )
        for key in ("title_blocklist", "content_blocklist", "url_blocklist", "allowlist_domains"):
            if key in source:
                lines.append(f"{key} = {json.dumps(source[key])}")
        for query in source.get("queries", [{"text": "AI news", "categories": ["general"]}]):
            lines.extend(
                [
                    "[[sources.queries]]",
                    f"text = {json.dumps(query['text'])}",
                    f"categories = {json.dumps(query.get('categories', ['general']))}",
                ]
            )
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def source(
    source_id: str = "src1",
    *,
    host: str = "search.example.test",
    queries: int = 1,
    adapter_type: str = "searxng",
    source_role: str = "discovery",
    category: str = "ai",
    **extra,
) -> dict:
    value = {
        "source_id": source_id,
        "host": host,
        "adapter_type": adapter_type,
        "source_role": source_role,
        "category_scope": [category],
        "queries": [
            {"text": f"query {source_id} {index}", "categories": ["general"]}
            for index in range(queries)
        ],
    }
    value.update(extra)
    return value


def response(
    *,
    status: int = 200,
    body: bytes | None = None,
    content_type: str = "application/json",
    headers: tuple[tuple[str, str], ...] = (),
) -> FetchResponse:
    if body is None:
        body = b'{"results":[]}'
    return FetchResponse(
        status=status,
        headers=(("Content-Type", content_type),) + headers,
        body=body,
        final_url="https://upstream.example.test/result?secret=not-logged",
    )


def result_body(*items: dict) -> bytes:
    return json.dumps({"results": list(items)}, separators=(",", ":")).encode()


def article(
    url: str = "https://allowed.example.com/story?id=1",
    *,
    title: str = "A useful story",
    content: str = "Useful details",
    published: str = "2026-01-01T00:00:00Z",
    external_id: str = "external-1",
) -> dict:
    return {
        "url": url,
        "title": title,
        "content": content,
        "publishedDate": published,
        "id": external_id,
        "author": "Reporter",
    }


class ScriptedFactory:
    def __init__(self, scripts: dict[str, list[FetchResponse | BaseException]]) -> None:
        self.scripts = {key: list(values) for key, values in scripts.items()}
        self.calls: dict[str, list[FetchRequest]] = defaultdict(list)

    def __call__(self, contract):
        async def transport(request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
            self.calls[contract.source_id].append(request)
            script = self.scripts[contract.source_id]
            value = script.pop(0) if len(script) > 1 else script[0]
            if isinstance(value, BaseException):
                raise value
            return FetchResponse(value.status, value.headers, value.body, request.url)

        return transport


class FakeTiming:
    def __init__(self) -> None:
        self.value = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.value

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value += seconds
        await asyncio.sleep(0)


class RunnerCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "state.db"
        create_v4_db(self.db)
        self.sources = write_sources(self.root / "sources.toml", [source()])

    async def asyncTearDown(self) -> None:
        self.temp.cleanup()

    async def run_with(
        self,
        factory,
        *,
        run_at: str = RUN_AT,
        sources: Path | None = None,
        source_ids=None,
        max_queries=None,
        timing: FakeTiming | None = None,
    ) -> IngestReport:
        timing = timing or FakeTiming()
        return await run_ingest(
            self.db,
            sources or self.sources,
            TOPICS,
            POLICY,
            run_at,
            source_ids=source_ids,
            max_queries=max_queries,
            transport_factory=factory,
            async_sleep=timing.sleep,
            monotonic=timing.monotonic,
            utc_now=lambda: FINISHED_AT,
        )

    def rows(self, sql: str, params=()) -> list[tuple]:
        connection = sqlite3.connect(self.db)
        try:
            return connection.execute(sql, params).fetchall()
        finally:
            connection.close()

    def make_due(self, *source_ids: str) -> None:
        connection = sqlite3.connect(self.db)
        try:
            for source_id in source_ids:
                connection.execute(
                    "UPDATE source_registry SET next_due_at=NULL WHERE source_id=?", (source_id,)
                )
            connection.commit()
        finally:
            connection.close()


class SchemaAndLeaseTests(RunnerCase):
    async def test_absent_database_refused_without_creation(self) -> None:
        missing = self.root / "missing.db"
        with self.assertRaises(FileNotFoundError):
            await run_ingest(missing, self.sources, TOPICS, POLICY, RUN_AT)
        self.assertFalse(missing.exists())

    async def test_non_v4_database_refused_without_byte_mutation(self) -> None:
        connection = sqlite3.connect(self.db)
        connection.execute("DELETE FROM schema_migrations WHERE version=4")
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.close()
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        with self.assertRaisesRegex(ValueError, "markers"):
            await run_ingest(self.db, self.sources, TOPICS, POLICY, RUN_AT)
        self.assertEqual(hashlib.sha256(self.db.read_bytes()).hexdigest(), before)

    async def test_config_sync_preserves_runtime_due_and_updates_hash(self) -> None:
        config = load_registry(self.sources, TOPICS, POLICY)
        connection = _open_writer(self.db)
        try:
            _sync_sources(connection, config.sources, RUN_AT)
            first_hash = connection.execute(
                "SELECT config_hash FROM source_registry WHERE source_id='src1'"
            ).fetchone()[0]
            connection.execute(
                "UPDATE source_registry SET next_due_at='2030-01-01T00:00:00Z' WHERE source_id='src1'"
            )
            changed = write_sources(
                self.root / "changed.toml", [source(title_blocklist=["changed"])]
            )
            changed_config = load_registry(changed, TOPICS, POLICY)
            _sync_sources(connection, changed_config.sources, "2026-01-01T00:01:00Z")
            due, second_hash = connection.execute(
                "SELECT next_due_at,config_hash FROM source_registry WHERE source_id='src1'"
            ).fetchone()
            self.assertEqual(due, "2030-01-01T00:00:00Z")
            self.assertNotEqual(first_hash, second_hash)
        finally:
            connection.close()

    async def test_claim_is_atomic_and_reclaimable_at_lease_expiry(self) -> None:
        config = load_registry(self.sources, TOPICS, POLICY)
        connection = _open_writer(self.db)
        try:
            _sync_sources(connection, config.sources, RUN_AT)
            first = _claim_queries(connection, config.sources, RUN_AT, source_ids=None, max_queries=None)
            self.assertEqual(len(first), 1)
            self.assertEqual(first[0].lease_expires_at, "2026-01-01T00:05:00Z")
            self.assertEqual(
                _claim_queries(
                    connection,
                    config.sources,
                    "2026-01-01T00:04:59Z",
                    source_ids=None,
                    max_queries=None,
                ),
                (),
            )
            reclaimed = _claim_queries(
                connection,
                config.sources,
                "2026-01-01T00:05:00Z",
                source_ids=None,
                max_queries=None,
            )
            self.assertEqual(len(reclaimed), 1)
        finally:
            connection.close()


class PersistenceTests(RunnerCase):
    async def test_success_persists_provenance_attempt_state_and_completion_cadence(self) -> None:
        factory = ScriptedFactory(
            {"src1": [response(body=result_body(article()))]}
        )
        report = await self.run_with(factory)
        self.assertEqual(report.total_items_inserted, 1)
        self.assertEqual(report.total_items_duplicate, 0)
        self.assertEqual(report.sources_with_errors, 0)
        item = self.rows(
            """SELECT source_id,external_id,category,original_url,canonical_url,publisher,
                      source_role,author_handle,retrieval_method,raw_content_hash,title,body,
                      raw,retrieved_at,published_at,updated_at,publication_evidence
               FROM source_items"""
        )[0]
        self.assertEqual(item[0:3], ("src1", "external-1", "ai"))
        self.assertEqual(item[6:9], ("discovery", "Reporter", "searxng-query"))
        self.assertEqual(len(item[9]), 64)
        self.assertEqual(item[13], FINISHED_AT)
        self.assertEqual(item[14], RUN_AT)
        attempt = self.rows(
            "SELECT status,returned_count,novel_count,duplicate_count,error_count,error FROM query_attempts"
        )[0]
        self.assertEqual(attempt, ("success", 1, 1, 0, 0, None))
        state = self.rows(
            "SELECT last_http_status,updated_at FROM fetch_state WHERE source_id='src1'"
        )[0]
        self.assertEqual(state, (200, FINISHED_AT))
        self.assertEqual(
            self.rows("SELECT next_due_at FROM source_registry WHERE source_id='src1'")[0][0],
            "2026-01-01T00:30:10Z",
        )

    async def test_exact_replay_is_idempotent_with_stable_plan(self) -> None:
        first = ScriptedFactory({"src1": [response(body=result_body(article()))]})
        await self.run_with(first)
        self.make_due("src1")
        second = ScriptedFactory({"src1": [response(body=result_body(article()))]})
        report = await self.run_with(second, run_at="2026-01-01T01:00:00Z")
        self.assertEqual(report.total_items_inserted, 0)
        self.assertEqual(report.total_items_duplicate, 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM source_items")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM query_plans")[0][0], 1)
        self.assertEqual(self.rows("SELECT COUNT(*) FROM query_attempts")[0][0], 2)

    async def test_304_preserves_prior_validators(self) -> None:
        first = ScriptedFactory(
            {"src1": [response(headers=(("ETag", '"v1"'), ("Last-Modified", "Wed, 01 Jan 2026 00:00:00 GMT")))]}
        )
        await self.run_with(first)
        self.make_due("src1")
        second = ScriptedFactory({"src1": [response(status=304, body=b"", content_type="text/plain")]})
        await self.run_with(second, run_at="2026-01-01T01:00:00Z")
        state = self.rows(
            "SELECT etag,last_modified,last_http_status FROM fetch_state WHERE source_id='src1'"
        )[0]
        self.assertEqual(state, ('"v1"', "Wed, 01 Jan 2026 00:00:00 GMT", 304))

    async def test_filtering_and_adapter_rejections_are_typed_and_partial(self) -> None:
        self.sources = write_sources(
            self.root / "filtered.toml",
            [
                source(
                    title_blocklist=["blocked"],
                    allowlist_domains=["allowed.example.com"],
                )
            ],
        )
        body = result_body(
            article(),
            article(title="Blocked announcement", external_id="blocked"),
            article(url="https://notallowed.example.net/story", external_id="domain"),
            {"title": "Missing URL"},
        )
        report = await self.run_with(ScriptedFactory({"src1": [response(body=body)]}))
        query = report.query_results[0]
        self.assertEqual(query.status, "partial")
        self.assertEqual(query.inserted_count, 1)
        self.assertEqual(query.filtered_count, 2)
        self.assertEqual(query.rejected_count, 1)
        self.assertEqual(query.rejections[0].code, "MISSING_URL")
        self.assertEqual(
            {item.reason.split(":", 1)[0] for item in query.filtered_items},
            {"title_blocklist", "allowlist_domain"},
        )

    async def test_rss_source_dispatches_feed_api_and_persists(self) -> None:
        self.sources = write_sources(
            self.root / "rss.toml",
            [
                source(
                    adapter_type="rss",
                    source_role="primary",
                    queries=1,
                    host="feed.example.test",
                )
            ],
        )
        # RSS QuerySeed.text must be a URL.
        text = self.sources.read_text()
        text = text.replace('text = "query src1 0"', 'text = "https://feed.example.test/rss.xml"')
        self.sources.write_text(text)
        xml = b"""<?xml version='1.0'?><rss version='2.0'><channel><item>
            <guid>rss-1</guid><title>Release</title><link>https://feed.example.test/item</link>
            <description>Details</description><pubDate>Wed, 01 Jan 2026 00:00:00 GMT</pubDate>
            </item></channel></rss>"""
        factory = ScriptedFactory(
            {"src1": [response(body=xml, content_type="application/rss+xml")]}
        )
        report = await self.run_with(factory)
        self.assertEqual(report.total_items_inserted, 1)
        self.assertIn("https://feed.example.test/rss.xml", factory.calls["src1"][0].url)
        self.assertEqual(self.rows("SELECT retrieval_method FROM source_items")[0][0], "rss-poll")


class RetryAndFailureTests(RunnerCase):
    async def test_500_retries_then_succeeds_and_reports_retry(self) -> None:
        timing = FakeTiming()
        factory = ScriptedFactory(
            {"src1": [response(status=500), response(body=result_body(article()))]}
        )
        report = await self.run_with(factory, timing=timing)
        self.assertEqual(len(factory.calls["src1"]), 2)
        self.assertEqual(report.total_retries, 1)
        self.assertEqual(report.query_results[0].status, "success")
        self.assertIn(1.0, timing.sleeps)

    async def test_429_honors_retry_after_and_is_rate_limited_after_exhaustion(self) -> None:
        timing = FakeTiming()
        limited = response(status=429, headers=(("Retry-After", "10"), ("X-RateLimit-Reset", "1767226200")))
        factory = ScriptedFactory({"src1": [limited, limited, limited]})
        report = await self.run_with(factory, timing=timing)
        query = report.query_results[0]
        self.assertEqual(len(factory.calls["src1"]), 3)
        self.assertEqual(query.status, "rate_limited")
        self.assertEqual(query.retries, 2)
        self.assertEqual(query.retry_after_seconds, 10)
        self.assertGreaterEqual(timing.sleeps.count(10.0), 2)
        persisted = self.rows(
            "SELECT status,error_count,rate_limit_reset_at FROM query_attempts"
        )[0]
        self.assertEqual(persisted[0:2], ("rate_limited", 1))
        self.assertIsNotNone(persisted[2])

    async def test_410_is_terminal_and_5xx_is_failed_not_rate_limited(self) -> None:
        for status in (410, 500):
            with self.subTest(status=status):
                if status == 500:
                    scripted = [response(status=500)] * 3
                else:
                    scripted = [response(status=410)]
                factory = ScriptedFactory({"src1": scripted})
                report = await self.run_with(factory, run_at=f"2026-01-01T0{status % 10}:00:00Z")
                self.assertEqual(report.query_results[0].status, "failed")
                self.assertEqual(len(factory.calls["src1"]), 3 if status == 500 else 1)
                self.make_due("src1")

    async def test_network_error_retries(self) -> None:
        factory = ScriptedFactory(
            {"src1": [NetworkError("offline"), response(body=result_body(article()))]}
        )
        report = await self.run_with(factory)
        self.assertEqual(report.total_retries, 1)
        self.assertEqual(report.total_items_inserted, 1)

    async def test_mime_oversize_and_parse_failures_are_persisted_without_retry(self) -> None:
        cases = (
            response(content_type="text/html"),
            response(body=b"x" * (2 * 1024 * 1024 + 1)),
            response(body=b"not-json"),
        )
        for index, bad in enumerate(cases):
            with self.subTest(index=index):
                factory = ScriptedFactory({"src1": [bad]})
                report = await self.run_with(
                    factory, run_at=f"2026-01-01T0{index}:10:00Z"
                )
                query = report.query_results[0]
                self.assertEqual(query.status, "failed")
                self.assertEqual(query.retries, 0)
                self.assertIsNotNone(query.error)
                row = self.rows(
                    "SELECT status,error,last_http_status FROM query_attempts "
                    "JOIN fetch_state ON fetch_state.source_id='src1' ORDER BY started_at DESC LIMIT 1"
                )[0]
                self.assertEqual(row[0], "failed")
                self.assertNotIn("not-json", row[1])
                self.make_due("src1")

    async def test_unexpected_failure_isolated_and_persisted(self) -> None:
        self.sources = write_sources(
            self.root / "two.toml",
            [source("src1", host="one.test"), source("src2", host="two.test", category="world")],
        )
        factory = ScriptedFactory(
            {
                "src1": [ValueError("bug with https://host/path?token=secret")],
                "src2": [response(body=result_body(article(external_id="two")))],
            }
        )
        report = await self.run_with(factory)
        by_source = {result.source_id: result for result in report.source_results}
        self.assertIsNotNone(by_source["src1"].error)
        self.assertIsNone(by_source["src2"].error)
        stored_error = self.rows(
            "SELECT error FROM query_attempts WHERE status='failed'"
        )[0][0]
        self.assertNotIn("token=secret", stored_error)
        self.assertEqual(report.total_items_inserted, 1)


class LimitAndConcurrencyTests(RunnerCase):
    async def test_source_filter_and_global_query_limit_claim_only_selected_work(self) -> None:
        self.sources = write_sources(
            self.root / "limited.toml",
            [source("src1", queries=2), source("src2", queries=2, host="two.test", category="world")],
        )
        factory = ScriptedFactory({"src1": [response()], "src2": [response()]})
        report = await self.run_with(factory, source_ids=("src2",), max_queries=1)
        self.assertEqual(report.sources_claimed, 1)
        self.assertEqual(report.total_queries_run, 1)
        self.assertEqual(list(factory.calls), ["src2"])
        due = dict(self.rows("SELECT source_id,next_due_at FROM source_registry"))
        self.assertIsNone(due["src1"])
        self.assertIsNotNone(due["src2"])

    async def test_global_network_concurrency_never_exceeds_four(self) -> None:
        sources = [source(f"src{index}", host=f"host{index}.test") for index in range(5)]
        self.sources = write_sources(self.root / "five.toml", sources)

        class ConcurrentFactory:
            def __init__(self) -> None:
                self.active = 0
                self.maximum = 0
                self.release = asyncio.Event()

            def __call__(self, contract):
                async def transport(request, *, retrieved_at):
                    self.active += 1
                    self.maximum = max(self.maximum, self.active)
                    if self.active == 4:
                        self.release.set()
                    await self.release.wait()
                    await asyncio.sleep(0)
                    self.active -= 1
                    return FetchResponse(200, (("Content-Type", "application/json"),), b'{"results":[]}', request.url)

                return transport

        factory = ConcurrentFactory()
        report = await self.run_with(factory)
        self.assertEqual(report.total_queries_run, 5)
        self.assertEqual(factory.maximum, 4)

    async def test_same_host_is_serial_and_starts_at_least_one_second_apart(self) -> None:
        self.sources = write_sources(
            self.root / "same-host.toml",
            [source("src1", host="same.test"), source("src2", host="same.test", category="world")],
        )
        timing = FakeTiming()

        class HostFactory:
            def __init__(self) -> None:
                self.active = 0
                self.maximum = 0
                self.starts: list[float] = []

            def __call__(self, contract):
                async def transport(request, *, retrieved_at):
                    self.active += 1
                    self.maximum = max(self.maximum, self.active)
                    self.starts.append(timing.monotonic())
                    await asyncio.sleep(0)
                    self.active -= 1
                    return FetchResponse(200, (("Content-Type", "application/json"),), b'{"results":[]}', request.url)

                return transport

        factory = HostFactory()
        await self.run_with(factory, timing=timing)
        self.assertEqual(factory.maximum, 1)
        self.assertEqual(len(factory.starts), 2)
        self.assertGreaterEqual(factory.starts[1] - factory.starts[0], 1.0)


class StaticBoundaryTests(unittest.TestCase):
    def test_runner_has_no_migration_or_external_side_effect_calls(self) -> None:
        path = ROOT / "scripts/news_pipeline/ingest_runner.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports: set[str] = set()
        called_names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
                elif isinstance(node.func, ast.Attribute):
                    called_names.add(node.func.attr)
        self.assertTrue(imports.isdisjoint({"requests", "httpx", "aiohttp", "subprocess"}))
        self.assertTrue(called_names.isdisjoint({"init_db", "migrate_v3", "migrate_v4"}))
        self.assertTrue(called_names.isdisjoint({"send", "deliver", "publish", "post"}))

    def test_network_worker_has_no_database_parameter_or_sql(self) -> None:
        path = ROOT / "scripts/news_pipeline/ingest_runner.py"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        function = next(
            node for node in tree.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_fetch_job"
        )
        parameter_names = {arg.arg for arg in function.args.args + function.args.kwonlyargs}
        source_text = ast.unparse(function)
        self.assertNotIn("connection", parameter_names)
        self.assertNotIn("sqlite", source_text)
        self.assertNotIn("execute(", source_text)

    def test_report_types_are_immutable_and_slotted(self) -> None:
        report = IngestReport(RUN_AT, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        self.assertFalse(hasattr(report, "__dict__"))
        with self.assertRaises((AttributeError, TypeError)):
            report.sources_claimed = 1  # type: ignore[misc]

    def test_allowlist_matches_exact_domain_or_subdomain_not_substring(self) -> None:
        from news_pipeline.adapters.base import NormalizedItem

        def item(url: str) -> NormalizedItem:
            return NormalizedItem(
                "id", "src", None, "ai", url, url, "publisher", "discovery",
                "test", "a" * 64, RUN_AT,
            )

        kwargs = dict(
            title_blocklist=(), content_blocklist=(), url_blocklist=(),
            allowlist_domains=("example.com",),
        )
        self.assertIsNone(_filter_reason(item("https://news.example.com/x"), **kwargs))
        self.assertIsNotNone(_filter_reason(item("https://notexample.com/x"), **kwargs))


if __name__ == "__main__":
    unittest.main()
