from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from news_pipeline.adapters.base import FetchRequest, FetchResponse
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.db import init_db
from news_pipeline.event_store import process_phase4
from news_pipeline.ingest_runner import run_ingest
from news_pipeline.report_builder import run_report
from news_pipeline.process_runner import run_process
from news_pipeline.schema_v10 import migrate_v10
from news_pipeline.schema_v11 import migrate_v11
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9

from tests.news_pipeline.test_ingest_runner import write_sources


ARTICLE_URL = "https://archlinux.org/news/mkinitcpio-42-requires-manual-intervention-for-tpm2-based-unlocking-of-luks-devices/"
ARTICLE_TEXT = (
    "Starting with package version 42-1, the mkinitcpio systemd hook now includes "
    "systemd-pcrosseparator.service (as intended by systemd v261). "
    "This affects the measurements of PCR values 0-7, 9 and 12-14."
)
FEED = f'''<?xml version="1.0"?><rss version="2.0"><channel><title>Arch News</title>
<item><guid>mkinitcpio-42-2026</guid><title>Mkinitcpio 42 requires manual intervention for TPM2</title>
<link>{ARTICLE_URL}</link><description>mkinitcpio package version 42 requires re-enrollment.</description>
<pubDate>Tue, 22 Sep 2026 00:00:00 GMT</pubDate></item></channel></rss>'''.encode()
HTML = f"<html><head><title>Arch News</title></head><body><nav>Skip</nav><main><article><p>{ARTICLE_TEXT}</p></article></main></body></html>".encode()


class ArticleFetchPipelineIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_real_article_body_ingests_claims_verifies_and_reports_once(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            db = root / "news.db"
            init_db(str(db))
            con = sqlite3.connect(db, isolation_level=None)
            con.execute("PRAGMA foreign_keys=ON")
            for version, migration in enumerate(
                (migrate_v3, migrate_v4, migrate_v5, migrate_v6, migrate_v7, migrate_v8, migrate_v9, migrate_v10), 1
            ):
                migration(con, f"2026-10-06T00:00:{version:02d}Z")
            migrate_v11(con, "2026-10-06T00:00:20Z")
            con.close()
            source = {
                "source_id": "arch-first-party", "host": "archlinux.org", "adapter_type": "rss",
                "source_role": "primary", "category_scope": ["our_setup"],
                "allowlist_domains": ["archlinux.org"],
                "queries": [{"text": "https://archlinux.org/feeds/news/", "categories": ["news"]}],
            }
            sources = write_sources(root / "sources.toml", [source])
            provenance = root / "provenance.toml"
            provenance.write_text('''version = 1
[[publishers]]
rule_id = "archlinux-own-news"
host = "archlinux.org"
source_role = "primary"
independence_group = "archlinux-own-news"
categories = ["our_setup"]
authority_entities = ["mkinitcpio"]
allow_article_fetch = true
audit_note = "bounded public first-party retrieval rehearsal"
''', encoding="utf-8")

            def feeds(_contract):
                async def get(request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
                    return FetchResponse(200, (("Content-Type", "application/rss+xml"),), FEED, request.url)
                return get

            def articles(_contract):
                async def get(request: FetchRequest, *, retrieved_at: str) -> FetchResponse:
                    self.assertEqual(request.url, ARTICLE_URL)
                    self.assertEqual(request.max_response_bytes, 512 * 1024)
                    self.assertFalse(any(k.casefold() in {"authorization", "cookie"} for k, _ in request.headers))
                    return FetchResponse(200, (("Content-Type", "text/html; charset=utf-8"),), HTML, request.url)
                return get

            ingested = await run_ingest(
                db, sources, Path(__file__).parents[2] / "config/news-topics.example.toml",
                Path(__file__).parents[2] / "config/news-policy.example.toml",
                "2026-10-06T08:00:00Z", provenance_path=provenance,
                transport_factory=feeds, article_fetch_enabled=True,
                article_transport_factory=articles, async_sleep=lambda _s: asyncio.sleep(0),
                utc_now=lambda: "2026-10-06T08:00:10Z",
            )
            # Article enrichment replaces the RSS item's body/provenance in place;
            # it does not create a second source item.
            self.assertEqual(ingested.total_items_inserted, 1)
            con = sqlite3.connect(db)
            try:
                fetched = con.execute(
                    "SELECT source_item_id,body FROM source_items WHERE retrieval_method='publisher-article-fetch'"
                ).fetchall()
                self.assertEqual(len(fetched), 1)
                self.assertIn("systemd-pcrosseparator.service", fetched[0][1])
            finally:
                con.close()

            process_result = run_process(db, "2026-10-06T08:00:30Z", minimum_novelty_target=0)
            self.assertGreaterEqual(process_result.decisions_persisted, 1)
            processed = process_phase4(db, "2026-10-06T08:01:00Z")
            self.assertGreaterEqual(processed.processed_count, 1)
            con = sqlite3.connect(db)
            try:
                claim = con.execute(
                    "SELECT c.subject,p.authority_match,p.effective_source_role,e.exact_excerpt "
                    "FROM claims c JOIN claim_evidence e ON e.claim_id=c.claim_id "
                    "JOIN claim_evidence_provenance p ON p.evidence_id=e.evidence_id "
                    "WHERE p.source_item_id=?", (fetched[0][0],)
                ).fetchone()
                self.assertIsNotNone(claim)
                self.assertEqual(claim[0], "mkinitcpio")
                self.assertEqual(claim[1:3], (1, "primary"))
                self.assertIn("systemd-pcrosseparator.service", claim[3])
                report_root = root / "reports"
                report_root.mkdir()
                report = run_report(
                    db, report_root, datetime(2026, 10, 7, 8, 2, tzinfo=timezone.utc)
                )
                report_row = con.execute(
                    "SELECT story_count FROM subject_reports WHERE subject_id='our_setup'"
                ).fetchone()
                self.assertIsNotNone(report_row)
                self.assertEqual(report_row[0], 1, repr(report))
            finally:
                con.close()

            replay = process_phase4(db, "2026-10-06T08:03:00Z")
            self.assertEqual(replay.processed_count, 0)
            con = sqlite3.connect(db)
            try:
                self.assertEqual(con.execute(
                    "SELECT COUNT(*) FROM source_items WHERE retrieval_method='publisher-article-fetch'"
                ).fetchone()[0], 1)
                self.assertEqual(con.execute(
                    "SELECT COUNT(*) FROM event_versions WHERE verification_state='verified'"
                ).fetchone()[0], 1)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0], 0)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
