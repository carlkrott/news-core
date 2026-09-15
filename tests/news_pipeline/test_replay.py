from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from news_pipeline.db import get_counts
from news_pipeline.db import connect
from news_pipeline.replay import replay_snapshot


class ReplayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.snapshot = self.root / "snapshot"
        self.snapshot.mkdir()
        self.db_path = str(self.root / "state.db")
        (self.snapshot / "ai-2026-06-10.md").write_text(
            "- **AI item**\n"
            "  Summary\n"
            "  https://example.com/ai?utm_source=x\n"
            "_Fetched: 2026-06-10T01:00:00Z_\n",
            encoding="utf-8",
        )
        (self.snapshot / "world-2026-06-10-08.md").write_text(
            "- **World item**\n"
            "  > Query: world news\n"
            "  https://example.com/world\n",
            encoding="utf-8",
        )
        (self.snapshot / "hardware-2026-06-10.md").write_text(
            "- **Hardware item**\n  No URL here\n",
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_dry_run_computes_but_does_not_persist(self) -> None:
        result = replay_snapshot(str(self.snapshot), self.db_path, dry_run=True)
        self.assertEqual(result["files_processed"], 3)
        self.assertEqual(result["articles_inserted"], 3)
        counts = get_counts(self.db_path)
        self.assertEqual(counts["articles"], 0)
        self.assertEqual(counts["events"], 0)

    def test_replay_persists_provenance_and_never_delivery_or_decisions(self) -> None:
        result = replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        counts = get_counts(self.db_path)
        self.assertEqual(result["files_processed"], 3)
        self.assertEqual(result["articles_inserted"], 3)
        self.assertEqual(result["observations_inserted"], 5)
        self.assertEqual(result["events_inserted"], 3)
        self.assertEqual(result["facts_inserted"], 5)
        self.assertEqual(result["delivery_attempts_inserted"], 0)
        self.assertEqual(result["category_appearances"], {"ai": 1, "hardware": 1, "world": 1})
        self.assertEqual(result["category_counts"], result["category_appearances"])
        self.assertEqual(result["provenance_counts"], {"observed_historical": 3})
        self.assertEqual(counts["delivery_attempts"], 0)
        self.assertEqual(counts["decisions"], 0)
        self.assertEqual(counts["articles"], 3)

    def test_second_replay_inserts_zero_and_counts_do_not_grow(self) -> None:
        first = replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        before = get_counts(self.db_path)
        second = replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        after = get_counts(self.db_path)
        self.assertGreater(first["articles_inserted"], 0)
        for key in (
            "articles_inserted",
            "observations_inserted",
            "events_inserted",
            "facts_inserted",
            "delivery_attempts_inserted",
        ):
            self.assertEqual(second[key], 0, key)
        self.assertEqual(after, before)

    def test_url_less_identity_dedupes_across_files_but_keeps_appearances(self) -> None:
        (self.snapshot / "ai-2026-06-11.md").write_text(
            "- **Same story**\n  Shared snippet\n", encoding="utf-8"
        )
        (self.snapshot / "ai-2026-06-12.md").write_text(
            "- **Same story**\n  Shared snippet\n", encoding="utf-8"
        )
        (self.snapshot / "ai-2026-06-13.md").write_text(
            "- **Same story**\n  Changed snippet\n", encoding="utf-8"
        )
        result = replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        self.assertEqual(result["articles_inserted"], 5)
        self.assertEqual(result["category_appearances"]["ai"], 4)
        with connect(self.db_path) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM articles WHERE canonical_url IS NULL").fetchone()[0], 3)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM observations WHERE kind='parsed_article'").fetchone()[0], 6)
            self.assertEqual(con.execute("SELECT COUNT(DISTINCT article_id) FROM event_articles WHERE article_id IN (SELECT id FROM articles WHERE canonical_url IS NULL)").fetchone()[0], 3)

    def test_zero_article_query_failure_is_replayed_as_file_observation(self) -> None:
        path = self.snapshot / "ai-2026-07-10.md"
        path.write_text(
            "<!-- query LLM+AI failed: timed out -->\n"
            "<!-- query Anthropic failed: timed out -->\n"
            "_Fetched: 2026-07-10T14:34:24Z_\n", encoding="utf-8"
        )
        result = replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        self.assertEqual(result["articles_inserted"], 3)
        with connect(self.db_path) as con:
            row = con.execute("SELECT COUNT(*), COUNT(article_id), COUNT(event_id) FROM observations WHERE source_file='ai-2026-07-10.md'").fetchone()
            self.assertEqual(row, (3, 0, 3))
            self.assertEqual(con.execute("SELECT COUNT(*) FROM observations WHERE kind='query_failure'").fetchone()[0], 2)

    def test_event_observation_count_equals_total_observations_for_that_event(self) -> None:
        """``events.observation_count`` must equal the sum of every observation
        row attached to the event: parsed_article appearances (one per
        article), article-level observations (e.g. ``query_failure`` /
        ``query_comment``), plus file-level observations
        (``query_failure`` / ``query_comment`` / ``fetch_marker``)."""
        (self.snapshot / "ai-2026-06-15.md").write_text(
            "- **AI item A**\n"
            "  Summary A\n"
            "  > Query: extra\n"
            "  https://example.com/a\n"
            "_Fetched: 2026-06-15T01:00:00Z_\n"
            "- **AI item B**\n"
            "  Summary B\n"
            "  > Query: extra b\n"
            "  > Query failed: bad gateway\n"
            "  https://example.com/b\n"
            "<!-- query Late failure: bad row -->\n",
            encoding="utf-8",
        )
        replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        with connect(self.db_path) as con:
            rows = con.execute(
                "SELECT e.id, e.article_count, e.observation_count, "
                "(SELECT COUNT(*) FROM observations o WHERE o.event_id=e.id) AS actual, "
                "(SELECT o.source_file FROM observations o WHERE o.event_id=e.id LIMIT 1) AS source_file "
                "FROM events e"
            ).fetchall()
        self.assertGreater(len(rows), 0)
        for event_id, article_count, stored_count, actual, source_file in rows:
            self.assertEqual(
                stored_count,
                actual,
                f"event {event_id} ({source_file}): stored observation_count="
                f"{stored_count} actual={actual}",
            )
            # stored_count >= article_count is the universal minimum (every
            # article yields one parsed_article observation).
            self.assertGreaterEqual(stored_count, article_count)

    def test_query_observations_counts_are_reported_for_failure_file(self) -> None:
        """Real failure-only files must surface in
        ``replay_snapshot`` ``query_observations`` counters so the rebuilt
        state DB carries an honest accounting of query failure / comment /
        fetch_marker evidence.

        setUp seeds 3 files; the test adds a 4th with HTML query comments.
        Expected deltas introduced by the new file:

        - 2 file-level query_failure
        - 1 file-level query_comment
        - 1 file-level fetch_marker

        (setUp's ``ai-2026-06-10.md`` already contributed 1 fetch_marker.)
        """
        (self.snapshot / "ai-2026-07-10.md").write_text(
            "<!-- query LLM+AI failed: timeout -->\n"
            "<!-- query Anthropic failed: timeout -->\n"
            "<!-- query OpenAI: trial query -->\n"
            "_Fetched: 2026-07-10T14:34:24Z_\n",
            encoding="utf-8",
        )
        result = replay_snapshot(str(self.snapshot), self.db_path, dry_run=False)
        q = result["query_observations"]
        self.assertEqual(q["file_level_query_failure"], 2)
        self.assertEqual(q["file_level_query_comment"], 1)
        self.assertEqual(q["file_level_fetch_marker"], 2)  # 1 setUp + 1 new
        self.assertEqual(q["article_level_query_failure"], 0)
        self.assertEqual(q["article_level_query_comment"], 1)  # setUp world
        # Sanity: query counters should sum to a non-zero share.
        total_query = sum(q.values())
        self.assertGreater(total_query, 0)


if __name__ == "__main__":
    unittest.main()
