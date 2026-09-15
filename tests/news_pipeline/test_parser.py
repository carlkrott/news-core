from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from news_pipeline.models import Category
from news_pipeline.parser import parse_file


class ParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def write(self, name: str, body: str) -> Path:
        path = self.root / name
        path.write_text(body, encoding="utf-8")
        return path

    def test_title_only_and_missing_url_are_tolerated(self) -> None:
        result = parse_file(
            str(self.write("ai-2026-06-10.md", "# AI News\n- **Only a title**\n"))
        )
        self.assertEqual(len(result.articles), 1)
        article = result.articles[0]
        self.assertEqual(article.title, "Only a title")
        self.assertEqual(article.snippet, "")
        self.assertIsNone(article.original_url)
        self.assertIsNone(article.canonical_url)
        self.assertEqual(article.observed_at, "2026-06-09T23:00:00Z")

    def test_snippet_url_multiline_and_url_first(self) -> None:
        path = self.write(
            "world-2026-06-10-08.md",
            "# World News\n"
            "- **Story one**\n"
            "  First line\n"
            "\tSecond line\n"
            "  https://Example.COM/a?utm_source=x\n"
            "- **URL first**\n"
            "  https://example.com/b\n",
        )
        result = parse_file(str(path))
        self.assertEqual(result.articles[0].snippet, "First line\nSecond line")
        self.assertEqual(result.articles[0].original_url, "https://Example.COM/a?utm_source=x")
        self.assertEqual(result.articles[0].canonical_url, "https://example.com/a")
        self.assertEqual(result.articles[1].snippet, "")
        self.assertEqual(result.observed_at, "2026-06-10T07:00:00Z")

    def test_every_fetch_marker_closes_a_section_without_losing_articles(self) -> None:
        result = parse_file(
            str(
                self.write(
                    "ai-2026-06-10-08.md",
                    "- **Before marker**\n"
                    "  first\n"
                    "_Fetched: 2026-06-10T06:01:00Z_\n"
                    "- **Between markers**\n"
                    "  second\n"
                    "_Fetched: 2026-06-10T06:02:00Z_\n"
                    "- **After marker**\n"
                    "  third\n",
                )
            )
        )
        self.assertEqual([a.title for a in result.articles], [
            "Before marker", "Between markers", "After marker"
        ])
        self.assertEqual([a.fetch_marker for a in result.articles], [
            "2026-06-10T06:01:00Z", "2026-06-10T06:02:00Z", "2026-06-10T06:02:00Z"
        ])
        self.assertEqual([a.observed_at for a in result.articles], [
            "2026-06-10T06:01:00Z", "2026-06-10T06:02:00Z", "2026-06-10T06:02:00Z"
        ])
        self.assertEqual(result.fetch_markers, [
            "2026-06-10T06:01:00Z", "2026-06-10T06:02:00Z"
        ])

    def test_actual_html_query_failures_are_file_observations_even_without_articles(self) -> None:
        result = parse_file(
            str(
                self.write(
                    "ai-2026-07-10.md",
                    "<!-- query LLM+AI failed: timed out -->\n"
                    "<!-- query Anthropic failed: connection reset -->\n"
                    "_Fetched: 2026-07-10T14:34:24Z_\n",
                )
            )
        )
        self.assertEqual(result.articles, [])
        self.assertEqual([o.kind for o in result.observations], [
            "query_failure", "query_failure", "fetch_marker"
        ])
        self.assertIn("LLM+AI", result.observations[0].body or "")
        self.assertIn("timed out", result.observations[0].body or "")
        self.assertEqual(result.observations[0].raw, "<!-- query LLM+AI failed: timed out -->")

    def test_real_snapshot_preserves_all_ai_titles(self) -> None:
        # Repository-portable fixture path. Operators may stage a fixture
        # via the NEWS_PARSER_REPLAY_SNAPSHOT env override; otherwise the
        # test falls back to a repository-relative default and skips when
        # no fixture is staged.
        path = Path(os.environ.get(
            "NEWS_PARSER_REPLAY_SNAPSHOT",
            str(Path(__file__).resolve().parents[1]
            / "fixtures" / "parser-replay-snapshot" / "ai-2026-04-11.md"),
        ))
        if not path.exists():
            self.skipTest("parser replay-snapshot fixture not staged (set NEWS_PARSER_REPLAY_SNAPSHOT or add the repository fixture)")
        raw_count = sum(line.startswith("- **") for line in path.read_text(encoding="utf-8").splitlines())
        result = parse_file(str(path))
        self.assertEqual(raw_count, 24)
        self.assertEqual(len(result.articles), raw_count)

    def test_filename_hours_are_europe_london_and_markers_are_utc(self) -> None:
        result = parse_file(
            str(
                self.write(
                    "ai-2026-07-10-08.md",
                    "- **Story**\n"
                    "_Fetched: 2026-07-10T06:59:00Z_\n",
                )
            )
        )
        self.assertEqual(result.observed_at, "2026-07-10T07:00:00Z")
        self.assertEqual(result.articles[0].observed_at, "2026-07-10T06:59:00Z")

    def test_observations_fetch_footer_and_h2_skip(self) -> None:
        result = parse_file(
            str(
                self.write(
                    "our-setup-2026-06-14.md",
                    "# Our Setup — Tech Stack News — Sunday, 14 June 2026\n"
                    "## GPUs\n"
                    "- **Driver news**\n"
                    "  > Query: linux driver\n"
                    "  > Query failed: timeout\n"
                    "  https://example.com/driver\n"
                    "_Fetched: 2026-06-14T09:30:00Z_\n",
                )
            )
        )
        self.assertEqual(len(result.articles), 1)
        article = result.articles[0]
        self.assertEqual(
            [(o.kind, o.body) for o in article.observations],
            [("query_comment", "linux driver"), ("query_failure", "timeout")],
        )
        self.assertEqual(result.fetch_marker, "2026-06-14T09:30:00Z")
        self.assertEqual(article.fetch_marker, "2026-06-14T09:30:00Z")
        self.assertNotIn("GPUs", [a.title for a in result.articles])

    def test_all_eight_filename_categories(self) -> None:
        cases = {
            "ai-2026-01-01.md": Category.AI,
            "world-2026-01-01-23.md": Category.WORLD,
            "audio-engineering-2026-01-01.md": Category.AUDIO_ENGINEERING,
            "hardware-2026-01-01.md": Category.HARDWARE,
            "fantasy-novel-2026-01-01.md": Category.FANTASY_NOVEL,
            "audiovisual-2026-01-01.md": Category.AUDIOVISUAL,
            "av-corporate-2026-01-01.md": Category.AV_CORPORATE,
            "our-setup-2026-01-01.md": Category.OUR_SETUP,
        }
        for name, expected in cases.items():
            with self.subTest(name=name):
                result = parse_file(str(self.write(name, "- **Title**\n")))
                self.assertEqual(result.category, expected)

    def test_unknown_prefix_raises(self) -> None:
        with self.assertRaises(ValueError):
            parse_file(str(self.write("unknown-2026-01-01.md", "- **Title**\n")))

    def test_missing_filename_date_uses_mtime(self) -> None:
        path = self.write("ai-current.md", "- **Title**\n")
        path.touch()
        result = parse_file(str(path))
        self.assertRegex(result.observed_at, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")

    def test_file_level_query_failure_before_marker_attaches_to_following_marker(self) -> None:
        """File-level HTML query observations that appear before any ``_Fetched:``
        marker must be associated with that following marker so real failure-only
        files report honest fetch-section timing, not the filename fallback.
        """
        result = parse_file(
            str(
                self.write(
                    "ai-2026-07-10.md",
                    "<!-- query LLM+AI failed: timed out -->\n"
                    "<!-- query Anthropic failed: connection reset -->\n"
                    "_Fetched: 2026-07-10T14:34:24Z_\n",
                )
            )
        )
        kinds = [(o.kind, o.occurred_at) for o in result.observations]
        self.assertEqual(kinds, [
            ("query_failure", "2026-07-10T14:34:24Z"),
            ("query_failure", "2026-07-10T14:34:24Z"),
            ("fetch_marker", "2026-07-10T14:34:24Z"),
        ])
        for observation in result.observations:
            self.assertTrue(observation.occurred_at.endswith("Z"), observation.occurred_at)

    def test_file_level_query_between_two_markers_uses_the_following_marker(self) -> None:
        """With multiple markers, a file-level observation between two
        ``_Fetched:`` markers must pick the next following marker (not the
        previous one) so honest per-section timing survives."""
        result = parse_file(
            str(
                self.write(
                    "ai-2026-07-10.md",
                    "_Fetched: 2026-07-10T06:01:00Z_\n"
                    "<!-- query alpha failed: pre-second-marker -->\n"
                    "<!-- query beta: pre-second-marker comment -->\n"
                    "_Fetched: 2026-07-10T07:30:00Z_\n"
                    "<!-- query gamma failed: post-second-marker -->\n",
                )
            )
        )
        kinds_times = [(o.kind, o.occurred_at) for o in result.observations]
        self.assertEqual(kinds_times, [
            ("fetch_marker", "2026-07-10T06:01:00Z"),
            ("query_failure", "2026-07-10T07:30:00Z"),
            ("query_comment", "2026-07-10T07:30:00Z"),
            ("fetch_marker", "2026-07-10T07:30:00Z"),
            ("query_failure", "2026-07-10T07:30:00Z"),
        ])


if __name__ == "__main__":
    unittest.main()
