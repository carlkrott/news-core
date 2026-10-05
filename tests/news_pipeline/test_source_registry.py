from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from news_pipeline.live_contracts import CATEGORY_VALUES
from news_pipeline.source_registry import load_registry

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"
SOURCES = CONFIG / "news-sources.example.toml"
TOPICS = CONFIG / "news-topics.example.toml"
POLICY = CONFIG / "news-policy.example.toml"


class RegistryLoadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_registry(SOURCES, TOPICS, POLICY)

    def test_all_sources_topics_and_queries_load_deterministically(self) -> None:
        self.assertEqual(len(self.config.sources), 9)
        self.assertEqual(len(self.config.topics), 8)
        self.assertEqual(sum(len(source.queries) for source in self.config.sources), 9)
        self.assertEqual(tuple(source.source_id for source in self.config.sources), tuple(sorted(source.source_id for source in self.config.sources)))
        self.assertEqual(set(self.config.topics_by_category), set(CATEGORY_VALUES))

    def test_example_filtering_rules_load_as_fictional_fixture_data(self) -> None:
        sources = self.config.sources_by_id
        audio = sources["example-discovery-audio"]
        self.assertIn("smart speaker", audio.title_blocklist)
        self.assertIn("home theater", audio.title_blocklist)
        self.assertEqual(sources["example-discovery-ai"].queries[0].categories, ("it", "news"))
        self.assertEqual(sources["example-discovery-operations"].queries[0].categories, ("news", "general"))

    def test_example_policy_invariants_load(self) -> None:
        self.assertTrue(self.config.pipeline.dry_run)
        self.assertFalse(self.config.pipeline.reddit_direct_ingestion)
        self.assertEqual(self.config.pipeline.raw_body_retention_days, 30)
        self.assertEqual(self.config.report.time, "08:00")
        self.assertEqual(self.config.report.timezone, "Europe/London")
        self.assertEqual(self.config.report.channel, "telegram")
        self.assertTrue(self.config.report.verified_events_only)
        self.assertEqual(self.config.report.watchlist_max_items, 3)
        self.assertTrue(self.config.topics_by_category["world"].consequential_only)

    def test_trust_outlet_decisions_are_deferred_not_active(self) -> None:
        self.assertTrue(any("trusted, distrusted" in decision for decision in self.config.deferred_decisions))
        policy_text = POLICY.read_text(encoding="utf-8")
        self.assertNotIn("[trust", policy_text)

    def test_lookup_maps_are_read_only(self) -> None:
        with self.assertRaises(TypeError):
            self.config.sources_by_id["x"] = self.config.sources[0]  # type: ignore[index]


class RegistryRejectionTests(unittest.TestCase):
    def _load_changed(self, *, sources: str | None = None, topics: str | None = None, policy: str | None = None) -> None:
        values = {
            "news-sources.toml": sources if sources is not None else SOURCES.read_text(encoding="utf-8"),
            "news-topics.toml": topics if topics is not None else TOPICS.read_text(encoding="utf-8"),
            "news-policy.toml": policy if policy is not None else POLICY.read_text(encoding="utf-8"),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in values.items():
                (root / name).write_text(value, encoding="utf-8")
            load_registry(root / "news-sources.toml", root / "news-topics.toml", root / "news-policy.toml")

    def test_unknown_source_key_rejected(self) -> None:
        text = SOURCES.read_text(encoding="utf-8").replace("enabled = true", "enabled = true\nunknown = 1", 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_missing_source_key_rejected(self) -> None:
        text = SOURCES.read_text(encoding="utf-8").replace('host = "search.example.com"\n', "", 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_duplicate_source_id_rejected(self) -> None:
        text = SOURCES.read_text(encoding="utf-8").replace("example-discovery-world", "example-discovery-ai", 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_invalid_category_rejected(self) -> None:
        text = SOURCES.read_text(encoding="utf-8").replace('category_scope = ["ai"]', 'category_scope = ["invalid"]', 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_empty_query_list_rejected(self) -> None:
        minimal = """version = 1
[[sources]]
source_id = "empty"
adapter_type = "searxng"
source_role = "discovery"
host = "localhost"
category_scope = ["ai"]
enabled = true
queries = []
"""
        with self.assertRaises(ValueError):
            self._load_changed(sources=minimal)

    def test_direct_reddit_source_rejected_but_protective_blocklist_allowed(self) -> None:
        text = SOURCES.read_text(encoding="utf-8").replace('host = "search.example.com"', 'host = "reddit.com"', 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)
        protective = SOURCES.read_text(encoding="utf-8").replace(
            'source_id = "example-discovery-operations"\nadapter_type = "searxng"\nsource_role = "discovery"\nhost = "search.example.com"\ncategory_scope = ["our_setup"]\nenabled = true\n',
            'source_id = "example-discovery-operations"\nadapter_type = "searxng"\nsource_role = "discovery"\nhost = "search.example.com"\ncategory_scope = ["our_setup"]\nenabled = true\nurl_blocklist = ["reddit.com/r/"]\n',
        )
        self._load_changed(sources=protective)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "sources.toml").write_text(protective, encoding="utf-8")
            (root / "topics.toml").write_text(TOPICS.read_text(encoding="utf-8"), encoding="utf-8")
            (root / "policy.toml").write_text(POLICY.read_text(encoding="utf-8"), encoding="utf-8")
            config = load_registry(root / "sources.toml", root / "topics.toml", root / "policy.toml")
        self.assertIn("reddit.com/r/", config.sources_by_id["example-discovery-operations"].url_blocklist)

    def test_rss_polls_are_one_request_bound_to_configured_host(self) -> None:
        from news_pipeline.source_registry import _parse_sources

        def parse(feed_host: str, request_urls: tuple[str, ...]):
            source = {
                "source_id": "rss-bounded-test",
                "adapter_type": "rss",
                "source_role": "discovery",
                "host": "feed.example.test",
                "category_scope": ["ai"],
                "enabled": True,
                "cadence_minutes": 60,
                "queries": [
                    {
                        "text": request_url,
                        "categories": ["news"],
                        "pipeline_category": "ai",
                    }
                    for request_url in request_urls
                ],
            }
            source["host"] = feed_host
            return _parse_sources({"version": 2, "sources": [source]})

        accepted = parse("feed.example.test", ("https://feed.example.test/rss.xml",))
        self.assertEqual(len(accepted[0].queries), 1)
        with self.assertRaisesRegex(ValueError, "exactly one feed request"):
            parse("feed.example.test", (
                "https://feed.example.test/rss.xml",
                "https://feed.example.test/second.xml",
            ))
        with self.assertRaisesRegex(ValueError, "configured host"):
            parse("feed.example.test", ("https://other.example.test/rss.xml",))

    def test_inconsistent_topic_reference_rejected(self) -> None:
        text = TOPICS.read_text(encoding="utf-8").replace('category = "our_setup"', 'category = "ai"', 1)
        with self.assertRaises(ValueError):
            self._load_changed(topics=text)

    def test_active_trust_table_rejected(self) -> None:
        text = POLICY.read_text(encoding="utf-8") + "\n[trust]\ntrusted = [\"example.com\"]\n"
        with self.assertRaises(ValueError):
            self._load_changed(policy=text)

    def test_reddit_policy_enablement_rejected(self) -> None:
        text = POLICY.read_text(encoding="utf-8").replace("reddit_direct_ingestion = false", "reddit_direct_ingestion = true")
        with self.assertRaises(ValueError):
            self._load_changed(policy=text)


if __name__ == "__main__":
    unittest.main()
