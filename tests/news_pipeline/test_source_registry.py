from __future__ import annotations

import ast
import os
import tempfile
import unittest
from pathlib import Path

from news_pipeline.live_contracts import CATEGORY_VALUES
from news_pipeline.source_registry import load_registry

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"
LEGACY = Path(os.environ.get("NEWS_PIPELINE_LEGACY_BIN", ROOT / "legacy-bin"))


def _assignment(path: Path, name: str) -> object:
    text = path.read_text(encoding="utf-8")
    shell_tail = text.split("PYEOF'", 1)[1]
    payload = shell_tail.split("\n", 1)[1].split("\nPYEOF", 1)[0]
    tree = ast.parse(payload)
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == name for target in node.targets):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{name} not found in {path}")


def _legacy_queries(filename: str) -> tuple[tuple[str, tuple[str, ...]], ...]:
    path = LEGACY / filename
    if filename == "news-oursetup.sh":
        topics = _assignment(path, "TOPICS")
        return tuple((query, ("news", "general")) for _, queries in topics for query in queries)  # type: ignore[union-attr]
    queries = _assignment(path, "queries")
    return tuple((query, tuple(categories.split(","))) for query, categories in queries)  # type: ignore[union-attr]


class RegistryLoadTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.config = load_registry(CONFIG / "news-sources.toml", CONFIG / "news-topics.toml", CONFIG / "news-policy.toml")

    def test_all_sources_topics_and_queries_load_deterministically(self) -> None:
        self.assertEqual(len(self.config.sources), 8)
        self.assertEqual(len(self.config.topics), 8)
        self.assertEqual(sum(len(source.queries) for source in self.config.sources), 72)
        self.assertEqual(tuple(source.source_id for source in self.config.sources), tuple(sorted(source.source_id for source in self.config.sources)))
        self.assertEqual(set(self.config.topics_by_category), set(CATEGORY_VALUES))

    def test_queries_and_per_query_categories_match_every_legacy_fetcher(self) -> None:
        mapping = {
            "searxng-ai-main": "news-ai.sh",
            "searxng-world-main": "news-world.sh",
            "searxng-audio-main": "news-audio.sh",
            "searxng-hardware-main": "news-hardware.sh",
            "searxng-fantasy-novel-main": "news-fantasy.sh",
            "searxng-audiovisual-main": "news-audiovisual.sh",
            "searxng-av-corporate-main": "news-av.sh",
            "searxng-our-setup-main": "news-oursetup.sh",
        }
        for source_id, filename in mapping.items():
            source = self.config.sources_by_id[source_id]
            actual = tuple((query.text, query.categories) for query in source.queries)
            self.assertEqual(actual, _legacy_queries(filename), source_id)

    def test_blocklists_and_allowlists_match_legacy_fetchers(self) -> None:
        sources = self.config.sources_by_id
        audio = sources["searxng-audio-main"]
        self.assertEqual(set(audio.title_blocklist), _assignment(LEGACY / "news-audio.sh", "AUDIO_BLOCKLIST"))
        fantasy = sources["searxng-fantasy-novel-main"]
        self.assertEqual(set(fantasy.title_blocklist), _assignment(LEGACY / "news-fantasy.sh", "BLOCKLIST"))
        self.assertEqual(set(fantasy.allowlist_domains), _assignment(LEGACY / "news-fantasy.sh", "ALLOWLIST_DOMAINS"))
        audiovisual = sources["searxng-audiovisual-main"]
        blocked = _assignment(LEGACY / "news-audiovisual.sh", "BLOCKED")
        self.assertEqual(set(audiovisual.title_blocklist), blocked)
        self.assertEqual(set(audiovisual.content_blocklist), blocked)
        self.assertEqual(set(audiovisual.allowlist_domains), _assignment(LEGACY / "news-audiovisual.sh", "ALLOWED_DOMAINS"))
        our_setup = sources["searxng-our-setup-main"]
        self.assertEqual(set(our_setup.url_blocklist), _assignment(LEGACY / "news-oursetup.sh", "BLOCKLIST"))
        self.assertIn("reddit.com/r/", our_setup.url_blocklist)

    def test_approved_policy_defaults_are_active(self) -> None:
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
        policy_text = (CONFIG / "news-policy.toml").read_text(encoding="utf-8")
        self.assertNotIn("[trust", policy_text)

    def test_lookup_maps_are_read_only(self) -> None:
        with self.assertRaises(TypeError):
            self.config.sources_by_id["x"] = self.config.sources[0]  # type: ignore[index]


class RegistryRejectionTests(unittest.TestCase):
    def _load_changed(self, *, sources: str | None = None, topics: str | None = None, policy: str | None = None) -> None:
        values = {
            "news-sources.toml": sources if sources is not None else (CONFIG / "news-sources.toml").read_text(encoding="utf-8"),
            "news-topics.toml": topics if topics is not None else (CONFIG / "news-topics.toml").read_text(encoding="utf-8"),
            "news-policy.toml": policy if policy is not None else (CONFIG / "news-policy.toml").read_text(encoding="utf-8"),
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in values.items():
                (root / name).write_text(value, encoding="utf-8")
            load_registry(root / "news-sources.toml", root / "news-topics.toml", root / "news-policy.toml")

    def test_unknown_source_key_rejected(self) -> None:
        text = (CONFIG / "news-sources.toml").read_text(encoding="utf-8").replace("enabled = true", "enabled = true\nunknown = 1", 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_missing_source_key_rejected(self) -> None:
        text = (CONFIG / "news-sources.toml").read_text(encoding="utf-8").replace('host = "127.0.0.1:8888"\n', "", 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_duplicate_source_id_rejected(self) -> None:
        text = (CONFIG / "news-sources.toml").read_text(encoding="utf-8").replace("searxng-world-main", "searxng-ai-main", 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)

    def test_invalid_category_rejected(self) -> None:
        text = (CONFIG / "news-sources.toml").read_text(encoding="utf-8").replace('category_scope = ["ai"]', 'category_scope = ["invalid"]', 1)
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
        text = (CONFIG / "news-sources.toml").read_text(encoding="utf-8").replace('host = "127.0.0.1:8888"', 'host = "reddit.com"', 1)
        with self.assertRaises(ValueError):
            self._load_changed(sources=text)
        self.assertIn("reddit.com/r/", load_registry(CONFIG / "news-sources.toml", CONFIG / "news-topics.toml", CONFIG / "news-policy.toml").sources_by_id["searxng-our-setup-main"].url_blocklist)

    def test_inconsistent_topic_reference_rejected(self) -> None:
        text = (CONFIG / "news-topics.toml").read_text(encoding="utf-8").replace('category = "our_setup"', 'category = "ai"', 1)
        with self.assertRaises(ValueError):
            self._load_changed(topics=text)

    def test_active_trust_table_rejected(self) -> None:
        text = (CONFIG / "news-policy.toml").read_text(encoding="utf-8") + "\n[trust]\ntrusted = [\"example.com\"]\n"
        with self.assertRaises(ValueError):
            self._load_changed(policy=text)

    def test_reddit_policy_enablement_rejected(self) -> None:
        text = (CONFIG / "news-policy.toml").read_text(encoding="utf-8").replace("reddit_direct_ingestion = false", "reddit_direct_ingestion = true")
        with self.assertRaises(ValueError):
            self._load_changed(policy=text)


if __name__ == "__main__":
    unittest.main()
