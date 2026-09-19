from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from news_pipeline.models import (
    CATEGORY_TO_SUBJECT,
    Category,
    Subject,
    SubjectDecision,
    assign_subject,
    subject_for_category,
)
from news_pipeline.source_registry import load_registry

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config"


class SubjectRoutingTests(unittest.TestCase):
    def test_every_category_maps_to_exactly_one_subject(self) -> None:
        self.assertEqual(set(CATEGORY_TO_SUBJECT), set(Category))
        self.assertEqual(
            {subject_for_category(category) for category in Category},
            set(Subject),
        )
        self.assertEqual(subject_for_category(Category.AUDIOVISUAL), Subject.PROFESSIONAL_AV)
        self.assertEqual(subject_for_category("av_corporate"), Subject.PROFESSIONAL_AV)

    def test_product_line_examples_route_by_isolated_category_lane(self) -> None:
        examples = (
            (("audio_engineering",), Subject.AUDIO_ENGINEERING),
            (("av_corporate",), Subject.PROFESSIONAL_AV),
            (("audiovisual",), Subject.PROFESSIONAL_AV),
            (("audio_engineering", "audiovisual"), None),
        )
        for categories, expected_subject in examples:
            assignment = assign_subject(categories)
            if expected_subject is None:
                self.assertIsNone(assignment.subject)
                self.assertEqual(assignment.decision, SubjectDecision.PENDING_SUBJECT_REVIEW)
            else:
                self.assertEqual(assignment.subject, expected_subject)
                self.assertEqual(assignment.decision, SubjectDecision.ASSIGNED)

    def test_empty_or_unknown_category_is_pending(self) -> None:
        empty = assign_subject(())
        self.assertIsNone(empty.subject)
        self.assertEqual(empty.decision, SubjectDecision.PENDING_SUBJECT_REVIEW)
        with self.assertRaises(ValueError):
            assign_subject(("not-a-category",))


class SubjectConfigTests(unittest.TestCase):
    def _load_examples(self, *, version: int = 2):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_text = (CONFIG / "news-sources.example.toml").read_text(encoding="utf-8")
            topics_text = (CONFIG / "news-topics.example.toml").read_text(encoding="utf-8")
            policy_text = (CONFIG / "news-policy.example.toml").read_text(encoding="utf-8")
            source_text = source_text.replace("version = 2", f"version = {version}", 1)
            topics_text = topics_text.replace("version = 2", f"version = {version}", 1)
            policy_text = policy_text.replace("version = 2", f"version = {version}", 1)
            paths = {
                "sources": root / "news-sources.toml",
                "topics": root / "news-topics.toml",
                "policy": root / "news-policy.toml",
            }
            paths["sources"].write_text(source_text, encoding="utf-8")
            paths["topics"].write_text(topics_text, encoding="utf-8")
            paths["policy"].write_text(policy_text, encoding="utf-8")
            return load_registry(paths["sources"], paths["topics"], paths["policy"])

    def test_public_examples_use_per_subject_contract(self) -> None:
        config = self._load_examples()
        self.assertEqual(config.version, 2)
        self.assertEqual(config.report.scope, "per_subject")
        self.assertEqual(set(config.report.subjects), set(Subject))
        self.assertEqual(config.topics_by_category["audiovisual"].subject, Subject.PROFESSIONAL_AV)
        self.assertEqual(config.topics_by_category["av_corporate"].subject, Subject.PROFESSIONAL_AV)
        self.assertEqual(config.subjects_by_id[Subject.AUDIO_ENGINEERING].max_story_count, 8)
        self.assertEqual(config.subjects_by_id[Subject.AUDIO_ENGINEERING].recency_days, 14)
        self.assertTrue(config.subjects_by_id[Subject.PROFESSIONAL_AV].inclusion_rules)
        self.assertTrue(config.subjects_by_id[Subject.PROFESSIONAL_AV].exclusion_rules)
        self.assertTrue(config.subjects_by_id[Subject.PROFESSIONAL_AV].materiality_rule)

    def test_version_one_is_rejected_with_migration_error(self) -> None:
        with self.assertRaisesRegex(ValueError, "version 2.*migrate"):
            self._load_examples(version=1)


if __name__ == "__main__":
    unittest.main()
