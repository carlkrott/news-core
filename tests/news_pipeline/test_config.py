from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock


class ConfigTests(unittest.TestCase):
    def test_constants_are_locked_to_safe_defaults(self) -> None:
        import news_pipeline.config as config

        self.assertIs(config.DRY_RUN, True)
        self.assertEqual(
            config.STATE_DB_PATH,
            "/state/news-state.db",
        )
        self.assertFalse(hasattr(config, "set_dry_run"))
        with self.assertRaises(AttributeError):
            with mock.patch.object(config, "set_dry_run", create=False):
                pass

    def test_environment_cannot_override_dry_run(self) -> None:
        with mock.patch.dict(os.environ, {"NEWS_PIPELINE_DRY_RUN": "false"}):
            sys.modules.pop("news_pipeline.config", None)
            import news_pipeline.config as config

            self.assertIs(config.DRY_RUN, True)

    def test_source_uses_literal_final(self) -> None:
        import news_pipeline.config as config

        source = Path(config.__file__).read_text(encoding="utf-8")
        self.assertIn("DRY_RUN: Final[bool] = True", source)
        self.assertNotIn("os.environ", source)


if __name__ == "__main__":
    unittest.main()
