"""Focused tests for additive schema-v10 generation receipts."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from news_pipeline.db import init_db
from news_pipeline.delivery_schema_v6 import migrate_v6
from news_pipeline.schema_v3 import migrate_v3
from news_pipeline.schema_v4 import migrate_v4
from news_pipeline.schema_v5 import migrate_v5
from news_pipeline.schema_v7 import migrate_v7
from news_pipeline.schema_v8 import migrate_v8
from news_pipeline.schema_v9 import migrate_v9


APPLIED_AT = "2026-09-21T12:00:00Z"
REPLAY_AT = "2026-09-21T12:05:00Z"


class SchemaV10Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "state.db"
        init_db(str(self.path))
        self.connection = sqlite3.connect(self.path, isolation_level=None)
        self.connection.execute("PRAGMA foreign_keys=ON")
        migrate_v3(self.connection, APPLIED_AT)
        migrate_v4(self.connection, APPLIED_AT)
        migrate_v5(self.connection, APPLIED_AT)
        migrate_v6(self.connection, APPLIED_AT)
        migrate_v7(self.connection, APPLIED_AT)
        migrate_v8(self.connection, APPLIED_AT)
        migrate_v9(self.connection, APPLIED_AT)

    def tearDown(self) -> None:
        self.connection.close()
        self.directory.cleanup()

    def test_migration_is_atomic_exact_and_idempotent(self) -> None:
        from news_pipeline.schema_v10 import (
            SCHEMA_VERSION,
            V10_COLUMNS,
            V10_TABLES,
            migrate_v10,
            validate_v10,
        )

        self.assertEqual(SCHEMA_VERSION, 10)
        self.assertEqual(V10_TABLES, ("subject_generation_receipts",))
        self.assertTrue(migrate_v10(self.connection, APPLIED_AT))
        self.assertFalse(migrate_v10(self.connection, REPLAY_AT))
        self.assertEqual(
            self.connection.execute(
                "SELECT applied_at FROM schema_migrations WHERE version=10"
            ).fetchone(),
            (APPLIED_AT,),
        )
        self.assertEqual(
            tuple(
                row[1]
                for row in self.connection.execute(
                    "PRAGMA table_info(subject_generation_receipts)"
                )
            ),
            V10_COLUMNS["subject_generation_receipts"],
        )
        validate_v10(self.connection)

    def test_jobs_cli_applies_and_replays_v10_explicitly(self) -> None:
        from news_pipeline.jobs import main

        self.connection.close()
        first_output = StringIO()
        with redirect_stdout(first_output):
            self.assertEqual(
                main([
                    "migrate-v10",
                    "--db", str(self.path),
                    "--applied-at", APPLIED_AT,
                ]),
                0,
            )
        self.assertIn('"applied":true', first_output.getvalue())

        replay_output = StringIO()
        with redirect_stdout(replay_output):
            self.assertEqual(
                main([
                    "migrate-v10",
                    "--db", str(self.path),
                    "--applied-at", REPLAY_AT,
                ]),
                0,
            )
        self.assertIn('"applied":false', replay_output.getvalue())

        self.connection = sqlite3.connect(self.path)
        self.assertEqual(
            self.connection.execute(
                "SELECT applied_at FROM schema_migrations WHERE version=10"
            ).fetchone(),
            (APPLIED_AT,),
        )
        self.assertEqual(
            self.connection.execute("PRAGMA integrity_check").fetchone()[0],
            "ok",
        )
        self.assertEqual(self.connection.execute("PRAGMA foreign_key_check").fetchall(), [])


if __name__ == "__main__":
    unittest.main()
