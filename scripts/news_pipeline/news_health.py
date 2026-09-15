"""Read-only Phase 6 health checks; never contacts a provider."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from .delivery_schema_v6 import validate_v6


def _connect_read_only(path: str | Path) -> sqlite3.Connection:
    db = Path(path)
    if not db.is_file():
        raise FileNotFoundError(db)
    connection = sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True, timeout=5.0)
    connection.execute("PRAGMA foreign_keys=ON")
    return connection


def health_snapshot(db_path: str | Path) -> dict[str, Any]:
    """Return a bounded JSON-safe snapshot with no credentials or network calls."""
    output: dict[str, Any] = {
        "schema_v6": False,
        "schema_versions": [],
        "integrity": "unavailable",
        "foreign_key_errors": [],
        "unresolved_delivery_attempts": 0,
        "counts": {},
        "ready": False,
    }
    try:
        connection = _connect_read_only(db_path)
    except (OSError, sqlite3.Error) as exc:
        output["error"] = type(exc).__name__
        return output
    try:
        try:
            output["schema_versions"] = [
                int(row[0])
                for row in connection.execute(
                    "SELECT version FROM schema_migrations ORDER BY version"
                )
            ]
        except sqlite3.Error:
            output["error"] = "schema_migrations_unavailable"
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        output["integrity"] = integrity[0] if integrity else "unavailable"
        output["foreign_key_errors"] = [
            list(row) for row in connection.execute("PRAGMA foreign_key_check")
        ][:20]
        try:
            validate_v6(connection)
            output["schema_v6"] = True
        except (ValueError, sqlite3.Error) as exc:
            output["schema_error"] = str(exc)
        if output["schema_v6"]:
            for table in (
                "reports", "report_events", "event_versions", "report_deliveries",
                "report_delivery_attempts",
            ):
                output["counts"][table] = int(
                    connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                )
            output["unresolved_delivery_attempts"] = int(
                connection.execute(
                    """SELECT COUNT(*) FROM report_delivery_attempts
                       WHERE state IN ('prepared','ambiguous')"""
                ).fetchone()[0]
            )
        output["ready"] = bool(
            output["schema_v6"]
            and output["integrity"] == "ok"
            and not output["foreign_key_errors"]
            and output["unresolved_delivery_attempts"] == 0
        )
        return output
    finally:
        connection.close()


__all__ = ["health_snapshot"]
