from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

TABLES: tuple[str, ...] = (
    "schema_migrations", "runs", "articles", "observations", "events",
    "event_articles", "fact_fingerprints", "decisions", "delivery_attempts",
    "manual_review", "query_telemetry",
)
_CATEGORIES = "'ai','world','audio_engineering','hardware','fantasy_novel','audiovisual','av_corporate','our_setup'"
_KINDS = "'parsed_article','query_failure','query_comment','fetch_marker'"

_MIGRATION_1 = f"""
CREATE TABLE IF NOT EXISTS runs(
 id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
 kind TEXT NOT NULL CHECK(kind IN ('historical_replay','live_ingest')),
 provenance TEXT NOT NULL CHECK(provenance IN ('observed_historical','observed_live','manual')),
 source_dir TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS articles(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id),
 category TEXT NOT NULL CHECK(category IN ({_CATEGORIES})), canonical_url TEXT,
 original_url TEXT, title TEXT NOT NULL, snippet TEXT, source_file TEXT NOT NULL,
 observed_at TEXT, fetch_marker TEXT, provenance TEXT NOT NULL CHECK(provenance IN ('observed_historical','observed_live','manual')),
 created_at TEXT NOT NULL, normalized_title TEXT NOT NULL DEFAULT '',
 identity_confidence REAL NOT NULL DEFAULT 0.0 CHECK(identity_confidence >= 0.0 AND identity_confidence <= 1.0),
 identity_basis TEXT NOT NULL DEFAULT 'legacy' CHECK(identity_basis IN ('canonical_url','title_snippet','title_only','legacy')));
CREATE TABLE IF NOT EXISTS observations(
 id TEXT PRIMARY KEY, article_id TEXT REFERENCES articles(id), event_id TEXT REFERENCES events(id),
 category TEXT NOT NULL CHECK(category IN ({_CATEGORIES})), source_file TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ({_KINDS})), body TEXT, raw TEXT, occurred_at TEXT, created_at TEXT NOT NULL,
 CHECK(article_id IS NOT NULL OR event_id IS NOT NULL));
CREATE TABLE IF NOT EXISTS events(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), category TEXT NOT NULL,
 started_at TEXT NOT NULL, ended_at TEXT, article_count INTEGER NOT NULL DEFAULT 0,
 observation_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL CHECK(status IN ('pending','running','complete','failed')));
CREATE TABLE IF NOT EXISTS event_articles(
 event_id TEXT NOT NULL REFERENCES events(id), article_id TEXT NOT NULL REFERENCES articles(id), PRIMARY KEY(event_id, article_id));
CREATE TABLE IF NOT EXISTS fact_fingerprints(
 id TEXT PRIMARY KEY, article_id TEXT NOT NULL REFERENCES articles(id),
 fingerprint_kind TEXT NOT NULL CHECK(fingerprint_kind IN ('title_hash','title_ngram','url_canonical','host_publisher','content_shingle')),
 value TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(article_id, fingerprint_kind, value));
CREATE INDEX IF NOT EXISTS idx_fact_fingerprints_value ON fact_fingerprints(value);
CREATE INDEX IF NOT EXISTS idx_fact_fingerprints_kind ON fact_fingerprints(fingerprint_kind);
CREATE TABLE IF NOT EXISTS decisions(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), article_id TEXT REFERENCES articles(id),
 decision_kind TEXT NOT NULL CHECK(decision_kind IN ('keep','suppress','merge','promote','demote','manual_review')),
 reason TEXT, decided_at TEXT NOT NULL, decided_by TEXT);
CREATE TABLE IF NOT EXISTS delivery_attempts(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), article_id TEXT REFERENCES articles(id),
 channel TEXT NOT NULL CHECK(channel IN ('telegram','email','webhook','in_app')),
 status TEXT NOT NULL CHECK(status IN ('queued','sent','failed','skipped','dry_run')),
 attempted_at TEXT, completed_at TEXT, error TEXT);
CREATE TABLE IF NOT EXISTS manual_review(
 id TEXT PRIMARY KEY, article_id TEXT NOT NULL REFERENCES articles(id), reason TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('open','resolved','dismissed')), opened_at TEXT NOT NULL,
 resolved_at TEXT, notes TEXT);
CREATE TABLE IF NOT EXISTS query_telemetry(
 id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES runs(id), query_text TEXT, source TEXT,
 returned_count INTEGER, error_count INTEGER, started_at TEXT NOT NULL, finished_at TEXT);
CREATE INDEX IF NOT EXISTS idx_observations_article ON observations(article_id);
CREATE INDEX IF NOT EXISTS idx_observations_event ON observations(event_id);
CREATE INDEX IF NOT EXISTS idx_observations_kind ON observations(kind);
CREATE INDEX IF NOT EXISTS idx_articles_identity ON articles(normalized_title, identity_basis);
"""


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def stable_id(*parts: str | None, length: int | None = None) -> str:
    digest = hashlib.sha256("\x1f".join(part or "" for part in parts).encode("utf-8")).hexdigest()
    return digest[:length] if length else digest


def run_id(kind: str, source_dir: str | None, started_at_minute: str) -> str:
    return stable_id(kind, source_dir, started_at_minute)


def article_id(canonical_url: str | None, normalized_title: str, normalized_snippet: str, category: str, source_file: str | None = None) -> str:
    if canonical_url:
        key = "url\x1f" + canonical_url
    elif normalized_snippet:
        key = "fallback\x1f" + category + "\x1f" + normalized_title + "\x1f" + normalized_snippet
    else:
        # Empty snippets are low confidence; keep same-file appearances stable but do
        # not merge unrelated title-only evidence from different source files.
        key = "title-only\x1f" + category + "\x1f" + normalized_title + "\x1f" + (source_file or "")
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


class _ClosingConnection(sqlite3.Connection):
    """SQLite connection whose context manager commits/rolls back and closes."""

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self.close()


def connect(path: str) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(
        db_path,
        timeout=10.0,
        factory=_ClosingConnection,
    )
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=10000")
    con.execute("PRAGMA foreign_keys=ON")
    return con


def _legacy_observation_columns(con: sqlite3.Connection) -> set[str]:
    return {row[1] for row in con.execute("PRAGMA table_info(observations)")}


def _apply_v2(con: sqlite3.Connection) -> None:
    article_cols = {row[1] for row in con.execute("PRAGMA table_info(articles)")}
    if "normalized_title" not in article_cols:
        con.execute("ALTER TABLE articles ADD COLUMN normalized_title TEXT NOT NULL DEFAULT ''")
        con.execute("ALTER TABLE articles ADD COLUMN identity_confidence REAL NOT NULL DEFAULT 0.0")
        con.execute("ALTER TABLE articles ADD COLUMN identity_basis TEXT NOT NULL DEFAULT 'legacy'")
        con.execute("UPDATE articles SET normalized_title=lower(trim(title)), identity_confidence=CASE WHEN canonical_url IS NOT NULL THEN 1.0 ELSE 0.0 END, identity_basis=CASE WHEN canonical_url IS NOT NULL THEN 'canonical_url' ELSE 'legacy' END")
    cols = _legacy_observation_columns(con)
    if "event_id" not in cols or "category" not in cols or "source_file" not in cols or "raw" not in cols:
        con.execute("ALTER TABLE observations RENAME TO observations_v1")
        con.execute(f"""CREATE TABLE observations(
            id TEXT PRIMARY KEY, article_id TEXT REFERENCES articles(id), event_id TEXT REFERENCES events(id),
            category TEXT NOT NULL CHECK(category IN ({_CATEGORIES})), source_file TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ({_KINDS})), body TEXT, raw TEXT, occurred_at TEXT, created_at TEXT NOT NULL,
            CHECK(article_id IS NOT NULL OR event_id IS NOT NULL))""")
        con.execute("""INSERT INTO observations(id,article_id,event_id,category,source_file,kind,body,raw,occurred_at,created_at)
            SELECT o.id,o.article_id,NULL,a.category,a.source_file,o.kind,o.body,NULL,o.occurred_at,o.created_at
            FROM observations_v1 o JOIN articles a ON a.id=o.article_id""")
        con.execute("DROP TABLE observations_v1")
    con.execute("CREATE INDEX IF NOT EXISTS idx_observations_article ON observations(article_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_observations_event ON observations(event_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_observations_kind ON observations(kind)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_articles_identity ON articles(normalized_title, identity_basis)")


def init_db(path: str) -> None:
    with connect(path) as con:
        con.execute("CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)")
        if con.execute("SELECT 1 FROM schema_migrations WHERE version=1").fetchone() is None:
            con.executescript(_MIGRATION_1)
            con.execute("INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?)", (utc_now(),))
        if con.execute("SELECT 1 FROM schema_migrations WHERE version=2").fetchone() is None:
            _apply_v2(con)
            con.execute("INSERT INTO schema_migrations(version, applied_at) VALUES (2, ?)", (utc_now(),))


def get_counts(path: str) -> dict[str, int]:
    init_db(path)
    with connect(path) as con:
        return {table: int(con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]) for table in TABLES}
