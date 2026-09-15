from __future__ import annotations

import hashlib
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from .db import article_id, connect, init_db, run_id, stable_id
from .models import Provenance, ParseResult, ParsedArticle, ArticleObservation, FileObservation
from .parser import parse_file


def _minute(timestamp: str) -> str:
    return timestamp[:16] + "Z"


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", value.casefold()).strip()


def _parse_ts(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("+00:00", "Z")


def _validate_utc(value: str) -> str:
    """Ensure the timestamp is a valid UTC ISO-8601 string ending in ``Z``.

    The parser already returns strings in UTC ``Z`` form. This helper raises a
    clear error if anything slips through future code paths.
    """
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"event timestamp is not UTC ISO with Z suffix: {value!r}")
    try:
        datetime.fromisoformat(value[:-1].replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"event timestamp is not ISO-8601: {value!r}") from exc
    return value


def _event_times(parsed: ParseResult) -> tuple[str, str]:
    """Return ``(started_at, ended_at)`` for the event covering this file.

    - UTC ``Z`` timestamps only (invalid strings raise ``ValueError``).
    - Zero-article / multi-marker / observation-only files are all supported.
    - ``started_at <= ended_at`` is guaranteed.
    """
    values: list[str] = []
    filename_fallback = parsed.observed_at
    if filename_fallback:
        values.append(filename_fallback)
    for article in parsed.articles:
        if article.fetch_marker:
            values.append(article.fetch_marker)
    for marker in parsed.fetch_markers:
        values.append(marker)
    for article in parsed.articles:
        ts = _parse_ts(article.observed_at)
        if ts:
            values.append(ts)
    for article in parsed.articles:
        for observation in article.observations:
            ts = _parse_ts(observation.occurred_at)
            if ts:
                values.append(ts)
    for observation in parsed.observations:
        ts = _parse_ts(observation.occurred_at)
        if ts:
            values.append(ts)
    if not values:
        raise ValueError(
            f"no timestamps available for event covering {parsed.source_file}"
        )
    validated = [_validate_utc(v) for v in values]
    validated.sort()
    return validated[0], validated[-1]


def _count_observations_for_article(article: ParsedArticle) -> int:
    """Count every observation row that ``replay_snapshot`` will associate
    with this article: the ``parsed_article`` appearance itself plus any
    article-level (e.g. ``query_failure`` / ``query_comment``) observations."""
    return 1 + len(article.observations)


def replay_snapshot(snapshot_dir: str, target_db: str, *, dry_run: bool = True) -> dict:
    """Replay immutable historical evidence with appearance-level observations."""
    root = Path(snapshot_dir)
    if not root.is_dir():
        raise ValueError(f"snapshot directory does not exist: {snapshot_dir}")
    files = sorted((p for p in root.glob("*.md") if p.is_file()), key=lambda p: p.name)
    init_db(target_db)
    counters = Counter(files_processed=0, articles_inserted=0, observations_inserted=0,
                       events_inserted=0, facts_inserted=0, delivery_attempts_inserted=0,
                       query_failure_obs=0, query_comment_obs=0, fetch_marker_obs=0,
                       article_level_query_failure_obs=0, article_level_query_comment_obs=0)
    category_appearances: Counter[str] = Counter()
    provenance_counts: Counter[str] = Counter()
    con = connect(target_db)
    try:
        con.execute("BEGIN")
        for path in files:
            parsed = parse_file(str(path))
            counters["files_processed"] += 1
            category_appearances[parsed.category.value] += len(parsed.articles)
            provenance_counts[Provenance.OBSERVED_HISTORICAL.value] += len(parsed.articles)
            started_at, ended_at = _event_times(parsed)
            run_key = run_id("historical_replay", str(root), _minute(started_at))
            con.execute("INSERT OR IGNORE INTO runs(id,started_at,finished_at,kind,provenance,source_dir,notes) VALUES (?,?,?,?,?,?,?)",
                (run_key, started_at, ended_at, "historical_replay", Provenance.OBSERVED_HISTORICAL.value, str(root), "Phase 1 deterministic replay of immutable backup snapshot"))
            event_key = stable_id("event", str(root), path.name)
            # observation_count must equal ALL observations attached to this event:
            # parsed_article appearance per article + article-level observations +
            # file-level observations (query_failure / query_comment / fetch_marker).
            total_observations = sum(
                _count_observations_for_article(article) for article in parsed.articles
            ) + len(parsed.observations)
            counters["events_inserted"] += con.execute("INSERT OR IGNORE INTO events(id,run_id,category,started_at,ended_at,article_count,observation_count,status) VALUES (?,?,?,?,?,?,?,?)",
                (event_key, run_key, parsed.category.value, started_at, ended_at, len(parsed.articles), total_observations, "complete")).rowcount
            for index, article in enumerate(parsed.articles):
                normalized_title = _norm(article.title)
                normalized_snippet = _norm(article.snippet)
                confidence = 1.0 if article.canonical_url else (0.75 if normalized_snippet else 0.35)
                basis = "canonical_url" if article.canonical_url else ("title_snippet" if normalized_snippet else "title_only")
                article_key = article_id(article.canonical_url, normalized_title, normalized_snippet, article.category.value, article.source_file)
                created_at = article.observed_at
                counters["articles_inserted"] += con.execute("INSERT OR IGNORE INTO articles(id,run_id,category,canonical_url,original_url,title,snippet,source_file,observed_at,fetch_marker,provenance,created_at,normalized_title,identity_confidence,identity_basis) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (article_key, run_key, article.category.value, article.canonical_url, article.original_url, article.title, article.snippet, article.source_file, article.observed_at, article.fetch_marker, Provenance.OBSERVED_HISTORICAL.value, created_at, normalized_title, confidence, basis)).rowcount
                con.execute("INSERT OR IGNORE INTO event_articles(event_id,article_id) VALUES (?,?)", (event_key, article_key))
                obs_key = stable_id("observation", path.name, str(index), "parsed_article", article.title, article.snippet, article.observed_at)
                counters["observations_inserted"] += con.execute("INSERT OR IGNORE INTO observations(id,article_id,event_id,category,source_file,kind,body,raw,occurred_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (obs_key, article_key, event_key, article.category.value, path.name, "parsed_article", article.snippet or article.title, None, article.observed_at, created_at)).rowcount
                for obs_index, observation in enumerate(article.observations):
                    observation_key = stable_id("observation", path.name, str(index), str(obs_index), observation.kind, observation.body, article.observed_at)
                    counters["observations_inserted"] += con.execute("INSERT OR IGNORE INTO observations(id,article_id,event_id,category,source_file,kind,body,raw,occurred_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (observation_key, article_key, event_key, article.category.value, path.name, observation.kind, observation.body, observation.raw, observation.occurred_at or article.observed_at, created_at)).rowcount
                    if observation.kind == "query_failure":
                        counters["article_level_query_failure_obs"] += 1
                    elif observation.kind == "query_comment":
                        counters["article_level_query_comment_obs"] += 1
                for kind, value in [("title_hash", hashlib.sha256(normalized_title.encode()).hexdigest())] + ([ ("url_canonical", article.canonical_url) ] if article.canonical_url else []):
                    fact_key = stable_id("fact", article_key, kind, value)
                    counters["facts_inserted"] += con.execute("INSERT OR IGNORE INTO fact_fingerprints(id,article_id,fingerprint_kind,value,created_at) VALUES (?,?,?,?,?)", (fact_key, article_key, kind, value, created_at)).rowcount
            for obs_index, observation in enumerate(parsed.observations):
                observation_key = stable_id("file-observation", path.name, str(obs_index), observation.kind, observation.body, observation.occurred_at)
                counters["observations_inserted"] += con.execute("INSERT OR IGNORE INTO observations(id,article_id,event_id,category,source_file,kind,body,raw,occurred_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (observation_key, None, event_key, parsed.category.value, path.name, observation.kind, observation.body, observation.raw, observation.occurred_at or parsed.observed_at, parsed.observed_at)).rowcount
                if observation.kind == "query_failure":
                    counters["query_failure_obs"] += 1
                elif observation.kind == "query_comment":
                    counters["query_comment_obs"] += 1
                elif observation.kind == "fetch_marker":
                    counters["fetch_marker_obs"] += 1
        if dry_run:
            con.rollback()
        else:
            con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
    result = {
        "files_processed": counters["files_processed"],
        "articles_inserted": counters["articles_inserted"],
        "observations_inserted": counters["observations_inserted"],
        "events_inserted": counters["events_inserted"],
        "facts_inserted": counters["facts_inserted"],
        "delivery_attempts_inserted": 0,
        "category_appearances": dict(sorted(category_appearances.items())),
        "provenance_counts": dict(sorted(provenance_counts.items())),
        "query_observations": {
            "file_level_query_failure": counters["query_failure_obs"],
            "file_level_query_comment": counters["query_comment_obs"],
            "file_level_fetch_marker": counters["fetch_marker_obs"],
            "article_level_query_failure": counters["article_level_query_failure_obs"],
            "article_level_query_comment": counters["article_level_query_comment_obs"],
        },
    }
    result["category_counts"] = result["category_appearances"]
    return result
