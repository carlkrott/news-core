from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Category(str, Enum):
    AI = "ai"
    WORLD = "world"
    AUDIO_ENGINEERING = "audio_engineering"
    HARDWARE = "hardware"
    FANTASY_NOVEL = "fantasy_novel"
    AUDIOVISUAL = "audiovisual"
    AV_CORPORATE = "av_corporate"
    OUR_SETUP = "our_setup"


class Provenance(str, Enum):
    OBSERVED_HISTORICAL = "observed_historical"
    OBSERVED_LIVE = "observed_live"
    MANUAL = "manual"


@dataclass(frozen=True, slots=True)
class CanonicalUrl:
    original: str
    canonical: str


@dataclass(frozen=True, slots=True)
class ArticleObservation:
    kind: str
    body: str | None = None
    occurred_at: str | None = None
    raw: str | None = None


@dataclass(frozen=True, slots=True)
class FileObservation:
    kind: str
    body: str | None = None
    occurred_at: str | None = None
    raw: str | None = None


@dataclass(slots=True)
class ParsedArticle:
    title: str
    category: Category
    source_file: str
    observed_at: str
    snippet: str = ""
    canonical_url: str | None = None
    original_url: str | None = None
    fetch_marker: str | None = None
    observations: list[ArticleObservation] = field(default_factory=list)


@dataclass(slots=True)
class ParseResult:
    source_file: str
    category: Category
    observed_at: str
    articles: list[ParsedArticle]
    fetch_marker: str | None = None
    fetch_markers: list[str] = field(default_factory=list)
    header_date: str | None = None
    observations: list[FileObservation] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RunRecord:
    id: str
    started_at: str
    kind: str
    provenance: Provenance
    source_dir: str | None = None
    finished_at: str | None = None
    notes: str | None = None
