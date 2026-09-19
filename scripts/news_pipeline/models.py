from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType


class Category(str, Enum):
    AI = "ai"
    WORLD = "world"
    AUDIO_ENGINEERING = "audio_engineering"
    HARDWARE = "hardware"
    FANTASY_NOVEL = "fantasy_novel"
    AUDIOVISUAL = "audiovisual"
    AV_CORPORATE = "av_corporate"
    OUR_SETUP = "our_setup"


class Subject(str, Enum):
    AI = "ai"
    WORLD = "world"
    AUDIO_ENGINEERING = "audio_engineering"
    PROFESSIONAL_AV = "professional_av"
    HARDWARE = "hardware"
    FANTASY_NOVEL = "fantasy_novel"
    OUR_SETUP = "our_setup"


class SubjectDecision(str, Enum):
    ASSIGNED = "assigned"
    PENDING_SUBJECT_REVIEW = "pending_subject_review"


CATEGORY_TO_SUBJECT = MappingProxyType({
    Category.AI: Subject.AI,
    Category.WORLD: Subject.WORLD,
    Category.AUDIO_ENGINEERING: Subject.AUDIO_ENGINEERING,
    Category.HARDWARE: Subject.HARDWARE,
    Category.FANTASY_NOVEL: Subject.FANTASY_NOVEL,
    Category.AUDIOVISUAL: Subject.PROFESSIONAL_AV,
    Category.AV_CORPORATE: Subject.PROFESSIONAL_AV,
    Category.OUR_SETUP: Subject.OUR_SETUP,
})


def subject_for_category(category: Category | str) -> Subject:
    """Return the single report subject owned by one ingest category."""
    value = category.value if isinstance(category, Category) else category
    try:
        normalized = Category(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unknown news category: {category!r}") from exc
    return CATEGORY_TO_SUBJECT[normalized]


@dataclass(frozen=True, slots=True)
class SubjectAssignment:
    subject: Subject | None
    decision: SubjectDecision
    categories: tuple[str, ...]
    reason: str

    def __post_init__(self) -> None:
        if type(self.categories) is not tuple or len(self.categories) != len(set(self.categories)):
            raise ValueError("subject assignment categories must be a unique tuple")
        for category in self.categories:
            subject_for_category(category)
        if self.decision is SubjectDecision.ASSIGNED and self.subject is None:
            raise ValueError("assigned subject decisions require a subject")
        if self.decision is SubjectDecision.PENDING_SUBJECT_REVIEW and self.subject is not None:
            raise ValueError("pending subject decisions cannot select a subject")
        if type(self.reason) is not str or not self.reason.strip():
            raise ValueError("subject assignment reason must be non-empty")


def assign_subject(categories: tuple[str, ...]) -> SubjectAssignment:
    """Assign one subject or fail closed when category evidence is ambiguous."""
    if type(categories) is not tuple or len(categories) != len(set(categories)):
        raise ValueError("categories must be a unique tuple")
    if not categories:
        return SubjectAssignment(None, SubjectDecision.PENDING_SUBJECT_REVIEW, (), "missing_category")
    normalized = tuple(category.value for category in Category if category.value in categories)
    if len(normalized) != len(categories):
        unknown = tuple(value for value in categories if value not in normalized)
        raise ValueError(f"unknown news categories: {unknown!r}")
    subjects = {subject_for_category(category) for category in normalized}
    if len(subjects) == 1:
        return SubjectAssignment(next(iter(subjects)), SubjectDecision.ASSIGNED, normalized, "single_subject")
    return SubjectAssignment(None, SubjectDecision.PENDING_SUBJECT_REVIEW, normalized, "multiple_subjects")


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
