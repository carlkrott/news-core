"""Standard-library RSS 2.0, Atom 1.0, and RSS 1.0/RDF adapter."""
from __future__ import annotations

import xml.etree.ElementTree as ET

from ..canonicalization import canonicalize_url, non_article_url_reason
from ..live_contracts import QuerySeed, SourceAdapter, SourceContract, stable_id
from .base import (
    Adapter,
    AdapterError,
    FetchResult,
    FetchValidators,
    ItemRejection,
    NormalizedItem,
    ParseError,
    Transport,
    normalize_timestamp,
)

ACCEPTED_FEED_TYPES: tuple[str, ...] = (
    "application/rss+xml",
    "application/atom+xml",
    "application/rdf+xml",
    "application/xml",
    "text/xml",
)
_RAW_ITEM_LIMIT = 80 * 1024


class RssAdapter(Adapter):
    """Fetch and normalize one feed without database or retry side effects."""

    ACCEPTED_CONTENT_TYPES = ACCEPTED_FEED_TYPES
    MAX_ITEMS_PER_RESPONSE = 50

    def __init__(
        self,
        source_id: str | SourceContract,
        host: str | None = None,
        category: str | None = None,
        source_role: str | None = None,
        *,
        schedule=None,
        transport: Transport | None = None,
    ) -> None:
        if (
            isinstance(source_id, SourceContract)
            and source_id.adapter_type is not SourceAdapter.RSS
        ):
            raise ValueError("RssAdapter requires an rss SourceContract")
        super().__init__(
            source_id,
            host,
            category,
            source_role,
            schedule=schedule,
            transport=transport,
        )

    async def fetch_feed(
        self,
        feed: str | QuerySeed,
        *,
        retrieved_at: str,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FetchResult:
        """Fetch a URL supplied directly or in ``QuerySeed.text``."""
        feed_url = feed.text if isinstance(feed, QuerySeed) else feed
        return await self.fetch(
            feed_url,
            retrieved_at=retrieved_at,
            headers=(
                (
                    "Accept",
                    "application/rss+xml, application/atom+xml, "
                    "application/rdf+xml, application/xml, text/xml",
                ),
                ("User-Agent", "news-pipeline/2.0 (standard-library adapter)"),
            ),
            validators=FetchValidators(etag=etag, last_modified=last_modified),
        )

    def _parse(
        self,
        body_bytes: bytes,
        *,
        url: str,
        retrieved_at: str,
    ) -> tuple[
        tuple[NormalizedItem, ...],
        tuple[ItemRejection, ...],
        AdapterError | None,
    ]:
        try:
            root = ET.fromstring(body_bytes)
        except ET.ParseError as exc:
            return (), (), ParseError(f"invalid XML from {url}: {exc}")
        if _local_name(root.tag).casefold() == "feed":
            entries = _children(root, "entry")
            parser = self._parse_atom_entry
        else:
            channel = _first_child(root, "channel")
            if _local_name(root.tag).casefold() == "rdf":
                entries = _children(root, "item")
            elif channel is not None:
                entries = _children(channel, "item")
            else:
                return (), (), ParseError(f"RSS feed {url} has no <channel> element")
            parser = self._parse_rss_item

        items: list[NormalizedItem] = []
        rejections: list[ItemRejection] = []
        for index, entry in enumerate(entries[: self.MAX_ITEMS_PER_RESPONSE]):
            item, rejection = parser(entry, index, retrieved_at)
            if rejection is not None:
                rejections.append(rejection)
            elif item is not None:
                items.append(item)
        return tuple(items), tuple(rejections), None

    def _parse_rss_item(
        self,
        item: ET.Element,
        index: int,
        retrieved_at: str,
    ) -> tuple[NormalizedItem | None, ItemRejection | None]:
        guid = _child_text(item, "guid")
        original_url = _child_text(item, "link")
        guid_element = _first_child(item, "guid")
        if not original_url and guid and guid_element is not None:
            if guid_element.get("isPermaLink", "true").casefold() == "true":
                original_url = guid
        if not original_url:
            return None, ItemRejection(
                index, "MISSING_URL", f"item[{index}] has no link or permalink guid"
            )
        canonical_url, rejection = _canonical_or_rejection(original_url, index, "item")
        if rejection is not None:
            return None, rejection
        assert canonical_url is not None

        published_at, evidence, unknown_reason = _publication_fields(
            _child_text(item, "pubDate") or _child_text(item, "date"),
            evidence_prefix="feed-metadata",
        )
        raw = _bounded_xml(item)
        return NormalizedItem(
            source_item_id=stable_id(
                "source-item", self.source_id, guid or canonical_url
            ),
            source_id=self.source_id,
            external_id=guid,
            category=self.category,
            original_url=original_url,
            canonical_url=canonical_url,
            publisher=self._publisher_from_url(original_url),
            source_role=self.source_role,
            retrieval_method="rss-poll",
            raw_content_hash=self._raw_hash(raw),
            retrieved_at=retrieved_at,
            published_at=published_at,
            publication_evidence=evidence,
            unknown_date_reason=unknown_reason,
            title=_child_text(item, "title"),
            body=_child_text(item, "encoded") or _child_text(item, "description"),
            raw=raw,
            author_handle=_child_text(item, "creator")
            or _child_text(item, "author"),
        ), None

    def _parse_atom_entry(
        self,
        entry: ET.Element,
        index: int,
        retrieved_at: str,
    ) -> tuple[NormalizedItem | None, ItemRejection | None]:
        links = _children(entry, "link")
        original_url = next(
            (
                link.get("href", "").strip()
                for link in links
                if link.get("rel", "alternate") == "alternate"
                and link.get("href", "").strip()
            ),
            None,
        )
        if original_url is None:
            original_url = next(
                (link.get("href", "").strip() for link in links if link.get("href", "").strip()),
                None,
            )
        if not original_url:
            return None, ItemRejection(
                index, "MISSING_URL", f"entry[{index}] has no link with href"
            )
        canonical_url, rejection = _canonical_or_rejection(original_url, index, "entry")
        if rejection is not None:
            return None, rejection
        assert canonical_url is not None

        external_id = _child_text(entry, "id")
        published_value = _child_text(entry, "published")
        updated_value = _child_text(entry, "updated")
        updated_at = normalize_timestamp(updated_value) if updated_value else None
        if published_value is not None:
            published_at, evidence, unknown_reason = _publication_fields(
                published_value,
                evidence_prefix="feed-metadata",
            )
        elif updated_at is not None:
            published_at = updated_at
            evidence = f"feed-updated:{updated_value}"
            unknown_reason = None
        else:
            published_at, evidence, unknown_reason = _publication_fields(
                None,
                evidence_prefix="feed-metadata",
            )
        raw = _bounded_xml(entry)
        author_element = _first_child(entry, "author")
        author = _child_text(author_element, "name") if author_element is not None else None
        return NormalizedItem(
            source_item_id=stable_id(
                "source-item", self.source_id, external_id or canonical_url
            ),
            source_id=self.source_id,
            external_id=external_id,
            category=self.category,
            original_url=original_url,
            canonical_url=canonical_url,
            publisher=self._publisher_from_url(original_url),
            source_role=self.source_role,
            retrieval_method="rss-poll",
            raw_content_hash=self._raw_hash(raw),
            retrieved_at=retrieved_at,
            published_at=published_at,
            updated_at=updated_at,
            publication_evidence=evidence,
            unknown_date_reason=unknown_reason,
            title=_child_text(entry, "title"),
            body=_child_text(entry, "content") or _child_text(entry, "summary"),
            raw=raw,
            author_handle=author,
        ), None


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _children(element: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in element if _local_name(child.tag) == name]


def _first_child(element: ET.Element, name: str) -> ET.Element | None:
    return next((child for child in element if _local_name(child.tag) == name), None)


def _child_text(element: ET.Element, name: str) -> str | None:
    child = _first_child(element, name)
    if child is None:
        return None
    value = "".join(child.itertext()).strip()
    return value or None


def _canonical_or_rejection(
    original_url: str,
    index: int,
    label: str,
) -> tuple[str | None, ItemRejection | None]:
    try:
        canonical = canonicalize_url(original_url)
    except ValueError:
        return None, ItemRejection(
            index,
            "INVALID_URL",
            f"{label}[{index}] has an invalid HTTP(S) URL",
        )
    route_reason = non_article_url_reason(canonical)
    if route_reason is not None:
        return None, ItemRejection(
            index,
            "NON_ARTICLE_URL",
            f"{label}[{index}] is a {route_reason.replace('_', ' ')} route",
        )
    return canonical, None


def _publication_fields(
    value: str | None,
    *,
    evidence_prefix: str,
) -> tuple[str | None, str | None, str | None]:
    if value is None:
        return None, "missing", "missing-published-date"
    normalized = normalize_timestamp(value)
    if normalized is None:
        return None, "unparseable", "unparseable-published-date"
    return normalized, f"{evidence_prefix}:{value}", None


def _bounded_xml(element: ET.Element) -> str:
    encoded = ET.tostring(element, encoding="utf-8", method="xml")[:_RAW_ITEM_LIMIT]
    return encoded.decode("utf-8", errors="ignore")
