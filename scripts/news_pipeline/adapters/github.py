"""Single-release adapter for explicitly allowlisted first-party GitHub repositories."""
from __future__ import annotations

import hashlib
import json
from urllib.parse import unquote, urlsplit

from ..canonicalization import canonicalize_url
from ..live_contracts import QuerySeed, SourceAdapter, SourceContract, SourceRole, stable_id
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

LATEST_RELEASE_API = "https://api.github.com/repos/ggml-org/llama.cpp/releases/latest"
LATEST_RELEASE_APIS = {
    "ggml-org/llama.cpp": LATEST_RELEASE_API,
    "anthropics/claude-code": "https://api.github.com/repos/anthropics/claude-code/releases/latest",
}
RELEASE_SOURCE_CONTRACTS = {
    "github-llamacpp-release": {
        "endpoint": LATEST_RELEASE_APIS["ggml-org/llama.cpp"],
        "category": "our_setup",
        "publisher": "llama.cpp",
        "page_prefix": "/ggml-org/llama.cpp/releases/tag/",
    },
    "github-anthropic-claude-code-release": {
        "endpoint": LATEST_RELEASE_APIS["anthropics/claude-code"],
        "category": "ai",
        "publisher": "Anthropic",
        "page_prefix": "/anthropics/claude-code/releases/tag/",
    },
}


class GitHubReleaseAdapter(Adapter):
    """Fetch one latest stable release; never paginates or changes hosts."""

    ACCEPTED_CONTENT_TYPES = ("application/json",)
    MAX_ITEMS_PER_RESPONSE = 1

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
        source = source_id if isinstance(source_id, SourceContract) else None
        source_key = source.source_id if source is not None else str(source_id)
        release = RELEASE_SOURCE_CONTRACTS.get(source_key)
        if release is None:
            raise ValueError("GitHub release adapter source is not explicitly allowlisted")
        if source is not None and (
            source.adapter_type is not SourceAdapter.GITHUB
            or source.host != "api.github.com"
            or source.source_role is not SourceRole.DISCOVERY
            or source.category_scope != (release["category"],)
            or len(source.queries) != 1
            or source.queries[0].text != release["endpoint"]
        ):
            raise ValueError("GitHub release source does not match its exact allowlisted repository contract")
        super().__init__(
            source_id,
            host,
            category,
            source_role,
            schedule=schedule,
            transport=transport,
        )

    async def fetch_release(
        self,
        endpoint: str | QuerySeed,
        *,
        retrieved_at: str,
        etag: str | None = None,
        last_modified: str | None = None,
    ) -> FetchResult:
        url = endpoint.text if isinstance(endpoint, QuerySeed) else endpoint
        release = RELEASE_SOURCE_CONTRACTS[self.source_id]
        if url != release["endpoint"]:
            raise ValueError("GitHub release request must use the source's exact admitted HTTPS API endpoint")
        return await self.fetch(
            url,
            retrieved_at=retrieved_at,
            headers=(
                ("Accept", "application/vnd.github+json"),
                ("X-GitHub-Api-Version", "2022-11-28"),
                ("User-Agent", "news-pipeline/2.0 (public GitHub release API)"),
            ),
            validators=FetchValidators(etag=etag, last_modified=last_modified),
        )

    def _parse(
        self,
        body_bytes: bytes,
        *,
        url: str,
        retrieved_at: str,
    ) -> tuple[tuple[NormalizedItem, ...], tuple[ItemRejection, ...], AdapterError | None]:
        release = RELEASE_SOURCE_CONTRACTS[self.source_id]
        if url != release["endpoint"]:
            return (), (), ParseError("GitHub release API response URL changed")
        try:
            value = json.loads(body_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            return (), (), ParseError(f"invalid JSON from {url}: {exc}")
        if type(value) is not dict:
            return (), (), ParseError("GitHub latest-release payload must be a JSON object")

        release_id = value.get("id")
        tag = value.get("tag_name")
        html_url = value.get("html_url")
        published_raw = value.get("published_at")
        body = value.get("body")
        if (
            type(release_id) is not int
            or type(release_id) is bool
            or release_id < 1
            or type(tag) is not str
            or not tag.strip()
            or type(html_url) is not str
            or type(published_raw) is not str
            or type(body) is not str
            or not body.strip()
        ):
            return (), (), ParseError("GitHub latest-release payload lacks required first-party release fields")
        if value.get("draft") is not False or value.get("prerelease") is not False:
            return (), (ItemRejection(0, "NON_STABLE_RELEASE", "latest release is draft or prerelease"),), None

        parsed = urlsplit(html_url)
        tag_path = parsed.path.removeprefix(release["page_prefix"])
        if (
            parsed.scheme != "https"
            or parsed.hostname != "github.com"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or not parsed.path.startswith(release["page_prefix"])
            or not tag_path
            or unquote(tag_path) != tag
        ):
            return (), (), ParseError(f"release canonical URL is outside the allowlisted repository for {self.source_id}")
        try:
            canonical_url = canonicalize_url(html_url)
        except ValueError:
            return (), (), ParseError("release canonical URL is invalid")
        if canonical_url is None:
            return (), (), ParseError("release canonical URL could not be normalized")
        if canonical_url != html_url:
            return (), (), ParseError("release URL is not canonical")

        published_at = normalize_timestamp(published_raw)
        if published_at is None:
            return (), (ItemRejection(0, "INVALID_PUBLISHED_AT", "release publication timestamp is invalid"),), None
        updated_raw = value.get("updated_at")
        updated_at = normalize_timestamp(updated_raw) if isinstance(updated_raw, str) else None
        if updated_raw is not None and updated_at is None:
            return (), (ItemRejection(0, "INVALID_UPDATED_AT", "release update timestamp is invalid"),), None

        title_value = value.get("name")
        title = title_value.strip() if isinstance(title_value, str) and title_value.strip() else tag
        body_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        raw = json.dumps(
            {
                "id": release_id,
                "tag_name": tag,
                "name": title,
                "html_url": html_url,
                "published_at": published_raw,
                "updated_at": updated_raw,
                "body": body,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return (
            NormalizedItem(
                source_item_id=stable_id(
                    "source-item", self.source_id, str(release_id), canonical_url,
                    published_at, body_hash,
                ),
                source_id=self.source_id,
                external_id=str(release_id),
                category=self.category,
                original_url=canonical_url,
                canonical_url=canonical_url,
                publisher=release["publisher"],
                source_role=self.source_role,
                retrieval_method="github-release-api",
                raw_content_hash=body_hash,
                retrieved_at=retrieved_at,
                published_at=published_at,
                updated_at=updated_at,
                publication_evidence=f"metadata:{published_raw}",
                title=title,
                body=body,
                raw=raw,
            ),
        ), (), None
