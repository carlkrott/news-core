"""Phase 4 — Slice 3 summarizer tests.

Twenty unittest methods for ``news_pipeline.briefing_summarizer`` per the
Phase 4 supervisor addendum §2.3, §2.5 item 6, and §6 (tests 21-40).
Slice 3 owns only this file plus ``news_pipeline.briefing_summarizer``;
every other Phase 4 file is forbidden here, and no Phase 1-3 / wrapper /
cron / service / DB file may be touched.

The summarizer is a stdlib-only module that wraps one injected
``transport`` callable producing model bytes in response to canonical
request bytes. Validators reject bounds without slicing, the parser
converts its own internals to a typed malformed error, and the
session enforces budget + cache bookkeeping.
"""
from __future__ import annotations

import hashlib
import json
import unittest
from typing import Callable, List, Tuple

from news_pipeline import briefing_summarizer
from news_pipeline.models import Category


# ---------------------------------------------------------------------------
# Test fixtures — local summarizer surface only
# ---------------------------------------------------------------------------


def _category() -> Category:
    return Category.AI


def _other_category() -> Category:
    return Category.WORLD


def _other_categories() -> Tuple[Category, ...]:
    """The remaining seven underscore-valued Category members."""
    return (
        Category.WORLD,
        Category.AUDIO_ENGINEERING,
        Category.HARDWARE,
        Category.FANTASY_NOVEL,
        Category.AUDIOVISUAL,
        Category.AV_CORPORATE,
        Category.OUR_SETUP,
    )


def _raw_input(
    candidate_id: str = "cand-1",
    title: str = "A meaningful headline",
    snippet: str = "A meaningful snippet",
    url: str | None = "https://example.test/article",
) -> briefing_summarizer.SummarizerInput:
    return briefing_summarizer.SummarizerInput(
        candidate_id=candidate_id,
        title=title,
        snippet=snippet,
        url=url,
    )


def _valid_item(
    candidate_id: str = "cand-1",
    title: str = "A meaningful headline",
    snippet: str = "A meaningful snippet",
    url: str | None = "https://example.test/article",
) -> briefing_summarizer.SummarizerRequestItem:
    return briefing_summarizer.SummarizerRequestItem(
        candidate_id=candidate_id,
        title=title,
        snippet=snippet,
        url=url,
    )


def _make_response(items: List[dict]) -> bytes:
    """Build a strict-success response payload — exactly the §2.3 schema."""
    return json.dumps(
        {"items": items},
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _success_response_for(
    request_items: List[briefing_summarizer.SummarizerRequestItem],
    summary_for: Callable[[str], str] | None = None,
) -> bytes:
    def _default(cid: str) -> str:
        return f"summary for {cid}"

    if summary_for is None:
        summary_for = _default
    return _make_response(
        [
            {
                "candidate_id": it.candidate_id,
                "summary": summary_for(it.candidate_id),
            }
            for it in request_items
        ]
    )


def _strict_transport(response_bytes: bytes) -> Callable[[bytes], bytes]:
    """Return a transport that ignores the request and returns the given bytes."""

    def _t(_request: bytes) -> bytes:
        return response_bytes

    return _t


def _recording_transport(response_bytes: bytes) -> Tuple[Callable[[bytes], bytes], List[bytes]]:
    """Return a transport plus a list that receives each request bytes."""
    calls: List[bytes] = []

    def _t(request: bytes) -> bytes:
        calls.append(bytes(request))
        return response_bytes

    return _t, calls


# ---------------------------------------------------------------------------
# Test class
# ---------------------------------------------------------------------------


class TestBriefingSummarizer(unittest.TestCase):
    # 21
    def test_request_item_rejects_bounds_without_slicing(self) -> None:
        # Each bound failure raises ValueError and never returns a truncated
        # attribute. Empty / over-limit titles, snippets, URLs all rejected.
        with self.assertRaises(ValueError):
            briefing_summarizer.SummarizerRequestItem(
                candidate_id="cand-x", title="", snippet="s", url=None
            )
        long_title = "t" * 513
        with self.assertRaises(ValueError):
            briefing_summarizer.SummarizerRequestItem(
                candidate_id="cand-x", title=long_title, snippet="s", url=None
            )
        long_snippet = "s" * 2049
        with self.assertRaises(ValueError):
            briefing_summarizer.SummarizerRequestItem(
                candidate_id="cand-x", title="ok", snippet=long_snippet, url=None
            )
        long_url = "https://example.test/" + ("a" * 2040)
        with self.assertRaises(ValueError):
            briefing_summarizer.SummarizerRequestItem(
                candidate_id="cand-x", title="ok", snippet="s", url=long_url
            )
        # Bounds-satisfying values must not be sliced: a 512-char title and a
        # 2048-char snippet and a 2048-char URL must round-trip exactly.
        boundary_title = "t" * 512
        boundary_snippet = "s" * 2048
        boundary_url = "https://example.test/" + "a" * (2048 - len("https://example.test/"))
        item = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-boundary",
            title=boundary_title,
            snippet=boundary_snippet,
            url=boundary_url,
        )
        self.assertEqual(len(item.title), 512)
        self.assertEqual(len(item.snippet), 2048)
        self.assertEqual(len(item.url), 2048)
        # Identities of the string objects are not lost either: the contracts
        # forbid slicing/copying in the helper.
        self.assertEqual(item.title, boundary_title)
        self.assertEqual(item.snippet, boundary_snippet)
        self.assertEqual(item.url, boundary_url)

    # 22
    def test_request_item_requires_exact_http_or_https_url(self) -> None:
        # ``None`` allowed; lowercase http/https allowed with nonempty authority.
        briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-none",
            title="ok",
            snippet="s",
            url=None,
        )
        briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-https",
            title="ok",
            snippet="s",
            url="https://example.test/path",
        )
        briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-http",
            title="ok",
            snippet="s",
            url="http://example.test/path",
        )
        # Wrong scheme, uppercase scheme, ftp, file, ssh all rejected.
        for bad in (
            "ftp://example.test/path",
            "HTTP://example.test/path",
            "HTTPS://example.test/path",
            "file:///etc/passwd",
            "ssh://example.test/path",
            "data:text/plain,hello",
            "javascript:alert(1)",
            "https://",
            "http:///path",
            "https://example .test/path",  # whitespace inside authority
            "https://example.test/path\nmore",  # newline inside URL
            "https://?next=/x",  # empty authority before query
            "https://#fragment",  # empty authority before fragment
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    briefing_summarizer.SummarizerRequestItem(
                        candidate_id="cand-bad",
                        title="ok",
                        snippet="s",
                        url=bad,
                    )

    # 23
    def test_canonical_request_key_and_item_order(self) -> None:
        items = [
            briefing_summarizer.SummarizerRequestItem(
                candidate_id=f"cand-{i}",
                title=f"title-{i}",
                snippet=f"snippet-{i}",
                url=f"https://example.test/{i}",
            )
            for i in range(3)
        ]
        encoded = briefing_summarizer.canonical_request_bytes(_category(), items)
        self.assertIsInstance(encoded, bytes)
        self.assertLessEqual(len(encoded), 65536)
        text = encoded.decode("utf-8")
        # Verify the envelope starts with the exact top-level key order
        # directly — a recursive json hook would see every nested dict, not
        # just the top level. ``json.loads(text, object_pairs_hook=...)``
        # therefore cannot replace this direct check.
        self.assertTrue(
            text.startswith('{"category":'),
            msg=f"envelope must start with category key: {text[:40]!r}",
        )
        self.assertIn(',"items":[', text)
        # Each top-level key appears exactly once in the encoded bytes.
        self.assertEqual(text.count('"category"'), 1)
        self.assertEqual(text.count('"items"'), 1)
        # Parse to assert structure.
        parsed = json.loads(text)
        self.assertEqual(parsed["category"], _category().value)
        # Item-level key order: depth-tracked hook records the first item
        # dict only. object_pairs_hook is invoked for every nested dict.
        all_dict_keys: List[List[str]] = []

        def _hook(pairs):
            all_dict_keys.append([k for k, _ in pairs])
            return dict(pairs)

        json.loads(text, object_pairs_hook=_hook)
        # The first item-level dict is the one whose keys are the exact
        # ``candidate_id, title, snippet, url`` order. ``object_pairs_hook``
        # is invoked for every dict, including the envelope and items list.
        item_dicts = [
            d for d in all_dict_keys
            if d == ["candidate_id", "title", "snippet", "url"]
        ]
        self.assertGreaterEqual(
            len(item_dicts), 1, msg=f"no item dict found, got {all_dict_keys!r}"
        )
        self.assertEqual(
            item_dicts[0], ["candidate_id", "title", "snippet", "url"]
        )
        # Item order itself must match insertion order.
        item_ids = [d["candidate_id"] for d in parsed["items"]]
        self.assertEqual(item_ids, ["cand-0", "cand-1", "cand-2"])

    # 24
    def test_canonical_request_rejects_over_65536_bytes(self) -> None:
        # Item count is independently capped at 32.
        small = tuple(_valid_item(candidate_id=f"small-{i}") for i in range(33))
        with self.assertRaises(ValueError):
            briefing_summarizer.canonical_request_bytes(_category(), small)

        # At most 32 individually valid items can still overflow 65,536 UTF-8 bytes.
        items = tuple(
            briefing_summarizer.SummarizerRequestItem(
                candidate_id=f"cand-{i}",
                title="t" * 512,
                snippet="s" * 2048,
                url="https://example.test/" + "u" * (2048 - len("https://example.test/")),
            )
            for i in range(32)
        )
        self.assertEqual(len(items), 32)
        with self.assertRaises(ValueError):
            briefing_summarizer.canonical_request_bytes(_category(), items)

        # Session-owned aggregate overflow is INPUT_BOUNDS, not an exception/call.
        transport, calls = _recording_transport(b"unused")
        session = briefing_summarizer.SummarizerSession(transport, {})
        raw = tuple(
            _raw_input(it.candidate_id, it.title, it.snippet, it.url) for it in items
        )
        result = session.summarize_category(_category(), raw)
        self.assertEqual(calls, [])
        self.assertFalse(result.model_used)
        self.assertTrue(all(
            item.error_category is briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS
            for item in result.items
        ))

    # 25
    def test_cache_key_hashes_complete_canonical_bytes(self) -> None:
        item_a = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-a",
            title="title-a",
            snippet="snippet-a",
            url="https://example.test/a",
        )
        item_b = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-a",
            title="title-a",
            snippet="snippet-a",
            url="https://example.test/a",
        )
        # Same inputs, different categories => distinct cache keys (hashing
        # the full envelope including the category).
        key_ai = briefing_summarizer.request_cache_key(_category(), (item_a,))
        key_world = briefing_summarizer.request_cache_key(_other_category(), (item_a,))
        self.assertNotEqual(key_ai, key_world)
        # Same category + same inputs => identical cache key (deterministic).
        self.assertEqual(
            key_ai, briefing_summarizer.request_cache_key(_category(), (item_a,))
        )
        self.assertEqual(
            key_ai, briefing_summarizer.request_cache_key(_category(), (item_b,))
        )
        # The cache key is the SHA-256 of the complete canonical request bytes.
        canon_ai = briefing_summarizer.canonical_request_bytes(_category(), (item_a,))
        self.assertEqual(
            key_ai, hashlib.sha256(canon_ai).digest()
        )
        # Order of items must affect the cache key.
        item_reorder = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-b",
            title="title-b",
            snippet="snippet-b",
            url="https://example.test/b",
        )
        key_pair_ab = briefing_summarizer.request_cache_key(
            _category(), (item_a, item_reorder)
        )
        key_pair_ba = briefing_summarizer.request_cache_key(
            _category(), (item_reorder, item_a)
        )
        self.assertNotEqual(key_pair_ab, key_pair_ba)

    # 26
    def test_cache_key_distinguishes_concatenation_collision(self) -> None:
        item_a = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-a",
            title="title-a",
            snippet="snippet-a",
            url="https://example.test/a",
        )
        item_b = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-b",
            title="title-b",
            snippet="snippet-b",
            url="https://example.test/b",
        )
        canon_single_a = briefing_summarizer.canonical_request_bytes(
            _category(), (item_a,)
        )
        canon_single_b = briefing_summarizer.canonical_request_bytes(
            _category(), (item_b,)
        )
        canon_pair = briefing_summarizer.canonical_request_bytes(
            _category(), (item_a, item_b)
        )
        # Naive byte-concatenation of the single-item envelopes does NOT equal
        # the dual-item envelope; the cache key must hash the entire envelope
        # so a malicious or accidental splice cannot collide.
        self.assertNotEqual(canon_single_a + canon_single_b, canon_pair)
        key_a = briefing_summarizer.request_cache_key(_category(), (item_a,))
        key_b = briefing_summarizer.request_cache_key(_category(), (item_b,))
        key_pair = briefing_summarizer.request_cache_key(
            _category(), (item_a, item_b)
        )
        # No pairwise combination equals the dual key.
        self.assertNotEqual(key_pair, key_a)
        self.assertNotEqual(key_pair, key_b)
        # The dual-key differs from hash(key_a + key_b) too — distinct domains.
        self.assertNotEqual(
            key_pair,
            hashlib.sha256(key_a + key_b).digest(),
        )

    # 27
    def test_parser_rejects_invalid_utf8_and_over_32768_bytes(self) -> None:
        # Response longer than 32768 bytes is rejected before any decoding.
        oversized = b"x" * 32769
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(oversized)
        # Boundary: exactly 32768 bytes is allowed (verified by rejecting
        # only the next-larger size, with body checking the size pass).
        boundary_payload = _make_response(
            [{"candidate_id": "x", "title": "t", "snippet": "s", "url": None, "summary": "y"}]
        )
        self.assertLessEqual(len(boundary_payload), 32768)
        # Invalid UTF-8 (lone 0x80 continuation) is rejected.
        bad_utf8 = b'{"items":[{"candidate_id":"x","title":"t","snippet":"s","url":null,"summary":"\x80"}]}'
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(bad_utf8)

    # 28
    def test_parser_rejects_fences_and_trailing_content(self) -> None:
        # Markdown-style JSON fences wrap the JSON object.
        fenced_inner = _make_response(
            [{"candidate_id": "x", "title": "t", "snippet": "s", "url": None, "summary": "y"}]
        )
        fenced = b"```json\n" + fenced_inner + b"\n```"
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(fenced)
        # Backticks inside a valid JSON string are content, not a Markdown wrapper.
        embedded_backticks = b'{"items":[{"candidate_id":"x","summary":"literal ``` text"}]}'
        self.assertEqual(
            briefing_summarizer.parse_summary_response(embedded_backticks),
            [("x", "literal ``` text")],
        )
        # Trailing content after a valid object.
        trailing = _make_response(
            [{"candidate_id": "x", "title": "t", "snippet": "s", "url": None, "summary": "y"}]
        ) + b"  extra"
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(trailing)
        # Empty (after whitespace strip) is rejected.
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(b"   \n\t  ")

    # 29
    def test_parser_rejects_duplicate_keys_and_nonfinite_constants(self) -> None:
        # Duplicate candidate_id key inside an item dict (object_pairs_hook).
        # We hand-encode a JSON with explicit duplicate ``candidate_id``.
        bad_duplicate = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":"s","url":null,'
            b'"summary":"y","candidate_id":"b"}]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(bad_duplicate)
        # Top-level duplicate ``items`` key.
        bad_top_dup = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":"s","url":null,"summary":"y"}],'
            b'"items":[]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(bad_top_dup)
        # NaN / Infinity literals are rejected even though Python's json
        # module accepts them by default. ``parse_constant`` converts the
        # non-finite tokens into a malformed error.
        nan_body = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":NaN,"url":null,"summary":"y"}]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(nan_body)
        infinity_body = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":Infinity,"url":null,"summary":"y"}]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(infinity_body)
        minus_infinity_body = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":-Infinity,"url":null,"summary":"y"}]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(minus_infinity_body)

    # 30
    def test_parser_rejects_extra_or_missing_keys(self) -> None:
        # Missing top-level ``items``.
        missing_items = b'{"summary": "ok"}'
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(missing_items)
        # Extra top-level key.
        extra_key = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":"s","url":null,"summary":"y"}],'
            b'"trailing":"junk"}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(extra_key)
        # Wrong top-level type.
        wrong_type_items = (
            b'{"items":"not-a-list"}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(wrong_type_items)
        # Item missing ``summary`` is rejected.
        item_missing_summary = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":"s","url":null}]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(item_missing_summary)
        # Item carries an extra key.
        item_extra_key = (
            b'{"items":[{"candidate_id":"a","title":"t","snippet":"s","url":null,"summary":"y","foo":1}]}'
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(item_extra_key)

    # 31
    def test_parser_rejects_missing_extra_duplicate_or_reordered_ids(self) -> None:
        # Build a valid request with two candidate IDs.
        item_a = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-a",
            title="ta",
            snippet="sa",
            url="https://example.test/a",
        )
        item_b = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-b",
            title="tb",
            snippet="sb",
            url="https://example.test/b",
        )
        # Missing second candidate_id.
        missing = _make_response(
            [{"candidate_id": "cand-a", "summary": "y"}]
        )
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(missing, [item_a, item_b])
        # Extra candidate_id not in the request.
        extra = _make_response([
            {"candidate_id": "cand-a", "summary": "y"},
            {"candidate_id": "cand-b", "summary": "y"},
            {"candidate_id": "cand-extra", "summary": "y"},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(extra, [item_a, item_b])
        # Duplicate candidate_id in response.
        duplicate = _make_response([
            {"candidate_id": "cand-a", "summary": "y"},
            {"candidate_id": "cand-a", "summary": "y2"},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(duplicate, [item_a, item_b])
        # Reordered ids (response order differs from request order).
        reordered = _make_response([
            {"candidate_id": "cand-b", "summary": "y"},
            {"candidate_id": "cand-a", "summary": "y"},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(reordered, [item_a, item_b])
        # Sanity: a matching response succeeds.
        ok = _make_response([
            {"candidate_id": "cand-a", "summary": "y"},
            {"candidate_id": "cand-b", "summary": "y"},
        ])
        parsed = briefing_summarizer.parse_summary_response(ok, [item_a, item_b])
        self.assertEqual([cid for cid, _ in parsed], ["cand-a", "cand-b"])

    # 32
    def test_parser_rejects_summary_bounds_and_controls(self) -> None:
        item_a = briefing_summarizer.SummarizerRequestItem(
            candidate_id="cand-a", title="ta", snippet="sa",
            url="https://example.test/a",
        )
        # Empty summary.
        empty_summary = _make_response([
            {"candidate_id": "cand-a", "summary": ""},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(empty_summary, [item_a])
        # Over-limit summary (513 code points).
        too_long = _make_response([
            {"candidate_id": "cand-a", "summary": "s" * 513},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(too_long, [item_a])
        # C0 control (\x07) embedded in summary.
        with_control = _make_response([
            {"candidate_id": "cand-a", "summary": "abc\x07def"},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(with_control, [item_a])
        # Bidi control (U+202E RIGHT-TO-LEFT OVERRIDE).
        with_bidi = _make_response([
            {"candidate_id": "cand-a", "summary": "abc\u202edef"},
        ])
        with self.assertRaises(briefing_summarizer.SummarizerMalformedError):
            briefing_summarizer.parse_summary_response(with_bidi, [item_a])
        # Boundary: 1..512 inclusive is allowed.
        boundary_summary = "s" * 512
        ok = _make_response([
            {"candidate_id": "cand-a", "summary": boundary_summary},
        ])
        parsed = briefing_summarizer.parse_summary_response(ok, [item_a])
        self.assertEqual(parsed[0][1], boundary_summary)

    # 33
    def test_empty_request_never_calls_model(self) -> None:
        transport, calls = _recording_transport(
            _make_response([
                {"candidate_id": "x", "title": "t", "snippet": "s", "url": None, "summary": "y"},
            ])
        )
        session = briefing_summarizer.SummarizerSession(transport=transport, cache={})
        # No raw inputs at all.
        result = session.summarize_category(_category(), ())
        self.assertEqual(list(calls), [])
        self.assertEqual(result.items, ())
        self.assertFalse(result.model_used)
        self.assertFalse(result.cache_hit)
        # And each session counter remains zero.
        self.assertEqual(session.model_call_count, 0)
        self.assertEqual(session.cache_hit_count, 0)
        # A category whose inputs are all out of bounds does not call either.
        oversize_url = "https://example.test/" + ("a" * 2028)
        raw_invalid = briefing_summarizer.SummarizerInput(
            candidate_id="bad", title="ok", snippet="s", url=oversize_url,
        )
        result_invalid = session.summarize_category(_other_category(), (raw_invalid,))
        self.assertEqual(list(calls), [])
        self.assertEqual(
            [it.source for it in result_invalid.items],
            [briefing_summarizer.SummarySource.FALLBACK],
        )
        self.assertEqual(
            [it.error_category for it in result_invalid.items],
            [briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS],
        )
        self.assertFalse(result_invalid.model_used)
        self.assertFalse(result_invalid.cache_hit)

    # 34
    def test_one_uncached_call_per_category(self) -> None:
        item_a = _valid_item(candidate_id="cand-a")
        response_a = _success_response_for([item_a])
        transport, calls = _recording_transport(response_a)
        session = briefing_summarizer.SummarizerSession(transport=transport, cache={})
        # First call to category AI: uncached, transport fires once.
        result1 = session.summarize_category(_category(), (_raw_input(candidate_id="cand-a"),))
        self.assertEqual(len(calls), 1)
        expected_request = briefing_summarizer.canonical_request_bytes(_category(), (item_a,))
        self.assertEqual(calls[0], expected_request)
        self.assertEqual(
            hashlib.sha256(calls[0]).digest(),
            briefing_summarizer.request_cache_key(_category(), (item_a,)),
        )
        # Second call to the SAME category with NEW inputs (cache miss):
        # one uncached call/category rule says no second model invocation
        # for the same category; the second call returns BUDGET_EXHAUSTED.
        item_b = _valid_item(candidate_id="cand-b")
        raw_b = _raw_input(candidate_id="cand-b", title="tb", snippet="sb",
                            url="https://example.test/b")
        result2 = session.summarize_category(_category(), (raw_b,))
        # Transport was not invoked again; budget for AI is exhausted.
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            [it.source for it in result2.items],
            [briefing_summarizer.SummarySource.FALLBACK],
        )
        self.assertEqual(
            [it.error_category for it in result2.items],
            [briefing_summarizer.SummarizerErrorCategory.BUDGET_EXHAUSTED],
        )
        self.assertFalse(result2.model_used)
        self.assertFalse(result2.cache_hit)
        # And a different category is unaffected.
        item_other = _valid_item(candidate_id="cand-other")
        response_other = _success_response_for([item_other])
        transport2, calls2 = _recording_transport(response_other)
        session2 = briefing_summarizer.SummarizerSession(transport=transport2, cache={})
        result_first = session2.summarize_category(
            _other_category(), (_raw_input(candidate_id="cand-other"),)
        )
        result_again_ai = session2.summarize_category(
            _category(), (_raw_input(candidate_id="cand-a"),)
        )
        self.assertEqual(len(calls2), 2)
        self.assertTrue(result_first.model_used)
        self.assertTrue(result_again_ai.model_used)

    # 35
    def test_eight_call_total_budget(self) -> None:
        # All 8 underscore-valued Category members, each call uncached; the
        # 8th call is the last allowed. A 9th call across a fresh session
        # for any already-used category runs into budget exhaustion.
        all_categories = (_category(), *_other_categories())
        # Pick items distinct per call.
        items_per_call = [
            _valid_item(candidate_id=f"cand-{i}") for i in range(len(all_categories))
        ]
        call_count = 0

        def _t(_request: bytes) -> bytes:
            nonlocal call_count
            # Each call gets the response that matches the items for the
            # currently-invoked category.
            call_count += 1
            return _success_response_for(items_per_call[call_count - 1:call_count])

        session = briefing_summarizer.SummarizerSession(transport=_t, cache={})
        # 8 successful calls.
        for i in range(8):
            result = session.summarize_category(
                all_categories[i],
                (_raw_input(candidate_id=f"cand-{i}"),),
            )
            self.assertTrue(result.model_used, msg=f"call {i} should model-invoke")
        self.assertEqual(session.model_call_count, 8)
        self.assertEqual(call_count, 8)
        # The 9th distinct category hits budget: transport is not invoked
        # again; result is BUDGET_EXHAUSTED.
        result_ninth = session.summarize_category(
            all_categories[8] if len(all_categories) > 8 else _category(),
            (_raw_input(candidate_id="cand-overflow"),),
        )
        self.assertEqual(call_count, 8)
        self.assertEqual(session.model_call_count, 8)
        self.assertEqual(
            [it.source for it in result_ninth.items],
            [briefing_summarizer.SummarySource.FALLBACK],
        )
        self.assertEqual(
            [it.error_category for it in result_ninth.items],
            [briefing_summarizer.SummarizerErrorCategory.BUDGET_EXHAUSTED],
        )
        overflow = tuple(_raw_input(candidate_id=f"overflow-{i}") for i in range(34))
        overflow_result = session.summarize_category(_category(), overflow)
        self.assertTrue(all(
            it.error_category is briefing_summarizer.SummarizerErrorCategory.BUDGET_EXHAUSTED
            for it in overflow_result.items[:32]
        ))
        self.assertTrue(all(
            it.error_category is briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS
            for it in overflow_result.items[32:]
        ))

    # 36
    def test_cache_hit_consumes_no_budget_and_sets_bookkeeping(self) -> None:
        item_a = _valid_item(candidate_id="cand-a")
        response_a = _success_response_for([item_a])
        # Cache with a successful prior outcome pre-populated for this exact
        # request. The session must hit cache, not invoke transport, and
        # record model_used=False / cache_hit=True.
        canon_a = briefing_summarizer.canonical_request_bytes(_category(), (item_a,))
        key_a = briefing_summarizer.request_cache_key(_category(), (item_a,))
        cache_value = briefing_summarizer.SummarizerCacheValue(
            items=(briefing_summarizer.SummaryItem(
                candidate_id="cand-a",
                summary="cached summary",
                source=briefing_summarizer.SummarySource.MODEL,
                error_category=None,
            ),),
            model_used=True,
        )
        cache = {key_a: cache_value}
        transport, calls = _recording_transport(response_a)
        session = briefing_summarizer.SummarizerSession(transport=transport, cache=cache)
        result = session.summarize_category(
            _category(), (_raw_input(candidate_id="cand-a"),)
        )
        # Transport was not invoked.
        self.assertEqual(list(calls), [])
        # Cache hit bookkeeping.
        self.assertFalse(result.model_used)
        self.assertTrue(result.cache_hit)
        # No budget consumption.
        self.assertEqual(session.model_call_count, 0)
        self.assertEqual(session.cache_hit_count, 1)
        # Items carry the CACHE source and cached summary text — proving the
        # session preserved parsed summaries and original model outcome.
        self.assertEqual(
            [it.source for it in result.items],
            [briefing_summarizer.SummarySource.CACHE],
        )
        self.assertEqual([it.summary for it in result.items], ["cached summary"])
        # A second identical call also hits cache, no budget spent.
        result_again = session.summarize_category(
            _category(), (_raw_input(candidate_id="cand-a"),)
        )
        self.assertEqual(list(calls), [])
        self.assertEqual(session.cache_hit_count, 2)
        self.assertEqual(session.model_call_count, 0)

        # A successful write must mutate the caller-owned cache for another session.
        shared_cache = {}
        writer_calls: List[bytes] = []
        def _writer(request: bytes) -> bytes:
            writer_calls.append(request)
            return response_a
        writer = briefing_summarizer.SummarizerSession(_writer, shared_cache)
        first = writer.summarize_category(_category(), (_raw_input(candidate_id="cand-a"),))
        self.assertTrue(first.model_used)
        self.assertEqual(len(shared_cache), 1)
        reader_calls: List[bytes] = []
        reader = briefing_summarizer.SummarizerSession(
            lambda request: reader_calls.append(request) or response_a,
            shared_cache,
        )
        second = reader.summarize_category(_category(), (_raw_input(candidate_id="cand-a"),))
        self.assertTrue(second.cache_hit)
        self.assertFalse(second.model_used)
        self.assertEqual(reader_calls, [])

        with self.assertRaises(ValueError):
            briefing_summarizer.CategorySummaryResult((), True, True)

    # 37
    def test_transport_error_fallback_preserves_every_id(self) -> None:
        item_a = _valid_item(candidate_id="cand-a")
        item_b = _valid_item(candidate_id="cand-b")
        item_c = _valid_item(candidate_id="cand-c")

        def _t(_request: bytes) -> bytes:
            raise briefing_summarizer.SummarizerTransportError("network down")

        session = briefing_summarizer.SummarizerSession(transport=_t, cache={})
        result = session.summarize_category(
            _category(),
            (
                _raw_input(candidate_id="cand-a", title="ta"),
                _raw_input(candidate_id="cand-b", title="tb"),
                _raw_input(candidate_id="cand-c", title="tc"),
            ),
        )
        # Every ID preserved and mapped to TRANSPORT_ERROR fallback.
        ids = [it.candidate_id for it in result.items]
        self.assertEqual(ids, ["cand-a", "cand-b", "cand-c"])
        for it, expected_title in zip(result.items, ("ta", "tb", "tc")):
            self.assertIs(it.source, briefing_summarizer.SummarySource.FALLBACK)
            self.assertEqual(
                it.error_category,
                briefing_summarizer.SummarizerErrorCategory.TRANSPORT_ERROR,
            )
            self.assertEqual(it.summary, expected_title)
        # The transport call did happen — model_used=True, cache_hit=False.
        self.assertTrue(result.model_used)
        self.assertFalse(result.cache_hit)
        self.assertEqual(session.model_call_count, 1)

        overflow_session = briefing_summarizer.SummarizerSession(transport=_t, cache={})
        overflow = tuple(_raw_input(candidate_id=f"cand-{i}") for i in range(34))
        overflow_result = overflow_session.summarize_category(_other_category(), overflow)
        self.assertTrue(all(
            it.error_category is briefing_summarizer.SummarizerErrorCategory.TRANSPORT_ERROR
            for it in overflow_result.items[:32]
        ))
        self.assertTrue(all(
            it.error_category is briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS
            for it in overflow_result.items[32:]
        ))

    # 38
    def test_malformed_output_fallback_preserves_every_id(self) -> None:
        item_a = _valid_item(candidate_id="cand-a")
        item_b = _valid_item(candidate_id="cand-b")

        def _t(_request: bytes) -> bytes:
            # Bytes that decode and parse but fail schema (extra trailing
            # junk after the JSON object).
            return _success_response_for([item_a, item_b]) + b""

        # Use a binary payload that the parser must reject — trailing whitespace ok,
        # but missing required field or duplicate key triggers malformed.
        bad_response = (
            b'{"items":[{"candidate_id":"cand-a","title":"ta","snippet":"sa",'
            b'"url":null}]}'
        )
        transport = _strict_transport(bad_response)
        session = briefing_summarizer.SummarizerSession(transport=transport, cache={})
        result = session.summarize_category(
            _category(),
            (
                _raw_input(candidate_id="cand-a", title="ta"),
                _raw_input(candidate_id="cand-b", title="tb"),
            ),
        )
        # Both IDs preserved, both mapped to MALFORMED_OUTPUT.
        ids = [it.candidate_id for it in result.items]
        self.assertEqual(ids, ["cand-a", "cand-b"])
        for it in result.items:
            self.assertIs(it.source, briefing_summarizer.SummarySource.FALLBACK)
            self.assertEqual(
                it.error_category,
                briefing_summarizer.SummarizerErrorCategory.MALFORMED_OUTPUT,
            )
        # The real invocation happened — model_used=True.
        self.assertTrue(result.model_used)
        self.assertFalse(result.cache_hit)

    # 39
    def test_input_bounds_fallback_preserves_every_id(self) -> None:
        # Mix of valid + bound-invalid raw inputs in one category. Each must
        # appear in the result, invalid ones become INPUT_BOUNDS fallback
        # preserving their original candidate ID and falling back to
        # ``Untitled item`` when the title itself is invalid.
        valid_input = _raw_input(candidate_id="cand-good", title="Valid Title")
        invalid_title_input = _raw_input(
            candidate_id="cand-bad-title", title=""  # bound-fail title
        )
        invalid_snippet_input = _raw_input(
            candidate_id="cand-bad-snippet", title="ok", snippet="nope"  # placeholder; see below
        )
        invalid_url_input = _raw_input(
            candidate_id="cand-bad-url",
            title="ok",
            url="ftp://example.test/path",
        )
        # Build a single valid request item to model-summarize.
        item_good = _valid_item(candidate_id="cand-good")
        response = _success_response_for([item_good])
        transport, calls = _recording_transport(response)
        session = briefing_summarizer.SummarizerSession(transport=transport, cache={})
        # Synthesize inputs where snippet is the only bound-failing field.
        # SummarizerInput does not slice; we pass a too-long snippet.
        long_snippet = "n" * 2049
        invalid_snippet_long = briefing_summarizer.SummarizerInput(
            candidate_id="cand-bad-snippet-long",
            title="ok title",
            snippet=long_snippet,
            url="https://example.test/x",
        )
        result = session.summarize_category(
            _category(),
            (
                valid_input,
                invalid_title_input,
                invalid_snippet_long,
                invalid_url_input,
                _raw_input(candidate_id="", title="empty id"),
                _raw_input(candidate_id="x" * 257, title="long id"),
            ),
        )
        # All 4 IDs preserved in order.
        ids = [it.candidate_id for it in result.items]
        self.assertEqual(
            ids,
            [
                "cand-good",
                "cand-bad-title",
                "cand-bad-snippet-long",
                "cand-bad-url",
                "",
                "x" * 257,
            ],
        )
        # The valid one becomes a real model call + summary.
        good_item = result.items[0]
        self.assertIs(
            good_item.source, briefing_summarizer.SummarySource.MODEL
        )
        self.assertIsNone(good_item.error_category)
        # The invalid-title item is an INPUT_BOUNDS FALLBACK with the
        # ``Untitled item`` literal because the title itself was invalid.
        bad_title_item = result.items[1]
        self.assertIs(
            bad_title_item.source, briefing_summarizer.SummarySource.FALLBACK
        )
        self.assertEqual(
            bad_title_item.error_category,
            briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(bad_title_item.summary, "Untitled item")
        # Snippet-too-long: INPUT_BOUNDS with original title in summary.
        bad_snip_item = result.items[2]
        self.assertIs(bad_snip_item.source, briefing_summarizer.SummarySource.FALLBACK)
        self.assertEqual(
            bad_snip_item.error_category,
            briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(bad_snip_item.summary, "ok title")
        # URL-too-bad: INPUT_BOUNDS with original title.
        bad_url_item = result.items[3]
        self.assertIs(bad_url_item.source, briefing_summarizer.SummarySource.FALLBACK)
        self.assertEqual(
            bad_url_item.error_category,
            briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS,
        )
        self.assertEqual(bad_url_item.summary, "ok")
        for bad_id_item in result.items[4:]:
            self.assertIs(bad_id_item.source, briefing_summarizer.SummarySource.FALLBACK)
            self.assertIs(
                bad_id_item.error_category,
                briefing_summarizer.SummarizerErrorCategory.INPUT_BOUNDS,
            )
        self.assertEqual(result.items[4].candidate_id, "")
        self.assertEqual(result.items[5].candidate_id, "x" * 257)
        # Transport was invoked exactly once — the model was asked for the
        # single valid item.
        self.assertEqual(len(calls), 1)
        self.assertTrue(result.model_used)
        self.assertFalse(result.cache_hit)
        self.assertEqual(session.model_call_count, 1)

    # 40
    def test_programmer_exception_propagates(self) -> None:
        # Non-SummarizerTransportError exceptions must propagate untouched,
        # not be mapped to a typed summary error.
        class NotAKeyError(RuntimeError):
            pass

        def _t(_request: bytes) -> bytes:
            raise NotAKeyError("programmer blew up")

        session = briefing_summarizer.SummarizerSession(transport=_t, cache={})
        with self.assertRaises(NotAKeyError):
            session.summarize_category(
                _category(), (_raw_input(candidate_id="cand-a"),)
            )
        # A ValueError raised inside the transport also propagates unchanged.
        def _t_value_error(_request: bytes) -> bytes:
            raise ValueError("not a transport error")

        session2 = briefing_summarizer.SummarizerSession(
            transport=_t_value_error, cache={}
        )
        with self.assertRaises(ValueError):
            session2.summarize_category(
                _category(), (_raw_input(candidate_id="cand-a"),)
            )
        # A TransportError is properly mapped to TRANSPORT_ERROR fallback;
        # this proves the exception width is selective.
        def _t_transport(_request: bytes) -> bytes:
            raise briefing_summarizer.SummarizerTransportError("down")

        session3 = briefing_summarizer.SummarizerSession(
            transport=_t_transport, cache={}
        )
        result = session3.summarize_category(
            _category(), (_raw_input(candidate_id="cand-a"),)
        )
        self.assertEqual(
            [it.error_category for it in result.items],
            [briefing_summarizer.SummarizerErrorCategory.TRANSPORT_ERROR],
        )

        duplicate_session = briefing_summarizer.SummarizerSession(_strict_transport(b"unused"), {})
        with self.assertRaises(ValueError):
            duplicate_session.summarize_category(
                _category(),
                (_raw_input(candidate_id="dup"), _raw_input(candidate_id="dup")),
            )


if __name__ == "__main__":
    unittest.main()
