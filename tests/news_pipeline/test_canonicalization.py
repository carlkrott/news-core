from __future__ import annotations

import unittest

from news_pipeline.canonicalization import canonicalize_url, retain_original


class CanonicalizationTests(unittest.TestCase):
    def test_lowercases_and_strips_default_ports(self) -> None:
        self.assertEqual(
            canonicalize_url("HTTP://Example.COM:80/a"),
            "http://example.com/a",
        )
        self.assertEqual(
            canonicalize_url("HTTPS://Example.COM:443/a"),
            "https://example.com/a",
        )

    def test_does_not_upgrade_http_to_https(self) -> None:
        self.assertEqual(canonicalize_url("http://example.com/a"), "http://example.com/a")

    def test_idna_encodes_host_and_preserves_www(self) -> None:
        self.assertEqual(
            canonicalize_url("https://WWW.BÜCHER.de/News"),
            "https://www.xn--bcher-kva.de/News",
        )

    def test_drops_tracking_sorts_query_and_strips_fragment(self) -> None:
        self.assertEqual(
            canonicalize_url(
                "https://Example.com/x?z=2&utm_source=x&a=3&fbclid=y&a=1&ref=no#frag"
            ),
            "https://example.com/x?a=1&a=3&z=2",
        )

    def test_removes_dot_segments(self) -> None:
        self.assertEqual(
            canonicalize_url("https://example.com/a/./b/../c/"),
            "https://example.com/a/c/",
        )

    def test_rejects_non_web_schemes(self) -> None:
        for url in ("ftp://example.com/x", "file:///etc/passwd", "javascript:alert(1)"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                canonicalize_url(url)

    def test_empty_and_none_are_none_and_original_is_exact(self) -> None:
        self.assertIsNone(canonicalize_url(None))
        self.assertIsNone(canonicalize_url(""))
        original = "HTTPS://Example.COM/x?utm_source=y#frag"
        self.assertEqual(retain_original(original), original)

    def test_requires_host_and_valid_port(self) -> None:
        for url in ("https:///missing", "https://example.com:bad/x"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                canonicalize_url(url)


if __name__ == "__main__":
    unittest.main()
