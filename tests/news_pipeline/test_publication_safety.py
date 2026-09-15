"""Focused publication-safety tests.

These tests verify the ``check_publication_safety`` scanner accepts the
candidate export surface and rejects synthetic negative fixtures that
exercise each detection rule.
"""

from __future__ import annotations

import ast
import importlib
import json
import os
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPTS_DIR = ROOT / "scripts"
CHECKER = SCRIPTS_DIR / "check_publication_safety.py"

# The scanner lives outside any package, so inject its directory into
# ``sys.path`` before importing.
sys.path.insert(0, str(SCRIPTS_DIR))


def _load_checker():
    """Load the scanner module fresh so tests do not cache state."""

    if "check_publication_safety" in sys.modules:
        del sys.modules["check_publication_safety"]
    return importlib.import_module("check_publication_safety")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _make_tmpdir(prefix: str) -> str:
    import tempfile

    d = tempfile.mkdtemp(prefix=prefix)
    return d


class PublicationSafetyAllowlist(unittest.TestCase):
    """The candidate export tree must scan clean."""

    def test_allowlisted_files_exist_on_disk(self) -> None:
        """Every allowlisted file must exist on disk under the candidate root."""

        # Sanity-check that the publication-safety scanner source and the
        # unit test source both exist; without these, ``scan`` would
        # produce REQUIRED_DOC_MISSING findings.
        self.assertTrue(CHECKER.exists(), "checker script must exist")
        self.assertTrue(
            (ROOT / "tests" / "news_pipeline" / "test_publication_safety.py").exists()
        )

    def test_candidate_root_with_only_allowlisted_files_scans_clean(self) -> None:
        """Mirror the export into a tmp root with only allowlisted files."""

        cps = _load_checker()

        d = _make_tmpdir("publication-safety-")
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        mirror = Path(d) / "mirror"
        mirror.mkdir(parents=True)
        # Copy each allowlisted file by basename only; this enforces that
        # the scanner does not accidentally depend on a parent directory
        # that exists but is otherwise empty.
        allowlisted = [
            ".gitignore",
            ".dockerignore",
            ".gitattributes",
            "pyproject.toml",
            "README.md",
            "ARCHITECTURE.md",
            "SECURITY.md",
            "CONTRIBUTING.md",
            "BUILD.md",
            "LICENSE",
            "config/news-policy.example.toml",
            "config/news-sources.example.toml",
            "config/news-topics.example.toml",
            "scripts/check_publication_safety.py",
            "tests/news_pipeline/test_publication_safety.py",
        ]
        for rel in allowlisted:
            src = ROOT / rel
            self.assertTrue(src.exists(), f"required candidate file missing: {rel}")
            dst = mirror / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)

        exit_code, findings = cps.scan(mirror)
        if exit_code != 0:
            rendered = "\n".join(f.render() for f in findings)
            self.fail(f"publication scan rejected mirror:\n{rendered}")

    def test_real_export_helper_copies_core_and_excludes_private_candidate_files(self) -> None:
        cps = _load_checker()
        d = Path(_make_tmpdir("publication-export-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        destination = d / "public"
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(ROOT),
                "--destination",
                str(destination),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertTrue((destination / "scripts/news_pipeline/jobs.py").is_file())
        self.assertTrue((destination / "scripts/news_container/worker.py").is_file())
        self.assertFalse((destination / "private-evidence").exists())
        self.assertFalse((destination / "config/news-sources.toml").exists())
        exit_code, findings = cps.scan(destination)
        self.assertEqual(exit_code, 0, "\n".join(f.render() for f in findings))

    def test_real_export_from_unrelated_cwd_has_complete_inventory(self) -> None:
        """An absolute-source export is complete even when cwd is unrelated."""

        cps = _load_checker()
        d = Path(_make_tmpdir("publication-export-cwd-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        destination = d / "public"
        unrelated = d / "unrelated"
        unrelated.mkdir()
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(ROOT),
                "--destination",
                str(destination),
            ],
            cwd=unrelated,
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)

        expected: set[str] = set()
        for entry in cps._iter_files(ROOT):
            if entry.is_dir():
                continue
            rel = entry.relative_to(ROOT)
            if cps.is_export_excluded(rel):
                continue
            allowed, _ = cps._classify_path(rel, source_entry=entry)
            if allowed:
                expected.add(rel.as_posix())
        actual = {
            p.relative_to(destination).as_posix()
            for p in destination.rglob("*")
            if p.is_file()
        }
        self.assertEqual(actual, expected)

    def test_export_rejects_symlink_special_file_and_hardlink_alias(self) -> None:
        """Unsafe source entries never become part of a staged mirror."""

        cases = ("symlink", "fifo", "hardlink")
        for case in cases:
            with self.subTest(case=case):
                d = Path(_make_tmpdir(f"publication-export-{case}-"))
                self.addCleanup(shutil.rmtree, d, ignore_errors=True)
                source = d / "source"
                shutil.copytree(ROOT, source, symlinks=True)
                destination = d / "public"
                if case == "symlink":
                    outside = d / "outside.txt"
                    outside.write_text("outside\n", encoding="utf-8")
                    (source / "extra.txt").symlink_to(outside)
                elif case == "fifo":
                    os.mkfifo(source / "extra.pipe")
                else:
                    jobs = source / "scripts/news_pipeline/jobs.py"
                    target = source / "scripts/news_pipeline/process_runner.py"
                    jobs.unlink()
                    os.link(target, jobs)
                proc = subprocess.run(
                    [
                        sys.executable,
                        str(SCRIPTS_DIR / "export_public_tree.py"),
                        "--source",
                        str(source),
                        "--destination",
                        str(destination),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=30,
                )
                self.assertNotEqual(proc.returncode, 0)
                self.assertFalse(destination.exists())

    def test_export_rejects_destination_alias(self) -> None:
        d = Path(_make_tmpdir("publication-export-destination-alias-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        outside = d / "outside"
        outside.mkdir()
        destination = d / "public"
        destination.symlink_to(outside, target_is_directory=True)
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(ROOT),
                "--destination",
                str(destination),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertTrue(destination.is_symlink())

    def test_export_cleans_partial_staging_after_scan_failure(self) -> None:
        d = Path(_make_tmpdir("publication-export-cleanup-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        source = d / "source"
        shutil.copytree(ROOT, source, symlinks=True)
        policy = source / "config/news-policy.example.toml"
        key = "api" + "_key"
        value = "abc12345" + "def67890"
        policy.write_text(
            policy.read_text(encoding="utf-8") + f'{key} = "{value}"\n',
            encoding="utf-8",
        )
        destination = d / "public"
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(source),
                "--destination",
                str(destination),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertFalse(destination.exists())
        self.assertEqual(list(d.glob(".public.partial-*")), [])

    def _tmpdir(self) -> str:
        # Kept for backwards-compat with potential subclasses; new tests
        # should prefer ``_make_tmpdir`` so the cleanup is registered
        # explicitly.
        return _make_tmpdir("publication-safety-")


class PublicationSafetySyntheticNegatives(unittest.TestCase):
    """Each rule must reject a synthetic negative fixture."""

    def setUp(self) -> None:
        cps = _load_checker()
        self._scan = cps.scan

    def _make(self, files: dict[str, str]) -> Path:
        import tempfile

        d = Path(tempfile.mkdtemp(prefix="publication-safety-negative-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        for rel, content in files.items():
            target = d / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        return d

    def _expect_code(self, files: dict[str, str], *, code: str) -> None:
        root = self._make(files)
        exit_code, findings = self._scan(root)
        self.assertNotEqual(exit_code, 0, "negative fixture must be rejected")
        self.assertTrue(
            any(f.code == code for f in findings),
            f"expected finding with code {code!r}, got: {[f.code for f in findings]}",
        )

    def test_rejects_private_user_home(self) -> None:
        self._expect_code(
            {
                "config/news-sources.example.toml": (
                    "# Public-export example source registry.\n"
                    "[sources]\n"
                    'source_id = "leak"\n'
                    'host = "/home/korphaus/state.db"\n'
                ),
            },
            code="MAINTAINER_LITERAL",
        )

    def test_rejects_carl_literal(self) -> None:
        self._expect_code(
            {
                "config/news-sources.example.toml": (
                    "# Public-export example source registry.\n"
                    "[sources]\n"
                    'source_id = "leak"\n'
                    'host = "carl.local"\n'
                ),
            },
            code="MAINTAINER_LITERAL",
        )

    def test_rejects_zeroclaw_reference(self) -> None:
        self._expect_code(
            {
                "config/news-sources.example.toml": (
                    "# Public-export example source registry.\n"
                    "[sources]\n"
                    'source_id = "leak"\n'
                    'host = ".zeroclaw.adapter"\n'
                ),
            },
            code="MAINTAINER_LITERAL",
        )

    def test_rejects_private_ipv4(self) -> None:
        self._expect_code(
            {
                "config/news-sources.example.toml": (
                    "# Public-export example source registry.\n"
                    "[sources]\n"
                    'source_id = "leak"\n'
                    'host = "192.168.1.42"\n'
                ),
            },
            code="PRIVATE_IP_LITERAL",
        )

    def test_rejects_tailnet_literal(self) -> None:
        self._expect_code(
            {
                "config/news-sources.example.toml": (
                    "# Public-export example source registry.\n"
                    "[sources]\n"
                    'source_id = "leak"\n'
                    'host = "100.100.100.100"\n'
                ),
            },
            code="TAILNET_LITERAL",
        )

    def test_rejects_bearer_token(self) -> None:
        self._expect_code(
            {
                "config/news-sources.example.toml": (
                    "# Public-export example source registry.\n"
                    "[sources]\n"
                    'source_id = "leak"\n'
                    'host = "example.com"\n'
                    'header = "Authorization: ' + "Bearer " +
                    'abcdefghijklmnopqrstuvwxyz123456"\n'
                ),
            },
            code="CREDENTIAL_PATTERN",
        )

    def test_rejects_forbidden_suffix(self) -> None:
        self._expect_code(
            {
                "README.md": "irrelevant\n",
                "notes.db": "select 1;\n",
            },
            code="FORBIDDEN_ENTRY",
        )

    def test_rejects_pycache(self) -> None:
        self._expect_code(
            {
                "README.md": "irrelevant\n",
                "scripts/__pycache__/foo.cpython-311.pyc": "x",
            },
            code="FORBIDDEN_ENTRY",
        )

    def test_rejects_symlink(self) -> None:
        # Build a symlink manually.
        import tempfile

        d = Path(tempfile.mkdtemp(prefix="publication-safety-symlink-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        (d / "README.md").write_text("hello\n", encoding="utf-8")
        target = d / "elsewhere"
        target.write_text("private\n", encoding="utf-8")
        # Create a symlink with a non-allowlisted name.
        link = d / "extra.txt"
        link.symlink_to(target)
        exit_code, findings = self._scan(d)
        self.assertNotEqual(exit_code, 0)
        self.assertTrue(any(f.code == "FORBIDDEN_ENTRY" for f in findings))

    def test_rejects_missing_required_doc(self) -> None:
        # Empty tree: required docs missing.
        import tempfile

        d = Path(tempfile.mkdtemp(prefix="publication-safety-missing-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        exit_code, findings = self._scan(d)
        self.assertNotEqual(exit_code, 0)
        self.assertTrue(any(f.code == "REQUIRED_DOC_MISSING" for f in findings))

    def test_secret_detector_fixture_basename_is_recognised(self) -> None:
        """The scanner's fixture allowlist must identify synthetic fixtures."""

        cps = _load_checker()

        yes = cps._is_fixture(
            Path("tests/news_pipeline/fixtures/phase6_searxng_token_fixture.json")
        )
        no = cps._is_fixture(Path("config/news-sources.example.toml"))
        self.assertTrue(yes)
        self.assertFalse(no)

    def test_content_allowances_are_spans_not_whole_file_bypasses(self) -> None:
        """Documentation and rule sources are scanned outside exact spans."""

        cps = _load_checker()

        self.assertIn("README.md", cps.CONTENT_SPAN_ALLOWANCES)
        self.assertIn("MAINTAINER_LITERALS", cps._SCANNER_ALLOWANCE_ASSIGNMENTS)
        self.assertFalse(
            cps._span_allowed("README.md", "CREDENTIAL_PATTERN", "unused", 0, 1)
        )

    def test_scanner_detects_forbidden_content_outside_docs(self) -> None:
        """Without the documentation fixture, identical forbidden content is rejected.

        This guards against the scanner silently skipping content rules:
        if the same maintainer literal / private IP / Tailnet fragment
        is moved into a non-documentation allowlisted file, the scanner
        must flag it.
        """

        cps = _load_checker()
        d = _make_tmpdir("publication-safety-cross-")
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        root = Path(d)
        (root / "README.md").write_text("ok\n", encoding="utf-8")
        (root / "ARCHITECTURE.md").write_text("ok\n", encoding="utf-8")
        (root / "SECURITY.md").write_text("ok\n", encoding="utf-8")
        (root / "CONTRIBUTING.md").write_text("ok\n", encoding="utf-8")
        (root / "BUILD.md").write_text("ok\n", encoding="utf-8")
        (root / "pyproject.toml").write_text("# placeholder\n", encoding="utf-8")
        # Embed a maintainer literal and a private IPv4 literal in
        # non-doc files -- both must be rejected.
        (root / "config").mkdir()
        (root / "config/news-policy.example.toml").write_text(
            'host = "/home/korphaus/state.db"\n',
            encoding="utf-8",
        )
        (root / "config/news-sources.example.toml").write_text(
            'host = "192.168.7.7"\n',
            encoding="utf-8",
        )
        (root / "config/news-topics.example.toml").write_text("# placeholder\n", encoding="utf-8")
        (root / "scripts").mkdir()
        (root / "scripts/check_publication_safety.py").write_text(
            (Path(CHECKER).read_text(encoding="utf-8")),
            encoding="utf-8",
        )
        (root / "tests/news_pipeline").mkdir(parents=True)
        (root / "tests/news_pipeline/test_publication_safety.py").write_text(
            "# placeholder\n",
            encoding="utf-8",
        )

        exit_code, findings = cps.scan(root)
        self.assertNotEqual(exit_code, 0)
        codes = {f.code for f in findings}
        self.assertIn("MAINTAINER_LITERAL", codes)
        self.assertIn("PRIVATE_IP_LITERAL", codes)

    @staticmethod
    def _synthetic_credential_assignment() -> str:
        key = "api" + "_key"
        value = "abc12345" + "def67890"
        return f'{key} = "{value}"\n'

    def test_ordinary_literal_and_benign_delivery_source_are_clean(self) -> None:
        """Ordinary short values and delivery's token variable are benign."""

        import tempfile

        base = Path(tempfile.mkdtemp(prefix="publication-safety-benign-"))
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(ROOT),
                "--destination",
                str(base / "mirror"),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        mirror = base / "mirror"
        policy = mirror / "config/news-policy.example.toml"
        policy.write_text(policy.read_text(encoding="utf-8") + 'token = "ordinary"\n', encoding="utf-8")
        exit_code, findings = self._scan(mirror)
        self.assertEqual(exit_code, 0, "\n".join(f.render() for f in findings))
        self.assertNotIn("CREDENTIAL_PATTERN", {f.code for f in findings})
        self.assertTrue((mirror / "scripts/news_pipeline/delivery.py").is_file())

    def test_credentials_in_docs_host_scanner_and_publication_test_are_rejected(self) -> None:
        """Content allowances are spans, never whole-file bypasses."""

        import tempfile

        base = Path(tempfile.mkdtemp(prefix="publication-safety-injected-"))
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(ROOT),
                "--destination",
                str(base / "mirror"),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        clean = base / "mirror"
        targets = (
            "README.md",
            "ARCHITECTURE.md",
            "SECURITY.md",
            "CONTRIBUTING.md",
            "BUILD.md",
            "host/news_egress_broker.py",
            "scripts/check_publication_safety.py",
            "tests/news_pipeline/test_publication_safety.py",
        )
        for rel in targets:
            with self.subTest(path=rel):
                case = base / "case"
                if case.exists():
                    shutil.rmtree(case)
                shutil.copytree(clean, case)
                target = case / rel
                target.write_text(
                    target.read_text(encoding="utf-8")
                    + self._synthetic_credential_assignment(),
                    encoding="utf-8",
                )
                exit_code, findings = self._scan(case)
                self.assertNotEqual(exit_code, 0)
                self.assertTrue(any(f.code == "CREDENTIAL_PATTERN" for f in findings))


class PublicationSafetyFixtures(unittest.TestCase):
    """Fixtures used to exercise the scanner's detectors in fixture mode.

    These names are recognised by ``check_publication_safety`` and the
    scanner is allowed to see their credential-like content because they
    exist solely to exercise the detector.
    """

    FIXTURE_BASENAMES = {
        "phase6_searxng_token_fixture.json",
        "phase6_feed_token_fixture.xml",
        "phase6_telegram_token_fixture.toml",
    }


class PublicationSafetySyntax(unittest.TestCase):
    """The scanner must be syntactically valid Python and import-clean."""

    def test_checker_is_syntactically_valid(self) -> None:
        source = _read(CHECKER)
        ast.parse(source, filename=str(CHECKER))

    def test_checker_py_compiles(self) -> None:
        import py_compile

        py_compile.compile(str(CHECKER), doraise=True)

    def test_checker_imports_and_scans(self) -> None:
        # Importing the checker must not require any third-party
        # dependencies.
        cps = _load_checker()

        # ``scan`` must be callable.
        self.assertTrue(callable(cps.scan))


class PublicationSafetyHelp(unittest.TestCase):
    def test_checker_help_runs(self) -> None:
        # ``--help`` must exit 0 and mention the scanner role.
        result = subprocess.run(
            [sys.executable, str(CHECKER), "--help"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0)
        self.assertIn("publication-safety", result.stdout.lower())


class PublicationSafetyUnitTests(unittest.TestCase):
    """The unittest discovery command must surface this file."""

    def test_discoverable_via_unittest(self) -> None:
        loader = unittest.TestLoader()
        suite = loader.loadTestsFromName(
            "tests.news_pipeline.test_publication_safety",
        )
        # At least one test should be discovered.
        self.assertGreater(suite.countTestCases(), 0)


class PublicationSafetyGitHubMetadata(unittest.TestCase):
    """GitHub metadata is admitted by exact path, not a directory glob."""

    def test_github_metadata_uses_a_closed_exact_allowlist(self) -> None:
        cps = _load_checker()
        expected = {
            "CHANGELOG.md",
            "LICENSE",
            ".github/CODEOWNERS",
            ".github/dependabot.yml",
            ".github/PULL_REQUEST_TEMPLATE.md",
            ".github/ISSUE_TEMPLATE/config.yml",
            ".github/ISSUE_TEMPLATE/bug_report.yml",
            ".github/ISSUE_TEMPLATE/feature_request.yml",
            ".github/ISSUE_TEMPLATE/security.md",
            ".github/workflows/ci.yml",
            ".github/workflows/security.yml",
            ".github/workflows/release.yml",
        }
        self.assertTrue(expected <= cps.PUBLIC_ALLOWLIST)
        self.assertNotIn(".github/workflows/other.yml", cps.PUBLIC_ALLOWLIST)

        d = Path(_make_tmpdir("publication-github-allowlist-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        for rel in expected:
            entry = d / rel
            entry.parent.mkdir(parents=True, exist_ok=True)
            entry.write_text("# synthetic publication metadata\n", encoding="utf-8")
            self.assertEqual(
                cps._classify_path(Path(rel), source_entry=entry),
                (True, ""),
                rel,
            )
        unknown = d / ".github" / "workflows" / "other.yml"
        unknown.write_text("# not allowlisted\n", encoding="utf-8")
        self.assertEqual(
            cps._classify_path(
                Path(".github/workflows/other.yml"), source_entry=unknown
            ),
            (False, "not in export allowlist"),
        )

    def test_export_includes_metadata_and_excludes_private_candidate_material(self) -> None:
        d = Path(_make_tmpdir("publication-github-export-"))
        self.addCleanup(shutil.rmtree, d, ignore_errors=True)
        destination = d / "public"
        proc = subprocess.run(
            [
                sys.executable,
                str(SCRIPTS_DIR / "export_public_tree.py"),
                "--source",
                str(ROOT),
                "--destination",
                str(destination),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        for rel in (
            "LICENSE",
            ".github/CODEOWNERS",
            "CHANGELOG.md",
            ".github/ISSUE_TEMPLATE/security.md",
            ".github/dependabot.yml",
            ".github/workflows/ci.yml",
            ".github/workflows/security.yml",
            ".github/workflows/release.yml",
        ):
            self.assertTrue((destination / rel).is_file(), rel)
        for rel in (
            ".release_manifest",
            "private-evidence",
            "config/news-policy.toml",
        ):
            self.assertFalse((destination / rel).exists(), rel)


if __name__ == "__main__":
    unittest.main()
