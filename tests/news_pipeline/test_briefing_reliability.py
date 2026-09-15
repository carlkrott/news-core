"""Phase 4 Slice 5 reliability harness tests 68-84."""
from __future__ import annotations

import unittest

from news_pipeline import briefing_reliability as reliability


class TestReliabilityHarness(unittest.TestCase):
    def _shell(self, path: str, text: str) -> reliability.SourceText:
        return reliability.SourceText(path, reliability.SourceKind.SHELL, text)

    def _python(self, path: str, text: str) -> reliability.SourceText:
        return reliability.SourceText(path, reliability.SourceKind.PYTHON, text)

    def _codes(self, result):
        return tuple(item.finding_code for item in result.findings)

    def _clean_shell(self, extra: str = ""):
        return self._shell(
            "/audit/wrapper.sh",
            """#!/bin/bash
FANTASY_QUERY='fantasy'
AVIND_QUERY='av'
OURSETUP_QUERY='our setup'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
""" + extra,
        )

    def test_detects_final_false_conditional_exit(self) -> None:
        source = self._clean_shell('[[ "$1" == fallback ]] && process_category hardware\n')
        result = reliability.analyze_reliability((source,), ())
        self.assertIn(reliability.FindingCode.FINAL_FALSE_CONDITIONAL_EXIT, self._codes(result))

    def test_detects_duplicate_fantasy_and_avind_assignments_and_calls(self) -> None:
        source = self._shell(
            "/audit/duplicates.sh",
            """FANTASY_QUERY='one'
FANTASY_QUERY='two'
AVIND_QUERY='one'
AVIND_QUERY='two'
OURSETUP_QUERY='one'
process_category fantasy-novel
process_category fantasy-novel
process_category audiovisual
process_category audiovisual
process_category our-setup
""",
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertEqual(
            self._codes(result),
            (reliability.FindingCode.DUPLICATE_CATEGORY_ASSIGNMENT_OR_CALL,),
        )

    def test_detects_our_setup_assignment_and_call_omission(self) -> None:
        source = self._shell(
            "/audit/missing-oursetup.sh",
            """FANTASY_QUERY='one'
AVIND_QUERY='one'
process_category fantasy-novel
process_category audiovisual
""",
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertIn(reliability.FindingCode.OUR_SETUP_OMITTED, self._codes(result))

    def test_detects_5000_8000_truncation_mismatch(self) -> None:
        source = self._python(
            "/audit/summarize.py",
            """def embedded_summary(text):
    return text[:5000]

def standalone_summary(text):
    return text[:8000]
""",
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertIn(reliability.FindingCode.TRUNCATION_LIMIT_MISMATCH, self._codes(result))

    def test_detects_suppressed_or_unchecked_curl(self) -> None:
        """Phase 4 follow-up: in-place strengthening.

        Production code is unchanged. The single source contains TWO
        offending curls on consecutive lines:
          line 9  ``curl -s https://example.test/data > /dev/null``
          line 10 ``curl https://example.test/unchecked``
        The scanner must report EXACTLY ONE CURL_RESULT_SUPPRESSED
        finding (per-source deduplication) anchored to the first
        offender (line 9). No new numbered test method is added.
        """
        source = self._clean_shell(
            "curl -s https://example.test/data > /dev/null\n"
            "curl https://example.test/unchecked\n"
        )
        result = reliability.analyze_reliability((source,), ())
        codes = self._codes(result)
        # Presence — preserves original assertion semantics.
        self.assertIn(reliability.FindingCode.CURL_RESULT_SUPPRESSED, codes)
        # Exactly one such finding: per-source deduplication on a
        # single source with two offenders.
        curl_findings = tuple(
            item for item in result.findings
            if item.finding_code is reliability.FindingCode.CURL_RESULT_SUPPRESSED
        )
        self.assertEqual(len(curl_findings), 1)
        # Anchored to the first offender. The shell source has 7 prefix
        # lines (see ``_clean_shell``); the first offending curl is
        # therefore on line 9 (the scanner numbers lines verbatim,
        # even for ``#!/bin/bash`` shebang).
        first_offender_line = 8
        self.assertEqual(curl_findings[0].virtual_path, source.virtual_path)
        self.assertEqual(curl_findings[0].line_number, first_offender_line)

    def test_detects_exception_text_path_to_telegram_payload(self) -> None:
        source = self._shell(
            "/audit/exception.sh",
            """FANTASY_QUERY='fantasy'
AVIND_QUERY='av'
OURSETUP_QUERY='our setup'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
payload=$(python3 - <<'PY'
def summarize():
    try:
        raise ValueError('model failed')
    except ValueError as exc:
        return f'failure: {exc}'
print(summarize())
PY
)
send_telegram "$payload"
""",
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertIn(reliability.FindingCode.EXCEPTION_TEXT_CAN_REACH_PAYLOAD, self._codes(result))

    def test_detects_hardcoded_credential_without_retaining_secret(self) -> None:
        secret = "super-secret-token-987"
        source = self._shell(
            "/audit/credentials.sh",
            """FANTASY_QUERY='fantasy'
AVIND_QUERY='av'
OURSETUP_QUERY='our setup'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
BOT_TOKEN='""" + secret + "'\n",
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertIn(reliability.FindingCode.HARDCODED_CREDENTIAL_ASSIGNMENT, self._codes(result))
        self.assertNotIn(secret, repr(result))
        credential = next(item for item in result.findings if item.finding_code is reliability.FindingCode.HARDCODED_CREDENTIAL_ASSIGNMENT)
        self.assertEqual(credential.excerpt, "BOT_TOKEN=<redacted>")

    def test_detects_wrapper_failure_misattributed_to_model(self) -> None:
        source = self._clean_shell("curl -s https://example.test/data > /dev/null\n")
        health = reliability.HealthRecord(
            run_id="run-1",
            wrapper_path="/audit/wrapper.sh",
            exit_code=7,
            stderr_code="curl_failed",
            model_error_code=None,
            summary_payload_sent=False,
        )
        result = reliability.analyze_reliability((source,), (health,))
        self.assertIn(reliability.FindingCode.WRAPPER_FAILURE_MISATTRIBUTED_TO_MODEL, self._codes(result))

    def test_ignores_comments_quoted_examples_and_uninvoked_text(self) -> None:
        source = self._clean_shell(
            "# FANTASY_QUERY='duplicate secret'\n"
            "echo 'process_category fantasy-novel'\n"
            "echo \"&& process_category audiovisual \"\n"
            "echo \"BOT_TOKEN=quoted-secret\"\n"
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertEqual(result.findings, ())

    def test_ignores_single_categories_and_present_our_setup(self) -> None:
        result = reliability.analyze_reliability((self._clean_shell(),), ())
        self.assertEqual(result.findings, ())

    def test_ignores_equal_truncation_caps(self) -> None:
        source = self._python(
            "/audit/equal.py",
            """def embedded_summary(text):
    return text[:5000]

def standalone_summary(text):
    return text[:5000]
""",
        )
        self.assertEqual(reliability.analyze_reliability((source,), ()).findings, ())

    def test_ignores_checked_curl(self) -> None:
        source = self._clean_shell(
            "curl --fail-with-body -sS -w '%{http_code}' https://example.test/data -o /tmp/data\n"
        )
        self.assertNotIn(
            reliability.FindingCode.CURL_RESULT_SUPPRESSED,
            self._codes(reliability.analyze_reliability((source,), ())),
        )

    def test_ignores_stderr_only_exception(self) -> None:
        source = self._shell(
            "/audit/stderr-only.sh",
            """FANTASY_QUERY='fantasy'
AVIND_QUERY='av'
OURSETUP_QUERY='our setup'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
payload=$(python3 - <<'PY'
import sys
def summarize():
    try:
        raise ValueError('model failed')
    except ValueError as exc:
        print(f'failure: {exc}', file=sys.stderr)
        return 'fallback'
print(summarize())
PY
)
send_telegram "$payload"
""",
        )
        self.assertNotIn(
            reliability.FindingCode.EXCEPTION_TEXT_CAN_REACH_PAYLOAD,
            self._codes(reliability.analyze_reliability((source,), ())),
        )

    def test_ignores_env_or_file_credentials(self) -> None:
        source = self._shell(
            "/audit/env-creds.sh",
            """FANTASY_QUERY='fantasy'
AVIND_QUERY='av'
OURSETUP_QUERY='our setup'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
BOT_TOKEN="${BOT_TOKEN}"
API_SECRET="$(cat /run/secrets/api-token)"
PASSWORD=$(< /run/secrets/password)
""",
        )
        self.assertNotIn(
            reliability.FindingCode.HARDCODED_CREDENTIAL_ASSIGNMENT,
            self._codes(reliability.analyze_reliability((source,), ())),
        )

    def test_ignores_genuine_model_error_health_record(self) -> None:
        source = self._clean_shell("curl -s https://example.test/data > /dev/null\n")
        health = reliability.HealthRecord(
            run_id="run-model-error",
            wrapper_path="/audit/wrapper.sh",
            exit_code=1,
            stderr_code="wrapper_failed",
            model_error_code="transport_error",
            summary_payload_sent=False,
        )
        result = reliability.analyze_reliability((source,), (health,))
        self.assertNotIn(reliability.FindingCode.WRAPPER_FAILURE_MISATTRIBUTED_TO_MODEL, self._codes(result))

    def test_findings_are_deterministically_sorted(self) -> None:
        sources = (
            self._shell(
                "/audit/z.sh",
                """FANTASY_QUERY='x'
AVIND_QUERY='x'
OURSETUP_QUERY='x'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
BOT_TOKEN='hidden'
""",
            ),
            self._clean_shell('[[ "$1" == fallback ]] && process_category hardware\n'),
        )
        result = reliability.analyze_reliability(sources, ())
        observed = [(item.finding_code, item.virtual_path, item.line_number) for item in result.findings]
        self.assertEqual(observed, sorted(observed, key=lambda row: (list(reliability.FindingCode).index(row[0]), row[1], row[2])))

    def test_no_evidence_contains_fixture_secret(self) -> None:
        secret = "do-not-leak-abcdef"
        source = self._shell(
            "/audit/secret.sh",
            """FANTASY_QUERY='fantasy'
AVIND_QUERY='av'
OURSETUP_QUERY='our setup'
process_category fantasy-novel
process_category audiovisual
process_category our-setup
PASSWORD='""" + secret + "'\n",
        )
        result = reliability.analyze_reliability((source,), ())
        self.assertNotIn(secret, repr(result))
        for evidence in result.findings:
            self.assertLessEqual(len(evidence.excerpt), 120)
            self.assertNotIn(secret, evidence.excerpt)


if __name__ == "__main__":
    unittest.main()
