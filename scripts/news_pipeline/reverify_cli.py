"""Operator-invoked, one-source-item claim re-verification CLI."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import NoReturn

from .event_store import regenerate_release_claim, reverify_source_item_claims


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        self.exit(
            2,
            json.dumps(
                {"error": {"type": "arguments", "message": message}},
                separators=(",", ":"),
            )
            + "\n",
        )


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="news-reverify-claims")
    parser.add_argument("--db", required=True)
    parser.add_argument("--source-item-id", required=True)
    parser.add_argument(
        "--regenerate-release-claim",
        action="store_true",
        help="regenerate the exact grounded release-tag claim for a first-party llama.cpp release item",
    )
    args = parser.parse_args(argv)
    evaluated_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )
    if args.regenerate_release_claim:
        try:
            regenerated = regenerate_release_claim(
                args.db, args.source_item_id, evaluated_at
            )
        except Exception as exc:
            print(
                json.dumps(
                    {"error": {"type": "runtime", "message": str(exc)}},
                    separators=(",", ":"),
                ),
                file=sys.stderr,
            )
            return 1
        print(
            json.dumps(
                {
                    "source_item_id": regenerated.source_item_id,
                    "evaluated_at": evaluated_at,
                    "claims_inserted": regenerated.claims_inserted,
                    "evidence_inserted": regenerated.evidence_inserted,
                    "claims_verified": regenerated.claims_verified,
                    "events_created": regenerated.events_created,
                    "versions_appended": regenerated.versions_appended,
                    "event_links_inserted": regenerated.event_links_inserted,
                },
                separators=(",", ":"),
            )
        )
        return 0
    try:
        report = reverify_source_item_claims(
            args.db, args.source_item_id, evaluated_at
        )
    except Exception as exc:
        print(
            json.dumps(
                {"error": {"type": "runtime", "message": str(exc)}},
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    print(
        json.dumps(
            {
                "source_item_id": report.source_item_id,
                "evaluated_at": evaluated_at,
                "claims_selected": report.claims_selected,
                "claims_verified": report.claims_verified,
                "versions_appended": report.versions_appended,
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
