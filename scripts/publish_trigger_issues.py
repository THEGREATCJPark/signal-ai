#!/usr/bin/env python3
"""Publish selected x-trigger GitHub issues through the trigger publisher."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.x_trigger_scan import extract_candidate_from_issue_body  # noqa: E402
from scripts.x_trigger_review import publish_trigger_candidate  # noqa: E402


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _issue_numbers(raw: str) -> list[int]:
    out: list[int] = []
    for item in raw.replace(",", "\n").splitlines():
        item = item.strip().lstrip("#")
        if item:
            out.append(int(item))
    return out


def _read_issue_numbers(args: argparse.Namespace) -> list[int]:
    numbers: list[int] = []
    if args.issues:
        numbers.extend(_issue_numbers(args.issues))
    if args.issues_file:
        numbers.extend(_issue_numbers(Path(args.issues_file).read_text(encoding="utf-8")))
    seen = set()
    deduped = []
    for number in numbers:
        if number not in seen:
            deduped.append(number)
            seen.add(number)
    return deduped


def fetch_issue_body(repo: str, token: str, issue_number: int) -> str:
    response = requests.get(
        f"https://api.github.com/repos/{repo}/issues/{issue_number}",
        headers=_github_headers(token),
        timeout=30,
    )
    response.raise_for_status()
    return str((response.json() or {}).get("body") or "")


def main(argv: list[str] | None = None) -> int:
    load_dotenv(ROOT / ".env", override=False)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issues", help="Comma/newline separated issue numbers")
    parser.add_argument("--issues-file", type=Path, help="File containing issue numbers")
    parser.add_argument("--platform", choices=["telegram", "x", "both"], default="telegram")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    repo = os.getenv("GITHUB_REPOSITORY")
    token = os.getenv("GITHUB_TOKEN")
    if not repo or not token:
        raise RuntimeError("GITHUB_REPOSITORY and GITHUB_TOKEN are required")

    numbers = _read_issue_numbers(args)
    if not numbers:
        raise RuntimeError("No issue numbers provided")

    failures: list[str] = []
    for number in numbers:
        try:
            body = fetch_issue_body(repo, token, number)
            candidate = extract_candidate_from_issue_body(body)
            published = publish_trigger_candidate(candidate, platform=args.platform, dry_run=args.dry_run)
            print(f"[trigger-issue] #{number}: published={published or 'dry-run/no new target'}")
        except Exception as exc:
            failures.append(f"#{number}: {exc}")
            print(f"[trigger-issue] #{number}: failed: {exc}", file=sys.stderr)

    if failures:
        raise RuntimeError("; ".join(failures))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
