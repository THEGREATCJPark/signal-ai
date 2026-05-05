#!/usr/bin/env python3
"""Handle GitHub issue comments for X trigger approvals."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.x_trigger_scan import ensure_github_labels, extract_candidate_from_issue_body

load_dotenv()

APPROVE_COMMANDS = {"/approve-trigger", "/approve", "approve", "yes", "y", "예", "ㅇ", "승인"}
REJECT_COMMANDS = {"/reject-trigger", "/reject", "reject", "no", "n", "아니오", "아니요", "ㄴ", "거절"}
DEFAULT_ALLOWED_ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR"}


def parse_review_command(comment_body: str) -> str | None:
    for raw_line in comment_body.splitlines():
        line = raw_line.strip().lower()
        if not line:
            continue
        first = line.split()[0]
        if first in APPROVE_COMMANDS:
            return "approve"
        if first in REJECT_COMMANDS:
            return "reject"
    return None


def reviewer_is_allowed(
    login: str,
    author_association: str,
    allowlist: list[str] | None = None,
) -> bool:
    allowed = {item.strip().lower() for item in (allowlist or []) if item.strip()}
    if allowed:
        return login.strip().lower() in allowed
    return author_association.strip().upper() in DEFAULT_ALLOWED_ASSOCIATIONS


def build_trigger_article(candidate: dict[str, Any], now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(timezone.utc)
    summary = candidate.get("summary") or {}
    tweet = candidate.get("tweet") or {}
    account = candidate.get("account") or {}
    cid = str(candidate.get("id") or f"x-{tweet.get('id', '')}").strip()
    article_id = cid if cid.startswith("trigger-") else f"trigger-{cid}"
    return {
        "id": article_id,
        "source": "x_trigger",
        "title": str(summary.get("title") or f"@{account.get('username', 'x')} 새 게시글").strip(),
        "headline": str(summary.get("title") or "").strip(),
        "url": tweet.get("url") or "",
        "score": int((tweet.get("public_metrics") or {}).get("like_count") or 0),
        "comments": int((tweet.get("public_metrics") or {}).get("reply_count") or 0),
        "timestamp": tweet.get("created_at") or now.isoformat(),
        "created_at": tweet.get("created_at") or now.isoformat(),
        "summary": str(summary.get("body") or "").strip(),
        "body": str(summary.get("body") or "").strip(),
        "category": account.get("category") or "x_trigger",
        "trust": "high" if account.get("category") == "official" else "reviewed",
        "tags": ["x_trigger", str(account.get("category") or "watch"), str(account.get("username") or "")],
        "raw_json": candidate,
    }


def _platforms(platform: str) -> list[str]:
    return ["telegram", "x"] if platform == "both" else [platform]


def publish_trigger_candidate(candidate: dict[str, Any], *, platform: str = "both", dry_run: bool = False) -> list[str]:
    article = build_trigger_article(candidate)
    if dry_run:
        print(json.dumps(article, ensure_ascii=False, indent=2))
        return []

    from bot.telegram_bot import send_article
    from bot.x_poster import post_article
    from db.articles import upsert_generated_articles
    from publisher.state import article_key, get_state

    state = get_state()
    article_id = article_key(article)
    upsert_generated_articles([article])
    published: list[str] = []

    for target in _platforms(platform):
        if state.is_published(article_id, target):
            print(f"[trigger] already published to {target}: {article_id}")
            continue
        if target == "telegram":
            send_article(article)
        elif target == "x":
            post_article(article)
        else:
            raise ValueError(f"unsupported platform: {target}")
        state.mark_published(article_id, target)
        published.append(target)

    state.save()
    return published


class GitHubIssueClient:
    def __init__(self, token: str | None = None, repo: str | None = None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        self.repo = repo or os.getenv("GITHUB_REPOSITORY")
        if not self.token or not self.repo:
            raise RuntimeError("GITHUB_TOKEN and GITHUB_REPOSITORY are required")
        self.base = f"https://api.github.com/repos/{self.repo}"
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def add_comment(self, issue_number: int, body: str) -> None:
        response = requests.post(
            f"{self.base}/issues/{issue_number}/comments",
            headers=self.headers,
            json={"body": body},
            timeout=30,
        )
        response.raise_for_status()

    def add_labels(self, issue_number: int, labels: list[str]) -> None:
        ensure_github_labels(labels, token=self.token, repo=self.repo)
        response = requests.post(
            f"{self.base}/issues/{issue_number}/labels",
            headers=self.headers,
            json={"labels": labels},
            timeout=30,
        )
        response.raise_for_status()

    def close_issue(self, issue_number: int, reason: str = "completed") -> None:
        response = requests.patch(
            f"{self.base}/issues/{issue_number}",
            headers=self.headers,
            json={"state": "closed", "state_reason": reason},
            timeout=30,
        )
        response.raise_for_status()


def _labels(issue: dict[str, Any]) -> set[str]:
    return {str(label.get("name") or "") for label in issue.get("labels") or []}


def handle_event(event: dict[str, Any], *, platform: str = "both", dry_run: bool = False) -> int:
    action = event.get("action")
    if action != "created":
        print(f"[trigger] ignored issue_comment action={action}")
        return 0

    issue = event.get("issue") or {}
    comment = event.get("comment") or {}
    labels = _labels(issue)
    if "x-trigger" not in labels:
        print("[trigger] ignored comment on non-trigger issue")
        return 0
    if "trigger-approved" in labels or "trigger-rejected" in labels:
        print("[trigger] ignored already reviewed issue")
        return 0

    command = parse_review_command(str(comment.get("body") or ""))
    if not command:
        print("[trigger] no review command in comment")
        return 0

    allowlist = [x.strip() for x in os.getenv("TRIGGER_REVIEWERS", "").split(",") if x.strip()]
    login = str((comment.get("user") or {}).get("login") or "")
    association = str(comment.get("author_association") or "")
    issue_number = int(issue["number"])
    client = GitHubIssueClient()

    if not reviewer_is_allowed(login, association, allowlist):
        client.add_comment(issue_number, f"@{login} is not authorized to review trigger posts.")
        print(f"[trigger] unauthorized reviewer: {login} ({association})")
        return 1

    if command == "reject":
        client.add_labels(issue_number, ["trigger-rejected"])
        client.add_comment(issue_number, f"Rejected by @{login}.")
        client.close_issue(issue_number, reason="not_planned")
        print(f"[trigger] rejected by {login}")
        return 0

    candidate = extract_candidate_from_issue_body(str(issue.get("body") or ""))
    published = publish_trigger_candidate(candidate, platform=platform, dry_run=dry_run)
    client.add_labels(issue_number, ["trigger-approved"])
    client.add_comment(
        issue_number,
        f"Approved by @{login}. Published to: {', '.join(published) if published else 'dry-run/no new target'}.",
    )
    client.close_issue(issue_number, reason="completed")
    print(f"[trigger] approved by {login}; published={published}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Handle X trigger GitHub issue review comments")
    parser.add_argument("--event-path", default=os.getenv("GITHUB_EVENT_PATH"), help="GitHub event JSON path")
    parser.add_argument("--platform", choices=["telegram", "x", "both"], default=os.getenv("TRIGGER_PUBLISH_PLATFORM", "both"))
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not args.event_path:
        raise SystemExit("GITHUB_EVENT_PATH or --event-path is required")
    event = json.loads(Path(args.event_path).read_text(encoding="utf-8"))
    return handle_event(event, platform=args.platform, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
