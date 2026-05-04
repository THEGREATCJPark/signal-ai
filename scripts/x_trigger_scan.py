#!/usr/bin/env python3
"""Detect watched X account posts and open GitHub review issues."""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import xml.etree.ElementTree as ET
from copy import deepcopy
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote

import requests
from dotenv import load_dotenv

load_dotenv()

ROOT = Path(__file__).resolve().parents[1]
STATE_KEY = "x_trigger_state"
LOCAL_STATE_PATH = ROOT / "data" / "x_trigger_state.json"
ISSUE_LABELS = ["x-trigger", "needs-review"]
ISSUE_LABEL_COLORS = {
    "x-trigger": "5319e7",
    "needs-review": "fbca04",
}
ISSUE_PAYLOAD_PREFIX = "x-trigger-payload:"
DEFAULT_FREE_FEED_BASE_URLS = [
    "https://rsshub.app",
    "https://rss.detools.dev",
    "https://rsshub.rssforever.com",
    "https://rsshub.pseudoyu.com",
]
GEMINI_MODEL = os.getenv("TRIGGER_SUMMARY_MODEL", "gemma-4-26b-a4b-it")
GEMINI_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"

DEFAULT_ACCOUNTS = [
    {"username": "OpenAI", "category": "official", "group": "공식 발표"},
    {"username": "OpenAIDevs", "category": "official", "group": "공식 발표"},
    {"username": "AnthropicAI", "category": "official", "group": "공식 발표"},
    {"username": "claudeai", "category": "official", "group": "공식 발표"},
    {"username": "ClaudeDevs", "category": "official", "group": "공식 발표"},
    {"username": "GoogleDeepMind", "category": "official", "group": "공식 발표"},
    {"username": "GoogleAI", "category": "official", "group": "공식 발표"},
    {"username": "xai", "category": "official", "group": "공식 발표"},
    {"username": "testingcatalog", "category": "rumor", "group": "선행 루머/탐지"},
    {"username": "btibor91", "category": "rumor", "group": "선행 루머/탐지"},
    {"username": "arena", "category": "rumor", "group": "선행 루머/탐지"},
    {"username": "ArtificialAnlys", "category": "rumor", "group": "선행 루머/탐지"},
    {"username": "sama", "category": "person", "group": "핵심 인물"},
    {"username": "karpathy", "category": "person", "group": "핵심 인물"},
    {"username": "demishassabis", "category": "person", "group": "핵심 인물"},
    {"username": "gdb", "category": "person", "group": "핵심 인물"},
    {"username": "polynoamial", "category": "person", "group": "핵심 인물"},
    {"username": "OfficialLoganK", "category": "person", "group": "핵심 인물"},
    {"username": "steph_palazzolo", "category": "scoop", "group": "회사 내부/스쿱"},
    {"username": "alexeheath", "category": "scoop", "group": "회사 내부/스쿱"},
    {"username": "aaronpholmes", "category": "scoop", "group": "회사 내부/스쿱"},
]


def account_key(username: str) -> str:
    return username.strip().lstrip("@").lower()


def normalize_account(account: dict[str, Any] | str) -> dict[str, str]:
    if isinstance(account, str):
        username = account
        category = "watch"
        group = "관심 계정"
        tier = "manual"
    else:
        username = str(account.get("username") or account.get("handle") or "")
        category = str(account.get("category") or "watch")
        group = str(account.get("group") or "관심 계정")
        tier = str(account.get("tier") or "manual")
    username = username.strip().lstrip("@")
    if not username:
        raise ValueError("watched account is missing username")
    return {"username": username, "category": category, "group": group, "tier": tier, "key": account_key(username)}


def load_accounts(path: str | os.PathLike | None = None) -> list[dict[str, str]]:
    if path is None and (ROOT / "config" / "x_trigger_accounts.json").exists():
        path = ROOT / "config" / "x_trigger_accounts.json"
    raw = json.loads(Path(path).read_text(encoding="utf-8")) if path else DEFAULT_ACCOUNTS
    return [normalize_account(account) for account in raw]


SCOPE_TIERS = {
    "auto": {"auto"},
    "core": {"auto", "core"},
    "fast": {"auto", "core", "fast"},
    "scoop": {"auto", "core", "scoop"},
    "oss": {"auto", "core", "oss"},
    "all": {"auto", "core", "fast", "scoop", "oss", "manual"},
}


def filter_accounts_for_scope(accounts: list[dict[str, str]], scope: str) -> list[dict[str, str]]:
    allowed = SCOPE_TIERS.get(scope)
    if allowed is None:
        raise ValueError(f"unsupported trigger account scope: {scope}")
    return [account for account in accounts if account.get("tier", "manual") in allowed]


def _tweet_id_int(tweet_id: str) -> int:
    try:
        return int(tweet_id)
    except (TypeError, ValueError):
        return 0


def _copy_state(state: dict[str, Any] | None) -> dict[str, Any]:
    out = deepcopy(state or {})
    out.setdefault("last_seen_ids", {})
    out.setdefault("updated_at", None)
    return out


def detect_new_tweets(
    tweets_by_account: dict[str, list[dict[str, Any]]],
    previous_state: dict[str, Any] | None,
    *,
    bootstrap: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Return unseen tweets and the next state.

    When an account has no previous cursor, the default behavior records the
    newest tweet as a baseline without creating a candidate. Pass bootstrap=True
    for manual backfills.
    """
    state = _copy_state(previous_state)
    last_seen = state["last_seen_ids"]
    candidates: list[dict[str, Any]] = []

    for username, tweets in tweets_by_account.items():
        key = account_key(username)
        ordered = sorted(tweets, key=lambda t: _tweet_id_int(str(t.get("id") or "")))
        newest = ordered[-1]["id"] if ordered else None
        previous = str(last_seen.get(key) or "")

        if not previous and newest and not bootstrap:
            last_seen[key] = str(newest)
            continue

        for tweet in ordered:
            tid = str(tweet.get("id") or "")
            if not tid:
                continue
            if previous and _tweet_id_int(tid) <= _tweet_id_int(previous):
                continue
            candidates.append({"username": username, "tweet": tweet})

        if newest:
            last_seen[key] = str(newest)

    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    return candidates, state


def configured_free_feed_base_urls() -> list[str]:
    raw = os.getenv("X_TRIGGER_FEED_BASE_URLS") or os.getenv("RSSHUB_BASE_URLS")
    if raw:
        return [item.strip().rstrip("/") for item in raw.split(",") if item.strip()]
    return DEFAULT_FREE_FEED_BASE_URLS


def build_free_feed_url(base_url: str, username: str, max_results: int = 1) -> str:
    base = base_url.rstrip("/")
    handle = quote(username.strip().lstrip("@"), safe="")
    limit = max(1, int(max_results or 1))
    return f"{base}/twitter/user/{handle}/excludeReplies=1&includeRts=0?limit={limit}"


def _first_text(element: ET.Element, names: list[str]) -> str:
    for name in names:
        found = element.find(name)
        if found is not None and found.text:
            return found.text.strip()
    return ""


def _clean_feed_text(text: str) -> str:
    text = unescape(text or "")
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _status_id_from_url(url: str) -> str:
    match = re.search(r"/status(?:es)?/(\d+)", url or "")
    return match.group(1) if match else ""


def parse_feed_tweets(feed_xml: str, username: str) -> list[dict[str, Any]]:
    root = ET.fromstring(feed_xml)
    items = root.findall(".//item")
    if not items:
        items = root.findall(".//{http://www.w3.org/2005/Atom}entry")
    tweets: list[dict[str, Any]] = []
    for item in items:
        link = _first_text(item, ["link"])
        if not link:
            atom_link = item.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = str(atom_link.attrib.get("href") or "")
        guid = _first_text(item, ["guid", "id"])
        tweet_id = _status_id_from_url(link) or _status_id_from_url(guid)
        if not tweet_id:
            continue
        description = _first_text(item, ["description", "summary", "content"])
        title = _first_text(item, ["title"])
        text = _clean_feed_text(description) or _clean_feed_text(title)
        tweets.append({
            "id": tweet_id,
            "text": text,
            "url": link or f"https://x.com/{username}/status/{tweet_id}",
            "created_at": _first_text(item, ["pubDate", "published", "updated"]),
            "public_metrics": {},
            "free_source": "rsshub",
        })
    return tweets


def _safe_json_from_text(text: str) -> dict[str, Any]:
    s = text.strip()
    s = re.sub(r"^```(?:json)?\s*|\s*```$", "", s, flags=re.M).strip()
    start = s.find("{")
    end = s.rfind("}")
    if start != -1 and end > start:
        return json.loads(s[start : end + 1])
    return {}


def fallback_summary(tweet: dict[str, Any], account: dict[str, Any]) -> dict[str, str]:
    text = " ".join(str(tweet.get("text") or "").split())
    title = text[:54].rstrip()
    if len(text) > 54:
        title += "..."
    if not title:
        title = f"@{account['username']} 새 게시글"
    body = text or "원문 텍스트가 비어 있습니다."
    if len(body) > 500:
        body = body[:497].rstrip() + "..."
    return {
        "title": title,
        "body": body,
        "confidence": "fallback",
    }


def build_summary_prompt(tweet: dict[str, Any], account: dict[str, Any]) -> str:
    metrics = tweet.get("public_metrics") or {}
    return f"""역할: First Light AI 트리거 편집자

관심 X 계정에서 새 게시글이 올라왔다. 이 게시글을 검수자가 빠르게 판단할 수 있게 한국어로 요약하라.

규칙:
- 원문에 없는 사실을 만들지 말 것.
- 공식 계정이면 발표/제품/개발자 영향 위주로 정리.
- 루머/탐지 계정이면 불확실성을 분명히 표시.
- 제목은 18~42자.
- 본문은 2~4문장.
- JSON 객체 하나만 출력.

스키마:
{{"title":"검수 카드 제목","body":"검수용 요약","confidence":"official|rumor|inference"}}

계정: @{account['username']}
그룹: {account.get('group', '')}
분류: {account.get('category', '')}
게시 시각: {tweet.get('created_at', '')}
지표: {json.dumps(metrics, ensure_ascii=False)}
원문:
{tweet.get('text', '')}
"""


def summarize_tweet(
    tweet: dict[str, Any],
    account: dict[str, Any],
    *,
    ai_call: Callable[[str, bool], str] | None = None,
) -> dict[str, str]:
    if ai_call is None:
        ai_call = call_google_model
    prompt = build_summary_prompt(tweet, account)
    try:
        raw = ai_call(prompt, json_mode=True)
        parsed = _safe_json_from_text(raw)
        title = str(parsed.get("title") or "").strip()
        body = str(parsed.get("body") or parsed.get("summary") or "").strip()
        if title and len(body) >= 8:
            return {
                "title": title,
                "body": body,
                "confidence": str(parsed.get("confidence") or account.get("category") or "unknown"),
            }
    except Exception as exc:
        print(f"[trigger] summary fallback for @{account['username']}: {exc}", file=sys.stderr)
    return fallback_summary(tweet, account)


def _load_google_keys() -> list[str]:
    for env_name in ("GOOGLE_API_KEYS", "GEMINI_API_KEYS"):
        raw = os.getenv(env_name)
        if raw:
            return [key.strip() for key in raw.split(",") if key.strip()]
    for env_name in ("GOOGLE_API_KEY", "GEMINI_API_KEY"):
        raw = os.getenv(env_name)
        if raw:
            return [raw.strip()]
    key_file = Path.home() / ".config" / "legal_evidence_rag" / "keys.env"
    if key_file.exists():
        for line in key_file.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                return [key.strip() for key in line.split("=", 1)[1].split(",") if key.strip()]
    return []


def call_google_model(prompt: str, json_mode: bool = False) -> str:
    keys = _load_google_keys()
    if not keys:
        raise RuntimeError("GOOGLE_API_KEY/GOOGLE_API_KEYS is not configured")
    generation_config: dict[str, Any] = {"temperature": 0.25, "maxOutputTokens": 2048}
    if json_mode:
        generation_config["responseMimeType"] = "application/json"
    if "gemma" in GEMINI_MODEL.lower():
        generation_config["thinkingConfig"] = {"thinkingLevel": "minimal"}
    endpoint = GEMINI_ENDPOINT.format(model=GEMINI_MODEL)
    last_error = None
    for key in keys:
        response = requests.post(
            f"{endpoint}?key={key}",
            json={"contents": [{"parts": [{"text": prompt}]}], "generationConfig": generation_config},
            timeout=120,
        )
        if not response.ok:
            last_error = RuntimeError(f"model endpoint {response.status_code}: {response.text[:200]}")
            continue
        parts = response.json()["candidates"][0]["content"]["parts"]
        text = "".join(part.get("text", "") for part in parts if not part.get("thought"))
        if text.strip():
            return text
    raise last_error or RuntimeError("model endpoint returned no text")


class FreeXFeedClient:
    def __init__(self, base_urls: list[str] | None = None):
        self.base_urls = [url.rstrip("/") for url in (base_urls or configured_free_feed_base_urls())]
        if not self.base_urls:
            raise RuntimeError("No free X feed base URLs configured")

    def fetch_account_tweets(self, account: dict[str, str], max_results: int = 1) -> list[dict[str, Any]]:
        errors = []
        for base_url in self.base_urls:
            feed_url = build_free_feed_url(base_url, account["username"], max_results=max_results)
            try:
                response = requests.get(
                    feed_url,
                    timeout=45,
                    headers={"User-Agent": "FirstLightAI/1.0 (+https://github.com/THEGREATCJPark/signal-ai)"},
                )
                if not response.ok:
                    errors.append(f"{base_url}: {response.status_code}")
                    continue
                tweets = parse_feed_tweets(response.text, account["username"])
                if tweets:
                    return tweets[:max(1, int(max_results or 1))]
                errors.append(f"{base_url}: empty feed")
            except Exception as exc:
                errors.append(f"{base_url}: {exc}")
        print(f"[trigger] free feed failed for @{account['username']}: {'; '.join(errors)}", file=sys.stderr)
        return []

    def fetch_recent_by_accounts(
        self,
        accounts: list[dict[str, str]],
        *,
        max_results: int = 5,
        state: dict[str, Any] | None = None,
    ) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
        next_state = _copy_state(state)
        result: dict[str, list[dict[str, Any]]] = {}
        for account in accounts:
            result[account["username"]] = self.fetch_account_tweets(account, max_results=max_results)
        next_state["updated_at"] = datetime.now(timezone.utc).isoformat()
        return result, next_state


def candidate_id(tweet_id: str) -> str:
    return f"x-{tweet_id}"


def enrich_candidate(raw_candidate: dict[str, Any], account: dict[str, Any]) -> dict[str, Any]:
    tweet = dict(raw_candidate["tweet"])
    if not tweet.get("url"):
        tweet["url"] = f"https://x.com/{account['username']}/status/{tweet['id']}"
    summary = summarize_tweet(tweet, account)
    return {
        "id": candidate_id(str(tweet["id"])),
        "account": {k: v for k, v in account.items() if k != "key"},
        "tweet": tweet,
        "summary": summary,
        "detected_at": datetime.now(timezone.utc).isoformat(),
    }


def _payload_token(candidate: dict[str, Any]) -> str:
    raw = json.dumps(candidate, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def build_issue_body(candidate: dict[str, Any]) -> str:
    account = candidate["account"]
    tweet = candidate["tweet"]
    summary = candidate["summary"]
    return f"""## Trigger review

**계정:** @{account['username']} ({account.get('group', account.get('category', 'watch'))})
**원문:** {tweet.get('url', '')}
**게시 시각:** {tweet.get('created_at', '')}

### 요약
**{summary.get('title', '')}**

{summary.get('body', '')}

### 원문
> {str(tweet.get('text', '')).replace(chr(10), chr(10) + '> ')}

### 검수
댓글 중 하나를 남기면 GitHub Actions가 처리합니다.

`/approve-trigger` 또는 `yes` 또는 `예`

`/reject-trigger` 또는 `no` 또는 `아니오`

<!-- {ISSUE_PAYLOAD_PREFIX}{_payload_token(candidate)} -->
"""


def extract_candidate_from_issue_body(body: str) -> dict[str, Any]:
    marker = f"<!-- {ISSUE_PAYLOAD_PREFIX}"
    start = body.find(marker)
    if start == -1:
        raise ValueError("issue body does not contain x trigger payload")
    start += len(marker)
    end = body.find(" -->", start)
    if end == -1:
        raise ValueError("issue body has an unterminated x trigger payload")
    raw = base64.b64decode(body[start:end].strip()).decode("utf-8")
    return json.loads(raw)


def load_trigger_state(path: str | os.PathLike | None = None) -> dict[str, Any]:
    if path:
        p = Path(path)
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    if os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        from db.articles import load_pipeline_state

        return load_pipeline_state(STATE_KEY) or {}
    return json.loads(LOCAL_STATE_PATH.read_text(encoding="utf-8")) if LOCAL_STATE_PATH.exists() else {}


def save_trigger_state(state: dict[str, Any], path: str | os.PathLike | None = None) -> None:
    if path:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return
    if os.getenv("SUPABASE_URL") and os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        from db.articles import save_pipeline_state

        save_pipeline_state(STATE_KEY, state)
        return
    LOCAL_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOCAL_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def ensure_github_labels(labels: list[str], *, token: str, repo: str) -> None:
    headers = _github_headers(token)
    for label in labels:
        encoded = quote(label, safe="")
        existing = requests.get(
            f"https://api.github.com/repos/{repo}/labels/{encoded}",
            headers=headers,
            timeout=20,
        )
        if existing.status_code == 200:
            continue
        if existing.status_code != 404:
            raise RuntimeError(f"GitHub label lookup failed {existing.status_code}: {existing.text[:300]}")
        created = requests.post(
            f"https://api.github.com/repos/{repo}/labels",
            headers=headers,
            json={
                "name": label,
                "color": ISSUE_LABEL_COLORS.get(label, "ededed"),
                "description": "First Light AI X trigger workflow",
            },
            timeout=20,
        )
        if created.status_code not in (200, 201):
            raise RuntimeError(f"GitHub label create failed {created.status_code}: {created.text[:300]}")


def create_github_issue(candidate: dict[str, Any], *, token: str | None = None, repo: str | None = None) -> str:
    token = token or os.getenv("GITHUB_TOKEN")
    repo = repo or os.getenv("GITHUB_REPOSITORY")
    if not token or not repo:
        raise RuntimeError("GITHUB_TOKEN and GITHUB_REPOSITORY are required to create review issues")
    ensure_github_labels(ISSUE_LABELS, token=token, repo=repo)
    title = f"[Trigger] @{candidate['account']['username']}: {candidate['summary']['title']}"
    response = requests.post(
        f"https://api.github.com/repos/{repo}/issues",
        headers=_github_headers(token),
        json={"title": title[:256], "body": build_issue_body(candidate), "labels": ISSUE_LABELS},
        timeout=45,
    )
    if not response.ok:
        raise RuntimeError(f"GitHub issue create failed {response.status_code}: {response.text[:500]}")
    data = response.json()
    return data.get("html_url") or data.get("url") or ""


def maybe_notify_telegram(candidate: dict[str, Any], issue_url: str) -> None:
    chat_id = os.getenv("TRIGGER_REVIEW_TELEGRAM_CHAT_ID")
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not chat_id or not token:
        return
    text = (
        f"First Light AI trigger review\n\n"
        f"@{candidate['account']['username']}: {candidate['summary']['title']}\n"
        f"{candidate['summary']['body']}\n\n"
        f"Review: {issue_url}"
    )
    requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text[:4000], "disable_web_page_preview": False},
        timeout=20,
    ).raise_for_status()


def run_scan(args: argparse.Namespace) -> int:
    accounts = filter_accounts_for_scope(load_accounts(args.accounts), args.scope)
    by_key = {account["key"]: account for account in accounts}
    previous_state = load_trigger_state(args.state)
    tweets_by_account, lookup_state = FreeXFeedClient().fetch_recent_by_accounts(
        accounts,
        max_results=args.max_results,
        state=previous_state,
    )
    raw_candidates, next_state = detect_new_tweets(
        tweets_by_account,
        lookup_state,
        bootstrap=args.backfill,
    )
    if not raw_candidates:
        print("[trigger] no new watched X posts")
        save_trigger_state(next_state, args.state)
        return 0

    opened = []
    for raw in raw_candidates:
        account = by_key.get(account_key(raw["username"])) or normalize_account(raw["username"])
        candidate = enrich_candidate(raw, account)
        if args.dry_run:
            print(json.dumps(candidate, ensure_ascii=False, indent=2))
            continue
        issue_url = create_github_issue(candidate)
        maybe_notify_telegram(candidate, issue_url)
        opened.append(issue_url)
        print(f"[trigger] opened review issue: {issue_url}")
    if not args.dry_run:
        save_trigger_state(next_state, args.state)
    print(f"[trigger] candidates: {len(raw_candidates)}, issues: {len(opened)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan watched X accounts and queue trigger reviews")
    parser.add_argument("--accounts", help="JSON account config path")
    parser.add_argument("--state", help="Local state path override for tests/manual runs")
    parser.add_argument("--scope", choices=sorted(SCOPE_TIERS), default=os.getenv("TRIGGER_SCAN_SCOPE", "auto"),
                        help="Account tier scope to scan. Defaults to low-cost auto.")
    parser.add_argument("--max-results", type=int, default=int(os.getenv("TRIGGER_SCAN_MAX_RESULTS", "1")))
    parser.add_argument("--backfill", action="store_true", help="Create candidates even for accounts without cursors")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main() -> int:
    return run_scan(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
