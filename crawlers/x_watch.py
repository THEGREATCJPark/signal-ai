#!/usr/bin/env python3
"""X/Twitter account watch crawler.

Primary backend: Nitter RSS. Optional fallback: official X API v2 when
X_BEARER_TOKEN is configured. Output uses the shared crawler JSONL schema.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import html
from html.parser import HTMLParser
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any
from urllib.parse import quote
import xml.etree.ElementTree as ET

import requests

try:
    from ._common import post, save
except ImportError:  # pragma: no cover - script execution path
    from _common import post, save


SOURCE = "x_watch"
DEFAULT_NITTER_INSTANCES = ("https://nitter.net",)
DEFAULT_REQUEST_DELAY_SECONDS = 1.5
DEFAULT_TIMEOUT_SECONDS = 25
USER_AGENT = "FirstLightAI/0.1 (+local hourly X watch)"
DC_CREATOR = "{http://purl.org/dc/elements/1.1/}creator"


@dataclass(frozen=True)
class WatchedAccount:
    handle: str
    group: str


WATCH_ACCOUNTS: tuple[WatchedAccount, ...] = (
    WatchedAccount("OpenAI", "official"),
    WatchedAccount("OpenAIDevs", "official"),
    WatchedAccount("AnthropicAI", "official"),
    WatchedAccount("claudeai", "official"),
    WatchedAccount("ClaudeDevs", "official"),
    WatchedAccount("GoogleDeepMind", "official"),
    WatchedAccount("GoogleAI", "official"),
    WatchedAccount("xai", "official"),
    WatchedAccount("testingcatalog", "rumor_detection"),
    WatchedAccount("btibor91", "rumor_detection"),
    WatchedAccount("arena", "rumor_detection"),
    WatchedAccount("ArtificialAnlys", "rumor_detection"),
    WatchedAccount("sama", "key_people"),
    WatchedAccount("karpathy", "key_people"),
    WatchedAccount("demishassabis", "key_people"),
    WatchedAccount("gdb", "key_people"),
    WatchedAccount("polynoamial", "key_people"),
    WatchedAccount("OfficialLoganK", "key_people"),
    WatchedAccount("steph_palazzolo", "scoops"),
    WatchedAccount("alexeheath", "scoops"),
    WatchedAccount("aaronpholmes", "scoops"),
)


class FeedUnavailable(RuntimeError):
    pass


class TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() in {"br", "p", "div", "li", "blockquote", "hr"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def text(self) -> str:
        return normalize_text("".join(self.parts))


def normalize_text(value: str | None) -> str:
    text = html.unescape(value or "")
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    return text


def strip_html(value: str | None) -> str:
    parser = TextExtractor()
    parser.feed(value or "")
    parser.close()
    return parser.text()


def strip_title_prefix(value: str) -> str:
    return re.sub(r"^(RT by @[^:]+:\s*|R to @[^:]+:\s*)", "", value).strip()


def tweet_kind(title: str) -> str:
    if title.startswith("RT by @"):
        return "retweet"
    if title.startswith("R to @"):
        return "reply"
    return "post"


def parse_datetime(value: str | None) -> datetime:
    if not value:
        raise ValueError("missing datetime")
    if value.endswith("Z"):
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    elif "," in value:
        parsed = parsedate_to_datetime(value)
    else:
        parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def tweet_id_from_link(link: str | None, guid: str | None) -> str | None:
    if guid and guid.strip():
        return guid.strip()
    if not link:
        return None
    match = re.search(r"/status/(\d+)", link)
    return match.group(1) if match else None


def handle_from_link(link: str | None, fallback: str) -> str:
    if link:
        match = re.search(r"https?://[^/]+/([^/?#]+)/status/\d+", link)
        if match:
            return match.group(1)
    return fallback


def x_status_url(link: str | None, tweet_id: str, fallback_handle: str) -> str:
    handle = handle_from_link(link, fallback_handle)
    return f"https://x.com/{handle}/status/{tweet_id}"


def parse_nitter_rss(xml_text: str, account: WatchedAccount, instance: str) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise FeedUnavailable(f"invalid RSS XML for @{account.handle}: {exc}") from exc

    rows: list[dict[str, Any]] = []
    for item in root.findall(".//item"):
        title = normalize_text(item.findtext("title"))
        description = strip_html(item.findtext("description"))
        content = description or strip_title_prefix(title)
        guid = normalize_text(item.findtext("guid"))
        link = normalize_text(item.findtext("link"))
        tweet_id = tweet_id_from_link(link, guid)
        pub_date = item.findtext("pubDate")
        if not tweet_id or not content or not pub_date:
            continue
        timestamp = parse_datetime(pub_date)
        author = normalize_text(item.findtext(DC_CREATOR)) or f"@{account.handle}"
        rows.append(
            post(
                source=SOURCE,
                source_id=tweet_id,
                source_url=x_status_url(link, tweet_id, account.handle),
                author=author,
                content=content,
                timestamp=timestamp,
                metadata={
                    "watched_account": account.handle,
                    "watch_group": account.group,
                    "tweet_kind": tweet_kind(title),
                    "nitter_instance": instance.rstrip("/"),
                    "nitter_link": link,
                    "rss_title": title,
                    "fetch_backend": "nitter_rss",
                },
            )
        )
    return rows


def parse_instances(value: str | None) -> list[str]:
    raw = value or os.getenv("NITTER_INSTANCES") or ",".join(DEFAULT_NITTER_INSTANCES)
    instances = [part.strip().rstrip("/") for part in raw.split(",") if part.strip()]
    return instances or list(DEFAULT_NITTER_INSTANCES)


def nitter_rss_url(instance: str, handle: str) -> str:
    return f"{instance.rstrip('/')}/{quote(handle)}/rss"


def fetch_nitter_account(
    account: WatchedAccount,
    instances: list[str],
    *,
    session: requests.Session,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    errors: list[str] = []
    for instance in instances:
        url = nitter_rss_url(instance, account.handle)
        try:
            response = session.get(url, timeout=timeout_seconds, headers={"User-Agent": USER_AGENT})
            response.raise_for_status()
            if not response.text.strip():
                raise FeedUnavailable("empty response")
            return parse_nitter_rss(response.text, account, instance)
        except (requests.RequestException, FeedUnavailable) as exc:
            errors.append(f"{instance}: {exc}")
    raise FeedUnavailable(f"all Nitter instances failed for @{account.handle}: {'; '.join(errors)}")


def x_api_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "User-Agent": USER_AGENT}


def fetch_x_api_account(
    account: WatchedAccount,
    *,
    session: requests.Session,
    bearer_token: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    max_results: int = 20,
) -> list[dict[str, Any]]:
    user_url = f"https://api.x.com/2/users/by/username/{quote(account.handle)}"
    user_response = session.get(
        user_url,
        params={"user.fields": "id,username,name"},
        timeout=timeout_seconds,
        headers=x_api_headers(bearer_token),
    )
    user_response.raise_for_status()
    user_data = user_response.json().get("data") or {}
    user_id = user_data.get("id")
    username = user_data.get("username") or account.handle
    if not user_id:
        raise FeedUnavailable(f"official X API did not return user id for @{account.handle}")

    tweets_url = f"https://api.x.com/2/users/{user_id}/tweets"
    tweets_response = session.get(
        tweets_url,
        params={
            "max_results": max(5, min(max_results, 100)),
            "tweet.fields": "created_at,public_metrics,referenced_tweets,entities",
        },
        timeout=timeout_seconds,
        headers=x_api_headers(bearer_token),
    )
    tweets_response.raise_for_status()
    tweets = tweets_response.json().get("data") or []

    rows = []
    for tweet in tweets:
        tweet_id = str(tweet.get("id") or "")
        text = normalize_text(tweet.get("text"))
        created_at = tweet.get("created_at")
        if not tweet_id or not text or not created_at:
            continue
        refs = tweet.get("referenced_tweets") or []
        kind = refs[0].get("type") if refs and isinstance(refs[0], dict) else "post"
        rows.append(
            post(
                source=SOURCE,
                source_id=tweet_id,
                source_url=f"https://x.com/{username}/status/{tweet_id}",
                author=f"@{username}",
                content=text,
                timestamp=parse_datetime(created_at),
                metadata={
                    "watched_account": account.handle,
                    "watch_group": account.group,
                    "tweet_kind": kind,
                    "public_metrics": tweet.get("public_metrics") or {},
                    "fetch_backend": "official_x_api",
                },
            )
        )
    return rows


def fetch_account(
    account: WatchedAccount,
    instances: list[str],
    *,
    session: requests.Session | None = None,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    x_bearer_token: str | None = None,
) -> list[dict[str, Any]]:
    session = session or requests.Session()
    try:
        return fetch_nitter_account(account, instances, session=session)
    except FeedUnavailable:
        token = x_bearer_token or os.getenv("X_BEARER_TOKEN")
        if not token:
            raise
        if request_delay_seconds > 0:
            time.sleep(request_delay_seconds)
        return fetch_x_api_account(account, session=session, bearer_token=token)


def fetch_all(
    accounts: tuple[WatchedAccount, ...] = WATCH_ACCOUNTS,
    *,
    instances: list[str] | None = None,
    session: requests.Session | None = None,
    request_delay_seconds: float = DEFAULT_REQUEST_DELAY_SECONDS,
    x_bearer_token: str | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    instances = instances or parse_instances(None)
    session = session or requests.Session()
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for index, account in enumerate(accounts):
        try:
            account_rows = fetch_account(
                account,
                instances,
                session=session,
                request_delay_seconds=request_delay_seconds,
                x_bearer_token=x_bearer_token,
            )
            rows.extend(account_rows)
            print(f"[x_watch] @{account.handle}: {len(account_rows)} posts", file=sys.stderr)
        except Exception as exc:
            message = f"@{account.handle}: {exc}"
            errors.append(message)
            print(f"[x_watch] WARN {message}", file=sys.stderr)
        if request_delay_seconds > 0 and index < len(accounts) - 1:
            time.sleep(request_delay_seconds)
    return rows, errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--instances", help="Comma-separated Nitter instance base URLs")
    parser.add_argument("--delay", type=float, default=DEFAULT_REQUEST_DELAY_SECONDS)
    parser.add_argument("--x-bearer-token", default=os.getenv("X_BEARER_TOKEN"))
    parser.add_argument("--output", type=Path, help="Optional JSONL output path")
    return parser.parse_args(argv)


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"[{SOURCE}] saved {len(rows)} posts -> {path}", file=sys.stderr)
    return path


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rows, errors = fetch_all(
        instances=parse_instances(args.instances),
        request_delay_seconds=max(0.0, args.delay),
        x_bearer_token=args.x_bearer_token,
    )
    if args.output:
        write_jsonl(args.output, rows)
    else:
        save(SOURCE, rows)
    if rows:
        return 0
    if errors:
        print("[x_watch] all accounts failed or returned no posts", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
