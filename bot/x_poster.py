import os
import re
import sys
import hashlib
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv
from requests_oauthlib import OAuth1

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

load_dotenv()

X_API_KEY = os.getenv("X_API_KEY")
X_API_SECRET = os.getenv("X_API_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")
X_CLIENT_ID = os.getenv("X_CLIENT_ID")
X_CLIENT_SECRET = os.getenv("X_CLIENT_SECRET")
X_REFRESH_TOKEN = os.getenv("X_REFRESH_TOKEN")
X_REFRESH_TOKEN_STATE_KEY = os.getenv("X_REFRESH_TOKEN_STATE_KEY", "x_oauth2_refresh_token")

TWEET_URL = os.getenv("X_TWEET_URL", "https://api.x.com/2/tweets")
V1_TWEET_URL = os.getenv("X_V1_TWEET_URL", "https://api.twitter.com/1.1/statuses/update.json")
TOKEN_URL = os.getenv("X_TOKEN_URL", "https://api.x.com/2/oauth2/token")
MAX_DAILY_SUMMARY_ITEMS = 5
# Keep a small safety margin because X applies weighted tweet length rules.
MAX_TWEET_WEIGHT = int(os.getenv("X_MAX_TWEET_WEIGHT", "260"))
URL_WEIGHT = int(os.getenv("X_URL_WEIGHT", "23"))
URL_RE = re.compile(r"https?://\S+")
KST = ZoneInfo("Asia/Seoul")


def _refresh_token_hash(refresh_token: str | None) -> str | None:
    if not refresh_token:
        return None
    return hashlib.sha256(refresh_token.encode("utf-8")).hexdigest()


def _load_pipeline_state() -> dict:
    from db.articles import load_pipeline_state

    return load_pipeline_state(X_REFRESH_TOKEN_STATE_KEY) or {}


def _load_stored_refresh_token(seed_refresh_token: str | None = None) -> str | None:
    """Load the latest rotated X refresh token from mutable pipeline state."""
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        return None
    try:
        state = _load_pipeline_state()
    except Exception as exc:
        print(f"[x] refresh token state load skipped: {exc}")
        return None
    expected_hash = _refresh_token_hash(seed_refresh_token)
    stored_hash = state.get("seed_hash")
    if expected_hash and stored_hash and stored_hash != expected_hash:
        print("[x] stored refresh_token ignored because X_REFRESH_TOKEN changed")
        return None
    token = state.get("refresh_token")
    return str(token).strip() if token else None


def _save_stored_refresh_token(refresh_token: str, seed_refresh_token: str | None = None) -> None:
    """Persist a rotated refresh token so scheduled runs do not reuse a stale one."""
    if not refresh_token:
        return
    if not os.getenv("SUPABASE_URL") or not os.getenv("SUPABASE_SERVICE_ROLE_KEY"):
        print("[x] WARNING: Supabase is not configured; cannot persist rotated refresh_token")
        return

    from datetime import datetime, timezone

    from db.articles import save_pipeline_state

    save_pipeline_state(
        X_REFRESH_TOKEN_STATE_KEY,
        {
            "refresh_token": refresh_token,
            "seed_hash": _refresh_token_hash(seed_refresh_token),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    print("[x] rotated refresh_token persisted to Supabase pipeline_state")


def _get_access_token() -> str:
    """Refresh an OAuth 2.0 user-context token, persisting rotated refresh tokens."""
    refresh_token = _load_stored_refresh_token(X_REFRESH_TOKEN) or X_REFRESH_TOKEN
    if not X_CLIENT_ID or not refresh_token:
        raise RuntimeError("X_CLIENT_ID or X_REFRESH_TOKEN environment variable is missing")

    auth = (X_CLIENT_ID, X_CLIENT_SECRET) if X_CLIENT_SECRET else None
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    if not X_CLIENT_SECRET:
        payload["client_id"] = X_CLIENT_ID

    resp = requests.post(TOKEN_URL, auth=auth, data=payload, timeout=30)
    if not resp.ok:
        print(f"[x] Token endpoint {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()

    data = resp.json()
    scope = data.get("scope")
    if scope:
        print(f"[x] OAuth2 granted scopes: {scope}")
    new_refresh = data.get("refresh_token")
    if new_refresh and new_refresh != refresh_token:
        _save_stored_refresh_token(new_refresh, X_REFRESH_TOKEN)
    return data["access_token"]


def _has_oauth1_credentials() -> bool:
    return all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET])


def _has_oauth2_refresh_credentials() -> bool:
    return bool(X_CLIENT_ID and (X_REFRESH_TOKEN or os.getenv("SUPABASE_URL")))


def _oauth1_auth() -> OAuth1:
    if not _has_oauth1_credentials():
        raise RuntimeError("X OAuth 1.0a credentials are incomplete")
    return OAuth1(
        client_key=X_API_KEY,
        client_secret=X_API_SECRET,
        resource_owner_key=X_ACCESS_TOKEN,
        resource_owner_secret=X_ACCESS_TOKEN_SECRET,
    )


def post_tweet(text: str) -> dict:
    """Post a tweet using OAuth 1.0a User Context only."""
    payload = {"text": _fit_tweet(text)}
    print(f"[x] Payload chars: {len(payload['text'])}, weighted: {_tweet_weight(payload['text'])}")

    print("[x] Auth mode: OAuth 1.0a User Context")
    resp = requests.post(
        TWEET_URL,
        auth=_oauth1_auth(),
        headers={"Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    print(f"[x] Status: {resp.status_code}, Response: {resp.text[:300]}")
    if resp.ok:
        return _tweet_response_data(resp)
    if resp.status_code in {401, 403}:
        if _has_oauth2_refresh_credentials():
            print("[x] OAuth 1.0a was rejected; retrying with OAuth 2.0 User Context")
            try:
                access_token = _get_access_token()
                resp = requests.post(
                    TWEET_URL,
                    headers={
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                    timeout=30,
                )
                print(f"[x] Status: {resp.status_code}, Response: {resp.text[:300]}")
                if resp.ok:
                    return _tweet_response_data(resp)
            except Exception as exc:
                print(f"[x] OAuth 2.0 fallback failed: {exc}")
        print("[x] v2 create tweet rejected; retrying with OAuth 1.0a v1.1 status update")
        resp = requests.post(
            V1_TWEET_URL,
            auth=_oauth1_auth(),
            data={"status": payload["text"]},
            timeout=30,
        )
        print(f"[x] Status: {resp.status_code}, Response: {resp.text[:300]}")
    resp.raise_for_status()
    return _tweet_response_data(resp)


def _tweet_response_data(resp: requests.Response) -> dict:
    data = resp.json()
    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        return data["data"]
    if isinstance(data, dict) and (data.get("id_str") or data.get("id")):
        return {
            "id": str(data.get("id_str") or data.get("id")),
            "text": str(data.get("text") or ""),
        }
    return data if isinstance(data, dict) else {}


def _char_weight(ch: str) -> int:
    return 2 if unicodedata.east_asian_width(ch) in {"F", "W"} else 1


def _tweet_weight(text: str) -> int:
    weight = 0
    pos = 0
    for match in URL_RE.finditer(text):
        weight += sum(_char_weight(ch) for ch in text[pos:match.start()])
        weight += URL_WEIGHT
        pos = match.end()
    weight += sum(_char_weight(ch) for ch in text[pos:])
    return weight


def _fit_tweet(text: str, limit: int | None = None) -> str:
    limit = limit or MAX_TWEET_WEIGHT
    text = text.strip()
    if _tweet_weight(text) <= limit:
        return text
    suffix = "..."
    suffix_weight = _tweet_weight(suffix)
    out = []
    weight = 0
    pos = 0
    for match in URL_RE.finditer(text):
        for ch in text[pos:match.start()]:
            ch_weight = _char_weight(ch)
            if weight + ch_weight + suffix_weight > limit:
                return "".join(out).rstrip() + suffix
            out.append(ch)
            weight += ch_weight
        url = match.group(0)
        if weight + URL_WEIGHT + suffix_weight > limit:
            return "".join(out).rstrip() + suffix
        out.append(url)
        weight += URL_WEIGHT
        pos = match.end()
    for ch in text[pos:]:
        ch_weight = _char_weight(ch)
        if weight + ch_weight + suffix_weight > limit:
            return "".join(out).rstrip() + suffix
        out.append(ch)
        weight += ch_weight
    return "".join(out).strip()


def _summary_lines(summary: str, max_lines: int) -> list[str]:
    summary = re.sub(r"\s+", " ", (summary or "").strip())
    if not summary or max_lines <= 0:
        return []
    parts = [part.strip() for part in re.split(r"(?<=[.!?。！？다요죠니다습니다])\s+", summary) if part.strip()]
    if not parts:
        parts = [summary]
    return parts[:max_lines]


def _article_title(article: dict) -> str:
    return str(article.get("title") or article.get("headline") or "").strip()


def _article_url(article: dict) -> str:
    return str(article.get("url") or article.get("source_url") or "").strip()


def _daily_date_label(now: datetime | None = None) -> str:
    current = now.astimezone(KST) if now else datetime.now(KST)
    return f"{current.month}\uc6d4 {current.day}\uc77c AI \ucd5c\uc804\ubc29 \uc18c\uc2dd"


def _fit_headline_with_tail(headline: str, tail_lines: list[str], *, limit: int = MAX_TWEET_WEIGHT) -> str:
    tail_lines = [line.strip() for line in tail_lines if line and line.strip()]
    if not tail_lines:
        return _fit_tweet(headline, limit)
    tail = "\n".join(tail_lines)
    remaining = max(0, limit - _tweet_weight(tail) - 1)
    fitted_headline = _fit_tweet(headline, remaining) if remaining else ""
    lines = [line for line in [fitted_headline, *tail_lines] if line]
    return _fit_tweet("\n".join(lines), limit)


def build_compact_article_post_text(article: dict, *, max_lines: int = 5, limit: int = MAX_TWEET_WEIGHT) -> str:
    """Build compact title/summary/source text for X posts."""
    title = _article_title(article)
    summary = str(article.get("summary") or article.get("body") or "").strip()
    url = _article_url(article)

    summary_budget = max(0, max_lines - 1 - (1 if url else 0))
    lines = [line for line in [title] if line]
    lines.extend(_summary_lines(summary, summary_budget))
    if url:
        lines.append(url)

    text = "\n".join(lines[:max_lines])
    if _tweet_weight(text) <= limit:
        return text

    fixed_lines = [line for line in [title] if line]
    if url:
        reserved = _tweet_weight("\n".join(fixed_lines + [url]))
        summary_limit = max(0, limit - reserved - 1)
        fitted_summary = _fit_tweet(" ".join(_summary_lines(summary, summary_budget)), summary_limit)
        lines = fixed_lines + ([fitted_summary] if fitted_summary else []) + [url]
    else:
        reserved = _tweet_weight("\n".join(fixed_lines))
        summary_limit = max(0, limit - reserved - (1 if fixed_lines else 0))
        fitted_summary = _fit_tweet(" ".join(_summary_lines(summary, summary_budget)), summary_limit)
        lines = fixed_lines + ([fitted_summary] if fitted_summary else [])
    return _fit_tweet("\n".join(lines[:max_lines]), limit)


def build_article_post_text(article: dict) -> str:
    title = _article_title(article)
    summary = str(article.get("summary") or "").strip()
    url = _article_url(article)

    parts = [part for part in [title, summary, url] if part]
    return _fit_tweet("\n\n".join(parts))


def _fit_trigger_post_text(
    headline: str,
    comment_lines: list[str],
    url: str,
    *,
    limit: int = MAX_TWEET_WEIGHT,
) -> str:
    comments = [line.strip() for line in comment_lines if line and line.strip()]
    lines = [line for line in [headline, *comments, url] if line]
    text = "\n".join(lines)
    if _tweet_weight(text) <= limit:
        return text

    if url:
        remaining = limit - _tweet_weight(url) - 1
        if remaining <= 0:
            return _fit_tweet(url, limit)
        fitted_headline = _fit_tweet(headline, remaining)
        used = _tweet_weight(fitted_headline)
        comment_budget = remaining - used - 1
        comment = " ".join(comments)
        fitted_comment = _fit_tweet(comment, comment_budget) if comment_budget > _tweet_weight("...") else ""
        return "\n".join(line for line in [fitted_headline, fitted_comment, url] if line)

    return _fit_tweet("\n".join([headline, *comments]), limit)


def build_trigger_post_text(article: dict) -> str:
    """Build magazine-style headline/comment/source text for trigger X posts."""
    headline = _article_title(article)
    url = _article_url(article)
    summary = str(article.get("summary") or article.get("body") or "").strip()
    comments = [re.sub(r"\s+", " ", summary)] if summary else []
    return _fit_trigger_post_text(headline, comments, url)


def daily_summary_articles(articles: list[dict]) -> list[dict]:
    """Return the articles that can actually fit in the one-tweet daily summary."""
    return [
        article for article in articles
        if _article_title(article)
    ][:MAX_DAILY_SUMMARY_ITEMS]


def build_daily_summary_text(articles: list[dict], now: datetime | None = None) -> str:
    lines = [_daily_date_label(now)]
    for i, article in enumerate(daily_summary_articles(articles), 1):
        title = _article_title(article)
        line = f"{i}. {title}"
        candidate = "\n".join(lines + [line])
        if _tweet_weight(candidate) <= MAX_TWEET_WEIGHT:
            lines.append(line)
            continue

        remaining = MAX_TWEET_WEIGHT - _tweet_weight("\n".join(lines)) - 1
        if remaining > _tweet_weight(f"{i}. ..."):
            lines.append(_fit_tweet(line, remaining))
        break

    return _fit_tweet("\n".join(lines))


def post_article(article: dict) -> dict:
    """Post a single article to X without digest branding."""
    if article.get("source") == "x_trigger":
        return post_tweet(build_trigger_post_text(article))
    return post_tweet(build_compact_article_post_text(article))


def post_daily_summary(articles: list[dict]) -> dict:
    """Post article titles to X without headers, footers, or hashtags."""
    return post_tweet(build_daily_summary_text(articles))
