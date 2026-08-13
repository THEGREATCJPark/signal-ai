#!/usr/bin/env python3
"""Scoring v2 for X trigger candidates."""
from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime, timezone
from typing import Any


VERSION = "x-trigger-score-v2"
AUTO_PUBLISH_SCORE_THRESHOLD = int(os.getenv("TRIGGER_AUTO_PUBLISH_SCORE_THRESHOLD", "60"))
REVIEW_SCORE_THRESHOLD = int(os.getenv("TRIGGER_REVIEW_SCORE_THRESHOLD", "65"))
MIN_ISSUE_SCORE = int(os.getenv("TRIGGER_MIN_ISSUE_SCORE", "45"))

OFFICIAL_HANDLES = {
    "openai",
    "openaidevs",
    "chatgptapp",
    "anthropicai",
    "claudeai",
    "claudedevs",
    "googledeepmind",
    "googleai",
    "geminiapp",
    "xai",
    "grok",
    "v0",
    "vercel",
    "zeddotdev",
    "zedindustries",
    "cursor_ai",
    "anysphere",
    "huggingface",
    "mistralai",
    "alibaba_qwen",
    "deepseek_ai",
    "aiatmeta",
}
INTERNAL_GROUP_TERMS = ("openai", "anthropic", "claude", "google", "gemini", "xai", "grok")
MAJOR_AI_TERMS = (
    "openai",
    "chatgpt",
    "gpt",
    "claude",
    "anthropic",
    "gemini",
    "google deepmind",
    "grok",
    "xai",
)
DEVELOPER_AI_TERMS = (
    "api",
    "developer",
    "developers",
    "agent",
    "agents",
    "coding",
    "browser use",
    "workflow",
    "v0",
    "zed",
    "cursor",
    "replit",
    "vercel",
    "tool",
    "sdk",
)
OSS_TERMS = ("open source", "oss", "local model", "hugging face", "huggingface", "qwen", "llama", "mistral", "deepseek", "ollama", "vllm")
PRODUCT_EVENT_TERMS = (
    "launch",
    "launches",
    "launched",
    "release",
    "released",
    "roll out",
    "rollout",
    "rolling out",
    "available",
    "ships",
    "shipping",
    "introducing",
    "api",
    "model",
    "pricing",
    "price",
    "feature",
    "policy",
    "subscription",
)
INTEGRATION_TERMS = ("integration", "integrates", "partnership", "partner", "support", "platform support")
CORPORATE_TERMS = ("relationship", "lawsuit", "regulation", "regulatory", "acquisition", "acquire", "internal", "scoop", "deal", "merger")
BENCHMARK_TERMS = ("benchmark", "eval", "evaluation", "performance", "score", "leaderboard", "swe-bench", "data")
WORKFLOW_TERMS = ("workflow", "case study", "longform", "analysis", "how i", "tutorial", "guide")
EVENT_PROMO_TERMS = ("ticket", "tickets", "conference", "webinar", "podcast", "event", "speaking", "join us", "register")
MARKET_TERMS = ("stock", "stocks", "share price", "market cap", "valuation", "market reaction", "investor", "investment", "tsmc", "intel", "capex", "주가", "시총", "투자")
PERSONAL_TERMS = ("my experience", "personal experience", "i tried", "vibes", "beautiful", "aesthetic", "monet", "감상", "미학")
HYPE_TERMS = ("huge if true", "insane", "wild", "massive", "game changer", "crazy", "hype")
RUMOR_TERMS = ("rumor", "rumour", "unverified", "leak", "leaked", "might", "may", "could", "speculation", "추측", "루머")
UNCERTAINTY_TERMS = ("reportedly", "sources say", "may", "might", "could", "appears", "seems")
EVIDENCE_URL_TERMS = ("openai.com", "anthropic.com", "deepmind.google", "ai.google", "developers.google", "docs.", "blog", "release", "releases", "changelog")


def _text_parts(candidate: dict[str, Any]) -> list[str]:
    account = candidate.get("account") or {}
    tweet = candidate.get("tweet") or {}
    summary = candidate.get("summary") or {}
    return [
        str(account.get("username") or ""),
        str(account.get("category") or ""),
        str(account.get("group") or ""),
        str(tweet.get("text") or ""),
        str(tweet.get("title") or ""),
        str(tweet.get("url") or ""),
        str(summary.get("title") or ""),
        str(summary.get("body") or ""),
        str(summary.get("confidence") or ""),
    ]


def _content_parts(candidate: dict[str, Any]) -> list[str]:
    tweet = candidate.get("tweet") or {}
    summary = candidate.get("summary") or {}
    return [
        str(tweet.get("text") or ""),
        str(tweet.get("title") or ""),
        str(tweet.get("url") or ""),
        str(summary.get("title") or ""),
        str(summary.get("body") or ""),
        str(summary.get("confidence") or ""),
    ]


def _haystack(candidate: dict[str, Any]) -> str:
    return " ".join(_text_parts(candidate)).lower()


def _content_haystack(candidate: dict[str, Any]) -> str:
    return " ".join(_content_parts(candidate)).lower()


def _contains(text: str, terms: tuple[str, ...] | set[str]) -> bool:
    return any(term in text for term in terms)


def _account_key(candidate: dict[str, Any]) -> str:
    account = candidate.get("account") or {}
    return str(account.get("username") or "").strip().lstrip("@").lower()


def _account_category(candidate: dict[str, Any]) -> str:
    account = candidate.get("account") or {}
    return str(account.get("category") or "").strip().lower()


def _account_group(candidate: dict[str, Any]) -> str:
    account = candidate.get("account") or {}
    return str(account.get("group") or "").strip().lower()


def _confidence(candidate: dict[str, Any], text: str) -> str:
    summary = candidate.get("summary") or {}
    raw = str(summary.get("confidence") or "").strip().lower()
    mapping = {
        "official": "verified",
        "verified": "verified",
        "source": "verified",
        "reported": "reported",
        "report": "reported",
        "scoop": "reported",
        "rumor": "rumor",
        "rumour": "rumor",
        "inference": "inference",
        "fallback": "unknown",
        "unknown": "unknown",
    }
    if raw in mapping:
        return mapping[raw]
    if _account_category(candidate) == "official":
        return "verified"
    if _contains(text, RUMOR_TERMS):
        return "rumor"
    if _account_category(candidate) == "scoop" or "scoop" in text:
        return "reported"
    return "unknown"


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _explicit_candidate_score(candidate: dict[str, Any]) -> int | None:
    for source in (candidate, candidate.get("summary") or {}, candidate.get("tweet") or {}):
        value = source.get("score") if isinstance(source, dict) else None
        if value is None:
            continue
        try:
            return max(0, min(100, int(value)))
        except (TypeError, ValueError):
            continue
    return None


def _status_id_from_url(url: str) -> str | None:
    match = re.search(r"/status(?:es)?/(\d+)", url or "")
    return match.group(1) if match else None


def _quoted_status_id(candidate: dict[str, Any]) -> str | None:
    tweet = candidate.get("tweet") or {}
    for key in (
        "quoted_status_id",
        "quoted_tweet_id",
        "original_status_id",
        "source_status_id",
        "referenced_tweet_id",
    ):
        value = str(tweet.get(key) or "").strip()
        if value:
            return value
    for key in ("quoted_url", "original_url", "source_url"):
        value = _status_id_from_url(str(tweet.get(key) or ""))
        if value:
            return value
    return None


def _normalize_text(text: str) -> str:
    text = text.lower()
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"@\w+", " ", text)
    text = re.sub(r"\b\d+(?:[.,]\d+)?\b", " ", text)
    text = re.sub(r"[^a-z0-9가-힣\s-]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _story_entities(text: str) -> list[str]:
    entity_aliases = [
        ("openai", ("openai", "chatgpt", "gpt")),
        ("anthropic", ("anthropic", "claude")),
        ("google", ("google", "gemini", "deepmind")),
        ("xai", ("xai", "grok")),
        ("v0", ("v0",)),
        ("zed", ("zed",)),
        ("cursor", ("cursor",)),
        ("apple", ("apple",)),
        ("tsmc", ("tsmc",)),
        ("intel", ("intel",)),
    ]
    found: list[str] = []
    for canonical, aliases in entity_aliases:
        if any(alias in text for alias in aliases):
            found.append(canonical)
    return found[:3]


def _story_event_terms(text: str) -> list[str]:
    event_aliases = [
        ("chatgpt-subscription", ("chatgpt subscription", "subscription integration")),
        ("personal-finance", ("personal finance",)),
        ("browser-use", ("browser use",)),
        ("pricing", ("pricing", "price")),
        ("performance", ("performance", "benchmark")),
        ("relationship", ("relationship", "deteriorated", "worsens")),
        ("api", ("api",)),
        ("rollout", ("rollout", "rolling out", "roll out")),
        ("launch", ("launch", "launches", "launched", "release", "released")),
        ("integration", ("integration", "integrates")),
    ]
    found: list[str] = []
    for canonical, aliases in event_aliases:
        if any(alias in text for alias in aliases):
            found.append(canonical)
    return found[:3]


def build_story_key(candidate: dict[str, Any]) -> str:
    status_id = _quoted_status_id(candidate)
    if status_id:
        return f"status:{status_id}"
    text = _normalize_text(" ".join(_text_parts(candidate)))
    entities = _story_entities(text)
    events = _story_event_terms(text)
    parts = [*entities, *events]
    if len(parts) >= 2:
        return ":".join(dict.fromkeys(parts[:4]))
    tokens = [
        token
        for token in text.split()
        if len(token) > 2 and token not in {"the", "and", "for", "with", "now", "new", "this", "that"}
    ]
    if tokens:
        return ":".join(tokens[:4])
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]
    return f"story:{digest}"


def _has_official_original(candidate: dict[str, Any], text: str) -> bool:
    if _account_category(candidate) == "official":
        return True
    tweet = candidate.get("tweet") or {}
    if _quoted_status_id(candidate):
        return True
    url_text = " ".join(str(tweet.get(key) or "") for key in ("url", "quoted_url", "original_url", "source_url")).lower()
    if any(f"x.com/{handle}" in url_text or f"twitter.com/{handle}" in url_text for handle in OFFICIAL_HANDLES):
        return True
    return "officially says" in text or "official post" in text or "official source" in text


def _score_source(candidate: dict[str, Any], text: str) -> tuple[int, str]:
    category = _account_category(candidate)
    group = _account_group(candidate)
    handle = _account_key(candidate)
    if category == "official" or handle in OFFICIAL_HANDLES:
        return 20, "official product/company account"
    if _has_official_original(candidate, text):
        return 20, "official original source is quoted"
    if category == "person" and (_contains(group, INTERNAL_GROUP_TERMS) or _contains(text, MAJOR_AI_TERMS)):
        return 17, "core insider or product leader"
    if category == "scoop":
        return 15, "trusted reporter or scoop account"
    if category in {"practitioner", "research", "benchmark", "coding_agent", "open_source"}:
        return 12, "practitioner, researcher, or developer source"
    if category in {"rumor", "detection", "fast_signal", "watch"}:
        return 8, "watcher or detection account"
    if "quote" in text or "reaction" in text:
        return 4, "secondary quote or commentary"
    return 0, "unclear source"


def _event_type_and_score(text: str) -> tuple[str, int, str]:
    if _contains(text, MARKET_TERMS):
        return "market_commentary", 2, "market or investment commentary"
    if _contains(text, EVENT_PROMO_TERMS):
        return "event_promo", 5, "event, ticket, or podcast promotion"
    if _contains(text, PERSONAL_TERMS):
        return "personal_anecdote", 3, "personal or aesthetic reaction"
    if _contains(text, RUMOR_TERMS):
        return "rumor", 12, "rumor or speculative item"
    if _contains(text, OSS_TERMS) and _contains(text, PRODUCT_EVENT_TERMS):
        return "open_source_release", 18, "open source model or tool release"
    if _contains(text, INTEGRATION_TERMS):
        return "integration", 22, "major integration or platform support"
    if _contains(text, PRODUCT_EVENT_TERMS):
        return "product_launch", 25, "model, product, API, pricing, feature, policy, or rollout"
    if _contains(text, CORPORATE_TERMS):
        return "industry_change", 20, "corporate, legal, regulatory, or internal industry change"
    if _contains(text, BENCHMARK_TERMS):
        return "benchmark_data", 16, "benchmark, research, or performance data"
    if _contains(text, WORKFLOW_TERMS):
        return "workflow_analysis", 10, "practical workflow or analysis"
    return "general_commentary", 3, "general commentary"


def _score_impact(text: str, event_type: str) -> tuple[int, str]:
    if _contains(text, MAJOR_AI_TERMS):
        return 20, "broad impact on major AI model or ChatGPT users"
    if _contains(text, DEVELOPER_AI_TERMS):
        return 17, "direct impact on developers, builders, or agent users"
    if _contains(text, OSS_TERMS):
        return 14, "meaningful open-source or local-model ecosystem change"
    if event_type in {"event_promo", "personal_anecdote", "market_commentary"}:
        return 4, "weak AI relevance"
    if "ai" in text or "model" in text:
        return 10, "limited impact on a specific AI tool or community"
    return 0, "not clearly AI news"


def _is_duplicate(story_key: str, state: dict[str, Any] | None) -> bool:
    if not state:
        return False
    for key in ("published_story_keys", "recent_story_keys", "story_keys"):
        container = state.get(key)
        if isinstance(container, dict) and story_key in container:
            return True
        if isinstance(container, (list, set, tuple)) and story_key in container:
            return True
    return False


def _score_freshness(candidate: dict[str, Any], now: datetime, duplicate: bool, reasons: list[str]) -> int:
    if duplicate:
        reasons.append("same story_key was already published recently")
        return 0
    tweet = candidate.get("tweet") or {}
    timestamp = _parse_datetime(str(tweet.get("created_at") or "")) or _parse_datetime(str(candidate.get("detected_at") or ""))
    if not timestamp:
        reasons.append("freshness unknown")
        return 8
    age_hours = max(0.0, (now - timestamp).total_seconds() / 3600)
    if age_hours <= 3:
        return 15
    if age_hours <= 12:
        return 12
    if age_hours <= 36:
        return 8
    if _contains(_haystack(candidate), MAJOR_AI_TERMS):
        return 4
    return 0


def _score_evidence(candidate: dict[str, Any], text: str) -> tuple[int, str]:
    category = _account_category(candidate)
    if category == "official" or _has_official_original(candidate, text) or _contains(text, EVIDENCE_URL_TERMS):
        return 15, "official source, demo, docs, blog, or release note"
    if category == "person":
        return 12, "first-party stakeholder statement"
    if category == "scoop":
        return 10, "trusted reporter or scoop account"
    if category in {"detection", "fast_signal", "rumor", "watch"}:
        return 5, "watcher or rumor observation"
    return 0, "no clear evidence"


def _score_publish_fit(text: str, event_type: str) -> tuple[int, str]:
    if event_type in {"product_launch", "integration", "open_source_release", "benchmark_data"}:
        return 10, "clear value in a concise Korean summary"
    if event_type in {"industry_change", "workflow_analysis", "rumor"}:
        return 7, "useful with brief context"
    if event_type in {"general_commentary"}:
        return 4, "interesting but low actionability"
    return 0, "too minor or promotional for publication"


def _penalty(name: str, points: int, reason: str) -> dict[str, Any]:
    return {"name": name, "points": points, "reason": reason}


def _is_secondary_official_requote(candidate: dict[str, Any], text: str) -> bool:
    return _account_category(candidate) != "official" and _has_official_original(candidate, text)


def _has_extra_quote_info(text: str) -> bool:
    return any(term in text for term in ("adds", "confirms", "details", "thread", "analysis", "says", "reports"))


def _collect_penalties_and_blocks(
    candidate: dict[str, Any],
    *,
    text: str,
    confidence: str,
    event_type: str,
    duplicate: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    penalties: list[dict[str, Any]] = []
    hard_blocks: list[str] = []
    summary_confidence = str((candidate.get("summary") or {}).get("confidence") or "").strip().lower()
    if summary_confidence == "fallback":
        hard_blocks.append("summary_generation_failed")
    if duplicate:
        penalties.append(_penalty("duplicate_story_key", -45, "same story_key was already published recently"))
        hard_blocks.append("duplicate_story_key")
    if _is_secondary_official_requote(candidate, text):
        penalties.append(_penalty("secondary_official_requote", -30, "secondary account is relaying an official original"))
        hard_blocks.append("secondary_official_requote")
    tweet_kind = str((candidate.get("tweet") or {}).get("tweet_kind") or "").lower()
    if tweet_kind in {"retweet", "quote"} and not _has_extra_quote_info(text):
        penalties.append(_penalty("rt_or_quote_without_new_info", -25, "RT/quote has no meaningful added information"))
        hard_blocks.append("rt_or_quote_without_new_info")
    if confidence in {"rumor", "inference"}:
        penalties.append(_penalty("weak_uncertain_evidence", -20, "rumor or inference lacks enough evidence"))
        hard_blocks.append(f"confidence_{confidence}")
    if event_type == "market_commentary":
        penalties.append(_penalty("market_centered", -25, "post is centered on stock, valuation, investment, or market reaction"))
        hard_blocks.append("market_commentary")
    if event_type == "personal_anecdote":
        penalties.append(_penalty("personal_reaction", -20, "personal use, aesthetic, or anecdotal reaction"))
        hard_blocks.append("personal_anecdote")
    if event_type == "event_promo":
        penalties.append(_penalty("event_promo", -20, "event promotion, ticket, podcast, or appearance notice"))
        hard_blocks.append("event_promo")
    if _contains(text, HYPE_TERMS):
        penalties.append(_penalty("hype_only", -15, "hype phrasing without enough concrete substance"))
    if not (_contains(text, MAJOR_AI_TERMS) or _contains(text, DEVELOPER_AI_TERMS) or _contains(text, OSS_TERMS) or "ai" in text):
        penalties.append(_penalty("weak_ai_relevance", -20, "weak direct relevance to AI news"))
    if "..." in str((candidate.get("tweet") or {}).get("text") or ""):
        penalties.append(_penalty("truncated_source", -10, "source text appears truncated"))
    if "breakthrough" in text and confidence != "verified":
        penalties.append(_penalty("overstated_summary", -20, "summary may overstate the source"))
    if confidence in {"rumor", "inference"} and not _contains(text, ("rumor", "reportedly", "may", "might", "could", "unverified")):
        penalties.append(_penalty("uncertainty_removed", -15, "uncertain source is phrased too definitively"))
    if event_type in {"rumor", "speculation", "market_commentary", "personal_anecdote", "event_promo"}:
        hard_blocks.append(event_type)
    return penalties, list(dict.fromkeys(hard_blocks))


def _decision(score: int, confidence: str, event_type: str, hard_blocks: list[str]) -> str:
    if confidence in {"rumor", "reported", "inference"}:
        if score >= REVIEW_SCORE_THRESHOLD:
            return "review"
        if score >= MIN_ISSUE_SCORE:
            return "watch"
        return "drop"
    if score >= AUTO_PUBLISH_SCORE_THRESHOLD and not hard_blocks and confidence == "verified":
        return "auto_both"
    if score >= 80 and not hard_blocks:
        return "auto_telegram_review_x"
    if score >= REVIEW_SCORE_THRESHOLD:
        return "review"
    if score >= MIN_ISSUE_SCORE:
        return "watch"
    return "drop"


def score_trigger_candidate_detail(
    candidate: dict[str, Any],
    *,
    now: datetime | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    text = _content_haystack(candidate)
    story_key = build_story_key(candidate)
    confidence = _confidence(candidate, text)
    event_type, event_score, event_reason = _event_type_and_score(text)
    duplicate = _is_duplicate(story_key, state)

    source_score, source_reason = _score_source(candidate, text)
    impact_score, impact_reason = _score_impact(text, event_type)
    freshness_score = _score_freshness(candidate, now, duplicate, [])
    evidence_score, evidence_reason = _score_evidence(candidate, text)
    publish_score, publish_reason = _score_publish_fit(text, event_type)

    breakdown = {
        "source": max(0, min(20, source_score)),
        "event": max(0, min(25, event_score)),
        "impact": max(0, min(20, impact_score)),
        "freshness": max(0, min(15, freshness_score)),
        "evidence": max(0, min(15, evidence_score)),
        "publish_fit": max(0, min(10, publish_score)),
    }
    explicit = _explicit_candidate_score(candidate)
    base_score = explicit if explicit is not None else sum(breakdown.values())
    penalties, hard_blocks = _collect_penalties_and_blocks(
        candidate,
        text=text,
        confidence=confidence,
        event_type=event_type,
        duplicate=duplicate,
    )
    score = max(0, min(100, base_score + sum(int(p["points"]) for p in penalties)))
    decision = _decision(score, confidence, event_type, hard_blocks)
    reasons = [
        source_reason,
        event_reason,
        impact_reason,
        evidence_reason,
        publish_reason,
    ]
    if explicit is not None:
        reasons.append(f"explicit score override: {explicit}")
    reasons.extend(p["reason"] for p in penalties[:3])

    detail = {
        "version": VERSION,
        "score": int(score),
        "decision": decision,
        "confidence": confidence,
        "event_type": event_type,
        "story_key": story_key,
        "breakdown": breakdown,
        "penalties": penalties,
        "hard_blocks": hard_blocks,
        "reasons": reasons,
    }
    candidate["scoring"] = detail
    candidate["auto_publish_score"] = int(score)
    return detail


def score_trigger_candidate(
    candidate: dict[str, Any],
    *,
    now: datetime | None = None,
    state: dict[str, Any] | None = None,
) -> int:
    return int(score_trigger_candidate_detail(candidate, now=now, state=state)["score"])


def should_auto_publish_candidate(
    candidate: dict[str, Any],
    *,
    now: datetime | None = None,
    state: dict[str, Any] | None = None,
) -> bool:
    decision = score_trigger_candidate_detail(candidate, now=now, state=state)["decision"]
    return decision in {"auto_both", "auto_telegram_review_x"}
