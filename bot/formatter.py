from datetime import datetime

SOURCE_EMOJI = {
    "hackernews": "🔶",
    "reddit": "🤖",
    "github_trending": "⭐",
    "huggingface": "🤗",
    "arxiv": "📄",
    "anthropic": "🅰️",
    "openai": "💚",
    "google_ai": "🔵",
    "geeknews": "🇰🇷",
}

SOURCE_LABEL = {
    "hackernews": "Hacker News",
    "reddit": "Reddit",
    "github_trending": "GitHub Trending",
    "huggingface": "HuggingFace",
    "arxiv": "arXiv",
    "anthropic": "Anthropic",
    "openai": "OpenAI",
    "google_ai": "Google AI",
    "geeknews": "GeekNews",
}


def _score_indicator(score: int) -> str:
    """점수 기반 중요도 표시"""
    if score >= 500:
        return "🔥🔥🔥"
    elif score >= 200:
        return "🔥🔥"
    elif score >= 100:
        return "🔥"
    return ""


def format_article(article: dict) -> str:
    """단일 기사를 텔레그램 HTML 포맷으로 변환"""
    source = article.get("source", "unknown")
    emoji = SOURCE_EMOJI.get(source, "📰")
    label = SOURCE_LABEL.get(source, source)
    title = article.get("title", "제목 없음")
    url = article.get("url", "")
    score = article.get("score", 0)
    comments = article.get("comments", 0)
    indicator = _score_indicator(score)

    lines = [
        f"{emoji} <b>{title}</b> {indicator}",
        "",
        f"📊 점수: {score} | 💬 댓글: {comments}",
        f"📌 출처: {label}",
    ]

    summary = article.get("summary")
    if summary:
        lines.append("")
        lines.append(f"📝 {summary}")

    # 이미지가 있으면 표시
    media = article.get("media", [])
    if media:
        lines.append("")
        lines.append(f"🖼 이미지 {len(media)}장 첨부")

    lines.append("")
    lines.append(f'🔗 <a href="{url}">원문 보기</a>')

    return "\n".join(lines)


def format_daily_digest(articles: list[dict]) -> str:
    """일일 다이제스트 포맷"""
    today = datetime.now().strftime("%Y년 %m월 %d일")
    header = f"📡 <b>Signal AI - {today} 브리핑</b>\n"
    header += "━" * 20 + "\n"

    body_parts = []
    for i, article in enumerate(articles, 1):
        source = article.get("source", "unknown")
        emoji = SOURCE_EMOJI.get(source, "📰")
        title = article.get("title", "")
        url = article.get("url", "")
        score = article.get("score", 0)
        indicator = _score_indicator(score)

        entry = f'{i}. {emoji} <a href="{url}">{title}</a> {indicator}'
        summary = article.get("summary")
        if summary:
            entry += f"\n   └ {summary}"
        body_parts.append(entry)

    footer = "\n" + "━" * 20
    footer += "\n🤖 Signal AI | AI 최전방 소식을 매일 아침"

    return header + "\n\n".join(body_parts) + footer
