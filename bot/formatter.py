from html import escape


def _as_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


def format_article(article: dict) -> str:
    """Format a single article as content-only Telegram HTML."""
    title = escape(_as_text(article.get("title"), "제목 없음"))
    summary = escape(_as_text(article.get("summary")))
    url = _as_text(article.get("url"))

    lines = [f"<b>{title}</b>"]
    if summary:
        lines.extend(["", summary])
    if url:
        safe_url = escape(url, quote=True)
        lines.extend(["", f'<a href="{safe_url}">원문 보기</a>'])

    return "\n".join(lines)


def format_daily_digest(articles: list[dict]) -> str:
    """Format a content-only digest without journal branding."""
    entries = []
    for i, article in enumerate(articles, 1):
        title = escape(_as_text(article.get("title"), "제목 없음"))
        summary = escape(_as_text(article.get("summary")))
        url = _as_text(article.get("url"))

        if url:
            safe_url = escape(url, quote=True)
            entry = f'{i}. <a href="{safe_url}">{title}</a>'
        else:
            entry = f"{i}. {title}"
        if summary:
            entry += f"\n{summary}"
        entries.append(entry)

    return "\n\n".join(entries)
