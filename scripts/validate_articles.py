#!/usr/bin/env python3
"""
articles.json 유효성 검증.

Usage:
    python scripts/validate_articles.py [path]
    기본 경로: docs/articles.json
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INPUT = os.path.join(ROOT, "docs", "articles.json")


def validate(path: str) -> tuple[bool, list[str]]:
    """articles JSON 파일의 유효성을 검증합니다.

    Returns:
        (is_valid, error_messages)
    """
    errors = []

    # 1. 파일 존재 여부
    if not os.path.exists(path):
        return False, [f"파일을 찾을 수 없습니다: {path}"]

    # 2. JSON 파싱
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return False, [f"JSON 파싱 실패: {e}"]

    # 3. 구조 확인
    if isinstance(data, dict):
        if "articles" not in data:
            return False, ["dict 형식이지만 'articles' 키가 없습니다."]
        articles = data["articles"]
        if not isinstance(articles, list):
            return False, ["'articles' 값이 리스트가 아닙니다."]
    elif isinstance(data, list):
        articles = data
    else:
        return False, [f"지원하지 않는 최상위 타입: {type(data).__name__}"]

    # 4. 빈 배열 허용 (경고만)
    if len(articles) == 0:
        print("경고: 기사가 0개입니다.")
        return True, []

    # 5. 각 article 검증
    ids_seen = set()
    for i, article in enumerate(articles):
        if not isinstance(article, dict):
            errors.append(f"articles[{i}]: dict가 아닙니다 ({type(article).__name__})")
            continue

        # headline 또는 title 중 하나는 있어야 함
        has_title = bool(article.get("headline") or article.get("title"))
        if not has_title:
            errors.append(f"articles[{i}]: 'headline' 또는 'title'이 없습니다.")

        # id 중복 검사
        aid = article.get("id", "")
        if aid:
            if aid in ids_seen:
                errors.append(f"articles[{i}]: 중복 id '{aid}'")
            ids_seen.add(aid)

    if errors:
        return False, errors

    print(f"검증 통과: {len(articles)}개 기사")
    return True, []


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    ok, errs = validate(path)
    if ok:
        print(f"PASS: {path}")
        sys.exit(0)
    else:
        for e in errs:
            print(f"FAIL: {e}")
        sys.exit(1)
