import json
import os
import glob as glob_module

from bot.telegram_bot import send_daily_digest
from bot.x_poster import post_daily_summary
from publisher.state import PublishedState, article_key

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _load_articles() -> list[dict]:
    """data/ 폴더에서 기사 로드. latest.json 우선, 없으면 *.json 전체"""
    latest = os.path.join(DATA_DIR, "latest.json")

    if os.path.exists(latest):
        with open(latest, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, list) else [data]

    # latest.json 없으면 전체 JSON 파일 로드
    articles = []
    for filepath in glob_module.glob(os.path.join(DATA_DIR, "*.json")):
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                articles.extend(data)
            else:
                articles.append(data)
    return articles


def run_daily_publish():
    """data/ 폴더에서 오늘의 기사를 읽어서 텔레그램 + X에 발행"""
    articles = _load_articles()

    if not articles:
        print("발행할 기사가 없습니다.")
        return

    # 점수 기준 내림차순 정렬
    articles.sort(key=lambda x: x.get("score", 0), reverse=True)

    # 상위 10개만 발행
    top_articles = articles[:10]

    # 텔레그램 발행
    try:
        send_daily_digest(top_articles)
        print(f"텔레그램: {len(top_articles)}개 기사 발행 완료")
    except Exception as e:
        print(f"텔레그램 발행 실패: {e}")

    # X 발행
    try:
        post_daily_summary(top_articles)
        print("X: 일일 요약 포스팅 완료")
    except Exception as e:
        print(f"X 발행 실패: {e}")


def publish(articles: list[dict], dry_run: bool = False, platform: str = "both",
           force: bool = False):
    """JSON 기사 목록을 Telegram/X에 발행.

    Args:
        articles: 정규화된 기사 dict 리스트
        dry_run: True이면 발행 없이 미리보기만
        platform: "telegram", "x", 또는 "both"
        force: True이면 published.json 무시, 전체 재발행
    """
    if not articles:
        print("발행할 기사가 없습니다.")
        return

    # 점수 기준 정렬, 상위 10개
    articles.sort(key=lambda x: x.get("score", 0), reverse=True)
    top_articles = articles[:10]

    state = PublishedState()
    platforms = ["telegram", "x"] if platform == "both" else [platform]

    for plat in platforms:
        if force:
            to_publish = top_articles
        else:
            to_publish = state.get_unpublished(top_articles, plat)

        if not to_publish:
            print(f"[{plat}] 새로 발행할 기사가 없습니다 (이미 발행됨)")
            continue

        if dry_run:
            print(f"\n[DRY-RUN] [{plat}] {len(to_publish)}개 기사 발행 예정:")
            for a in to_publish:
                print(f"  - {a.get('title', '제목 없음')}")
            continue

        # 실제 발행
        if plat == "telegram":
            try:
                send_daily_digest(to_publish)
                for a in to_publish:
                    state.mark_published(article_key(a), "telegram")
                print(f"[telegram] {len(to_publish)}개 기사 발행 완료")
            except Exception as e:
                print(f"[telegram] 발행 실패: {e}")

        elif plat == "x":
            try:
                post_daily_summary(to_publish)
                for a in to_publish:
                    state.mark_published(article_key(a), "x")
                print(f"[x] 일일 요약 포스팅 완료")
            except Exception as e:
                print(f"[x] 발행 실패: {e}")

    if not dry_run:
        state.save()


if __name__ == "__main__":
    run_daily_publish()
