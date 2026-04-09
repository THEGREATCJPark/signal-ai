import json
import os
import glob as glob_module

from bot.telegram_bot import send_daily_digest
from bot.x_poster import post_daily_summary

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def run_daily_publish():
    """data/ 폴더에서 오늘의 기사를 읽어서 텔레그램 + X에 발행"""
    articles = []

    json_files = glob_module.glob(os.path.join(DATA_DIR, "*.json"))
    for filepath in json_files:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                articles.extend(data)
            else:
                articles.append(data)

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


if __name__ == "__main__":
    run_daily_publish()
