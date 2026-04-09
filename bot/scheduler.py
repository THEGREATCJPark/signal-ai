import asyncio
import json
import os
import glob as glob_module

from bot.telegram_bot import send_daily_digest

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


async def run_daily_publish():
    """data/ 폴더에서 오늘의 기사를 읽어서 텔레그램에 발행"""
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

    await send_daily_digest(top_articles)
    print(f"{len(top_articles)}개 기사 발행 완료")


if __name__ == "__main__":
    asyncio.run(run_daily_publish())
