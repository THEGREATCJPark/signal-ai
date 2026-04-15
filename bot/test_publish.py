"""
샘플 데이터로 텔레그램 + X 발행 테스트
사용법: python -m bot.test_publish
"""

from bot.telegram_bot import send_article, send_test_message
from bot.x_poster import post_tweet

SAMPLE_ARTICLE = {
    "source": "hackernews",
    "title": "Signal AI 테스트 메시지입니다",
    "url": "https://github.com/THEGREATCJPark/signal-ai",
    "score": 999,
    "comments": 42,
    "timestamp": "2026-04-09T08:00:00Z",
    "raw_text": "테스트용 샘플 데이터",
}


def test_telegram():
    """텔레그램 채널 테스트"""
    print("텔레그램 테스트 메시지 발송 중...")
    try:
        send_test_message()
        print("텔레그램 테스트 성공!")
    except Exception as e:
        print(f"텔레그램 실패: {e}")

    print("텔레그램 기사 발송 중...")
    try:
        send_article(SAMPLE_ARTICLE)
        print("텔레그램 기사 발송 성공!")
    except Exception as e:
        print(f"텔레그램 기사 실패: {e}")


def test_x():
    """X 포스팅 테스트"""
    print("X 테스트 포스팅 중...")
    try:
        post_tweet("📡 Signal AI 봇 연결 테스트 완료! #SignalAI #AI")
        print("X 테스트 성공!")
    except Exception as e:
        print(f"X 실패: {e}")


if __name__ == "__main__":
    print("=== Signal AI 발행 테스트 ===\n")
    test_telegram()
    print()
    test_x()
    print("\n=== 테스트 완료 ===")
