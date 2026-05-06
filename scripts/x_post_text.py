import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from bot.x_poster import post_tweet


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Post a diagnostic text update to X.")
    parser.add_argument("text", help="Text to publish. It will be trimmed by the X poster if needed.")
    args = parser.parse_args(argv)

    result = post_tweet(args.text)
    tweet_id = result.get("id") if isinstance(result, dict) else None
    if tweet_id:
        print(f"[x] posted tweet id: {tweet_id}")
    else:
        print("[x] posted tweet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
