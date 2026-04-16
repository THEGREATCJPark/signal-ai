#!/usr/bin/env python3
"""Run all crawlers in parallel. Output: data/crawled/<source>-<date>.jsonl

고득점 기사 감지 시 GitHub Actions 발행 워크플로우를 자동 트리거.
"""
import json, os, subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

CRAWLERS = ["hn.py", "reddit.py", "arxiv.py", "hf_trending.py", "geeknews.py", "lesswrong.py", "discord.py"]
HERE = Path(__file__).parent
CRAWLED_DIR = HERE.parent / "data" / "crawled"

# ── Score 기반 발행 트리거 설정 ──────────────────────────────────
# 이 값 이상이면 "긴급 발행" 트리거 발동
# 추후 결과를 보면서 지속적으로 조정 필요
SCORE_THRESHOLDS = {
    "hackernews": 300,     # HN points
    "reddit": 500,         # Reddit upvotes
    "arxiv": 0,            # arXiv는 score 없음, 별도 판단 필요
    "huggingface": 100,    # HF likes
    "geeknews": 50,        # GeekNews는 점수가 낮음
    "lesswrong": 50,       # LessWrong baseScore
    "discord": 0,          # Discord는 score 없음
}
DEFAULT_THRESHOLD = 200


def run(script):
    t0 = time.time()
    try:
        r = subprocess.run(
            ["python3", str(HERE / script)],
            capture_output=True, text=True, timeout=180, cwd=HERE
        )
        elapsed = time.time() - t0
        out = (r.stderr + r.stdout).strip().split("\n")[-1]
        return script, r.returncode, elapsed, out
    except subprocess.TimeoutExpired:
        return script, -1, 180.0, "TIMEOUT"
    except Exception as e:
        return script, -2, 0.0, str(e)


def check_high_score_articles() -> list[dict]:
    """오늘 크롤링된 JSONL에서 고득점 기사를 찾는다."""
    today = datetime.now().strftime("%Y-%m-%d")
    high_score = []

    for path in CRAWLED_DIR.glob(f"*-{today}.jsonl"):
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    p = json.loads(line)
                except json.JSONDecodeError:
                    continue

                source = p.get("source", "unknown")
                meta = p.get("metadata") or {}
                score = meta.get("points", 0) or meta.get("score", 0) or meta.get("baseScore", 0)
                threshold = SCORE_THRESHOLDS.get(source, DEFAULT_THRESHOLD)

                if threshold > 0 and score >= threshold:
                    title = meta.get("title", p.get("content", "")[:80])
                    high_score.append({
                        "source": source,
                        "title": title,
                        "score": score,
                        "threshold": threshold,
                    })

    return high_score


def trigger_publish(articles: list[dict]):
    """GitHub Actions 발행 워크플로우를 트리거한다.

    gh CLI가 있고 GITHUB_TOKEN이 설정된 환경에서만 동작.
    """
    # gh CLI 존재 확인
    gh_check = subprocess.run(["which", "gh"], capture_output=True)
    if gh_check.returncode != 0:
        print("[trigger] gh CLI 없음 — 트리거 건너뜀", flush=True)
        return

    repo = os.getenv("GITHUB_REPOSITORY", "THEGREATCJPark/signal-ai")
    print(f"[trigger] {len(articles)}개 고득점 기사 감지 — 발행 트리거!", flush=True)
    for a in articles:
        print(f"  ⚡ [{a['source']}] {a['title']} (score: {a['score']} >= {a['threshold']})", flush=True)

    try:
        subprocess.run(
            ["gh", "workflow", "run", "daily_publish.yml",
             "-R", repo,
             "-f", "platform=both",
             "-f", "force=false",
             "-f", "limit=3"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        print("[trigger] GitHub Actions 워크플로우 트리거 완료!", flush=True)
    except subprocess.CalledProcessError as e:
        print(f"[trigger] 트리거 실패: {e.stderr}", file=sys.stderr)
    except FileNotFoundError:
        print("[trigger] gh CLI를 찾을 수 없음", file=sys.stderr)


def main():
    print(f"Running {len(CRAWLERS)} crawlers in parallel...", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=len(CRAWLERS)) as pool:
        futs = [pool.submit(run, s) for s in CRAWLERS]
        for f in as_completed(futs):
            script, rc, elapsed, out = f.result()
            status = "✓" if rc == 0 else "✗"
            print(f"  {status} {script} ({elapsed:.1f}s) — {out}", flush=True)
            results.append((script, rc, elapsed))

    ok = sum(1 for _, rc, _ in results if rc == 0)
    print(f"\nDone: {ok}/{len(CRAWLERS)} succeeded")

    # 고득점 기사 체크 → 자동 발행 트리거
    high = check_high_score_articles()
    if high:
        trigger_publish(high)
    else:
        print("[trigger] 고득점 기사 없음 — 트리거 안 함")

    sys.exit(0 if ok == len(CRAWLERS) else 1)

if __name__ == "__main__":
    main()
