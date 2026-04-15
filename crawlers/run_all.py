#!/usr/bin/env python3
"""Run all crawlers in parallel. Output: data/crawled/<source>-<date>.jsonl"""
import subprocess, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

CRAWLERS = ["hn.py", "reddit.py", "arxiv.py", "hf_trending.py", "geeknews.py", "lesswrong.py"]
HERE = Path(__file__).parent

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
    sys.exit(0 if ok == len(CRAWLERS) else 1)

if __name__ == "__main__":
    main()
