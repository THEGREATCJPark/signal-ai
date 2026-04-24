#!/usr/bin/env python3
"""Run public-source crawlers only.

This is the GitHub Actions entrypoint. Discord stays local because it depends
on a local exporter/token and must not run on GitHub-hosted runners.
"""
from __future__ import annotations

import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PUBLIC_CRAWLERS = (
    "hn.py",
    "reddit.py",
    "arxiv.py",
    "hf_trending.py",
    "geeknews.py",
    "lesswrong.py",
)

HERE = Path(__file__).parent


def run(script: str) -> tuple[str, int, float, str]:
    started = time.time()
    try:
        result = subprocess.run(
            ["python3", str(HERE / script)],
            capture_output=True,
            text=True,
            timeout=180,
            cwd=HERE,
        )
        elapsed = time.time() - started
        output = (result.stderr + result.stdout).strip().split("\n")[-1]
        return script, result.returncode, elapsed, output
    except subprocess.TimeoutExpired:
        return script, -1, 180.0, "TIMEOUT"
    except Exception as exc:
        return script, -2, 0.0, str(exc)


def main() -> None:
    print(f"Running {len(PUBLIC_CRAWLERS)} public crawlers in parallel...", flush=True)
    results: list[tuple[str, int, float]] = []
    with ThreadPoolExecutor(max_workers=len(PUBLIC_CRAWLERS)) as pool:
        futures = [pool.submit(run, script) for script in PUBLIC_CRAWLERS]
        for future in as_completed(futures):
            script, rc, elapsed, output = future.result()
            status = "OK" if rc == 0 else "FAIL"
            print(f"  {status} {script} ({elapsed:.1f}s) - {output}", flush=True)
            results.append((script, rc, elapsed))

    ok = sum(1 for _, rc, _ in results if rc == 0)
    print(f"\nDone: {ok}/{len(PUBLIC_CRAWLERS)} succeeded")
    sys.exit(0 if ok == len(PUBLIC_CRAWLERS) else 1)


if __name__ == "__main__":
    main()
