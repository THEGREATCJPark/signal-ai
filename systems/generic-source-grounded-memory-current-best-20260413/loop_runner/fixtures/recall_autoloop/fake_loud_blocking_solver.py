#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--round", type=int, required=True)
    args = parser.parse_args()

    workspace = Path(args.workspace)
    # Write enough data to fill a normal OS pipe if the runner does not drain it.
    for _ in range(4096):
        sys.stdout.write("x" * 1024 + "\n")
    sys.stdout.flush()

    done = workspace / "solver_result.json"
    done.write_text(
        json.dumps({"round": args.round, "status": "ready"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    time.sleep(4)


if __name__ == "__main__":
    main()
