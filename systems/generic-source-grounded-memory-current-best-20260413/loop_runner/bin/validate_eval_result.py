#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

REQUIRED = {"all_pass", "fail_tags", "pass_tags", "evidence", "verdict"}
VALID_VERDICTS = {"terminate", "progress", "regress", "no_progress"}


def main() -> int:
    path = Path(sys.argv[1])
    payload = json.loads(path.read_text(encoding="utf-8"))
    missing = REQUIRED - set(payload)
    if missing:
        raise SystemExit(f"missing keys: {sorted(missing)}")
    if payload["verdict"] not in VALID_VERDICTS:
        raise SystemExit(f"bad verdict: {payload['verdict']}")
    if not isinstance(payload["fail_tags"], list) or not isinstance(payload["pass_tags"], list):
        raise SystemExit("fail_tags/pass_tags must be arrays")
    if not isinstance(payload["evidence"], list):
        raise SystemExit("evidence must be an array")
    if "issues" in payload and not isinstance(payload["issues"], list):
        raise SystemExit("issues must be an array")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
