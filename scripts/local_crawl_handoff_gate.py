#!/usr/bin/env python3
"""Run local crawl handoff once per KST day after the due hour."""
from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from automation_gate import KST, coerce_return_code, default_runner, log_line, parse_iso, should_run


DEFAULT_STATE_PATH = ROOT / "data" / "local_crawl_handoff_state.json"
DEFAULT_LOG_PATH = ROOT / "logs" / "local_crawl_handoff.log"
DEFAULT_LOCK_PATH = ROOT / "data" / "local_crawl_handoff.lock"
DEFAULT_DUE_HOUR = 7
DEFAULT_COMMAND = [
    str(ROOT / "scripts" / "dispatch_local_crawl_handoff.py"),
]


def read_last_run_at(state_path: Path) -> datetime | None:
    try:
        data = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return parse_iso(data.get("last_run_at"))


def write_success_state(state_path: Path, when: datetime) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(
        json.dumps(
            {
                "last_run_at": when.astimezone(KST).isoformat(),
                "status": "success",
                "task": "local_crawl_handoff",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def run_command_if_due(
    *,
    root: Path = ROOT,
    state_path: Path = DEFAULT_STATE_PATH,
    log_path: Path = DEFAULT_LOG_PATH,
    lock_path: Path = DEFAULT_LOCK_PATH,
    now: datetime | None = None,
    command: list[str] | None = None,
    due_hour: int = DEFAULT_DUE_HOUR,
    runner=default_runner,
) -> int:
    root = Path(root)
    state_path = Path(state_path)
    log_path = Path(log_path)
    lock_path = Path(lock_path)
    command = command or DEFAULT_COMMAND
    now = (now or datetime.now(KST)).astimezone(KST)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("w", encoding="utf-8") as lock_file, log_path.open("a", encoding="utf-8") as log_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log_line(log_file, "skip: another local crawl handoff is active", now)
            return 0

        last_run_at = read_last_run_at(state_path)
        decision = should_run(now, last_run_at, publish_hour=due_hour)
        if not decision.run:
            log_line(log_file, f"skip: {decision.reason}", now)
            return 0

        log_line(log_file, f"local crawl handoff start: {decision.reason}", now)
        rc = coerce_return_code(
            runner(
                command,
                cwd=str(root),
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        )
        log_line(log_file, f"local crawl handoff exit={rc}")
        if rc == 0:
            write_success_state(state_path, now)
        return rc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--due-hour", type=int, default=DEFAULT_DUE_HOUR)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        args.command = DEFAULT_COMMAND
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run_command_if_due(
        root=args.root,
        state_path=args.state,
        log_path=args.log,
        lock_path=args.lock,
        due_hour=args.due_hour,
        command=args.command,
    )


if __name__ == "__main__":
    sys.exit(main())
