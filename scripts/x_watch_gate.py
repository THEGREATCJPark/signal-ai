#!/usr/bin/env python3
"""Run the local X watch ingest at most once per interval."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

if os.name == "nt":
    import msvcrt
else:
    import fcntl


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from automation_gate import KST, coerce_return_code, default_runner, log_line, parse_iso


DEFAULT_STATE_PATH = ROOT / "data" / "x_watch_state.json"
DEFAULT_LOG_PATH = ROOT / "logs" / "x_watch.log"
DEFAULT_LOCK_PATH = ROOT / "data" / "x_watch.lock"
DEFAULT_MIN_INTERVAL_MINUTES = 60
DEFAULT_COMMAND = [
    str(ROOT / "scripts" / "x_watch_ingest.py"),
]


def try_lock(file_obj) -> bool:
    try:
        if os.name == "nt":
            msvcrt.locking(file_obj.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            fcntl.flock(file_obj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return True
    except OSError:
        return False


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
                "task": "x_watch",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def should_run(now: datetime, last_run_at: datetime | None, min_interval_minutes: int) -> tuple[bool, str]:
    now_kst = now.astimezone(KST)
    if last_run_at is None:
        return True, "no previous successful X watch run"
    last_kst = last_run_at.astimezone(KST)
    next_due = last_kst + timedelta(minutes=min_interval_minutes)
    if now_kst >= next_due:
        return True, f"interval elapsed: last run {last_kst.isoformat()}, due {next_due.isoformat()}"
    return False, f"next run due at {next_due.isoformat()}"


def run_command_if_due(
    *,
    root: Path = ROOT,
    state_path: Path = DEFAULT_STATE_PATH,
    log_path: Path = DEFAULT_LOG_PATH,
    lock_path: Path = DEFAULT_LOCK_PATH,
    now: datetime | None = None,
    command: list[str] | None = None,
    min_interval_minutes: int = DEFAULT_MIN_INTERVAL_MINUTES,
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
        if not try_lock(lock_file):
            log_line(log_file, "skip: another X watch run is active", now)
            return 0

        last_run_at = read_last_run_at(state_path)
        run, reason = should_run(now, last_run_at, min_interval_minutes)
        if not run:
            log_line(log_file, f"skip: {reason}", now)
            return 0

        log_line(log_file, f"X watch start: {reason}", now)
        rc = coerce_return_code(
            runner(
                command,
                cwd=str(root),
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        )
        log_line(log_file, f"X watch exit={rc}")
        if rc == 0:
            write_success_state(state_path, now)
        return rc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--min-interval-minutes", type=int, default=DEFAULT_MIN_INTERVAL_MINUTES)
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
        min_interval_minutes=args.min_interval_minutes,
        command=args.command,
    )


if __name__ == "__main__":
    sys.exit(main())
