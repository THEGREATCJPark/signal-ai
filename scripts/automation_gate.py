#!/usr/bin/env python3
"""Run the daily publisher only when the KST daily slot is still missing."""
from __future__ import annotations

import argparse
import fcntl
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple


ROOT = Path(__file__).resolve().parents[1]
KST = timezone(timedelta(hours=9))
DEFAULT_STATE_PATH = ROOT / "articles.json"
DEFAULT_LOG_PATH = ROOT / "logs" / "signal_daily.log"
DEFAULT_LOCK_PATH = ROOT / "data" / "automation.lock"
DEFAULT_PUBLISH_HOUR = 8


class Decision(NamedTuple):
    run: bool
    reason: str


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed.astimezone(KST)


def read_last_run_at(state_path: Path) -> datetime | None:
    try:
        data = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except Exception:
        return None
    return parse_iso(data.get("last_run_at"))


def should_run(now: datetime, last_run_at: datetime | None, publish_hour: int = DEFAULT_PUBLISH_HOUR) -> Decision:
    now_kst = now.astimezone(KST)
    due_today = now_kst.replace(hour=publish_hour, minute=0, second=0, microsecond=0)
    if now_kst < due_today:
        return Decision(False, f"not due until {due_today.isoformat()}")

    if last_run_at is not None:
        last_kst = last_run_at.astimezone(KST)
        if last_kst.date() == now_kst.date():
            return Decision(False, f"already ran today at {last_kst.isoformat()}")
        return Decision(True, f"catch-up due: last run {last_kst.isoformat()}, target {due_today.isoformat()}")

    return Decision(True, f"catch-up due: no previous run, target {due_today.isoformat()}")


def default_runner(command: list[str], **kwargs) -> int:
    return subprocess.run(command, **kwargs).returncode


def coerce_return_code(result) -> int:
    if hasattr(result, "returncode"):
        return int(result.returncode)
    return int(result)


def log_line(log_file, message: str, now: datetime | None = None) -> None:
    ts = (now or datetime.now(KST)).astimezone(KST).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{ts}] {message}", file=log_file, flush=True)


def run_command_if_due(
    *,
    root: Path = ROOT,
    state_path: Path = DEFAULT_STATE_PATH,
    log_path: Path = DEFAULT_LOG_PATH,
    lock_path: Path = DEFAULT_LOCK_PATH,
    now: datetime | None = None,
    command: list[str] | None = None,
    publish_hour: int = DEFAULT_PUBLISH_HOUR,
    runner=default_runner,
) -> int:
    root = Path(root)
    state_path = Path(state_path)
    log_path = Path(log_path)
    lock_path = Path(lock_path)
    command = command or ["./run_cron.sh"]
    now = now or datetime.now(KST)

    log_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("w", encoding="utf-8") as lock_file, log_path.open("a", encoding="utf-8") as log_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            log_line(log_file, "skip: another automation run is active", now)
            return 0

        last_run_at = read_last_run_at(state_path)
        decision = should_run(now, last_run_at, publish_hour=publish_hour)
        if not decision.run:
            log_line(log_file, f"skip: {decision.reason}", now)
            return 0

        log_line(log_file, f"task gate start: {decision.reason}", now)
        rc = coerce_return_code(
            runner(
                command,
                cwd=str(root),
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        )
        log_line(log_file, f"task gate exit={rc}")
        return rc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--log", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--publish-hour", type=int, default=DEFAULT_PUBLISH_HOUR)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        args.command = ["./run_cron.sh"]
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run_command_if_due(
        root=args.root,
        state_path=args.state,
        log_path=args.log,
        lock_path=args.lock,
        publish_hour=args.publish_hour,
        command=args.command,
    )


if __name__ == "__main__":
    sys.exit(main())
