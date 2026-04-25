#!/usr/bin/env python3
"""Serve local crawler JSONL through a one-time tunnel and trigger GitHub Actions.

The local machine performs crawling only. GitHub Actions downloads the bundle and
uses repository secrets to upsert rows into Supabase `posts`.
"""
from __future__ import annotations

import argparse
import http.server
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CRAWLED_DIR = ROOT / "data" / "crawled"
CRAWLER_COMMANDS = (
    "crawlers/run_public.py",
    "crawlers/discord.py",
)
DEFAULT_REPO = "THEGREATCJPark/signal-ai"
DEFAULT_REF = "dev"
DEFAULT_WORKFLOW = "local-crawl-handoff.yml"
DEFAULT_TRIGGER_MODE = "secret-push"
BUNDLE_URL_SECRET = "LOCAL_CRAWL_BUNDLE_URL"
BATCH_SIZE_SECRET = "LOCAL_CRAWL_BATCH_SIZE"
TUNNEL_RE = re.compile(r"https://[-a-zA-Z0-9.]+\.trycloudflare\.com")


def ensure_local_only() -> None:
    if os.getenv("GITHUB_ACTIONS") == "true":
        raise SystemExit("This handoff must run on local WSL, not GitHub Actions.")


def run_crawlers(commands: tuple[str, ...] = CRAWLER_COMMANDS) -> None:
    for command in commands:
        subprocess.run([sys.executable, str(ROOT / command)], cwd=ROOT, check=True)


def today_jsonl_paths() -> list[Path]:
    today = time.strftime("%Y-%m-%d")
    return sorted(CRAWLED_DIR.glob(f"*-{today}.jsonl"))


def resolve_paths(paths: list[Path]) -> list[Path]:
    resolved = paths or today_jsonl_paths()
    if not resolved:
        raise SystemExit(f"No JSONL files found in {CRAWLED_DIR}")
    missing = [path for path in resolved if not path.exists()]
    if missing:
        joined = ", ".join(str(path) for path in missing)
        raise SystemExit(f"Missing JSONL file(s): {joined}")
    non_jsonl = [path for path in resolved if path.suffix != ".jsonl"]
    if non_jsonl:
        joined = ", ".join(str(path) for path in non_jsonl)
        raise SystemExit(f"Only .jsonl files can be handed off: {joined}")
    return sorted(path.resolve() for path in resolved)


def create_bundle(paths: list[Path], bundle_path: Path) -> None:
    with tarfile.open(bundle_path, "w:gz") as archive:
        for path in paths:
            archive.add(path, arcname=path.name)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def make_handler(bundle_path: Path, token: str):
    class HandoffHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib API
            if self.path != f"/bundle/{token}.tar.gz":
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/gzip")
            self.send_header("Content-Length", str(bundle_path.stat().st_size))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            with bundle_path.open("rb") as handle:
                shutil.copyfileobj(handle, self.wfile)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

    return HandoffHandler


def start_server(bundle_path: Path, token: str, port: int = 0):
    selected_port = port or _free_port()
    server = http.server.ThreadingHTTPServer(
        ("127.0.0.1", selected_port),
        make_handler(bundle_path, token),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, selected_port


def parse_tunnel_url(text: str) -> str | None:
    match = TUNNEL_RE.search(text)
    return match.group(0) if match else None


def start_cloudflared(port: int, timeout_s: int = 45) -> tuple[subprocess.Popen[str], str]:
    if not shutil.which("cloudflared"):
        raise RuntimeError("cloudflared is required for local handoff tunnel")

    proc = subprocess.Popen(
        ["cloudflared", "tunnel", "--url", f"http://127.0.0.1:{port}", "--no-autoupdate"],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    lines: list[str] = []
    deadline = time.time() + timeout_s
    assert proc.stdout is not None
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line:
            lines.append(line)
            url = parse_tunnel_url(line)
            if url:
                return proc, url
        elif proc.poll() is not None:
            break
        else:
            time.sleep(0.1)
    proc.terminate()
    raise RuntimeError("cloudflared did not produce a tunnel URL:\n" + "".join(lines[-20:]))


def trigger_workflow_dispatch(repo: str, ref: str, workflow: str, bundle_url: str, batch_size: int) -> None:
    subprocess.run(
        [
            "gh",
            "workflow",
            "run",
            workflow,
            "--repo",
            repo,
            "--ref",
            ref,
            "-f",
            f"bundle_url={bundle_url}",
            "-f",
            f"batch_size={batch_size}",
        ],
        cwd=ROOT,
        check=True,
    )


def set_repo_secret(repo: str, name: str, value: str) -> None:
    subprocess.run(
        ["gh", "secret", "set", name, "--repo", repo, "--body", value],
        cwd=ROOT,
        check=True,
    )


def trigger_secret_push(repo: str, ref: str, bundle_url: str, batch_size: int) -> None:
    set_repo_secret(repo, BUNDLE_URL_SECRET, bundle_url)
    set_repo_secret(repo, BATCH_SIZE_SECRET, str(batch_size))
    subprocess.run(
        ["git", "commit", "--allow-empty", "-m", "chore: trigger local crawl handoff"],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(["git", "push", "origin", f"HEAD:{ref}"], cwd=ROOT, check=True)


def trigger_workflow(
    repo: str,
    ref: str,
    workflow: str,
    bundle_url: str,
    batch_size: int,
    mode: str = DEFAULT_TRIGGER_MODE,
) -> str:
    if mode == "workflow-dispatch":
        trigger_workflow_dispatch(repo, ref, workflow, bundle_url, batch_size)
        return "workflow_dispatch"
    if mode == "secret-push":
        trigger_secret_push(repo, ref, bundle_url, batch_size)
        return "push"
    raise ValueError(f"Unknown trigger mode: {mode}")


def latest_run(repo: str, ref: str, workflow: str, event: str) -> dict[str, Any]:
    for _ in range(40):
        result = subprocess.run(
            [
                "gh",
                "run",
                "list",
                "--repo",
                repo,
                "--workflow",
                workflow,
                "--branch",
                ref,
                "--event",
                event,
                "--limit",
                "1",
                "--json",
                "databaseId,status,conclusion,url,createdAt",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        import json

        runs = json.loads(result.stdout)
        if runs:
            return runs[0]
        time.sleep(3)
    raise RuntimeError("No workflow_dispatch run appeared after trigger")


def watch_run(repo: str, run_id: int) -> None:
    subprocess.run(
        ["gh", "run", "watch", str(run_id), "--repo", repo, "--exit-status", "--interval", "5"],
        cwd=ROOT,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Local crawl JSONL -> GitHub Actions handoff")
    parser.add_argument("paths", nargs="*", type=Path, help="Optional JSONL files to hand off")
    parser.add_argument("--skip-crawl", action="store_true", help="Hand off existing JSONL files only")
    parser.add_argument("--repo", default=DEFAULT_REPO)
    parser.add_argument("--ref", default=DEFAULT_REF)
    parser.add_argument("--workflow", default=DEFAULT_WORKFLOW)
    parser.add_argument(
        "--trigger-mode",
        choices=("secret-push", "workflow-dispatch"),
        default=DEFAULT_TRIGGER_MODE,
        help="secret-push works while the workflow lives on dev only",
    )
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--no-wait", action="store_true", help="Trigger workflow and exit immediately")
    args = parser.parse_args()

    ensure_local_only()
    if not args.skip_crawl:
        run_crawlers()

    paths = resolve_paths(args.paths)
    token = secrets.token_urlsafe(32)
    tunnel_proc: subprocess.Popen[str] | None = None
    server = None
    with tempfile.TemporaryDirectory(prefix="first-light-handoff-") as tmp:
        bundle_path = Path(tmp) / "local-crawl.tar.gz"
        create_bundle(paths, bundle_path)
        print(f"Prepared {len(paths)} JSONL file(s), bundle {bundle_path.stat().st_size} bytes")

        server, port = start_server(bundle_path, token, port=args.port)
        try:
            tunnel_proc, tunnel_url = start_cloudflared(port)
            bundle_url = f"{tunnel_url}/bundle/{token}.tar.gz"
            event = trigger_workflow(
                args.repo,
                args.ref,
                args.workflow,
                bundle_url,
                args.batch_size,
                mode=args.trigger_mode,
            )
            if not args.no_wait:
                run = latest_run(args.repo, args.ref, args.workflow, event)
                print(f"Watching workflow run: {run['url']}")
                watch_run(args.repo, int(run["databaseId"]))
        finally:
            if server is not None:
                server.shutdown()
                server.server_close()
            if tunnel_proc is not None and tunnel_proc.poll() is None:
                tunnel_proc.terminate()
                try:
                    tunnel_proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    tunnel_proc.kill()


if __name__ == "__main__":
    main()
