#!/usr/bin/env python3
"""Discord — runs discord_export_text_only.py for last N days, parses messages."""
import sys, os, subprocess, re, hashlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from _common import post, save

ROOT = Path(__file__).parent.parent
CHANNEL_ID = "1365049274068631644"  # Dev Mode / general
GUILD = "Dev Mode"
CHANNEL = "general"
DAYS_BACK = 3
KST = timezone(timedelta(hours=9))

MSG_HEADER_RE = re.compile(r"^\[(\d{4})\. (\d{1,2})\. (\d{1,2})\. (오전|오후) (\d{1,2}):(\d{2})\]\s+(\S.*)$")

def parse_kst(y, mo, d, ampm, h, mi):
    hour = int(h)
    if ampm == "오후" and hour != 12: hour += 12
    if ampm == "오전" and hour == 12: hour = 0
    return datetime(int(y), int(mo), int(d), hour, int(mi), tzinfo=KST)

def run_export():
    """Run discord_export_text_only.py for last N days. Returns path to export file."""
    after = (datetime.now(KST) - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d 00:00:00")
    print(f"[discord] exporting --after-kst '{after}' (channel {CHANNEL_ID})", file=sys.stderr)

    r = subprocess.run([
        "python3", str(ROOT / "discord_export_text_only.py"),
        "--channel", CHANNEL_ID,
        "--after-kst", after,
        "--no-upload",
    ], capture_output=True, timeout=1800)
    # Decode with replacement (powershell sometimes outputs cp949 banner chars)
    stdout = r.stdout.decode("utf-8", errors="replace") if r.stdout else ""
    stderr = r.stderr.decode("utf-8", errors="replace") if r.stderr else ""
    r = subprocess.CompletedProcess(r.args, r.returncode, stdout, stderr)

    if r.returncode != 0:
        raise RuntimeError(f"Discord export failed: {r.stderr[-500:]}")

    # Parse 'final_file=...' from output
    for line in r.stdout.split("\n"):
        if line.startswith("final_file="):
            return Path(line.split("=", 1)[1].strip())
    raise RuntimeError(f"Could not locate final_file in output:\n{r.stdout[-500:]}")

def parse_export(path: Path):
    """Parse export file into individual message records."""
    text = path.read_text(encoding="utf-8")
    # Skip header (between two === lines)
    parts = text.split("=" * 62)
    body = parts[2] if len(parts) >= 3 else text

    posts = []
    current = None  # dict with header + lines accumulating
    for line in body.split("\n"):
        m = MSG_HEADER_RE.match(line)
        if m:
            # Flush previous
            if current and current["lines"]:
                posts.append(_build_post(current))
            y, mo, d, ampm, h, mi, author = m.groups()
            current = {
                "timestamp": parse_kst(y, mo, d, ampm, h, mi),
                "author": author.strip(),
                "lines": [],
            }
        elif current is not None:
            current["lines"].append(line)
    if current and current["lines"]:
        posts.append(_build_post(current))
    return posts

def _build_post(current):
    content = "\n".join(current["lines"]).strip()
    ts = current["timestamp"]
    # Stable id: hash(timestamp + author + first 100 chars of content)
    hash_input = f"{ts.isoformat()}|{current['author']}|{content[:100]}"
    source_id = hashlib.sha1(hash_input.encode("utf-8")).hexdigest()[:16]
    return post(
        source="discord",
        source_id=source_id,
        source_url=None,
        author=current["author"],
        content=content,
        timestamp=ts.astimezone(timezone.utc),
        metadata={"guild": GUILD, "channel": CHANNEL, "channel_id": CHANNEL_ID},
    )

if __name__ == "__main__":
    export_path = run_export()
    print(f"[discord] parsing {export_path}", file=sys.stderr)
    posts = parse_export(export_path)
    # Filter empty
    posts = [p for p in posts if p["content"].strip()]
    save("discord", posts)
