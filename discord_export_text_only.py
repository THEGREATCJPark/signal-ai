#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


KST = timezone(timedelta(hours=9))
SEP = "=" * 62
MARKERS = {
    "{Attachments}",
    "{Reactions}",
    "{Embed}",
    "{Stickers}",
    "{Forwarded Message}",
}
INLINE_URL_RE = re.compile(
    r"https://(?:"
    r"cdn\.discordapp\.com/attachments/"
    r"|images-ext-1\.discordapp\.net/external/"
    r"|cdn\.discordapp\.com/stickers/"
    r"|media\.discordapp\.net/attachments/"
    r")\S+"
)
LEADING_DECOR_RE = re.compile(r"^[^0-9A-Za-z가-힣]+")
WINDOWS_FORBIDDEN_RE = re.compile(r'[\\/:*?"<>|]+')
MESSAGE_TS_RE = re.compile(
    r"^\[(\d{4})\. (\d{1,2})\. (\d{1,2})\. (오전|오후) (\d{1,2}):(\d{2})\]"
)


SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = SCRIPT_DIR / "discord_export_config.env"


def load_env_file(path: Path) -> dict[str, str]:
    config: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", 1)
        config[key] = shlex.split(value, posix=True)[0]
    return config


def run(cmd: list[str], *, capture_output: bool = False) -> str:
    result = subprocess.run(
        cmd,
        check=True,
        text=True,
        capture_output=capture_output,
    )
    if capture_output:
        return result.stdout
    return ""


def powershell(command: str) -> str:
    out = run(
        ["powershell.exe", "-NoLogo", "-NoProfile", "-Command", command],
        capture_output=True,
    )
    return out.replace("\r", "").strip()


def now_kst() -> datetime:
    return parse_kst_string(powershell("Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz'"))


def parse_kst_string(value: str) -> datetime:
    value = value.strip()
    if re.search(r"[+-]\d{2}:\d{2}$", value):
        dt = datetime.fromisoformat(value)
        return dt.astimezone(KST)
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=KST)


def slug(dt: datetime) -> str:
    dt = dt.astimezone(KST)
    return dt.strftime("%Y-%m-%d_%H-%M-%S_KST")


def discord_header_dt(dt: datetime) -> str:
    dt = dt.astimezone(KST)
    period = "오전" if dt.hour < 12 else "오후"
    hour = dt.hour % 12 or 12
    return f"{dt.year}. {dt.month}. {dt.day}. {period} {hour}:{dt.minute:02d}"


def parse_message_ts(line: str) -> datetime:
    m = MESSAGE_TS_RE.match(line)
    if not m:
        raise ValueError(f"Could not parse message timestamp from: {line}")
    year, month, day, period, hour, minute = m.groups()
    hour_i = int(hour)
    if period == "오전":
        hour_24 = 0 if hour_i == 12 else hour_i
    else:
        hour_24 = 12 if hour_i == 12 else hour_i + 12
    return datetime(
        int(year),
        int(month),
        int(day),
        hour_24,
        int(minute),
        0,
        tzinfo=KST,
    )


def split_raw_sections(text: str) -> tuple[list[str], str]:
    first = text.find(SEP)
    second = text.find(SEP, first + len(SEP))
    if first != 0 or second < 0:
        raise ValueError("Unexpected exporter header format")
    header_end = second + len(SEP)
    header_lines = text[:header_end].splitlines()
    body = text[header_end:].lstrip("\r\n")
    return header_lines, body


def filter_blocks(text: str) -> list[str]:
    _, body = split_raw_sections(text)
    blocks = re.split(r"\n{3,}", body)
    kept: list[str] = []
    for block in blocks:
        stripped = block.strip("\n")
        if not stripped:
            continue
        lines = stripped.splitlines()
        if not lines or not lines[0].startswith("["):
            continue
        filtered = [lines[0]]
        for line in lines[1:]:
            if line.strip() in MARKERS:
                continue
            cleaned = INLINE_URL_RE.sub("", line).rstrip()
            if not cleaned.strip():
                continue
            if cleaned.strip() in MARKERS:
                continue
            filtered.append(cleaned)
        if any(line.strip() for line in filtered[1:]):
            kept.append("\n".join(filtered).rstrip())
    return kept


def sanitize_leaf(channel_line: str, fallback: str) -> str:
    leaf = channel_line.split("/", 1)[-1].strip()
    leaf = LEADING_DECOR_RE.sub("", leaf).strip()
    leaf = re.sub(r"\s+", "_", leaf)
    leaf = WINDOWS_FORBIDDEN_RE.sub("_", leaf)
    return leaf or fallback


def build_header(
    guild_line: str,
    channel_line: str,
    topic_line: str | None,
    after_dt: datetime | None,
    before_dt: datetime,
) -> str:
    lines = [SEP, guild_line, channel_line]
    if topic_line:
        lines.append(topic_line)
    if after_dt is not None:
        lines.append(f"After: {discord_header_dt(after_dt)}")
    lines.append(f"Before: {discord_header_dt(before_dt)}")
    lines.append(SEP)
    lines.append("")
    return "\n".join(lines)


def copy_to_checkpoint(source: Path, checkpoint_dir: Path) -> Path:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    dest = checkpoint_dir / source.name
    shutil.copy2(source, dest)
    return dest


def exporter_command(
    config: dict[str, str],
    *,
    channel_id: str,
    output_windows_path: str,
    before_dt: datetime,
    after_dt: datetime | None = None,
) -> list[str]:
    cmd = [
        config["EXPORTER_WSL_PATH"],
        "export",
        "-t",
        config["DISCORD_TOKEN"],
        "-c",
        channel_id,
        "-f",
        "PlainText",
        "-o",
        output_windows_path,
        "--before",
        before_dt.isoformat(timespec="seconds"),
        "--media",
        "false",
        "--markdown",
        "false",
        "--locale",
        "ko-KR",
    ]
    if after_dt is not None:
        cmd.extend(["--after", after_dt.isoformat(timespec="seconds")])
    return cmd


def main() -> int:
    parser = argparse.ArgumentParser(
        description="DiscordChatExporter wrapper that produces a text-only file and uploads it to Google Drive root.",
    )
    parser.add_argument("--channel", required=True, help="Discord channel ID")
    parser.add_argument(
        "--after-kst",
        help="Optional KST start time. Accepts 'YYYY-MM-DD HH:MM:SS' or with '+09:00'. Omit for full-history export.",
    )
    parser.add_argument(
        "--end-kst",
        help="Optional KST end time. Defaults to current Windows KST time.",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Skip Google Drive root upload.",
    )
    args = parser.parse_args()

    config = load_env_file(CONFIG_PATH)

    after_dt = parse_kst_string(args.after_kst) if args.after_kst else None
    export_end_dt = parse_kst_string(args.end_kst) if args.end_kst else now_kst()

    downloads_wsl_root = Path(config["DOWNLOADS_WSL_ROOT"])
    downloads_windows_root = config["DOWNLOADS_WINDOWS_ROOT"]
    checkpoint_root = Path(config["CHECKPOINT_ROOT"])

    base_raw_name = f"channel_{args.channel}__until_{slug(export_end_dt)}.raw.txt"
    base_raw_wsl = downloads_wsl_root / base_raw_name
    base_raw_windows = f"{downloads_windows_root}\\{base_raw_name}"

    print(f"[1/6] exporting raw history to {base_raw_windows}", flush=True)
    run(
        exporter_command(
            config,
            channel_id=args.channel,
            output_windows_path=base_raw_windows,
            before_dt=export_end_dt,
            after_dt=after_dt,
        )
    )

    checkpoint_dir = checkpoint_root / f"{export_end_dt.strftime('%Y-%m-%d')}-discord-export-{args.channel}"
    base_checkpoint = copy_to_checkpoint(base_raw_wsl, checkpoint_dir)

    base_text = base_checkpoint.read_text(encoding="utf-8")
    header_lines, _ = split_raw_sections(base_text)
    guild_line = next(line for line in header_lines if line.startswith("Guild: "))
    channel_line = next(line for line in header_lines if line.startswith("Channel: "))
    topic_line = next((line for line in header_lines if line.startswith("Topic: ")), None)
    first_message_line = next(line for line in base_text.splitlines() if line.startswith("["))
    first_message_dt = parse_message_ts(first_message_line)
    leaf_name = sanitize_leaf(channel_line, args.channel)

    latest_end_dt = export_end_dt
    tail_checkpoint: Path | None = None
    tail_text = ""
    tail_raw_wsl: Path | None = None

    post_export_now = now_kst()
    if post_export_now > export_end_dt:
        tail_name = (
            f"channel_{args.channel}__tail_{slug(export_end_dt)}__{slug(post_export_now)}.raw.txt"
        )
        tail_raw_wsl = downloads_wsl_root / tail_name
        tail_raw_windows = f"{downloads_windows_root}\\{tail_name}"
        print(f"[2/6] refreshing tail export to {tail_raw_windows}", flush=True)
        run(
            exporter_command(
                config,
                channel_id=args.channel,
                output_windows_path=tail_raw_windows,
                before_dt=post_export_now,
                after_dt=export_end_dt,
            )
        )
        tail_checkpoint = copy_to_checkpoint(tail_raw_wsl, checkpoint_dir)
        tail_text = tail_checkpoint.read_text(encoding="utf-8")
        latest_end_dt = post_export_now

    base_blocks = filter_blocks(base_text)
    tail_blocks = filter_blocks(tail_text) if tail_text else []

    start_dt = after_dt if after_dt is not None else first_message_dt
    final_name = f"{leaf_name}_{slug(start_dt)}__{slug(latest_end_dt)}.txt"
    final_wsl = downloads_wsl_root / final_name
    final_windows = f"{downloads_windows_root}\\{final_name}"

    print(f"[3/6] writing filtered text-only file {final_windows}", flush=True)
    header = build_header(guild_line, channel_line, topic_line, after_dt, latest_end_dt)
    final_text = header + "\n\n\n".join(base_blocks + tail_blocks).rstrip() + "\n"
    final_wsl.write_text(final_text, encoding="utf-8")

    if not args.no_upload:
        drive_windows = f"{config['GDRIVE_WINDOWS_ROOT']}\\{final_name}"
        print(f"[4/6] uploading to {drive_windows}", flush=True)
        powershell(
            f"Copy-Item -LiteralPath '{final_windows}' -Destination '{drive_windows}' -Force"
        )

    print("[5/6] cleaning temporary raw files from D:\\Downloads", flush=True)
    if base_raw_wsl.exists():
        base_raw_wsl.unlink()
    if tail_raw_wsl is not None and tail_raw_wsl.exists():
        tail_raw_wsl.unlink()

    checks = {
        marker: final_text.count(marker) for marker in MARKERS
    }
    checks.update(
        {
            "cdn_attachments": final_text.count("https://cdn.discordapp.com/attachments/"),
            "images_ext_external": final_text.count("https://images-ext-1.discordapp.net/external/"),
            "cdn_stickers": final_text.count("https://cdn.discordapp.com/stickers/"),
            "media_attachments": final_text.count("https://media.discordapp.net/attachments/"),
        }
    )

    print("[6/6] summary", flush=True)
    print(
        "\n".join(
            [
                f"channel_id={args.channel}",
                f"guild_line={guild_line}",
                f"channel_line={channel_line}",
                f"start_kst={start_dt.isoformat(timespec='seconds')}",
                f"end_kst={latest_end_dt.isoformat(timespec='seconds')}",
                f"base_checkpoint={base_checkpoint}",
                f"tail_checkpoint={tail_checkpoint}" if tail_checkpoint else "tail_checkpoint=",
                f"final_file={final_wsl}",
                f"final_bytes={final_wsl.stat().st_size}",
                f"blocks={len(base_blocks) + len(tail_blocks)}",
                f"checks={checks}",
            ]
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
