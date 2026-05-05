#!/usr/bin/env python3
"""Linux-native Discord export (GH Actions runner 용).

DiscordChatExporter.Cli (dotnet)을 subprocess로 호출해 PlainText로 덤프.
출력 파일은 기존 discord_export_text_only.py와 같은 '=============...' 헤더 구조로 후처리.
run_hourly.py의 read_chat_text()와 호환.

사용:
  python3 discord_export_linux.py --channel ID --after-kst '2026-04-13 00:40:00' [--out /tmp/...]
  → 표준출력에 'final_file=<path>' 한 줄 출력 (기존 스크립트와 동일)

환경변수:
  DISCORD_TOKEN     (필수)
  DCE_BIN           (선택, 기본: 'dotnet-exec DiscordChatExporter.Cli.dll' 또는 'discordchatexporter-cli')
"""
from __future__ import annotations
import argparse, os, re, shutil, subprocess, sys, shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path

KST = timezone(timedelta(hours=9))
SEP = "=" * 62

MARKERS = {
    "{Attachments}", "{Reactions}", "{Embed}", "{Stickers}", "{Forwarded Message}",
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
MESSAGE_TS_RE = re.compile(
    r"^\[(\d{4})\. (\d{1,2})\. (\d{1,2})\. (오전|오후) (\d{1,2}):(\d{2})\]"
)


def dce_cmd() -> list[str]:
    """DiscordChatExporter.Cli 실행 커맨드 자동 탐지."""
    override = os.environ.get("DCE_BIN")
    if override:
        return shlex.split(override)
    for name in ("discordchatexporter-cli", "dce"):
        p = shutil.which(name)
        if p: return [p]
    # dotnet tool global: ~/.dotnet/tools/
    home = Path.home() / ".dotnet" / "tools"
    for name in ("DiscordChatExporter.Cli", "discordchatexporter-cli"):
        p = home / name
        if p.exists(): return [str(p)]
    raise RuntimeError(
        "DiscordChatExporter.Cli를 못 찾음. 'dotnet tool install --global DiscordChatExporter.Cli' 설치 필요 "
        "또는 DCE_BIN 환경변수로 경로 지정."
    )


def parse_kst(s: str) -> datetime:
    """'YYYY-MM-DD HH:MM:SS' (KST 가정) → aware datetime."""
    s = s.strip()
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    return dt.replace(tzinfo=KST) if dt.tzinfo is None else dt.astimezone(KST)


def run_dce_export(channel: str, after_kst: datetime, token: str, out_txt: Path) -> None:
    """DiscordChatExporter.Cli export 실행."""
    # DCE interprets --after as local time. Passing a pre-converted UTC string
    # makes Linux exports include extra old messages when the host timezone is KST.
    after_str = after_kst.astimezone(KST).strftime("%Y-%m-%d %H:%M")
    cmd = dce_cmd() + [
        "export",
        "-t", token,
        "-c", channel,
        "--after", after_str,
        "--format", "PlainText",
        "--locale", "ko-KR",
        "-o", str(out_txt),
    ]
    print(f"[dce] running: {' '.join(c if c != token else 'TOKEN' for c in cmd)}", file=sys.stderr)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    print(f"[dce] rc={r.returncode} stdout_len={len(r.stdout)} stderr_len={len(r.stderr)}", file=sys.stderr)
    if r.stdout: print(f"[dce] stdout:\n{r.stdout[:2000]}", file=sys.stderr)
    if r.stderr: print(f"[dce] stderr:\n{r.stderr[:2000]}", file=sys.stderr)
    if r.returncode != 0:
        raise RuntimeError(f"DCE failed rc={r.returncode}")
    if not out_txt.exists():
        raise RuntimeError(f"DCE produced no output file at {out_txt}")
    sz = out_txt.stat().st_size
    print(f"[dce] output file: {out_txt} ({sz} bytes)", file=sys.stderr)
    if sz > 0:
        print(f"[dce] head:\n{out_txt.read_text(encoding='utf-8', errors='replace')[:800]}", file=sys.stderr)


def clean_text(raw: str) -> tuple[str, dict]:
    """DCE PlainText 결과에서 header 추출 + body 정리.
    Returns: (body_text_lines, header_info).
    포맷은 기존 discord_export_text_only.py 출력과 호환되도록:
      SEP
      Guild: ...
      Channel: ...
      Topic: ...
      After: ...
      Before: ...
      SEP
      <messages>
    """
    lines = raw.splitlines()
    # DCE의 metadata 블록은 파일 앞부분 수십 줄 안에 "Guild: ...", "Channel: ..." 같은 포맷으로 있거나
    # 우리가 그냥 요약 헤더를 재작성해도 됨. 여기선 DCE의 자체 헤더(첫 4줄 쯤)를 SEP 래핑.
    header_lines = []
    body_start = 0
    for i, ln in enumerate(lines[:30]):
        if re.match(r"^(Guild|Channel|Topic|After|Before):", ln):
            header_lines.append(ln)
            body_start = i + 1
    body = "\n".join(lines[body_start:])
    # 불필요한 DCE marker 제거 (임시: 채팅 원문 유지)
    for m in MARKERS:
        body = body.replace(m, "")
    # inline URL 제거 (Discord CDN 링크만)
    body = INLINE_URL_RE.sub("", body)
    return body, {"header": header_lines}


def assemble_output(channel: str, after_kst: datetime, before_kst: datetime, body: str, header_lines: list) -> str:
    """기존 출력 포맷과 호환: SEP\n헤더\nSEP\n<body>"""
    hdr = "\n".join(header_lines) if header_lines else "\n".join([
        f"Guild: (unknown)",
        f"Channel: (unknown)",
    ])
    period = "오전" if after_kst.hour < 12 else "오후"
    def fmt_kst(dt: datetime) -> str:
        dt = dt.astimezone(KST)
        p = "오전" if dt.hour < 12 else "오후"
        h = dt.hour % 12 or 12
        return f"{dt.year}. {dt.month}. {dt.day}. {p} {h}:{dt.minute:02d}"
    hdr += f"\nAfter: {fmt_kst(after_kst)}\nBefore: {fmt_kst(before_kst)}"
    return f"{SEP}\n{hdr}\n{SEP}\n\n{body}\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", required=True)
    ap.add_argument("--after-kst", required=True, help="'YYYY-MM-DD HH:MM:SS' KST")
    ap.add_argument("--out", default=None, help="output .txt path (default: /tmp/signal_chat_<ts>.txt)")
    args = ap.parse_args()

    token = os.environ.get("DISCORD_TOKEN")
    if not token:
        print("ERROR: DISCORD_TOKEN 환경변수 필요", file=sys.stderr)
        sys.exit(2)

    after_kst = parse_kst(args.after_kst)
    now_kst = datetime.now(KST)
    out_path = Path(args.out) if args.out else Path(f"/tmp/signal_chat_{now_kst.strftime('%Y%m%d_%H%M%S')}.txt")

    # DCE 직접 덤프 → 포맷 그대로 사용 (SEP===header===SEP===body)
    tmp_raw = out_path.with_suffix(".dce.txt")
    run_dce_export(args.channel, after_kst, token, tmp_raw)
    raw = tmp_raw.read_text(encoding="utf-8")
    print(f"[linux] raw size: {len(raw)} chars", file=sys.stderr)
    # marker + CDN URL만 제거
    cleaned = raw
    for m in MARKERS:
        cleaned = cleaned.replace(m, "")
    cleaned = INLINE_URL_RE.sub("", cleaned)
    out_path.write_text(cleaned, encoding="utf-8")
    # SEP 기준 split 검증
    parts = cleaned.split("=" * 62)
    print(f"[linux] SEP sections: {len(parts)}, body(parts[2]) size: {len(parts[2]) if len(parts)>=3 else -1}", file=sys.stderr)
    print(f"final_file={out_path}")


if __name__ == "__main__":
    main()
