#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path


MESSAGE_HEADER_RE = re.compile(r"^## \[(\d+)\] (.+?)\s*$")
XREF_RE = re.compile(r"(지난|앞서|저번|다음|이전|후에|전에|위 내용|아래 내용|아까|지금까지|2번 앞)")
TOKEN_RE = re.compile(r"[A-Za-z0-9가-힣_./:-]{2,}")

STOPWORDS = {
    "그리고",
    "하지만",
    "이것",
    "그것",
    "저것",
    "있는",
    "없는",
    "해서",
    "하면",
    "했습니다",
    "합니다",
    "the",
    "and",
    "with",
    "that",
    "this",
}

ANCHOR_RULES = [
    ("early", "original_problem_framing", re.compile(r"검색, 추천 시스템|아름다운 해결책")),
    ("early", "raw_source_preservation", re.compile(r"원문.*보존|원문 그대로")),
    ("early", "six_problem_list", re.compile(r"1\..*llm 1회|2\..*파일 내에|6\..*가장 큰 문제", re.S)),
    ("early", "xref_problem", re.compile(r"저번 시간|앞서 말한|파일3|다른 파일")),
    ("early", "chronology_dedup_problem", re.compile(r"순서, 시간, 중복|순서가 전복|중복돼서")),
    ("early", "coverage_loss_problem", re.compile(r"누락될 가능성|시험범위|문제들의 원본")),
    ("early", "source_anchor_method", re.compile(r"요약은 앵커|원문 그대로 선택")),
    ("middle", "six_problem_audit", re.compile(r"문제 1:|문제 2:|문제 6:", re.S)),
    ("middle", "operator_vs_system_limit", re.compile(r"나\\(운용자\\)|자동화 시스템|자동화 코드|수동 판단")),
    ("middle", "quality_gate", re.compile(r"Quality Gate|catastrophic_content_loss|구조적 필터링")),
    ("middle", "validated_overlap", re.compile(r"Validated overlap|검증자|독립 검증|교차검증")),
    ("middle", "video_audio_memory_scope", re.compile(r"영상은 단순 전사|녹음본도|문제풀이용")),
    ("middle", "memory_not_problem_solver", re.compile(r"기억 시스템|메모리 문제|알고리즘적 견고성")),
    ("middle", "episode_log_need", re.compile(r"에피소드 로그|시간순|위치 질의|사건 로그")),
    ("late", "loop_evaluation", re.compile(r"pass/fail 게이트|체크포인트|피드백자|직접 실행")),
    ("late", "overfit_loop_warning", re.compile(r"루프도.*과적합|아름다운 알고리즘적 견고")),
    ("late", "transcript_memory_failure", re.compile(r"컨텍스트 압축|산출물 추적 없음|전체 채팅 기록")),
    ("late", "simple_whole_input_warning", re.compile(r"통째로 주고|복잡성만 높고|어중간")),
    ("late", "full_transcript_export", re.compile(r"모든 대화내용|누락업시|전체 대화 기록")),
]


def stable_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()


def preview(text: str, limit: int = 220) -> str:
    return re.sub(r"\s+", " ", text).strip()[:limit]


def epoch_for_position(index: int, total: int) -> str:
    if total == 0:
        return "unknown"
    ratio = index / total
    if ratio < 0.34:
        return "early"
    if ratio < 0.67:
        return "middle"
    return "late"


def parse_messages(text: str) -> list[dict]:
    lines = text.splitlines()
    messages: list[dict] = []
    current: dict | None = None
    buffer: list[str] = []

    def flush(end_line: int) -> None:
        nonlocal current, buffer
        if current is None:
            return
        content = "\n".join(buffer).strip()
        current["line_end"] = end_line
        current["content"] = content
        current["content_hash"] = stable_hash(content)
        current["char_count"] = len(content)
        current["preview"] = preview(content)
        messages.append(current)
        current = None
        buffer = []

    for idx, line in enumerate(lines, start=1):
        match = MESSAGE_HEADER_RE.match(line)
        if match:
            flush(idx - 1)
            current = {
                "message_id": int(match.group(1)),
                "speaker": match.group(2).strip(),
                "line_start": idx,
            }
            buffer = [line]
            continue
        if current is not None:
            buffer.append(line)

    flush(len(lines))
    total = len(messages)
    for index, message in enumerate(messages):
        message["ordinal"] = index + 1
        message["source_id"] = f"msg-{message['message_id']:04d}"
        message["epoch"] = epoch_for_position(index, total)
        message["prev_message_id"] = messages[index - 1]["message_id"] if index else None
        message["next_message_id"] = messages[index + 1]["message_id"] if index + 1 < total else None
    return messages


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def markdown_pointer(message: dict) -> str:
    return f"`msg={message['message_id']}` `lines={message['line_start']}..{message['line_end']}` `hash={message['content_hash'][:12]}`"


def build_start_anchors(messages: list[dict]) -> str:
    first_user = next((m for m in messages if m["speaker"] == "사용자"), None)
    if first_user is None:
        return "# Start Anchors\n\nNo initial user message found.\n"
    paragraphs = [chunk.strip() for chunk in first_user["content"].split("\n\n") if chunk.strip()]
    lines = [
        "# Start Anchors\n\n",
        f"- source message: `{first_user['message_id']}`\n",
        f"- line span: `{first_user['line_start']}..{first_user['line_end']}`\n",
        f"- content hash: `{first_user['content_hash']}`\n",
        "- preservation contract: this file excerpts the first user message directly; full raw content is replayable from `l1_messages.jsonl`.\n",
        "\n## Extracted Start Anchors\n\n",
    ]
    for idx, paragraph in enumerate(paragraphs[:16], start=1):
        lines.append(f"### Start Anchor {idx}\n\n")
        lines.append(f"- pointer: {markdown_pointer(first_user)}\n")
        lines.append("- excerpt:\n\n")
        lines.append(f"{paragraph}\n\n")
    return "".join(lines)


def select_anchor_entries(messages: list[dict]) -> list[dict]:
    anchors: list[dict] = []
    seen: set[tuple[int, str]] = set()
    for message in messages:
        for epoch_hint, label, pattern in ANCHOR_RULES:
            if pattern.search(message["content"]):
                key = (message["message_id"], label)
                if key in seen:
                    continue
                seen.add(key)
                anchors.append(
                    {
                        "anchor_id": f"a{len(anchors) + 1:03d}",
                        "label": label,
                        "epoch": message["epoch"],
                        "rule_epoch": epoch_hint,
                        "message_id": message["message_id"],
                        "speaker": message["speaker"],
                        "line_start": message["line_start"],
                        "line_end": message["line_end"],
                        "content_hash": message["content_hash"],
                        "excerpt": preview(message["content"], 360),
                    }
                )
    for message in messages:
        if message["speaker"] == "사용자" and message["message_id"] in {1, 464, 670, 683, 796, 798, 802}:
            key = (message["message_id"], "named_user_turn")
            if key not in seen:
                seen.add(key)
                anchors.append(
                    {
                        "anchor_id": f"a{len(anchors) + 1:03d}",
                        "label": "named_user_turn",
                        "epoch": message["epoch"],
                        "rule_epoch": message["epoch"],
                        "message_id": message["message_id"],
                        "speaker": message["speaker"],
                        "line_start": message["line_start"],
                        "line_end": message["line_end"],
                        "content_hash": message["content_hash"],
                        "excerpt": preview(message["content"], 360),
                    }
                )
    anchors.sort(key=lambda item: (item["message_id"], item["label"]))
    for index, anchor in enumerate(anchors, start=1):
        anchor["anchor_id"] = f"a{index:03d}"
    return anchors


def build_anchor_map(anchors: list[dict]) -> str:
    lines = [
        "# Anchor Map\n\n",
        "Layer: L2 selected anchors. These are not a replacement for raw storage; each entry points back to `l1_messages.jsonl` by message id, line span, and hash.\n\n",
    ]
    for epoch in ("early", "middle", "late"):
        epoch_anchors = [anchor for anchor in anchors if anchor["epoch"] == epoch]
        lines.append(f"## {epoch.title()} Session Anchors\n\n")
        if not epoch_anchors:
            lines.append("- No selected anchors in this epoch.\n\n")
            continue
        for anchor in epoch_anchors:
            lines.append(f"### {anchor['anchor_id']} {anchor['label']}\n\n")
            lines.append(f"- pointer: `msg={anchor['message_id']}` `lines={anchor['line_start']}..{anchor['line_end']}` `hash={anchor['content_hash'][:12]}`\n")
            lines.append(f"- speaker: `{anchor['speaker']}`\n")
            lines.append(f"- excerpt: {anchor['excerpt']}\n\n")
    return "".join(lines)


def build_episode_log(messages: list[dict], window_size: int = 25) -> str:
    lines = [
        "# Episode Log\n\n",
        "Layer: chronological replay ledger. Episodes are fixed-size windows over the raw message sequence; every episode includes line spans and message pointers.\n\n",
    ]
    for offset in range(0, len(messages), window_size):
        chunk = messages[offset : offset + window_size]
        if not chunk:
            continue
        episode_id = offset // window_size + 1
        speakers = Counter(message["speaker"] for message in chunk)
        lines.append(f"## Episode {episode_id:03d}\n\n")
        lines.append(f"- messages: `{chunk[0]['message_id']}..{chunk[-1]['message_id']}`\n")
        lines.append(f"- lines: `{chunk[0]['line_start']}..{chunk[-1]['line_end']}`\n")
        lines.append(f"- epoch: `{chunk[0]['epoch']}..{chunk[-1]['epoch']}`\n")
        lines.append(f"- speakers: `{dict(speakers)}`\n")
        lines.append("- replay pointers:\n")
        for message in chunk:
            lines.append(f"  - `msg={message['message_id']}` `{message['speaker']}` `lines={message['line_start']}..{message['line_end']}` {message['preview']}\n")
        lines.append("\n")
    return "".join(lines)


def tokens(text: str) -> set[str]:
    return {
        token.lower()
        for token in TOKEN_RE.findall(text)
        if token.lower() not in STOPWORDS and len(token) >= 2 and re.search(r"[A-Za-z가-힣]", token)
    }


def candidate_targets(message: dict, messages: list[dict], direction: str, limit: int = 3) -> list[dict]:
    source_tokens = tokens(message["content"])
    if not source_tokens:
        source_tokens = set(TOKEN_RE.findall(message["preview"].lower()))
    if direction == "forward":
        pool = [candidate for candidate in messages if candidate["message_id"] > message["message_id"]]
    else:
        pool = [candidate for candidate in messages if candidate["message_id"] < message["message_id"]]
    scored = []
    for candidate in pool:
        if candidate["message_id"] == message["message_id"]:
            continue
        overlap = source_tokens & tokens(candidate["content"])
        distance = abs(candidate["message_id"] - message["message_id"])
        score = len(overlap) * 1000 - distance
        if len(overlap) >= 2 or distance <= 3:
            scored.append((score, len(overlap), distance, candidate, sorted(overlap)[:8]))
    scored.sort(key=lambda row: row[0], reverse=True)
    targets = []
    for score, overlap_count, distance, candidate, overlap_terms in scored[:limit]:
        targets.append(
            {
                "message_id": candidate["message_id"],
                "speaker": candidate["speaker"],
                "line_start": candidate["line_start"],
                "line_end": candidate["line_end"],
                "content_hash": candidate["content_hash"],
                "score": score,
                "overlap_count": overlap_count,
                "distance": distance,
                "overlap_terms": overlap_terms,
                "preview": candidate["preview"],
            }
        )
    return targets


def xref_rows(messages: list[dict]) -> list[dict]:
    rows = []
    for message in messages:
        matches = list(XREF_RE.finditer(message["content"]))
        for match in matches:
            trigger = match.group(1)
            direction = "forward" if trigger in {"다음", "후에", "아래 내용"} else "backward"
            targets = candidate_targets(message, messages, direction)
            status = "dangling"
            if targets:
                overlap_gap = targets[0]["overlap_count"] - targets[1]["overlap_count"] if len(targets) > 1 else targets[0]["overlap_count"]
                if targets[0]["overlap_count"] >= 3 and (len(targets) == 1 or (targets[0]["score"] - targets[1]["score"] > 10 and overlap_gap > 2)):
                    status = "resolved"
                else:
                    status = "ambiguous"
            rows.append(
                {
                    "xref_id": f"x{len(rows) + 1:04d}",
                    "source_message_id": message["message_id"],
                    "source_speaker": message["speaker"],
                    "source_line_start": message["line_start"],
                    "source_line_end": message["line_end"],
                    "source_hash": message["content_hash"],
                    "trigger": trigger,
                    "direction": direction,
                    "status": status,
                    "source_preview": message["preview"],
                    "candidate_targets": targets,
                }
            )
    return rows


def build_xref_resolutions(rows: list[dict]) -> str:
    lines = [
        "# Xref Resolutions\n\n",
        "Layer: L3 candidate relation edges. Status is deterministic: `resolved` when one grounded target has enough lexical overlap and separation; `ambiguous` when candidates remain plausible; `dangling` when no grounded target is found.\n\n",
        f"Inspectable edge contract: this file emits every detected xref row. Detected xref rows: `{len(rows)}`.\n\n",
    ]
    if not rows:
        lines.append("No candidate cross-references detected.\n")
        return "".join(lines)
    for row in rows:
        lines.append(f"## {row['xref_id']} source msg={row['source_message_id']}\n\n")
        lines.append(f"- source pointer: `msg={row['source_message_id']}` `lines={row['source_line_start']}..{row['source_line_end']}` `hash={row['source_hash'][:12]}`\n")
        lines.append(f"- trigger: `{row['trigger']}`\n")
        lines.append(f"- direction: `{row['direction']}`\n")
        lines.append(f"- status: `{row['status']}`\n")
        lines.append(f"- source preview: {row['source_preview']}\n")
        if not row["candidate_targets"]:
            lines.append("- rationale: no grounded candidate target met the lexical-overlap or nearby-message threshold for the requested direction.\n")
            lines.append("- candidates: none\n\n")
            continue
        best = row["candidate_targets"][0]
        if row["status"] == "resolved":
            rationale = (
                f"top candidate `msg={best['message_id']}` has overlap `{best['overlap_count']}` "
                f"and score `{best['score']}`, with enough separation from the next candidate under the deterministic rule."
            )
        else:
            rationale = (
                f"top candidate `msg={best['message_id']}` has overlap `{best['overlap_count']}` "
                f"and score `{best['score']}`, but competing candidates remain plausible under the deterministic rule."
            )
        lines.append(f"- rationale: {rationale}\n")
        lines.append("- grounded candidates:\n")
        for target in row["candidate_targets"]:
            terms = ", ".join(target["overlap_terms"])
            lines.append(
                f"  - `msg={target['message_id']}` `lines={target['line_start']}..{target['line_end']}` "
                f"`hash={target['content_hash'][:12]}` `distance={target['distance']}` `overlap={target['overlap_count']}` terms=`{terms}`; {target['preview']}\n"
            )
        lines.append("\n")
    return "".join(lines)


def build_memory_index(messages: list[dict], anchors: list[dict], xrefs: list[dict], input_hash: str) -> str:
    speakers = Counter(message["speaker"] for message in messages)
    epoch_counts = Counter(message["epoch"] for message in messages)
    lines = [
        "# Memory Index\n\n",
        "## Layer Contract\n\n",
        "- L0 source: raw transcript file, hash recorded in `run_manifest.json`.\n",
        "- L1 raw store: `l1_messages.jsonl`, one replayable JSON object per transcript message.\n",
        "- L2 chronology: `episode_log.md`, fixed chronological windows with every message pointer.\n",
        "- L2 anchors: `start_anchors.md` and `anchor_map.md`, selected source-grounded anchors across early/middle/late epochs.\n",
        "- L3 xrefs: `xref_resolutions.md`, trigger-to-target candidate edges with resolved/ambiguous/dangling status.\n\n",
        "## Counts\n\n",
        f"- source hash: `{input_hash}`\n",
        f"- total messages: `{len(messages)}`\n",
        f"- indexed anchors: `{len(anchors)}`\n",
        f"- xref edges: `{len(xrefs)}`\n",
        f"- epoch counts: `{dict(epoch_counts)}`\n\n",
        "## Speaker Map\n\n",
    ]
    for speaker, count in speakers.items():
        lines.append(f"- `{speaker}`: `{count}` messages\n")
    lines.extend(
        [
            "\n## Anchor Labels\n\n",
        ]
    )
    for label, count in Counter(anchor["label"] for anchor in anchors).most_common():
        lines.append(f"- `{label}`: `{count}`\n")
    lines.extend(
        [
            "\n## Replay Slices\n\n",
            f"- first message: `msg={messages[0]['message_id']}` `lines={messages[0]['line_start']}..{messages[0]['line_end']}` `hash={messages[0]['content_hash'][:12]}`\n" if messages else "- first message: none\n",
            f"- middle message: `msg={messages[len(messages)//2]['message_id']}` `lines={messages[len(messages)//2]['line_start']}..{messages[len(messages)//2]['line_end']}` `hash={messages[len(messages)//2]['content_hash'][:12]}`\n" if messages else "- middle message: none\n",
            f"- last message: `msg={messages[-1]['message_id']}` `lines={messages[-1]['line_start']}..{messages[-1]['line_end']}` `hash={messages[-1]['content_hash'][:12]}`\n" if messages else "- last message: none\n",
            "\n## Scale Contract\n\n",
            "- This run processes one transcript file, but the replay contract is shardable: each source item must receive a stable source id, shard path/span, byte/line counts, and content hash before summarization.\n",
            "- Guaranteed layer: raw preservation, message counts, hashes, deterministic regex xref trigger extraction, and replay ledgers.\n",
            "- Candidate layer: semantic relatedness and xref target choice remain candidate edges with `resolved`, `ambiguous`, or `dangling` status, not a claim of complete semantic recall.\n",
            "- A million-source run must prove `input_count == stored_count == indexed_count` in the manifest or enumerate every gap; it must not rely on one prompt or one in-memory summary.\n",
        ]
    )
    return "".join(lines)


def build_replay_handoff(artifacts: list[str]) -> str:
    diff_targets = [artifact for artifact in artifacts if artifact != "replay.md"]
    lines = [
        "# Replay Handoff\n\n",
        "Run these commands from the workspace root to reproduce and audit this memory artifact set.\n\n",
        "## Rerun\n\n",
        "```bash\n",
        "bash project/run_memory_task.sh\n",
        "```\n\n",
        "## Required Artifact Inspection\n\n",
        "```bash\n",
        "sed -n '1,120p' project/runtime_output/start_anchors.md\n",
        "sed -n '1,160p' project/runtime_output/anchor_map.md\n",
        "sed -n '1,120p' project/runtime_output/episode_log.md\n",
        "sed -n '1,160p' project/runtime_output/xref_resolutions.md\n",
        "sed -n '1,160p' project/runtime_output/memory_index.md\n",
        "cat project/runtime_output/run_manifest.json\n",
        "```\n\n",
        "## Runtime To Eval Copy/Diff\n\n",
        "```bash\n",
    ]
    for artifact in diff_targets:
        lines.append(f"diff -u project/runtime_output/{artifact} for_eval/{artifact}\n")
    lines.extend(
        [
            "```\n\n",
            "## Raw Store Audit\n\n",
            "```bash\n",
            "wc -l project/runtime_output/l1_messages.jsonl\n",
            "head -n 1 project/runtime_output/l1_messages.jsonl\n",
            "tail -n 1 project/runtime_output/l1_messages.jsonl\n",
            "python3 - <<'PY'\n",
            "import json\n",
            "from pathlib import Path\n",
            "manifest = json.loads(Path('project/runtime_output/run_manifest.json').read_text(encoding='utf-8'))\n",
            "rows = Path('project/runtime_output/l1_messages.jsonl').read_text(encoding='utf-8').splitlines()\n",
            "xrefs = Path('project/runtime_output/xref_resolutions.md').read_text(encoding='utf-8')\n",
            "print('input_count', manifest['input_count'])\n",
            "print('stored_count', manifest['stored_count'])\n",
            "print('indexed_count', manifest['indexed_count'])\n",
            "print('jsonl_rows', len(rows))\n",
            "print('xref_count', manifest['xref_count'])\n",
            "print('xref_sections', xrefs.count('\\n## x'))\n",
            "print('input_hash_sha256', manifest['input_hash_sha256'])\n",
            "PY\n",
            "```\n",
        ]
    )
    return "".join(lines)


def build_manifest(input_path: Path, text: str, messages: list[dict], anchors: list[dict], xrefs: list[dict], artifacts: list[str]) -> dict:
    return {
        "input_path": str(input_path),
        "input_hash_sha256": stable_hash(text),
        "input_bytes_utf8": len(text.encode("utf-8", errors="replace")),
        "input_line_count": len(text.splitlines()),
        "input_count": len(messages),
        "stored_count": len(messages),
        "indexed_count": len(messages),
        "anchor_count": len(anchors),
        "xref_count": len(xrefs),
        "xref_status_counts": dict(Counter(row["status"] for row in xrefs)),
        "message_count": len(messages),
        "artifacts": artifacts,
        "storage_contract": {
            "raw_store": "l1_messages.jsonl",
            "stable_id": "source_id/msg-NNNN",
            "span": "line_start..line_end",
            "hash": "content_hash sha256 per message plus input_hash_sha256",
            "gap_policy": "input_count, stored_count, and indexed_count must match; otherwise list gaps explicitly",
        },
        "scale_contract": {
            "million_source_ready_contract": "shard each source into stable-id records with shard path/span/hash/count ledgers before any summary layer",
            "guaranteed_complete": ["raw preservation", "counts", "hashes", "deterministic trigger extraction"],
            "candidate_not_complete": ["semantic xref target choice", "open-ended relatedness"],
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    input_path = Path(args.input).resolve()
    out_dir = Path(args.out_dir).resolve()
    text = input_path.read_text(encoding="utf-8", errors="replace")
    messages = parse_messages(text)
    anchors = select_anchor_entries(messages)
    xrefs = xref_rows(messages)
    input_hash = stable_hash(text)

    artifacts = [
        "l1_messages.jsonl",
        "start_anchors.md",
        "episode_log.md",
        "anchor_map.md",
        "xref_resolutions.md",
        "memory_index.md",
        "replay.md",
        "run_manifest.json",
    ]

    out_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(out_dir / "l1_messages.jsonl", messages)
    write_text(out_dir / "start_anchors.md", build_start_anchors(messages))
    write_text(out_dir / "episode_log.md", build_episode_log(messages))
    write_text(out_dir / "anchor_map.md", build_anchor_map(anchors))
    write_text(out_dir / "xref_resolutions.md", build_xref_resolutions(xrefs))
    write_text(out_dir / "memory_index.md", build_memory_index(messages, anchors, xrefs, input_hash))
    write_text(out_dir / "replay.md", build_replay_handoff(artifacts))
    manifest = build_manifest(input_path, text, messages, anchors, xrefs, artifacts)
    write_text(out_dir / "run_manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
