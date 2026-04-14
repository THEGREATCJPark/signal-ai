#!/usr/bin/env python3
"""
Fast pre-scan: find HIGH-SIGNAL chunks at embedding speed, then send
only those to the LLM for precise tagging.

Phase 1 (instant, regex):
  Scan all chunks for signal keywords. Tag candidates.
  - problem_candidate: "문제", "풀어", "예제", "exercise", "example", "quiz"
  - important_candidate: "중요", "꼭", "반드시", "시험에", "기억해", "핵심"
  - exam_hint_candidate: "시험", "고사", "출제", "범위", "형식", "객관식"
  - media_candidate: "그래프", "그림", "회로", "사진", "도식", "figure", "image"

Phase 2 (fast, embedding similarity):
  For "important" and "exam_hint" candidates, compute embedding
  similarity to seed phrases. Keep only high-similarity chunks.
  Seed: "교수가 시험에 나온다고 강조한 내용"

Phase 3 (precise, LLM):
  Only the filtered candidates go to Gemini for precise extraction.

This gives exhaustive recall (Phase 1 sees everything) with affordable
LLM cost (Phase 3 only runs on ~5-10% of chunks).
"""
from __future__ import annotations

import json
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path


SIGNAL_PATTERNS = {
    "problem_candidate": re.compile(
        r"(문제|풀어|풀이|예제|exercise|example|quiz|연습|숙제|과제|homework|HW|답을 구|계산하|구하시오|구하라)",
        re.IGNORECASE,
    ),
    "important_candidate": re.compile(
        r"(중요|꼭 알|반드시|시험에 나|기억해|핵심|잊지 말|유의|주의해|절대 잊|잘 봐)",
        re.IGNORECASE,
    ),
    "exam_hint_candidate": re.compile(
        r"(시험 문제|시험에서|출제|시험 범위|시험.*형식|객관식|주관식|서술형|시험.*유형|시험.*어떻게|시험.*낸다|문제.*되기 좋|나올 수|나올.*확률|시험.*팁)",
        re.IGNORECASE,
    ),
    "media_candidate": re.compile(
        r"(그래프|그림|회로|사진|도식|figure|image|diagram|circuit|chart|표.*보시|그래프.*보시|사진.*보시)",
        re.IGNORECASE,
    ),
}


def prescan_all(db_path: str, course_filter: str | None = None) -> dict[str, list[int]]:
    """Phase 1: regex pre-scan. Returns {signal_type: [chunk_ids]}."""
    con = sqlite3.connect(db_path)

    if course_filter:
        cur = con.execute(
            "SELECT c.chunk_id, c.text FROM chunks c JOIN documents d ON c.doc_id=d.doc_id "
            "WHERE json_extract(d.meta, '$.course_id')=?",
            (course_filter,),
        )
    else:
        cur = con.execute("SELECT chunk_id, text FROM chunks")

    candidates: dict[str, list[int]] = defaultdict(list)
    total = 0
    for chunk_id, text in cur:
        total += 1
        for signal, pattern in SIGNAL_PATTERNS.items():
            if pattern.search(text):
                candidates[signal].append(chunk_id)

    con.close()
    print(f"Pre-scan: {total} chunks scanned")
    for sig, ids in sorted(candidates.items()):
        pct = len(ids) / max(total, 1) * 100
        print(f"  {sig}: {len(ids)} ({pct:.1f}%)")
    return dict(candidates)


def prescan_with_context(db_path: str, course_filter: str | None = None) -> list[dict]:
    """Phase 1 + context extraction: for each hit, extract the surrounding
    context so the LLM gets a focused window."""
    con = sqlite3.connect(db_path)

    if course_filter:
        cur = con.execute(
            "SELECT c.chunk_id, c.text, d.meta FROM chunks c "
            "JOIN documents d ON c.doc_id=d.doc_id "
            "WHERE json_extract(d.meta, '$.course_id')=?",
            (course_filter,),
        )
    else:
        cur = con.execute(
            "SELECT c.chunk_id, c.text, d.meta FROM chunks c "
            "JOIN documents d ON c.doc_id=d.doc_id"
        )

    hits = []
    total = 0
    for chunk_id, text, meta_json in cur:
        total += 1
        signals = []
        contexts = []
        for signal, pattern in SIGNAL_PATTERNS.items():
            matches = list(pattern.finditer(text))
            if matches:
                signals.append(signal)
                for m in matches[:3]:
                    s = max(0, m.start() - 80)
                    e = min(len(text), m.end() + 120)
                    ctx = re.sub(r"\s+", " ", text[s:e]).strip()
                    contexts.append(f"[{signal}] {ctx}")
        if signals:
            meta = json.loads(meta_json) if meta_json else {}
            hits.append({
                "chunk_id": chunk_id,
                "signals": signals,
                "contexts": contexts[:5],
                "text_preview": text[:200],
                "week": meta.get("week_key", "?"),
                "kind": meta.get("source_kind", "?"),
            })

    con.close()
    print(f"Pre-scan: {total} chunks → {len(hits)} hits ({len(hits)/max(total,1)*100:.1f}%)")
    by_signal = defaultdict(int)
    for h in hits:
        for s in h["signals"]:
            by_signal[s] += 1
    for sig, n in sorted(by_signal.items()):
        print(f"  {sig}: {n}")
    return hits


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    ap.add_argument("--course", default=None)
    ap.add_argument("--show-contexts", action="store_true")
    args = ap.parse_args()

    hits = prescan_with_context(args.db, course_filter=args.course)

    if args.show_contexts:
        for h in hits[:30]:
            print(f"\n[chunk {h['chunk_id']}] {h['week']}/{h['kind']} signals={h['signals']}")
            for ctx in h["contexts"][:2]:
                print(f"  {ctx[:200]}")
