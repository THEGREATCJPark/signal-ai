#!/usr/bin/env python3
"""
Temporal layer — solves problems 2, 4, 6:

Problem 2 (deictic relation):
  "저번에 말한" → resolve to previous week's chunk
  Implementation: each chunk gets a (doc_id, week, ordinal) timeline position.
  Deictic words map to relative offsets on this timeline.

Problem 4 (duplicate counting):
  "a가 몇 번 언급됐는가" → dedup across overlapping chunks
  Implementation: each mention gets a (source_doc, char_offset) anchor.
  Same offset from same doc = same mention, counted once.

Problem 6 (ambiguous latest):
  "시험범위가 변경됐을 때 최신 것만"
  Implementation: for any topic, find ALL mentions sorted by time,
  return the LATEST. If contradictory, flag as "번복됨" with both versions.

All three share a common primitive: TIMELINE — a total order over chunks
with dedup-aware anchoring.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections import defaultdict
from typing import Optional


# ═══════════════════════════════════════════════════════════
# TIMELINE BUILDER
# ═══════════════════════════════════════════════════════════

WEEK_PATTERNS = [
    re.compile(r"week(\d+)", re.I),
    re.compile(r"(\d+)주차"),
    re.compile(r"(\d+)강"),
]

DEICTIC_PATTERNS = {
    "prev": re.compile(r"(저번|지난|이전|앞서|전에|전 시간|지난번|전번|작년|어제)"),
    "next": re.compile(r"(다음|이후|뒤에|후에|내일|다음 시간|다음번|내년)"),
    "same": re.compile(r"(아까|방금|위에서|지금|현재|이번|오늘)"),
    "numbered": re.compile(r"(\d+)\s*(?:주차|강|번째|회차|장|chapter|ch\.?)\s"),
}


def create_timeline_tables(con: sqlite3.Connection):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS timeline (
            chunk_id INTEGER PRIMARY KEY,
            doc_id INTEGER,
            week_num INTEGER,       -- parsed week number (1-based)
            ordinal INTEGER,        -- position within doc (chunk_idx)
            global_order INTEGER,   -- global timeline position
            source_kind TEXT,
            timestamp_hint TEXT     -- any date found in path/meta
        );
        CREATE TABLE IF NOT EXISTS deictic_refs (
            chunk_id INTEGER,
            deictic_word TEXT,
            direction TEXT,         -- prev/next/same/numbered
            target_week INTEGER,    -- resolved target week (NULL if unresolved)
            target_chunk_id INTEGER,-- resolved target chunk (NULL if unresolved)
            context TEXT,           -- surrounding text
            PRIMARY KEY (chunk_id, deictic_word, context)
        );
        CREATE TABLE IF NOT EXISTS mention_anchors (
            mention_id INTEGER PRIMARY KEY,
            chunk_id INTEGER,
            doc_id INTEGER,
            char_offset INTEGER,    -- offset in original doc
            mention_text TEXT,      -- the actual mentioned text
            topic TEXT,             -- normalized topic key
            week_num INTEGER
        );
        CREATE INDEX IF NOT EXISTS ix_timeline_week ON timeline(week_num);
        CREATE INDEX IF NOT EXISTS ix_timeline_order ON timeline(global_order);
        CREATE INDEX IF NOT EXISTS ix_mentions_topic ON mention_anchors(topic);
        CREATE INDEX IF NOT EXISTS ix_deictic ON deictic_refs(chunk_id);
    """)
    con.commit()


def build_timeline(con: sqlite3.Connection):
    """Assign every chunk a position on the global timeline."""
    create_timeline_tables(con)
    con.execute("DELETE FROM timeline")

    # Get all chunks with their doc metadata
    chunks = con.execute("""
        SELECT c.chunk_id, c.doc_id, c.idx, c.char_s,
               json_extract(d.meta, '$.week_key') as week_key,
               json_extract(d.meta, '$.source_kind') as source_kind,
               d.path
        FROM chunks c JOIN docs d ON c.doc_id = d.doc_id
        ORDER BY week_key, c.doc_id, c.idx
    """).fetchall()

    global_order = 0
    for cid, did, idx, char_s, week_key, source_kind, path in chunks:
        # Parse week number
        week_num = None
        if week_key:
            m = re.search(r"(\d+)", week_key)
            if m:
                week_num = int(m.group(1))
        if week_num is None and path:
            for pat in WEEK_PATTERNS:
                m = pat.search(path)
                if m:
                    week_num = int(m.group(1))
                    break

        # Extract date hint from path
        ts_hint = None
        if path:
            m = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2})", path)
            if m:
                ts_hint = m.group(1)

        con.execute(
            "INSERT OR REPLACE INTO timeline VALUES (?,?,?,?,?,?,?)",
            (cid, did, week_num, idx, global_order, source_kind or "", ts_hint),
        )
        global_order += 1

    con.commit()
    n = con.execute("SELECT COUNT(*) FROM timeline").fetchone()[0]
    weeks = con.execute("SELECT COUNT(DISTINCT week_num) FROM timeline WHERE week_num IS NOT NULL").fetchone()[0]
    print(f"  Timeline: {n} chunks, {weeks} distinct weeks")


# ═══════════════════════════════════════════════════════════
# DEICTIC RESOLVER
# ═══════════════════════════════════════════════════════════

def resolve_deictics(con: sqlite3.Connection):
    """Find deictic references and resolve them to target chunks."""
    con.execute("DELETE FROM deictic_refs")

    chunks = con.execute("""
        SELECT t.chunk_id, t.week_num, t.doc_id, c.text
        FROM timeline t JOIN chunks c ON t.chunk_id = c.chunk_id
    """).fetchall()

    resolved = 0
    unresolved = 0

    for cid, week_num, doc_id, text in chunks:
        for direction, pattern in DEICTIC_PATTERNS.items():
            for m in pattern.finditer(text):
                word = m.group(1)
                ctx_start = max(0, m.start() - 30)
                ctx_end = min(len(text), m.end() + 50)
                context = re.sub(r"\s+", " ", text[ctx_start:ctx_end]).strip()[:120]

                target_week = None
                target_chunk = None

                if direction == "prev" and week_num and week_num > 1:
                    target_week = week_num - 1
                elif direction == "next" and week_num:
                    target_week = week_num + 1
                elif direction == "same" and week_num:
                    target_week = week_num
                elif direction == "numbered":
                    num_match = re.search(r"(\d+)", word)
                    if num_match:
                        target_week = int(num_match.group(1))

                # Find a representative chunk from target week
                if target_week is not None:
                    row = con.execute(
                        "SELECT chunk_id FROM timeline WHERE week_num=? AND doc_id!=? LIMIT 1",
                        (target_week, doc_id),
                    ).fetchone()
                    if row:
                        target_chunk = row[0]
                        resolved += 1
                    else:
                        # Same doc, different position
                        row = con.execute(
                            "SELECT chunk_id FROM timeline WHERE week_num=? LIMIT 1",
                            (target_week,),
                        ).fetchone()
                        if row:
                            target_chunk = row[0]
                            resolved += 1
                        else:
                            unresolved += 1
                else:
                    unresolved += 1

                con.execute(
                    "INSERT OR IGNORE INTO deictic_refs VALUES (?,?,?,?,?,?)",
                    (cid, word, direction, target_week, target_chunk, context),
                )

    con.commit()
    total = con.execute("SELECT COUNT(*) FROM deictic_refs").fetchone()[0]
    print(f"  Deictics: {total} found, {resolved} resolved, {unresolved} unresolved")


# ═══════════════════════════════════════════════════════════
# DEDUP-AWARE MENTION COUNTING
# ═══════════════════════════════════════════════════════════

def build_mention_anchors(con: sqlite3.Connection, topic_pattern: str):
    """Find all mentions of a topic, anchored by (doc_id, char_offset) for dedup."""
    pat = re.compile(topic_pattern, re.I)

    chunks = con.execute("""
        SELECT c.chunk_id, c.doc_id, c.char_s, c.text, t.week_num
        FROM chunks c JOIN timeline t ON c.chunk_id = t.chunk_id
    """).fetchall()

    mentions = []
    seen_anchors = set()  # (doc_id, approximate_char_offset) for dedup

    for cid, did, char_s, text, week_num in chunks:
        for m in pat.finditer(text):
            # Anchor: doc_id + absolute char position
            abs_offset = (char_s or 0) + m.start()
            # Quantize to 50-char buckets for overlap dedup
            anchor = (did, abs_offset // 50)

            if anchor in seen_anchors:
                continue  # Duplicate from overlapping chunk
            seen_anchors.add(anchor)

            mention_text = text[max(0, m.start()-20):m.end()+30]
            mention_text = re.sub(r"\s+", " ", mention_text).strip()[:150]
            mentions.append((cid, did, abs_offset, mention_text, topic_pattern, week_num))

    return mentions


def count_mentions_deduped(con: sqlite3.Connection, topic: str) -> dict:
    """Count unique mentions of a topic, deduped across overlapping chunks."""
    mentions = build_mention_anchors(con, topic)

    # Group by week
    by_week = defaultdict(list)
    for cid, did, offset, text, _, week in mentions:
        by_week[week or 0].append({"chunk_id": cid, "doc_id": did, "offset": offset, "text": text})

    return {
        "topic": topic,
        "total_unique": len(mentions),
        "by_week": {w: len(ms) for w, ms in sorted(by_week.items())},
        "all_mentions": mentions,
    }


# ═══════════════════════════════════════════════════════════
# LATEST-FIRST QUERY (problem 6: 번복 추적)
# ═══════════════════════════════════════════════════════════

def find_latest_mention(con: sqlite3.Connection, topic_pattern: str) -> dict:
    """Find all mentions of a topic, sorted by time. Return latest first.
    If multiple weeks mention it, flag potential contradiction/update."""
    mentions = build_mention_anchors(con, topic_pattern)

    if not mentions:
        return {"topic": topic_pattern, "found": False, "mentions": []}

    # Sort by week (desc) then by ordinal (desc) = latest first
    sorted_mentions = sorted(
        mentions,
        key=lambda m: (m[5] or 0, m[2]),  # week_num, offset
        reverse=True,
    )

    # Detect potential contradiction: same topic in multiple weeks
    weeks_with_mentions = set(m[5] for m in mentions if m[5])
    contradiction = len(weeks_with_mentions) > 1

    latest = sorted_mentions[0]
    earliest = sorted_mentions[-1]

    return {
        "topic": topic_pattern,
        "found": True,
        "total_mentions": len(mentions),
        "latest": {
            "chunk_id": latest[0],
            "week": latest[5],
            "text": latest[3],
        },
        "earliest": {
            "chunk_id": earliest[0],
            "week": earliest[5],
            "text": earliest[3],
        },
        "contradiction_flag": contradiction,
        "weeks_mentioned": sorted(weeks_with_mentions),
        "note": "번복 가능성 있음 — 여러 주차에서 언급됨" if contradiction else "단일 시점 언급",
    }


# ═══════════════════════════════════════════════════════════
# INTEGRATED QUERY HELPERS
# ═══════════════════════════════════════════════════════════

def expand_with_deictics(con: sqlite3.Connection, chunk_id: int) -> list[dict]:
    """Given a chunk, find all deictic references and return linked chunks."""
    refs = con.execute(
        "SELECT deictic_word, direction, target_week, target_chunk_id, context "
        "FROM deictic_refs WHERE chunk_id=?",
        (chunk_id,),
    ).fetchall()

    results = []
    for word, direction, target_week, target_cid, context in refs:
        target_text = None
        if target_cid:
            row = con.execute("SELECT text FROM chunks WHERE chunk_id=?", (target_cid,)).fetchone()
            if row:
                target_text = row[0][:200]
        results.append({
            "deictic_word": word,
            "direction": direction,
            "target_week": target_week,
            "target_chunk_id": target_cid,
            "target_text_preview": target_text,
            "context": context,
        })
    return results


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    p1 = sub.add_parser("build")
    p1.add_argument("--db", required=True)

    p2 = sub.add_parser("deictics")
    p2.add_argument("--db", required=True)
    p2.add_argument("--chunk", type=int, default=None)

    p3 = sub.add_parser("count")
    p3.add_argument("--db", required=True)
    p3.add_argument("--topic", required=True)

    p4 = sub.add_parser("latest")
    p4.add_argument("--db", required=True)
    p4.add_argument("--topic", required=True)

    args = ap.parse_args()

    if args.cmd == "build":
        con = sqlite3.connect(args.db)
        print("[Timeline]", flush=True)
        build_timeline(con)
        print("[Deictics]", flush=True)
        resolve_deictics(con)
        con.close()

    elif args.cmd == "deictics":
        con = sqlite3.connect(args.db)
        if args.chunk:
            refs = expand_with_deictics(con, args.chunk)
            print(f"Deictics for chunk {args.chunk}: {len(refs)}")
            for r in refs:
                print(f"  '{r['deictic_word']}' ({r['direction']}) → week {r['target_week']}")
                print(f"    context: {r['context']}")
                if r['target_text_preview']:
                    print(f"    target: {r['target_text_preview'][:100]}...")
        else:
            # Show stats
            total = con.execute("SELECT COUNT(*) FROM deictic_refs").fetchone()[0]
            resolved = con.execute("SELECT COUNT(*) FROM deictic_refs WHERE target_chunk_id IS NOT NULL").fetchone()[0]
            print(f"Total deictics: {total}, resolved: {resolved}")
            for dir_type in ["prev", "next", "same", "numbered"]:
                n = con.execute("SELECT COUNT(*) FROM deictic_refs WHERE direction=?", (dir_type,)).fetchone()[0]
                print(f"  {dir_type}: {n}")
        con.close()

    elif args.cmd == "count":
        con = sqlite3.connect(args.db)
        result = count_mentions_deduped(con, args.topic)
        print(f"Topic '{args.topic}': {result['total_unique']} unique mentions (deduped)")
        print(f"By week: {result['by_week']}")
        for m in result['all_mentions'][:10]:
            print(f"  week {m[5]}, chunk {m[0]}: {m[3][:80]}")
        con.close()

    elif args.cmd == "latest":
        con = sqlite3.connect(args.db)
        result = find_latest_mention(con, args.topic)
        if result['found']:
            print(f"Topic '{args.topic}':")
            print(f"  Total unique mentions: {result['total_mentions']}")
            print(f"  Latest (week {result['latest']['week']}): {result['latest']['text'][:100]}")
            print(f"  Earliest (week {result['earliest']['week']}): {result['earliest']['text'][:100]}")
            print(f"  Weeks: {result['weeks_mentioned']}")
            print(f"  {result['note']}")
        else:
            print(f"Topic '{args.topic}': not found")
        con.close()
