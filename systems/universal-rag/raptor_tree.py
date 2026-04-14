#!/usr/bin/env python3
"""
RAPTOR-style hierarchical memory tree.

Builds a tree from bottom up:
  Level 0 (leaves): raw chunks (800 chars each)
  Level 1: groups of ~5 chunks summarized into one node (~400 chars)
  Level 2: groups of ~5 level-1 nodes summarized into one (~200 chars)
  Level 3 (root): single summary of the entire document/course

At query time, search can happen at ANY level — a broad query hits
level 2-3 (high-level summary), a specific query hits level 0 (raw chunk).

This is the "위계적 기억" the user described.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path


SUMMARIZE_PROMPT = """다음 {n}개의 텍스트 조각을 하나의 요약으로 합쳐주세요.

규칙:
- 핵심 개념, 용어, 수치를 보존
- 교수가 강조한 내용이 있으면 반드시 포함
- {max_chars}자 이내
- 요약만 출력, 다른 설명 금지

--- 텍스트 조각들 ---
{chunks_text}
---"""


def create_tree_tables(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS tree_nodes (
            node_id INTEGER PRIMARY KEY,
            level INTEGER,
            parent_id INTEGER,
            doc_id INTEGER,
            text TEXT,
            child_chunk_ids TEXT,
            created_at REAL
        );
        CREATE INDEX IF NOT EXISTS idx_tree_level ON tree_nodes(level);
        CREATE INDEX IF NOT EXISTS idx_tree_doc ON tree_nodes(doc_id);
    """)
    con.commit()


async def build_tree_for_doc(con, doc_id, client, group_size=5, max_levels=3):
    """Build RAPTOR tree for one document."""
    chunks = con.execute(
        "SELECT chunk_id, text FROM chunks WHERE doc_id=? ORDER BY chunk_idx",
        (doc_id,)
    ).fetchall()

    if len(chunks) < 3:
        return 0

    # Level 0: raw chunks are leaves (already in chunks table)
    current_level = [(cid, text) for cid, text in chunks]
    nodes_created = 0

    for level in range(1, max_levels + 1):
        if len(current_level) <= 1:
            break

        next_level = []
        max_chars = max(100, 400 // level)

        for i in range(0, len(current_level), group_size):
            group = current_level[i:i + group_size]
            if not group:
                continue

            child_ids = [str(g[0]) for g in group]
            combined = "\n---\n".join(g[1][:500] for g in group)

            prompt = SUMMARIZE_PROMPT.format(
                n=len(group),
                max_chars=max_chars,
                chunks_text=combined[:3000]
            )

            try:
                resp = await client.generate_content(prompt)
                summary = (resp.text or "").strip()[:max_chars + 100]
            except Exception:
                summary = combined[:max_chars]

            con.execute(
                "INSERT INTO tree_nodes (level, parent_id, doc_id, text, child_chunk_ids, created_at) "
                "VALUES (?, NULL, ?, ?, ?, ?)",
                (level, doc_id, summary, json.dumps(child_ids), time.time())
            )
            node_id = con.execute("SELECT last_insert_rowid()").fetchone()[0]
            next_level.append((node_id, summary))
            nodes_created += 1

        con.commit()
        current_level = next_level

    return nodes_created


async def build_raptor(db_path, course_filter=None, max_docs=5):
    """Build RAPTOR trees for documents in a course."""
    con = sqlite3.connect(db_path)
    create_tree_tables(con)

    if course_filter:
        cur = con.execute("""
            SELECT DISTINCT d.doc_id, d.path FROM documents d
            JOIN chunks c ON d.doc_id=c.doc_id
            WHERE json_extract(d.meta, '$.course_id')=?
            GROUP BY d.doc_id HAVING COUNT(c.chunk_id) >= 5
            ORDER BY COUNT(c.chunk_id) DESC
            LIMIT ?
        """, (course_filter, max_docs))
    else:
        cur = con.execute("""
            SELECT d.doc_id, d.path FROM documents d
            JOIN chunks c ON d.doc_id=c.doc_id
            GROUP BY d.doc_id HAVING COUNT(c.chunk_id) >= 5
            ORDER BY COUNT(c.chunk_id) DESC
            LIMIT ?
        """, (max_docs,))

    docs = cur.fetchall()
    print(f"Building RAPTOR trees for {len(docs)} documents", flush=True)

    ccon = sqlite3.connect("/tmp/ff_cookies.sqlite")
    ccur = ccon.cursor()
    ccur.execute("SELECT name, value FROM moz_cookies WHERE host=? AND name IN (?,?)",
                (".google.com", "__Secure-1PSID", "__Secure-1PSIDTS"))
    cookies = dict(ccur.fetchall()); ccon.close()

    from gemini_webapi import GeminiClient
    client = GeminiClient(cookies["__Secure-1PSID"], cookies["__Secure-1PSIDTS"])
    await client.init(timeout=30)

    total_nodes = 0
    for doc_id, path in docs:
        n_chunks = con.execute("SELECT COUNT(*) FROM chunks WHERE doc_id=?", (doc_id,)).fetchone()[0]
        nodes = await build_tree_for_doc(con, doc_id, client)
        total_nodes += nodes
        print(f"  doc {doc_id} ({n_chunks} chunks) → {nodes} tree nodes", flush=True)

    await client.close()
    con.close()
    print(f"\nRAPTOR done: {total_nodes} tree nodes across {len(docs)} docs")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    ap.add_argument("--course", default=None)
    ap.add_argument("--max-docs", type=int, default=3)
    args = ap.parse_args()
    asyncio.run(build_raptor(args.db, course_filter=args.course, max_docs=args.max_docs))
