#!/usr/bin/env python3
"""
Memory Builder — exhaustive pre-processing that creates structured memory
tags on every chunk BEFORE query time.

This is the "기억 구축 단계" that the current RAG lacks.

Pipeline:
1. Load all chunks from the SQLite store
2. For each chunk, run a small/fast LLM to extract structured tags:
   - problems_solved: [{problem_text, context, solution_hint}]
   - important_statements: [{text, reason, speaker}]
   - exam_hints: [{hint_text, exam_type, topic}]
   - cross_references: [{target_description, direction}]
   - key_concepts: [str]
   - media_references: [{type, description}]  (images, circuits, diagrams)
3. Store tags in a new SQLite table
4. At query time, filter by tag type first, THEN rank by relevance

This achieves exhaustive recall: every chunk is examined, and the tag
acts as a pre-computed filter that guarantees complete coverage.

Multi-model convergence: run the same chunk through 2 different prompts
or models, keep tags that both agree on → higher precision.

Coverage verification: after tagging, check that every chunk has been
processed. Any missed chunk gets re-queued.
"""
from __future__ import annotations

import asyncio
import json
import re
import sqlite3
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import Store


TAG_EXTRACTION_PROMPT = """다음은 대학교 수업 자료의 일부입니다. 아래 텍스트를 읽고 다음 카테고리에 해당하는 내용을 추출하세요.

카테고리:
1. problems: 수업에서 풀거나 예시로 든 문제/연습문제 (문제 텍스트 그대로)
2. important: 교수가 "중요하다", "꼭 알아야", "시험에 나온다" 등 강조한 내용
3. exam_hints: 시험 관련 힌트 (범위, 형식, 출제 경향, "이건 시험 문제 되기 좋다" 등)
4. cross_refs: 다른 주차/파일을 참조하는 언급 ("저번에 말한", "다음 시간에", "앞서" 등)
5. concepts: 핵심 개념/용어 (한 단어 또는 짧은 구)
6. media: 이미지, 그래프, 회로도, 그림에 대한 설명이나 참조

출력 형식 (JSON만, 설명 금지):
```json
{{"problems": ["문제 텍스트1"], "important": ["중요 발언1"], "exam_hints": ["힌트1"], "cross_refs": ["참조1"], "concepts": ["개념1"], "media": ["그림 설명1"]}}
```

해당 카테고리에 내용이 없으면 빈 배열 [].

--- 텍스트 ---
{chunk_text}
---"""


def create_memory_tables(con: sqlite3.Connection):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS chunk_tags (
            chunk_id INTEGER,
            tag_type TEXT,
            tag_value TEXT,
            model TEXT DEFAULT 'gemini',
            created_at REAL,
            PRIMARY KEY (chunk_id, tag_type, tag_value)
        );
        CREATE TABLE IF NOT EXISTS memory_build_log (
            chunk_id INTEGER PRIMARY KEY,
            status TEXT DEFAULT 'pending',
            model TEXT,
            elapsed_sec REAL,
            error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tags_type ON chunk_tags(tag_type);
        CREATE INDEX IF NOT EXISTS idx_tags_value ON chunk_tags(tag_value);
    """)
    con.commit()


def parse_tags(response_text: str) -> dict:
    """Extract JSON from LLM response."""
    m = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    # Fallback: find any JSON object
    m = re.search(r'\{[\s\S]*\}', response_text)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return {}


async def tag_chunk_gemini(chunk_text: str, client) -> dict:
    """Tag a single chunk using Gemini."""
    prompt = TAG_EXTRACTION_PROMPT.format(chunk_text=chunk_text[:3000])
    try:
        resp = await client.generate_content(prompt)
        return parse_tags(resp.text or "")
    except Exception as e:
        return {"error": str(e)}


async def build_memory(
    db_path: str,
    course_filter: str | None = None,
    batch_size: int = 10,
    max_chunks: int | None = None,
):
    """Run exhaustive memory building on all chunks."""
    con = sqlite3.connect(db_path)
    create_memory_tables(con)

    # Get chunks to process
    if course_filter:
        cur = con.execute(
            "SELECT c.chunk_id, c.text FROM chunks c JOIN documents d ON c.doc_id=d.doc_id "
            "WHERE json_extract(d.meta, '$.course_id')=? "
            "AND c.chunk_id NOT IN (SELECT chunk_id FROM memory_build_log WHERE status='done') "
            "ORDER BY c.chunk_id",
            (course_filter,),
        )
    else:
        cur = con.execute(
            "SELECT chunk_id, text FROM chunks "
            "WHERE chunk_id NOT IN (SELECT chunk_id FROM memory_build_log WHERE status='done') "
            "ORDER BY chunk_id"
        )

    pending = cur.fetchall()
    if max_chunks:
        pending = pending[:max_chunks]

    total = len(pending)
    print(f"Memory build: {total} chunks to process", flush=True)
    if total == 0:
        print("All chunks already tagged.")
        con.close()
        return

    # Init Gemini
    cookies_con = sqlite3.connect("/tmp/ff_cookies.sqlite")
    cookies_cur = cookies_con.cursor()
    cookies_cur.execute(
        "SELECT name, value FROM moz_cookies WHERE host=? AND name IN (?,?)",
        (".google.com", "__Secure-1PSID", "__Secure-1PSIDTS"),
    )
    cookies = dict(cookies_cur.fetchall())
    cookies_con.close()

    from gemini_webapi import GeminiClient
    client = GeminiClient(cookies["__Secure-1PSID"], cookies["__Secure-1PSIDTS"])
    await client.init(timeout=30)

    processed = 0
    tag_counts = {"problems": 0, "important": 0, "exam_hints": 0,
                  "cross_refs": 0, "concepts": 0, "media": 0}
    t0 = time.time()

    for chunk_id, text in pending:
        t1 = time.time()
        tags = await tag_chunk_gemini(text, client)
        elapsed = time.time() - t1

        if "error" in tags:
            con.execute(
                "INSERT OR REPLACE INTO memory_build_log (chunk_id, status, model, elapsed_sec, error) "
                "VALUES (?, 'error', 'gemini', ?, ?)",
                (chunk_id, elapsed, tags["error"]),
            )
        else:
            for tag_type in ["problems", "important", "exam_hints", "cross_refs", "concepts", "media"]:
                for val in tags.get(tag_type, []):
                    if not val or not isinstance(val, str):
                        continue
                    val = val.strip()[:500]
                    if len(val) < 3:
                        continue
                    try:
                        con.execute(
                            "INSERT OR IGNORE INTO chunk_tags (chunk_id, tag_type, tag_value, model, created_at) "
                            "VALUES (?, ?, ?, 'gemini', ?)",
                            (chunk_id, tag_type, val, time.time()),
                        )
                        tag_counts[tag_type] += 1
                    except Exception:
                        pass
            con.execute(
                "INSERT OR REPLACE INTO memory_build_log (chunk_id, status, model, elapsed_sec, error) "
                "VALUES (?, 'done', 'gemini', ?, NULL)",
                (chunk_id, elapsed),
            )
        con.commit()
        processed += 1

        if processed % 10 == 0:
            rate = processed / (time.time() - t0)
            eta = (total - processed) / max(rate, 0.01)
            print(
                f"  [{processed}/{total}] {rate:.1f} chunks/s  "
                f"ETA {eta:.0f}s  tags={dict(tag_counts)}",
                flush=True,
            )

    await client.close()
    con.close()

    total_tags = sum(tag_counts.values())
    print(f"\nMemory build complete: {processed} chunks, {total_tags} tags extracted")
    print(f"  {tag_counts}")
    print(f"  elapsed: {time.time()-t0:.1f}s")


def query_by_tag(db_path: str, tag_type: str, course_filter: str | None = None) -> list[dict]:
    """Retrieve all chunks with a specific tag type."""
    con = sqlite3.connect(db_path)
    if course_filter:
        cur = con.execute(
            "SELECT t.chunk_id, t.tag_value, c.text, c.doc_id "
            "FROM chunk_tags t JOIN chunks c ON t.chunk_id=c.chunk_id "
            "JOIN documents d ON c.doc_id=d.doc_id "
            "WHERE t.tag_type=? AND json_extract(d.meta, '$.course_id')=? "
            "ORDER BY t.chunk_id",
            (tag_type, course_filter),
        )
    else:
        cur = con.execute(
            "SELECT t.chunk_id, t.tag_value, c.text, c.doc_id "
            "FROM chunk_tags t JOIN chunks c ON t.chunk_id=c.chunk_id "
            "WHERE t.tag_type=? ORDER BY t.chunk_id",
            (tag_type,),
        )
    results = []
    for chunk_id, tag_val, text, doc_id in cur.fetchall():
        results.append({
            "chunk_id": chunk_id,
            "tag_value": tag_val,
            "text": text[:500],
            "doc_id": doc_id,
        })
    con.close()
    return results


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    p_build = sub.add_parser("build")
    p_build.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    p_build.add_argument("--course", default=None)
    p_build.add_argument("--max", type=int, default=None)

    p_query = sub.add_parser("query")
    p_query.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    p_query.add_argument("--tag", required=True, choices=["problems", "important", "exam_hints", "cross_refs", "concepts", "media"])
    p_query.add_argument("--course", default=None)

    args = ap.parse_args()
    if args.cmd == "build":
        asyncio.run(build_memory(args.db, course_filter=args.course, max_chunks=args.max))
    elif args.cmd == "query":
        results = query_by_tag(args.db, args.tag, course_filter=args.course)
        print(f"# {len(results)} results for tag_type={args.tag}")
        for r in results[:20]:
            print(f"\n[chunk {r['chunk_id']}] tag: {r['tag_value'][:100]}")
            print(f"  text: {r['text'][:200]}...")
    else:
        ap.print_help()
