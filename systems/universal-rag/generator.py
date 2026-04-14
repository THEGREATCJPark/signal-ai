"""
Answer generator: reads top-K chunks and produces a grounded answer.

Uses the Gemini proxy (or any OpenAI-compat endpoint) for generation.
The prompt enforces:
  - Answer ONLY from the provided evidence chunks
  - Cite chunk numbers for every claim
  - Return 'information not found' when evidence is insufficient
"""
from __future__ import annotations

import json
import re
import asyncio
import sqlite3
from pathlib import Path


ANSWER_PROMPT = """당신은 문서 기반 질의응답 시스템입니다. 아래에 검색 결과로 얻은 문서 발췌문(evidence chunks)이 있습니다.

규칙:
1. 오직 아래 발췌문에 있는 정보만 사용해서 답하세요.
2. 모든 주장에 [chunk N] 형태로 출처를 표기하세요.
3. 발췌문에 답이 없으면 "제공된 자료에서 해당 정보를 찾을 수 없습니다"라고 답하세요.
4. 추측하거나 외부 지식을 사용하지 마세요.

질문: {query}

--- Evidence Chunks ---
{chunks_block}
--- End ---

위 규칙을 지켜서 한국어로 답하세요."""


def format_chunks(results: list[dict], max_chars: int = 20000) -> str:
    lines = []
    total = 0
    for i, r in enumerate(results, 1):
        header = f"[chunk {i}] (source: {r.get('doc_meta', {}).get('source_name', '?')}, " \
                 f"kind: {r.get('doc_meta', {}).get('source_kind', '?')})"
        text = r["text"]
        if total + len(text) > max_chars:
            text = text[: max_chars - total]
        lines.append(header)
        lines.append(text)
        lines.append("")
        total += len(text)
        if total >= max_chars:
            break
    return "\n".join(lines)


async def generate_answer_gemini(query: str, results: list[dict]) -> dict:
    """Generate answer using gemini-webapi."""
    con = sqlite3.connect("/tmp/ff_cookies.sqlite")
    cur = con.cursor()
    cur.execute(
        "SELECT name, value FROM moz_cookies WHERE host=? AND name IN (?, ?)",
        (".google.com", "__Secure-1PSID", "__Secure-1PSIDTS"),
    )
    cookies = dict(cur.fetchall())

    from gemini_webapi import GeminiClient
    client = GeminiClient(cookies["__Secure-1PSID"], cookies["__Secure-1PSIDTS"])
    await client.init(timeout=30)

    chunks_block = format_chunks(results)
    prompt = ANSWER_PROMPT.format(query=query, chunks_block=chunks_block)

    resp = await client.generate_content(prompt)
    answer = resp.text or ""
    await client.close()

    return {
        "query": query,
        "answer": answer,
        "chunks_used": len(results),
        "sources": [
            {"chunk_id": r["chunk_id"], "doc_path": r["doc_path"],
             "score": r["score"]}
            for r in results
        ],
    }


def generate_answer_sync(query: str, results: list[dict]) -> dict:
    """Sync wrapper."""
    return asyncio.run(generate_answer_gemini(query, results))
