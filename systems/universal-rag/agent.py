#!/usr/bin/env python3
"""
Memory Agent — LLM이 검색 도구를 반복 호출하며 추론.

사전 계산 없음. 질의 시점에 LLM이:
1. 검색 (BM25+embed)
2. 결과 분석
3. 부족하면 재검색 (다른 키워드, 다른 주차, 다른 소스)
4. 충분하면 결론

이 구조는:
- 번복 탐지: 시간순 여러 결과 비교 → LLM이 변경 판단
- 지시어 해소: "저번에 말한 X" → X 추출 → 이전 주차 검색
- 파일간 관계: 슬라이드 → 같은 주차 녹음 검색 → 매칭
- 100만 스케일: 질의당 검색 O(1) + LLM 수 회
"""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import asyncio
import time
from pathlib import Path


AGENT_SYSTEM = """당신은 기억 검색 에이전트입니다. 사용자 질문에 답하기 위해 검색 도구를 사용합니다.

사용 가능한 도구:
1. SEARCH(query, week=None) — 키워드로 검색. week 지정하면 해당 주차만.
2. SEARCH_TIME(query) — 시간순으로 모든 언급 검색 (번복 탐지용).
3. ANSWER(text) — 최종 답변.
4. NEED_MORE(reason, new_query) — 정보 부족. 추가 검색 필요.

규칙:
- 한 번에 도구 1개만 호출
- 결과가 부족하면 NEED_MORE로 다른 검색어/주차 시도
- 최대 5회 반복
- "저번에 말한" → 이전 주차에서 검색
- 여러 시점에서 같은 주제 → 시간순 비교 → 최신 우선, 변경사항 명시
- 모든 주장에 [week N] 출처

출력 형식 (JSON):
{"tool": "SEARCH", "args": {"query": "...", "week": null}}
{"tool": "SEARCH_TIME", "args": {"query": "..."}}
{"tool": "ANSWER", "args": {"text": "..."}}
{"tool": "NEED_MORE", "args": {"reason": "...", "new_query": "..."}}
"""


def bm25_search(con, query, week=None, top_k=10):
    """BM25 search with optional week filter."""
    import math
    from brain import tokenize

    tokens = tokenize(query)
    try:
        N = int(con.execute("SELECT value FROM bm25_meta WHERE key='N'").fetchone()[0])
        avg = float(con.execute("SELECT value FROM bm25_meta WHERE key='avg'").fetchone()[0])
    except:
        return []

    scores = {}
    for t in set(tokens):
        dfr = con.execute("SELECT value FROM bm25_meta WHERE key=?", (f"df:{t}",)).fetchone()
        if not dfr: continue
        df = int(dfr[0])
        if df / max(N, 1) > 0.5: continue
        idf = math.log(1 + (N - df + 0.5) / (df + 0.5))
        for cid, tf in con.execute("SELECT chunk_id, tf FROM bm25 WHERE term=?", (t,)):
            dl = con.execute("SELECT tok_n FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
            dl = dl[0] if dl else avg
            scores[cid] = scores.get(cid, 0) + idf * (tf * 2.5) / (tf + 1.5 * (1 - 0.75 + 0.75 * dl / avg))

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Week filter
    if week is not None:
        filtered = []
        for cid, score in ranked:
            meta = con.execute(
                "SELECT json_extract(d.meta, '$.week_key') FROM docs d JOIN chunks c ON d.doc_id=c.doc_id WHERE c.chunk_id=?",
                (cid,)
            ).fetchone()
            if meta and meta[0] and str(week) in str(meta[0]):
                filtered.append((cid, score))
        ranked = filtered

    results = []
    for cid, score in ranked[:top_k]:
        chunk = con.execute("SELECT text, doc_id FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
        if not chunk: continue
        doc = con.execute("SELECT meta FROM docs WHERE doc_id=?", (chunk[1],)).fetchone()
        meta = json.loads(doc[0]) if doc else {}
        results.append({
            "chunk_id": cid,
            "score": round(score, 2),
            "week": meta.get("week_key", "?"),
            "kind": meta.get("source_kind", "?"),
            "text": chunk[0][:500],
        })
    return results


def time_sorted_search(con, query, top_k=20):
    """Search and return results sorted by week (oldest first)."""
    results = bm25_search(con, query, week=None, top_k=top_k)
    # Parse week number for sorting
    def week_num(r):
        m = re.search(r"(\d+)", r.get("week", "0"))
        return int(m.group(1)) if m else 0
    results.sort(key=week_num)
    return results


def format_results(results):
    """Format search results for the LLM."""
    if not results:
        return "(검색 결과 없음)"
    lines = []
    for i, r in enumerate(results, 1):
        lines.append(f"[{i}] week={r['week']} kind={r['kind']} score={r['score']}")
        lines.append(f"    {r['text'][:300]}")
    return "\n".join(lines)


def llm_call(prompt):
    """Call codex for agent reasoning."""
    try:
        r = subprocess.run(
            ["codex", "exec", "--skip-git-repo-check", "-"],
            input=prompt, capture_output=True, text=True, timeout=120,
        )
        out = r.stdout or ""
        # Try ALL JSON objects in output (codex prints boilerplate before the real answer)
        for line in out.splitlines():
            line = line.strip()
            if line.startswith("{") and "tool" in line:
                try:
                    return json.loads(line)
                except:
                    continue
        # Fallback: find any JSON object
        matches = list(re.finditer(r'\{[^{}]*"tool"[^{}]*\}', out))
        if matches:
            try:
                return json.loads(matches[-1].group(0))
            except:
                pass
        # Last fallback
        lines = [l for l in out.splitlines() if l.strip()
                 and not any(l.startswith(x) for x in ("codex", "tokens", "Let's", "---"))]
        text = "\n".join(lines).strip()
        if text:
            return {"tool": "ANSWER", "args": {"text": text}}
        return {"tool": "ANSWER", "args": {"text": "(no response)"}}
    except Exception as e:
        return {"tool": "ANSWER", "args": {"text": f"Error: {e}"}}


def run_agent(db_path: str, user_query: str, max_rounds: int = 5):
    """Run the agent loop."""
    con = sqlite3.connect(db_path)

    history = []
    history.append(f"사용자 질문: {user_query}")

    for round_num in range(1, max_rounds + 1):
        # Build prompt
        prompt = AGENT_SYSTEM + "\n\n" + "\n".join(history) + "\n\n다음 도구를 호출하세요 (JSON):"

        print(f"\n[Round {round_num}]", flush=True)
        action = llm_call(prompt)
        tool = action.get("tool", "ANSWER")
        args = action.get("args", {})

        print(f"  Tool: {tool}", flush=True)
        if tool == "ANSWER":
            answer = args.get("text", "")
            print(f"  Answer: {answer[:300]}", flush=True)
            con.close()
            return {"query": user_query, "answer": answer, "rounds": round_num, "history": history}

        elif tool == "SEARCH":
            query = args.get("query", user_query)
            week = args.get("week")
            print(f"  Query: '{query}' week={week}", flush=True)
            results = bm25_search(con, query, week=week, top_k=5)
            result_text = format_results(results)
            history.append(f"도구 호출: SEARCH(query='{query}', week={week})")
            history.append(f"검색 결과:\n{result_text}")
            print(f"  Results: {len(results)}", flush=True)

        elif tool == "SEARCH_TIME":
            query = args.get("query", user_query)
            print(f"  Query: '{query}' (time-sorted)", flush=True)
            results = time_sorted_search(con, query, top_k=10)
            result_text = format_results(results)
            history.append(f"도구 호출: SEARCH_TIME(query='{query}')")
            history.append(f"시간순 검색 결과:\n{result_text}")
            print(f"  Results: {len(results)}", flush=True)

        elif tool == "NEED_MORE":
            reason = args.get("reason", "")
            new_query = args.get("new_query", "")
            print(f"  Reason: {reason}", flush=True)
            print(f"  New query: {new_query}", flush=True)
            history.append(f"NEED_MORE: {reason}")
            if new_query:
                results = bm25_search(con, new_query, top_k=5)
                result_text = format_results(results)
                history.append(f"추가 검색 '{new_query}':\n{result_text}")

        else:
            # Unknown tool, treat as answer
            history.append(f"Unknown tool: {tool}")
            break

    # Max rounds reached
    con.close()
    return {"query": user_query, "answer": "(max rounds reached)", "rounds": max_rounds, "history": history}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--q", required=True)
    ap.add_argument("--rounds", type=int, default=5)
    args = ap.parse_args()

    result = run_agent(args.db, args.q, max_rounds=args.rounds)
    print(f"\n{'='*50}")
    print(f"FINAL ANSWER ({result['rounds']} rounds):")
    print(f"{'='*50}")
    print(result["answer"])
