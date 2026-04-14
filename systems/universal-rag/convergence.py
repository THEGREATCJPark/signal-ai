#!/usr/bin/env python3
"""
Multi-model convergence verification.

Idea: tag the SAME chunk with two different models/prompts.
Keep only tags that BOTH agree on → higher precision.

Implementation:
1. Pick chunks that already have Gemini tags
2. Re-tag them with Codex (different model, different prompt style)
3. Compare: if both produce the same tag_type for a chunk → "converged"
4. Converged tags get confidence="high", single-model tags get "medium"

This is the "중복을 통한 정확도 확보" from mini-artichokes.
"""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import time
from pathlib import Path


CODEX_TAG_PROMPT = """Read the following university lecture text and extract:

1. problems: Any practice problems, exercises, or homework questions (exact text)
2. important: Statements the professor emphasized as important
3. exam_hints: Any hints about exams (format, scope, "this will be on the test")
4. concepts: Key technical terms (short phrases)

Output JSON only, no explanation:
{{"problems":[],"important":[],"exam_hints":[],"concepts":[]}}

Text:
{chunk_text}
"""


def codex_tag(chunk_text: str, timeout: int = 120) -> dict:
    """Tag a chunk using codex exec."""
    prompt = CODEX_TAG_PROMPT.format(chunk_text=chunk_text[:3000])
    try:
        result = subprocess.run(
            ["codex", "exec", "--skip-git-repo-check", "-"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        out = result.stdout or ""
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}

    # Parse JSON from output
    m = re.search(r"\{[\s\S]*\}", out)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    return {"error": "parse_fail"}


def run_convergence(db_path: str, n_chunks: int = 10):
    """Pick n already-Gemini-tagged chunks, re-tag with codex, compare."""
    con = sqlite3.connect(db_path)

    # Get chunks that have Gemini tags
    cur = con.execute("""
        SELECT DISTINCT t.chunk_id, c.text
        FROM chunk_tags t JOIN chunks c ON t.chunk_id=c.chunk_id
        WHERE t.model='gemini'
        AND t.tag_type IN ('problems','important','exam_hints')
        ORDER BY RANDOM()
        LIMIT ?
    """, (n_chunks,))

    chunks = cur.fetchall()
    print(f"Convergence test: {len(chunks)} chunks (Gemini tags exist, re-tagging with Codex)")

    results = []
    for chunk_id, text in chunks:
        # Get Gemini tags
        gemini_tags = {}
        for r in con.execute("SELECT tag_type, tag_value FROM chunk_tags WHERE chunk_id=? AND model='gemini'", (chunk_id,)):
            gemini_tags.setdefault(r[0], []).append(r[1])

        # Get Codex tags
        t0 = time.time()
        codex_result = codex_tag(text)
        elapsed = time.time() - t0

        if "error" in codex_result:
            print(f"  chunk {chunk_id}: codex error ({codex_result['error']})")
            continue

        # Compare
        agreement = {}
        for tag_type in ["problems", "important", "exam_hints"]:
            g_has = len(gemini_tags.get(tag_type, [])) > 0
            c_has = len(codex_result.get(tag_type, [])) > 0
            if g_has and c_has:
                agreement[tag_type] = "converged"
            elif g_has and not c_has:
                agreement[tag_type] = "gemini_only"
            elif not g_has and c_has:
                agreement[tag_type] = "codex_only"
            else:
                agreement[tag_type] = "neither"

        # Store codex tags
        for tag_type in ["problems", "important", "exam_hints", "concepts"]:
            for val in codex_result.get(tag_type, []):
                if val and isinstance(val, str) and len(val.strip()) >= 3:
                    try:
                        con.execute(
                            "INSERT OR IGNORE INTO chunk_tags (chunk_id, tag_type, tag_value, model, created_at) VALUES (?,?,?,'codex',?)",
                            (chunk_id, tag_type, val.strip()[:500], time.time())
                        )
                    except:
                        pass
        con.commit()

        results.append({"chunk_id": chunk_id, "agreement": agreement, "elapsed": elapsed})
        converged = sum(1 for v in agreement.values() if v == "converged")
        print(f"  chunk {chunk_id}: {agreement} ({elapsed:.1f}s)")

    # Summary
    if results:
        total_checks = len(results) * 3  # 3 tag types per chunk
        converged = sum(1 for r in results for v in r["agreement"].values() if v == "converged")
        gemini_only = sum(1 for r in results for v in r["agreement"].values() if v == "gemini_only")
        codex_only = sum(1 for r in results for v in r["agreement"].values() if v == "codex_only")
        print(f"\nConvergence summary:")
        print(f"  converged: {converged}/{total_checks} ({converged/max(total_checks,1)*100:.0f}%)")
        print(f"  gemini_only: {gemini_only}")
        print(f"  codex_only: {codex_only}")

    con.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    ap.add_argument("--n", type=int, default=5)
    args = ap.parse_args()
    run_convergence(args.db, n_chunks=args.n)
