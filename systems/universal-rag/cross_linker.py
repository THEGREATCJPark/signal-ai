#!/usr/bin/env python3
"""
Cross-modal auto-linker.

Discovers relationships between chunks from DIFFERENT source types
(e.g., audio transcript ↔ slide, slide ↔ assignment) by matching
shared concepts extracted during the memory build phase.

How it works:
1. For each chunk, its `concepts` tags are the identity signature.
2. Two chunks from different source_kinds that share >=2 concepts
   AND belong to the same week are linked.
3. Links are stored as edges in a `chunk_links` table.
4. At query time, when a chunk is retrieved, its linked chunks are
   also surfaced — providing cross-modal verification.

This is the "자동 매칭 + 이중검증" the user described:
- Audio says "이 그래프를 보시면" → shares concepts with the slide chunk
  that has the actual graph → auto-linked → both come back together.

No rules about specific phrases. The concept overlap IS the discovery
mechanism — like backpropagation finding features, not hand-coding them.
"""
from __future__ import annotations

import json
import sqlite3
import time
from collections import defaultdict
from pathlib import Path


def build_links(db_path: str, min_shared_concepts: int = 2):
    """Build cross-modal links between chunks that share concepts."""
    con = sqlite3.connect(db_path)

    # Create links table
    con.executescript("""
        CREATE TABLE IF NOT EXISTS chunk_links (
            chunk_a INTEGER,
            chunk_b INTEGER,
            shared_concepts TEXT,
            link_strength INTEGER,
            same_week INTEGER DEFAULT 0,
            PRIMARY KEY (chunk_a, chunk_b)
        );
        CREATE INDEX IF NOT EXISTS idx_links_a ON chunk_links(chunk_a);
        CREATE INDEX IF NOT EXISTS idx_links_b ON chunk_links(chunk_b);
    """)
    con.execute("DELETE FROM chunk_links")

    # Load all concept tags grouped by chunk_id
    chunk_concepts: dict[int, set[str]] = defaultdict(set)
    cur = con.execute(
        "SELECT chunk_id, tag_value FROM chunk_tags WHERE tag_type='concepts'"
    )
    for chunk_id, concept in cur:
        chunk_concepts[chunk_id].add(concept.lower().strip())

    # Load chunk metadata (source_kind, week_key, doc_id)
    chunk_meta: dict[int, dict] = {}
    cur = con.execute(
        "SELECT c.chunk_id, d.meta FROM chunks c JOIN documents d ON c.doc_id=d.doc_id"
    )
    for chunk_id, meta_json in cur:
        meta = json.loads(meta_json) if meta_json else {}
        chunk_meta[chunk_id] = meta

    print(f"Building links: {len(chunk_concepts)} chunks with concepts", flush=True)

    # Find all pairs with shared concepts
    # Optimization: invert the concept → chunk_ids map
    concept_to_chunks: dict[str, list[int]] = defaultdict(list)
    for cid, concepts in chunk_concepts.items():
        for c in concepts:
            concept_to_chunks[c].append(cid)

    # For each pair of chunks sharing concepts, check cross-modal + same-week
    links_found = 0
    link_batch = []

    chunk_ids = sorted(chunk_concepts.keys())
    pair_scores: dict[tuple[int, int], set[str]] = defaultdict(set)

    for concept, cids in concept_to_chunks.items():
        if len(cids) > 100:
            continue  # Skip overly common concepts
        for i in range(len(cids)):
            for j in range(i + 1, len(cids)):
                a, b = min(cids[i], cids[j]), max(cids[i], cids[j])
                if a == b:
                    continue
                pair_scores[(a, b)].add(concept)

    for (a, b), shared in pair_scores.items():
        if len(shared) < min_shared_concepts:
            continue
        meta_a = chunk_meta.get(a, {})
        meta_b = chunk_meta.get(b, {})

        # Cross-document: different doc_ids (not just different source_kind)
        # Two chunks from the SAME document sharing concepts is trivial;
        # two chunks from DIFFERENT documents sharing concepts = real link.
        doc_a = con.execute("SELECT doc_id FROM chunks WHERE chunk_id=?", (a,)).fetchone()
        doc_b = con.execute("SELECT doc_id FROM chunks WHERE chunk_id=?", (b,)).fetchone()
        if doc_a and doc_b and doc_a[0] == doc_b[0]:
            continue  # Same document → skip

        # Same week bonus
        week_a = meta_a.get("week_key", "")
        week_b = meta_b.get("week_key", "")
        same_week = 1 if (week_a and week_a == week_b) else 0

        link_batch.append((
            a, b,
            json.dumps(sorted(shared), ensure_ascii=False),
            len(shared),
            same_week,
        ))
        links_found += 1

        if len(link_batch) >= 1000:
            con.executemany(
                "INSERT OR REPLACE INTO chunk_links "
                "(chunk_a, chunk_b, shared_concepts, link_strength, same_week) "
                "VALUES (?, ?, ?, ?, ?)",
                link_batch,
            )
            link_batch = []

    if link_batch:
        con.executemany(
            "INSERT OR REPLACE INTO chunk_links "
            "(chunk_a, chunk_b, shared_concepts, link_strength, same_week) "
            "VALUES (?, ?, ?, ?, ?)",
            link_batch,
        )
    con.commit()

    # Stats
    same_week_links = con.execute(
        "SELECT COUNT(*) FROM chunk_links WHERE same_week=1"
    ).fetchone()[0]
    avg_strength = con.execute(
        "SELECT AVG(link_strength) FROM chunk_links"
    ).fetchone()[0] or 0

    print(f"Links built: {links_found} total, {same_week_links} same-week, "
          f"avg strength: {avg_strength:.1f}")
    con.close()


def get_linked_chunks(db_path: str, chunk_id: int, min_strength: int = 2) -> list[dict]:
    """Get all chunks linked to a given chunk."""
    con = sqlite3.connect(db_path)
    results = []

    cur = con.execute(
        "SELECT chunk_b, shared_concepts, link_strength, same_week "
        "FROM chunk_links WHERE chunk_a=? AND link_strength>=? "
        "UNION "
        "SELECT chunk_a, shared_concepts, link_strength, same_week "
        "FROM chunk_links WHERE chunk_b=? AND link_strength>=? "
        "ORDER BY same_week DESC, link_strength DESC",
        (chunk_id, min_strength, chunk_id, min_strength),
    )
    for linked_id, shared_json, strength, same_week in cur.fetchall():
        chunk = con.execute(
            "SELECT chunk_id, text, doc_id FROM chunks WHERE chunk_id=?",
            (linked_id,),
        ).fetchone()
        if not chunk:
            continue
        doc = con.execute(
            "SELECT meta FROM documents WHERE doc_id=?", (chunk[2],)
        ).fetchone()
        meta = json.loads(doc[0]) if doc else {}
        results.append({
            "chunk_id": chunk[0],
            "text": chunk[1][:300],
            "shared_concepts": json.loads(shared_json),
            "link_strength": strength,
            "same_week": bool(same_week),
            "source_kind": meta.get("source_kind", "?"),
            "week_key": meta.get("week_key", "?"),
        })
    con.close()
    return results


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    p_build = sub.add_parser("build")
    p_build.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    p_build.add_argument("--min-shared", type=int, default=2)

    p_query = sub.add_parser("links")
    p_query.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    p_query.add_argument("--chunk", type=int, required=True)

    args = ap.parse_args()
    if args.cmd == "build":
        build_links(args.db, min_shared_concepts=args.min_shared)
    elif args.cmd == "links":
        results = get_linked_chunks(args.db, args.chunk)
        print(f"# {len(results)} linked chunks for chunk {args.chunk}")
        for r in results[:10]:
            print(f"\n  chunk {r['chunk_id']} ({r['source_kind']}, {r['week_key']}) "
                  f"strength={r['link_strength']} same_week={r['same_week']}")
            print(f"  concepts: {r['shared_concepts'][:5]}")
            print(f"  text: {r['text'][:150]}...")
