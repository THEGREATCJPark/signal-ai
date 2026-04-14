#!/usr/bin/env python3
"""
Complete RAG pipeline. Single entry point.

Usage:
  # Ingest a directory
  python3 -m rag.pipeline ingest --root /path/to/docs --db ./rag.db

  # Query
  python3 -m rag.pipeline query --db ./rag.db --q "중간고사 시험범위"

  # Query with answer generation
  python3 -m rag.pipeline query --db ./rag.db --q "중간고사 시험범위" --generate
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rag.store import Store
from rag.chunker import chunk_text
from rag.retriever import HybridRetriever, tokenize


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def infer_meta(path: Path, root: Path) -> dict:
    """Infer metadata from path structure."""
    import re
    parts = path.relative_to(root).parts
    meta = {"source_name": path.parent.name, "filename": path.name}
    for p in parts:
        m = re.match(r"course_(\d+)(?:_(.*))?", p)
        if m:
            meta["course_id"] = m.group(1)
            meta["course_name"] = m.group(2) or ""
        m = re.match(r"week(\d+)_", p)
        if m:
            meta["week_key"] = f"week{int(m.group(1)):02d}"
    if "syllabus" in str(path):
        meta["source_kind"] = "syllabus"
    elif "_assignments" in str(path):
        meta["source_kind"] = "assignment"
    elif "녹음" in str(path) or "audio" in str(path).lower():
        meta["source_kind"] = "lecture_audio"
    elif "동영상" in str(path) or "video" in str(path).lower():
        meta["source_kind"] = "lecture_video"
    else:
        meta["source_kind"] = "lecture_material"
    return meta


SKIP_DIRS = {"pages", "crops", "__pycache__", ".cache", "node_modules", ".git"}


def iter_text_files(root: Path):
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]
        for fn in sorted(filenames):
            low = fn.lower()
            if not (low.endswith(".md") or low.endswith(".txt")):
                continue
            p = Path(dirpath) / fn
            try:
                size = p.stat().st_size
            except OSError:
                continue
            if size == 0 or size > 5_000_000:
                continue
            yield p


def cmd_ingest(args):
    root = Path(args.root).resolve()
    store = Store(args.db)

    print(f"# Ingesting from {root}", flush=True)
    print(f"# DB: {args.db}", flush=True)

    # Phase 1: ingest documents + chunk
    t0 = time.time()
    doc_count = 0
    chunk_count = 0
    for path in iter_text_files(root):
        try:
            body = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if len(body.strip()) < 50:
            continue
        sha = sha256_file(path)
        meta = infer_meta(path, root)
        doc_id = store.upsert_doc(str(path), sha, meta)

        chunks = chunk_text(body, target_size=800, overlap=200)
        for c in chunks:
            c["tok_count"] = len(tokenize(c["text"]))
        store.insert_chunks(doc_id, chunks)
        doc_count += 1
        chunk_count += len(chunks)
        if doc_count % 50 == 0:
            print(f"  docs={doc_count} chunks={chunk_count} elapsed={time.time()-t0:.1f}s", flush=True)

    print(f"\n  Phase 1 done: {doc_count} docs, {chunk_count} chunks, {time.time()-t0:.1f}s")

    # Phase 2: build BM25 index
    print("  Building BM25 index...", flush=True)
    t1 = time.time()
    store.rebuild_bm25(tokenize)
    print(f"  BM25 done: {time.time()-t1:.1f}s")

    # Phase 3: embed chunks
    if not args.skip_embed:
        print("  Embedding chunks...", flush=True)
        t2 = time.time()
        from rag.embedder import LocalEmbedder
        embedder = LocalEmbedder()
        print(f"  Model loaded ({embedder.dim} dims), encoding...", flush=True)

        batch_size = 64
        all_ids = store.all_chunk_ids()
        for i in range(0, len(all_ids), batch_size):
            batch_ids = all_ids[i: i + batch_size]
            texts = []
            for cid in batch_ids:
                chunk = store.get_chunk(cid)
                texts.append(chunk["text"] if chunk else "")
            vecs = embedder.encode(texts)
            store.upsert_embeddings_batch(list(zip(batch_ids, vecs)))
            if (i + batch_size) % 500 < batch_size:
                print(f"    embedded {min(i+batch_size, len(all_ids))}/{len(all_ids)}", flush=True)
        store.con.commit()
        print(f"  Embedding done: {len(all_ids)} chunks, {time.time()-t2:.1f}s")
    else:
        print("  Skipping embeddings (--skip-embed)")

    print(f"\n# Ingest complete: {store.doc_count()} docs, {store.chunk_count()} chunks, "
          f"{store.embedding_count()} embeddings")
    store.close()


def cmd_query(args):
    store = Store(args.db)
    from rag.embedder import LocalEmbedder
    embedder = LocalEmbedder()

    retriever = HybridRetriever(store, embedder)
    print("Loading vectors...", flush=True)
    retriever.load_vectors()
    print(f"  {len(retriever._vec_cache)} vectors loaded")

    # Optional course filter via query prefix "course:288800 ..."
    query = args.q
    course_filter = None
    if query.startswith("course:"):
        parts = query.split(None, 1)
        course_filter = parts[0].split(":")[1]
        query = parts[1] if len(parts) > 1 else ""

    print(f"\n# Query: {query}" + (f"  (course={course_filter})" if course_filter else ""))
    results = retriever.search(query, top_k=args.top, course_filter=course_filter)

    print(f"# {len(results)} results\n")
    for i, r in enumerate(results, 1):
        meta = r.get("doc_meta", {})
        print(f"[{i}] score={r['score']}  "
              f"course={meta.get('course_id','?')}  "
              f"week={meta.get('week_key','?')}  "
              f"kind={meta.get('source_kind','?')}")
        print(f"    source: {meta.get('source_name','?')}")
        # Show text preview
        text_preview = r["text"][:300].replace("\n", " ")
        print(f"    {text_preview}")
        print()

    if args.generate and results:
        print("# Generating answer with Gemini...", flush=True)
        from rag.generator import generate_answer_sync
        answer = generate_answer_sync(query, results[:args.top])
        print(f"\n{'='*60}")
        print(f"ANSWER:")
        print(f"{'='*60}")
        print(answer["answer"])
        print(f"\nSources: {len(answer['sources'])} chunks used")

    store.close()


def main():
    ap = argparse.ArgumentParser(prog="rag.pipeline")
    sub = ap.add_subparsers(dest="cmd")

    p_ingest = sub.add_parser("ingest")
    p_ingest.add_argument("--root", required=True)
    p_ingest.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    p_ingest.add_argument("--skip-embed", action="store_true")

    p_query = sub.add_parser("query")
    p_query.add_argument("--db", default="/path/to/memory_lab/rag/data/rag.db")
    p_query.add_argument("--q", required=True)
    p_query.add_argument("--top", type=int, default=10)
    p_query.add_argument("--generate", action="store_true")

    args = ap.parse_args()
    if args.cmd == "ingest":
        cmd_ingest(args)
    elif args.cmd == "query":
        cmd_query(args)
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
