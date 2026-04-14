#!/usr/bin/env python3
"""
Unified Memory System — 총체적 구조.

Single class that integrates ALL layers:
  - L0 raw store (원문 보존)
  - L1 chunks + BM25 + embeddings (검색 기반)
  - L2 tags (전수조사 기반 기억)
  - L3 cross-links (관계 그래프)
  - L4 RAPTOR tree (위계적 기억)
  - L5 convergence (다중 모델 검증)

Ingest: one call builds ALL layers.
Query: one call searches ALL layers simultaneously.

Usage:
  ms = MemorySystem("./memory.db")
  ms.ingest("/path/to/corpus")                    # builds L0-L4
  answer = ms.query("중간고사 시험범위")            # searches L0-L4, generates answer
  answer = ms.query("문제 다 뽑아와", mode="exhaustive")  # tag-based full recall
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import re
import sqlite3
import struct
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from chunker import chunk_text
from retriever import tokenize, expand_query, weighted_tokens, cosine_sim, rrf_fuse


# ============================================================
# Signal patterns for fast prescan
# ============================================================
SIGNAL_PATTERNS = {
    "problem": re.compile(
        r"(문제|풀어|풀이|예제|exercise|example|quiz|연습|과제|homework|HW|답을 구|계산하|구하시오|구하라)",
        re.IGNORECASE,
    ),
    "important": re.compile(
        r"(중요|꼭 알|반드시|시험에 나|기억해|핵심|잊지 말|유의|주의해)",
        re.IGNORECASE,
    ),
    "exam_hint": re.compile(
        r"(시험 문제|시험에서|출제|시험 범위|시험.*형식|문제.*되기 좋|시험.*팁)",
        re.IGNORECASE,
    ),
    "cross_ref": re.compile(
        r"(저번|앞서|다음 시간|지난|이전에|뒤에서|위에서|아래에서|참고)",
        re.IGNORECASE,
    ),
    "media": re.compile(
        r"(그래프|그림|회로|사진|도식|figure|image|diagram|circuit|chart)",
        re.IGNORECASE,
    ),
}

TAG_PROMPT = """다음 텍스트에서 아래 카테고리에 해당하는 내용을 추출하세요.

1. problems: 풀이 문제/연습문제 (원문 그대로)
2. important: 교수가 강조한 내용
3. exam_hints: 시험 관련 힌트
4. cross_refs: 다른 주차/파일 참조
5. concepts: 핵심 용어
6. media: 이미지/그래프/회로 설명

JSON만 출력:
{{"problems":[],"important":[],"exam_hints":[],"cross_refs":[],"concepts":[],"media":[]}}

해당 없으면 빈 배열.

텍스트:
{text}"""

SUMMARIZE_PROMPT = """다음 {n}개 텍스트를 {max_chars}자 이내 하나의 요약으로 합쳐주세요.
핵심 개념, 수치, 교수 강조 내용 보존. 요약만 출력.

{chunks_text}"""

ANSWER_PROMPT = """문서 기반 QA 시스템입니다. 아래 evidence만 사용하세요.
모든 주장에 [chunk N] 출처 표기. 근거 없으면 "정보를 찾을 수 없습니다".

질문: {query}

--- Evidence ---
{evidence}
---

한국어로 답하세요."""


class MemorySystem:
    def __init__(self, db_path: str = "./memory.db"):
        self.db_path = db_path
        self.con = sqlite3.connect(db_path)
        self.con.execute("PRAGMA journal_mode=WAL")
        self._create_all_tables()
        self._embedder = None
        self._gemini = None
        self._vec_cache: dict[int, list[float]] = {}

    def _create_all_tables(self):
        self.con.executescript("""
            -- L0: documents
            CREATE TABLE IF NOT EXISTS documents (
                doc_id INTEGER PRIMARY KEY, path TEXT UNIQUE,
                sha256 TEXT, meta TEXT DEFAULT '{}'
            );
            -- L1: chunks + BM25
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id INTEGER PRIMARY KEY, doc_id INTEGER,
                chunk_idx INTEGER, text TEXT,
                char_start INTEGER, char_end INTEGER, tok_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS embeddings (
                chunk_id INTEGER PRIMARY KEY, vector BLOB
            );
            CREATE TABLE IF NOT EXISTS bm25_postings (
                term TEXT, chunk_id INTEGER, tf INTEGER,
                PRIMARY KEY (term, chunk_id)
            );
            CREATE TABLE IF NOT EXISTS bm25_meta (key TEXT PRIMARY KEY, value TEXT);
            -- L2: tags (전수조사 기억)
            CREATE TABLE IF NOT EXISTS chunk_tags (
                chunk_id INTEGER, tag_type TEXT, tag_value TEXT,
                model TEXT DEFAULT 'gemini', confidence TEXT DEFAULT 'single',
                created_at REAL,
                PRIMARY KEY (chunk_id, tag_type, tag_value)
            );
            -- L2: prescan signals
            CREATE TABLE IF NOT EXISTS chunk_signals (
                chunk_id INTEGER, signal_type TEXT,
                PRIMARY KEY (chunk_id, signal_type)
            );
            -- L3: cross-links (관계 그래프)
            CREATE TABLE IF NOT EXISTS chunk_links (
                chunk_a INTEGER, chunk_b INTEGER,
                shared_concepts TEXT, link_strength INTEGER,
                same_doc INTEGER DEFAULT 0,
                PRIMARY KEY (chunk_a, chunk_b)
            );
            -- L4: RAPTOR tree (위계적 기억)
            CREATE TABLE IF NOT EXISTS tree_nodes (
                node_id INTEGER PRIMARY KEY, level INTEGER,
                doc_id INTEGER, text TEXT,
                child_ids TEXT, created_at REAL
            );
            -- L5: build log
            CREATE TABLE IF NOT EXISTS build_log (
                chunk_id INTEGER PRIMARY KEY, phase TEXT,
                status TEXT DEFAULT 'pending', model TEXT
            );
            -- Indexes
            CREATE INDEX IF NOT EXISTS idx_chunks_doc ON chunks(doc_id);
            CREATE INDEX IF NOT EXISTS idx_bm25_term ON bm25_postings(term);
            CREATE INDEX IF NOT EXISTS idx_tags_type ON chunk_tags(tag_type);
            CREATE INDEX IF NOT EXISTS idx_signals ON chunk_signals(signal_type);
            CREATE INDEX IF NOT EXISTS idx_links_a ON chunk_links(chunk_a);
            CREATE INDEX IF NOT EXISTS idx_tree_level ON tree_nodes(level);
        """)
        self.con.commit()

    # ---- Embedder ----
    def _get_embedder(self):
        if self._embedder is None:
            from embedder import LocalEmbedder
            self._embedder = LocalEmbedder()
        return self._embedder

    # ---- Gemini ----
    async def _get_gemini(self):
        if self._gemini is None:
            ccon = sqlite3.connect("/tmp/ff_cookies.sqlite")
            cur = ccon.cursor()
            cur.execute("SELECT name, value FROM moz_cookies WHERE host=? AND name IN (?,?)",
                        (".google.com", "__Secure-1PSID", "__Secure-1PSIDTS"))
            cookies = dict(cur.fetchall()); ccon.close()
            from gemini_webapi import GeminiClient
            self._gemini = GeminiClient(cookies["__Secure-1PSID"], cookies["__Secure-1PSIDTS"])
            await self._gemini.init(timeout=30)
        return self._gemini

    async def _llm_call(self, prompt: str) -> str:
        client = await self._get_gemini()
        try:
            resp = await client.generate_content(prompt)
            return resp.text or ""
        except Exception as e:
            return f"<<error: {e}>>"

    # ============================================================
    # INGEST — builds all layers in one call
    # ============================================================
    def ingest(self, root: str, skip_embed: bool = False, tag_limit: int = 100):
        """Ingest a corpus directory. Builds L0→L4."""
        root = Path(root).resolve()
        print(f"[INGEST] {root}", flush=True)
        t0 = time.time()

        # L0+L1: chunk + BM25
        self._ingest_chunks(root)

        # L1: embeddings
        if not skip_embed:
            self._build_embeddings()

        # L1: BM25
        self._build_bm25()

        # L2: prescan + targeted tag
        self._prescan_signals()
        asyncio.run(self._tag_priority(limit=tag_limit))

        # L3: cross-links
        self._build_links()

        # L4: RAPTOR (top 3 biggest docs)
        asyncio.run(self._build_raptor(max_docs=3))

        print(f"\n[INGEST COMPLETE] {time.time()-t0:.0f}s", flush=True)
        self._print_stats()

    def _ingest_chunks(self, root: Path):
        SKIP = {"pages","crops","__pycache__",".cache","node_modules",".git"}
        doc_count = chunk_count = 0
        for dp, dn, fns in os.walk(root):
            dn[:] = [d for d in dn if d not in SKIP]
            for fn in sorted(fns):
                if not (fn.lower().endswith(".md") or fn.lower().endswith(".txt")):
                    continue
                p = Path(dp) / fn
                try:
                    body = p.read_text(encoding="utf-8", errors="replace")
                except: continue
                if len(body.strip()) < 50: continue
                sha = hashlib.sha256(body.encode("utf-8","replace")).hexdigest()
                meta = self._infer_meta(p, root)
                cur = self.con.execute(
                    "INSERT INTO documents (path,sha256,meta) VALUES (?,?,?) "
                    "ON CONFLICT(path) DO UPDATE SET sha256=excluded.sha256,meta=excluded.meta RETURNING doc_id",
                    (str(p), sha, json.dumps(meta, ensure_ascii=False)))
                doc_id = cur.fetchone()[0]
                chunks = chunk_text(body, target_size=800, overlap=200)
                self.con.execute("DELETE FROM chunks WHERE doc_id=?", (doc_id,))
                for i, c in enumerate(chunks):
                    toks = tokenize(c["text"])
                    self.con.execute(
                        "INSERT INTO chunks (doc_id,chunk_idx,text,char_start,char_end,tok_count) VALUES (?,?,?,?,?,?)",
                        (doc_id, i, c["text"], c["char_start"], c["char_end"], len(toks)))
                doc_count += 1; chunk_count += len(chunks)
                if doc_count % 100 == 0:
                    self.con.commit()
                    print(f"  L0: {doc_count} docs {chunk_count} chunks", flush=True)
        self.con.commit()
        print(f"  L0 done: {doc_count} docs, {chunk_count} chunks")

    def _build_embeddings(self):
        embedder = self._get_embedder()
        all_ids = [r[0] for r in self.con.execute("SELECT chunk_id FROM chunks ORDER BY chunk_id")]
        print(f"  L1 embed: {len(all_ids)} chunks...", flush=True)
        for i in range(0, len(all_ids), 64):
            batch = all_ids[i:i+64]
            texts = [self.con.execute("SELECT text FROM chunks WHERE chunk_id=?", (c,)).fetchone()[0] for c in batch]
            vecs = embedder.encode(texts)
            for cid, vec in zip(batch, vecs):
                self.con.execute("INSERT OR REPLACE INTO embeddings (chunk_id,vector) VALUES (?,?)",
                                (cid, struct.pack(f"{len(vec)}f", *vec)))
            if (i+64) % 500 < 64:
                print(f"    {min(i+64,len(all_ids))}/{len(all_ids)}", flush=True)
        self.con.commit()
        print(f"  L1 embed done")

    def _build_bm25(self):
        self.con.execute("DELETE FROM bm25_postings")
        self.con.execute("DELETE FROM bm25_meta")
        total_toks = 0; n = 0; df = Counter()
        batch = []
        for chunk_id, text in self.con.execute("SELECT chunk_id, text FROM chunks"):
            toks = tokenize(text); tf = Counter(toks)
            total_toks += len(toks); n += 1
            for t in tf: df[t] += 1
            for t, c in tf.items(): batch.append((t, chunk_id, c))
            if len(batch) > 50000:
                self.con.executemany("INSERT INTO bm25_postings VALUES (?,?,?)", batch)
                batch = []
        if batch: self.con.executemany("INSERT INTO bm25_postings VALUES (?,?,?)", batch)
        avg_dl = total_toks / max(1, n)
        self.con.execute("INSERT OR REPLACE INTO bm25_meta VALUES ('total_chunks',?)", (str(n),))
        self.con.execute("INSERT OR REPLACE INTO bm25_meta VALUES ('avg_dl',?)", (str(avg_dl),))
        for t, d in df.items():
            self.con.execute("INSERT OR REPLACE INTO bm25_meta VALUES (?,?)", (f"df:{t}", str(d)))
        self.con.commit()
        print(f"  L1 BM25 done: {n} chunks, {len(df)} terms")

    def _prescan_signals(self):
        self.con.execute("DELETE FROM chunk_signals")
        total = hit = 0
        for chunk_id, text in self.con.execute("SELECT chunk_id, text FROM chunks"):
            total += 1
            for sig, pat in SIGNAL_PATTERNS.items():
                if pat.search(text):
                    self.con.execute("INSERT OR IGNORE INTO chunk_signals VALUES (?,?)", (chunk_id, sig))
                    hit += 1
        self.con.commit()
        counts = {r[0]: r[1] for r in self.con.execute("SELECT signal_type, COUNT(*) FROM chunk_signals GROUP BY signal_type")}
        print(f"  L2 prescan: {total} chunks → {hit} signals {counts}")

    async def _tag_priority(self, limit=100):
        """Tag highest-priority prescan hits with LLM."""
        # Priority: exam_hint > problem > important
        todo = []
        for sig in ["exam_hint", "problem", "important"]:
            cur = self.con.execute(
                "SELECT cs.chunk_id FROM chunk_signals cs "
                "WHERE cs.signal_type=? AND cs.chunk_id NOT IN (SELECT chunk_id FROM build_log WHERE status='done') "
                "LIMIT ?", (sig, limit - len(todo)))
            todo.extend([r[0] for r in cur])
            if len(todo) >= limit: break
        todo = todo[:limit]
        print(f"  L2 tag: {len(todo)} priority chunks...", flush=True)
        if not todo: return

        done = 0; tc = Counter()
        for cid in todo:
            text = self.con.execute("SELECT text FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
            if not text: continue
            prompt = TAG_PROMPT.format(text=text[0][:3000])
            raw = await self._llm_call(prompt)
            tags = self._parse_tags(raw)
            for tt in ["problems","important","exam_hints","cross_refs","concepts","media"]:
                for val in tags.get(tt, []):
                    if val and isinstance(val, str) and len(val.strip()) >= 3:
                        self.con.execute("INSERT OR IGNORE INTO chunk_tags VALUES (?,?,?,'gemini','single',?)",
                                       (cid, tt, val.strip()[:500], time.time()))
                        tc[tt] += 1
            self.con.execute("INSERT OR REPLACE INTO build_log VALUES (?,'tag','done','gemini')", (cid,))
            self.con.commit(); done += 1
            if done % 20 == 0:
                print(f"    [{done}/{len(todo)}] {dict(tc)}", flush=True)
        print(f"  L2 tag done: {done} chunks, {sum(tc.values())} tags")

    def _build_links(self):
        """L3: cross-links from shared concepts."""
        self.con.execute("DELETE FROM chunk_links")
        concept_chunks: dict[str, list[int]] = defaultdict(list)
        for cid, val in self.con.execute("SELECT chunk_id, tag_value FROM chunk_tags WHERE tag_type='concepts'"):
            concept_chunks[val.lower().strip()].append(cid)
        pairs: dict[tuple, set] = defaultdict(set)
        for concept, cids in concept_chunks.items():
            if len(cids) > 50: continue
            for i in range(len(cids)):
                for j in range(i+1, len(cids)):
                    a, b = min(cids[i],cids[j]), max(cids[i],cids[j])
                    # Different docs only
                    da = self.con.execute("SELECT doc_id FROM chunks WHERE chunk_id=?", (a,)).fetchone()
                    db = self.con.execute("SELECT doc_id FROM chunks WHERE chunk_id=?", (b,)).fetchone()
                    if da and db and da[0] != db[0]:
                        pairs[(a,b)].add(concept)
        batch = []
        for (a,b), shared in pairs.items():
            if len(shared) >= 2:
                batch.append((a, b, json.dumps(sorted(shared), ensure_ascii=False), len(shared), 0))
        if batch:
            self.con.executemany("INSERT OR REPLACE INTO chunk_links VALUES (?,?,?,?,?)", batch)
        self.con.commit()
        print(f"  L3 links: {len(batch)} cross-doc links")

    async def _build_raptor(self, max_docs=3):
        """L4: RAPTOR tree for biggest documents."""
        cur = self.con.execute(
            "SELECT doc_id, COUNT(*) as n FROM chunks GROUP BY doc_id HAVING n >= 5 ORDER BY n DESC LIMIT ?",
            (max_docs,))
        docs = cur.fetchall()
        total = 0
        for doc_id, n_chunks in docs:
            chunks = self.con.execute("SELECT chunk_id, text FROM chunks WHERE doc_id=? ORDER BY chunk_idx", (doc_id,)).fetchall()
            current = [(cid, text) for cid, text in chunks]
            for level in range(1, 4):
                if len(current) <= 1: break
                next_level = []
                for i in range(0, len(current), 5):
                    group = current[i:i+5]
                    combined = "\n---\n".join(g[1][:400] for g in group)
                    max_c = max(80, 300 // level)
                    summary = await self._llm_call(SUMMARIZE_PROMPT.format(
                        n=len(group), max_chars=max_c, chunks_text=combined[:2500]))
                    summary = summary.strip()[:max_c+50]
                    self.con.execute("INSERT INTO tree_nodes (level,doc_id,text,child_ids,created_at) VALUES (?,?,?,?,?)",
                                   (level, doc_id, summary, json.dumps([g[0] for g in group]), time.time()))
                    nid = self.con.execute("SELECT last_insert_rowid()").fetchone()[0]
                    next_level.append((nid, summary)); total += 1
                current = next_level
            self.con.commit()
        print(f"  L4 RAPTOR: {total} tree nodes across {len(docs)} docs")

    # ============================================================
    # QUERY — searches all layers simultaneously
    # ============================================================
    def query(self, q: str, top_k: int = 10, mode: str = "hybrid",
              course_filter: str | None = None, generate: bool = True) -> dict:
        """
        Unified query across all layers.

        Modes:
          hybrid: BM25 + embedding + tag boost + link expansion + tree search → RRF
          exhaustive: tag-based full recall (for "다 가져와" queries)
          tag:<type>: direct tag query (e.g. tag:problems)
        """
        # Parse course filter from query prefix
        if q.startswith("course:"):
            parts = q.split(None, 1)
            course_filter = parts[0].split(":")[1]
            q = parts[1] if len(parts) > 1 else ""

        allowed = None
        if course_filter:
            allowed = set(r[0] for r in self.con.execute(
                "SELECT c.chunk_id FROM chunks c JOIN documents d ON c.doc_id=d.doc_id "
                "WHERE json_extract(d.meta,'$.course_id')=?", (course_filter,)))

        if mode.startswith("tag:"):
            return self._query_tag(mode.split(":")[1], course_filter, q)

        if mode == "exhaustive":
            return self._query_exhaustive(q, course_filter)

        # ---- Hybrid: search all layers ----
        variants = expand_query(q)
        wterms = weighted_tokens(variants)

        # R1: BM25
        r1 = self._bm25_search(wterms, allowed, top_k * 5)

        # R2: Embedding
        r2 = self._embed_search(q, allowed, top_k * 5)

        # R3: Tag boost — if query matches a tag type, boost those chunks
        r3 = self._tag_search(q, allowed)

        # R4: Tree search — search RAPTOR nodes
        r4 = self._tree_search(q, top_k * 2)

        # RRF fusion across all rankings
        rankings = [r for r in [r1, r2, r3, r4] if r]
        fused = rrf_fuse(rankings, k=10)

        # Build results with link expansion
        results = self._build_results(fused, top_k, variants)

        # Generate answer if requested
        answer = None
        if generate and results:
            answer = asyncio.run(self._generate_answer(q, results))

        return {
            "query": q,
            "mode": mode,
            "results": results,
            "answer": answer,
            "layers_used": [f"R{i+1}" for i, r in enumerate([r1,r2,r3,r4]) if r],
        }

    def _bm25_search(self, wterms, allowed, top_k):
        from store import Store
        # Inline BM25
        try:
            N = int(self.con.execute("SELECT value FROM bm25_meta WHERE key='total_chunks'").fetchone()[0])
            avg_dl = float(self.con.execute("SELECT value FROM bm25_meta WHERE key='avg_dl'").fetchone()[0])
        except: return []
        scores = {}
        for term, w in wterms:
            df_r = self.con.execute("SELECT value FROM bm25_meta WHERE key=?", (f"df:{term}",)).fetchone()
            if not df_r: continue
            df = int(df_r[0])
            if df / max(N,1) > 0.5: continue
            idf = math.log(1 + (N-df+0.5)/(df+0.5))
            for cid, tf in self.con.execute("SELECT chunk_id, tf FROM bm25_postings WHERE term=?", (term,)):
                if allowed and cid not in allowed: continue
                dl = self.con.execute("SELECT tok_count FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
                dl = dl[0] if dl else avg_dl
                denom = tf + 1.5 * (1 - 0.75 + 0.75 * dl / avg_dl)
                scores[cid] = scores.get(cid, 0) + w * idf * (tf * 2.5) / denom
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    def _embed_search(self, query, allowed, top_k):
        if not self._vec_cache:
            for cid, blob in self.con.execute("SELECT chunk_id, vector FROM embeddings"):
                if allowed and cid not in allowed: continue
                n = len(blob)//4
                self._vec_cache[cid] = list(struct.unpack(f"{n}f", blob))
        if not self._vec_cache: return []
        q_vec = self._get_embedder().encode([query])[0]
        scores = [(cid, cosine_sim(q_vec, vec)) for cid, vec in self._vec_cache.items()]
        return sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]

    def _tag_search(self, query, allowed):
        """Boost chunks whose tags match the query intent."""
        tag_map = {
            "문제": "problems", "풀이": "problems", "과제": "problems",
            "중요": "important", "강조": "important",
            "시험": "exam_hints", "출제": "exam_hints", "성향": "exam_hints",
            "회로": "media", "그림": "media", "사진": "media", "그래프": "media",
        }
        matched_types = set()
        for kw, tt in tag_map.items():
            if kw in query:
                matched_types.add(tt)
        if not matched_types: return []

        scores = []
        for tt in matched_types:
            if allowed:
                cur = self.con.execute(
                    "SELECT chunk_id, COUNT(*) FROM chunk_tags WHERE tag_type=? AND chunk_id IN ({}) GROUP BY chunk_id".format(
                        ",".join(str(x) for x in allowed)), (tt,))
            else:
                cur = self.con.execute("SELECT chunk_id, COUNT(*) FROM chunk_tags WHERE tag_type=? GROUP BY chunk_id", (tt,))
            for cid, cnt in cur:
                scores.append((cid, cnt * 10.0))  # Strong boost for tag matches
        return sorted(scores, key=lambda x: x[1], reverse=True)

    def _tree_search(self, query, top_k):
        """Search RAPTOR tree nodes."""
        nodes = self.con.execute("SELECT node_id, text FROM tree_nodes ORDER BY level DESC").fetchall()
        if not nodes: return []
        q_toks = set(tokenize(query))
        scores = []
        for nid, text in nodes:
            t_toks = set(tokenize(text))
            overlap = len(q_toks & t_toks)
            if overlap > 0:
                # Map tree node back to chunk_ids
                child_ids = self.con.execute("SELECT child_ids FROM tree_nodes WHERE node_id=?", (nid,)).fetchone()
                if child_ids:
                    try:
                        cids = json.loads(child_ids[0])
                        for cid in cids:
                            scores.append((int(cid), overlap * 5.0))
                    except: pass
        return sorted(scores, key=lambda x: x[1], reverse=True)[:top_k]

    def _build_results(self, fused, top_k, variants):
        results = []; seen = set()
        for cid, score in fused[:top_k * 2]:
            chunk = self.con.execute("SELECT chunk_id, text, doc_id, char_start, char_end FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
            if not chunk: continue
            prefix = chunk[1][:80]
            if prefix in seen: continue
            seen.add(prefix)
            doc = self.con.execute("SELECT path, meta FROM documents WHERE doc_id=?", (chunk[2],)).fetchone()
            meta = json.loads(doc[1]) if doc else {}

            # Get tags for this chunk
            tags = {}
            for tt, tv in self.con.execute("SELECT tag_type, tag_value FROM chunk_tags WHERE chunk_id=?", (cid,)):
                tags.setdefault(tt, []).append(tv)

            # Get linked chunks
            links = []
            for linked_id, in self.con.execute(
                "SELECT chunk_b FROM chunk_links WHERE chunk_a=? UNION SELECT chunk_a FROM chunk_links WHERE chunk_b=?",
                (cid, cid)):
                links.append(linked_id)

            results.append({
                "chunk_id": cid, "score": round(score, 4),
                "text": chunk[1], "doc_path": doc[0] if doc else "?",
                "doc_meta": meta, "tags": tags, "linked_chunks": links[:5],
            })
            if len(results) >= top_k: break
        return results

    def _query_tag(self, tag_type, course_filter, query):
        """Direct tag-based query — exhaustive recall for a tag type."""
        if course_filter:
            cur = self.con.execute("""
                SELECT t.chunk_id, t.tag_value, c.text
                FROM chunk_tags t JOIN chunks c ON t.chunk_id=c.chunk_id
                JOIN documents d ON c.doc_id=d.doc_id
                WHERE t.tag_type=? AND json_extract(d.meta,'$.course_id')=?
            """, (tag_type, course_filter))
        else:
            cur = self.con.execute(
                "SELECT t.chunk_id, t.tag_value, c.text FROM chunk_tags t JOIN chunks c ON t.chunk_id=c.chunk_id WHERE t.tag_type=?",
                (tag_type,))
        results = [{"chunk_id": r[0], "tag": r[1], "text": r[2][:300]} for r in cur]
        return {"query": query, "mode": f"tag:{tag_type}", "results": results, "answer": None}

    def _query_exhaustive(self, query, course_filter):
        """Exhaustive: combine tag results + BM25 results, dedup."""
        # Tag results
        tag_results = []
        for tt in ["problems","important","exam_hints","media"]:
            tag_results.extend(self._query_tag(tt, course_filter, query)["results"])
        # BM25 results
        variants = expand_query(query)
        wterms = weighted_tokens(variants)
        allowed = None
        if course_filter:
            allowed = set(r[0] for r in self.con.execute(
                "SELECT c.chunk_id FROM chunks c JOIN documents d ON c.doc_id=d.doc_id "
                "WHERE json_extract(d.meta,'$.course_id')=?", (course_filter,)))
        bm25 = self._bm25_search(wterms, allowed, 20)
        bm25_results = []
        for cid, score in bm25:
            text = self.con.execute("SELECT text FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
            bm25_results.append({"chunk_id": cid, "score": score, "text": text[0][:300] if text else ""})
        # Merge dedup
        seen = set()
        merged = []
        for r in tag_results + bm25_results:
            if r["chunk_id"] not in seen:
                seen.add(r["chunk_id"])
                merged.append(r)
        return {"query": query, "mode": "exhaustive", "results": merged, "answer": None}

    async def _generate_answer(self, query, results):
        evidence = []
        for i, r in enumerate(results[:10], 1):
            evidence.append(f"[chunk {i}] ({r.get('doc_meta',{}).get('source_name','?')})")
            evidence.append(r["text"][:800])
            evidence.append("")
        prompt = ANSWER_PROMPT.format(query=query, evidence="\n".join(evidence))
        answer = await self._llm_call(prompt)
        return answer

    # ---- Utilities ----
    def _infer_meta(self, path, root):
        meta = {"source_name": path.parent.name, "filename": path.name}
        for p in path.relative_to(root).parts:
            m = re.match(r"course_(\d+)(?:_(.*))?", p)
            if m: meta["course_id"] = m.group(1); meta["course_name"] = m.group(2) or ""
            m = re.match(r"week(\d+)_", p)
            if m: meta["week_key"] = f"week{int(m.group(1)):02d}"
        if "syllabus" in str(path): meta["source_kind"] = "syllabus"
        elif "_assignments" in str(path): meta["source_kind"] = "assignment"
        else: meta["source_kind"] = "lecture_material"
        return meta

    def _parse_tags(self, text):
        m = re.search(r"\{[\s\S]*\}", text)
        if m:
            try: return json.loads(m.group(0))
            except: pass
        return {}

    def _print_stats(self):
        docs = self.con.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
        chunks = self.con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
        embeds = self.con.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
        tags = self.con.execute("SELECT COUNT(*) FROM chunk_tags").fetchone()[0]
        links = self.con.execute("SELECT COUNT(*) FROM chunk_links").fetchone()[0]
        nodes = self.con.execute("SELECT COUNT(*) FROM tree_nodes").fetchone()[0]
        print(f"\n  Stats: {docs} docs, {chunks} chunks, {embeds} embeds, "
              f"{tags} tags, {links} links, {nodes} tree nodes")

    def close(self):
        if self._gemini:
            asyncio.run(self._gemini.close())
        self.con.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd")

    p_ingest = sub.add_parser("ingest")
    p_ingest.add_argument("--root", required=True)
    p_ingest.add_argument("--db", default="./memory.db")
    p_ingest.add_argument("--skip-embed", action="store_true")
    p_ingest.add_argument("--tag-limit", type=int, default=100)

    p_query = sub.add_parser("query")
    p_query.add_argument("--db", default="./memory.db")
    p_query.add_argument("--q", required=True)
    p_query.add_argument("--top", type=int, default=10)
    p_query.add_argument("--mode", default="hybrid", help="hybrid|exhaustive|tag:<type>")
    p_query.add_argument("--generate", action="store_true")

    args = ap.parse_args()
    if args.cmd == "ingest":
        ms = MemorySystem(args.db)
        ms.ingest(args.root, skip_embed=args.skip_embed, tag_limit=args.tag_limit)
        ms.close()
    elif args.cmd == "query":
        ms = MemorySystem(args.db)
        result = ms.query(args.q, top_k=args.top, mode=args.mode, generate=args.generate)
        print(f"\nQuery: {result['query']}")
        print(f"Mode: {result['mode']}")
        print(f"Layers: {result.get('layers_used','?')}")
        print(f"Results: {len(result['results'])}")
        for i, r in enumerate(result["results"][:5], 1):
            meta = r.get("doc_meta", {})
            print(f"\n[{i}] score={r.get('score','?')} {meta.get('source_kind','?')}/{meta.get('week_key','?')}")
            print(f"    tags: {list(r.get('tags',{}).keys())}")
            print(f"    links: {len(r.get('linked_chunks',[]))}")
            print(f"    text: {r['text'][:200]}...")
        if result.get("answer"):
            print(f"\n{'='*60}\nANSWER:\n{'='*60}")
            print(result["answer"])
    else:
        ap.print_help()
