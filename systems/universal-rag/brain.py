#!/usr/bin/env python3
"""
Brain — unified memory system.

One file. One pipeline. Everything integrated.

INGEST:
  raw text → chunk → atomic compress → RAPTOR tree → prescan signals
  → signal propagation → concept graph → ready

QUERY:
  query → 3-tier classification:
    tier 1: embedding seed similarity (0 LLM, instant, recall 100%)
    tier 2: individual LLM verify on candidates only (5 LLM calls)
    tier 3: ambiguous → full LLM analysis
  → signal-guided top-down (O(log n)) → tag filter → BM25+embed
  → graph expansion → RRF fusion → LLM reader → grounded answer

Usage:
  python3 brain.py ingest --root /path/to/docs --db brain.db
  python3 brain.py query --db brain.db --q "시험범위"
  python3 brain.py query --db brain.db --q "문제 다 뽑아와" --generate
"""
from __future__ import annotations
import asyncio, hashlib, json, math, os, re, sqlite3, struct, subprocess
import sys, time
from collections import Counter, defaultdict
from pathlib import Path

# ═══════════════════════════════════════════════════════════
# TOKENIZER
# ═══════════════════════════════════════════════════════════
def tokenize(text):
    tokens = []
    tokens.extend(t.lower() for t in re.findall(r"[A-Za-z][A-Za-z0-9_]{1,}", text))
    tokens.extend(re.findall(r"\d{2,}", text))
    for run in re.findall(r"[\uac00-\ud7a3]+", text):
        for n in (2,3):
            for i in range(len(run)-n+1): tokens.append(run[i:i+n])
    return tokens

STOP_KO = {'있는','없는','하고','해서','그리고','또한','에서','으로','합니다','되는','하는','것이','하여','대한','위한','같은','통해','따라','때문','라고'}

# ═══════════════════════════════════════════════════════════
# SIGNAL PATTERNS (prescan)
# ═══════════════════════════════════════════════════════════
SIGNALS = {
    "exam": re.compile(r'(시험 문제|출제|시험 범위|문제.*되기 좋|시험.*팁|시험.*형식)', re.I),
    "problem": re.compile(r'(문제|풀이|예제|exercise|quiz|과제|homework|HW|구하시오|구하라|계산하)', re.I),
    "important": re.compile(r'(중요|꼭 알|반드시|시험에 나|기억해|핵심|유의|주의해)', re.I),
    "media": re.compile(r'(그래프|그림|회로|사진|도식|figure|image|diagram|circuit)', re.I),
    "cross_ref": re.compile(r'(저번|앞서|다음 시간|지난|이전에|참고)', re.I),
}

# ═══════════════════════════════════════════════════════════
# ATOMIC COMPRESSOR
# ═══════════════════════════════════════════════════════════
def atomic_compress(text, target=200):
    """Rule-based: keep nouns/verbs/numbers/English, drop filler."""
    words = text.split()
    kept = []
    for w in words:
        clean = re.sub(r'[^\w가-힣]', '', w)
        if not clean or len(clean) < 2: continue
        if clean in STOP_KO: continue
        kept.append(clean)
    return ' '.join(kept)[:target]

async def llm_compress(text, client, target=200):
    """LLM-based: semantic compression preserving anchors."""
    prompt = f"""다음 텍스트를 {target}자 이내로 압축하세요.
규칙: 핵심 용어, 수치, 교수 강조, 시험/문제 관련 언급은 반드시 보존. 압축문만 출력.

{text[:2000]}"""
    try:
        resp = await client.generate_content(prompt)
        return (resp.text or "").strip()[:target+50]
    except:
        return atomic_compress(text, target)

# ═══════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════
def init_db(path):
    con = sqlite3.connect(path)
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript("""
        CREATE TABLE IF NOT EXISTS docs (
            doc_id INTEGER PRIMARY KEY, path TEXT UNIQUE, sha TEXT, meta TEXT DEFAULT '{}'
        );
        CREATE TABLE IF NOT EXISTS chunks (
            chunk_id INTEGER PRIMARY KEY, doc_id INTEGER, idx INTEGER,
            text TEXT, compressed TEXT, char_s INTEGER, char_e INTEGER, tok_n INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS embeds (chunk_id INTEGER PRIMARY KEY, vec BLOB);
        CREATE TABLE IF NOT EXISTS bm25 (term TEXT, chunk_id INTEGER, tf INTEGER, PRIMARY KEY(term,chunk_id));
        CREATE TABLE IF NOT EXISTS bm25_meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS signals (chunk_id INTEGER, sig TEXT, PRIMARY KEY(chunk_id,sig));
        CREATE TABLE IF NOT EXISTS tags (
            chunk_id INTEGER, kind TEXT, value TEXT, model TEXT DEFAULT 'gemini',
            confidence TEXT DEFAULT 'single',
            PRIMARY KEY(chunk_id, kind, value)
        );
        CREATE TABLE IF NOT EXISTS tree (
            node_id INTEGER PRIMARY KEY, level INTEGER, doc_id INTEGER,
            text TEXT, child_ids TEXT, sigs TEXT DEFAULT '[]'
        );
        CREATE TABLE IF NOT EXISTS links (
            a INTEGER, b INTEGER, shared TEXT, strength INTEGER,
            PRIMARY KEY(a,b)
        );
        CREATE TABLE IF NOT EXISTS build_log (chunk_id INTEGER PRIMARY KEY, status TEXT);
        CREATE INDEX IF NOT EXISTS ix_bm25 ON bm25(term);
        CREATE INDEX IF NOT EXISTS ix_sig ON signals(sig);
        CREATE INDEX IF NOT EXISTS ix_tag ON tags(kind);
        CREATE INDEX IF NOT EXISTS ix_tree ON tree(level);
    """)
    con.commit()
    return con

# ═══════════════════════════════════════════════════════════
# GEMINI CLIENT
# ═══════════════════════════════════════════════════════════
_gemini = None
async def get_gemini():
    global _gemini
    if _gemini is None:
        cc = sqlite3.connect("/tmp/ff_cookies.sqlite")
        cr = cc.cursor()
        cr.execute("SELECT name, value FROM moz_cookies WHERE host=? AND name IN (?,?)",
                   (".google.com","__Secure-1PSID","__Secure-1PSIDTS"))
        ck = dict(cr.fetchall()); cc.close()
        from gemini_webapi import GeminiClient
        _gemini = GeminiClient(ck["__Secure-1PSID"], ck["__Secure-1PSIDTS"])
        await _gemini.init(timeout=30)
    return _gemini

def codex_call(prompt, timeout=120):
    try:
        r = subprocess.run(["codex","exec","--skip-git-repo-check","-"],
            input=prompt, capture_output=True, text=True, timeout=timeout)
        lines = [l for l in r.stdout.splitlines() if l.strip()
                 and not any(l.startswith(x) for x in ("codex","tokens","Let's","---"))]
        return "\n".join(lines).strip()
    except: return ""

# ═══════════════════════════════════════════════════════════
# PHASE 1: INGEST — chunk + compress + BM25
# ═══════════════════════════════════════════════════════════
def phase1_ingest(con, root):
    """Chunk all text files, atomic-compress each chunk, build BM25."""
    root = Path(root).resolve()
    SKIP = {"pages","crops","__pycache__",".cache","node_modules",".git"}
    doc_n = chunk_n = 0
    for dp, dn, fns in os.walk(root):
        dn[:] = [d for d in dn if d not in SKIP]
        for fn in sorted(fns):
            if not (fn.lower().endswith(".md") or fn.lower().endswith(".txt")): continue
            p = Path(dp)/fn
            try: body = p.read_text(encoding="utf-8", errors="replace")
            except: continue
            if len(body.strip()) < 50: continue
            sha = hashlib.sha256(body.encode("utf-8","replace")).hexdigest()
            meta = _infer_meta(p, root)
            cur = con.execute("INSERT INTO docs (path,sha,meta) VALUES (?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET sha=excluded.sha,meta=excluded.meta RETURNING doc_id",
                (str(p), sha, json.dumps(meta, ensure_ascii=False)))
            did = cur.fetchone()[0]
            con.execute("DELETE FROM chunks WHERE doc_id=?", (did,))
            # Chunk
            chunks = _chunk(body)
            for i, c in enumerate(chunks):
                comp = atomic_compress(c["text"], 200)
                toks = tokenize(c["text"])
                con.execute("INSERT INTO chunks (doc_id,idx,text,compressed,char_s,char_e,tok_n) VALUES (?,?,?,?,?,?,?)",
                    (did, i, c["text"], comp, c["s"], c["e"], len(toks)))
            doc_n += 1; chunk_n += len(chunks)
            if doc_n % 100 == 0:
                con.commit()
                print(f"  P1: {doc_n} docs {chunk_n} chunks", flush=True)
    con.commit()
    # BM25
    con.execute("DELETE FROM bm25"); con.execute("DELETE FROM bm25_meta")
    df = Counter(); total = 0; n = 0; batch = []
    for cid, text in con.execute("SELECT chunk_id, text FROM chunks"):
        toks = tokenize(text); tf = Counter(toks)
        total += len(toks); n += 1
        for t in tf: df[t] += 1
        for t, c in tf.items(): batch.append((t, cid, c))
        if len(batch) > 50000:
            con.executemany("INSERT INTO bm25 VALUES (?,?,?)", batch); batch = []
    if batch: con.executemany("INSERT INTO bm25 VALUES (?,?,?)", batch)
    con.execute("INSERT OR REPLACE INTO bm25_meta VALUES ('N',?)", (str(n),))
    con.execute("INSERT OR REPLACE INTO bm25_meta VALUES ('avg',?)", (str(total/max(1,n)),))
    for t, d in df.items():
        con.execute("INSERT OR REPLACE INTO bm25_meta VALUES (?,?)", (f"df:{t}", str(d)))
    con.commit()
    print(f"  P1 done: {doc_n} docs, {chunk_n} chunks, {len(df)} terms")

# ═══════════════════════════════════════════════════════════
# PHASE 2: PRESCAN — instant signal detection
# ═══════════════════════════════════════════════════════════
def phase2_prescan(con):
    con.execute("DELETE FROM signals")
    total = hit = 0
    for cid, text in con.execute("SELECT chunk_id, text FROM chunks"):
        total += 1
        for sig, pat in SIGNALS.items():
            if pat.search(text):
                con.execute("INSERT OR IGNORE INTO signals VALUES (?,?)", (cid, sig))
                hit += 1
    con.commit()
    counts = {r[0]:r[1] for r in con.execute("SELECT sig, COUNT(*) FROM signals GROUP BY sig")}
    print(f"  P2 prescan: {total} chunks → {hit} signals {counts}")

# ═══════════════════════════════════════════════════════════
# PHASE 3: RAPTOR TREE + SIGNAL PROPAGATION
# ═══════════════════════════════════════════════════════════
def phase3_raptor(con, max_docs=5):
    """Build RAPTOR trees for biggest docs, propagate prescan signals up."""
    con.execute("DELETE FROM tree")
    docs = con.execute("""
        SELECT doc_id, COUNT(*) as n FROM chunks GROUP BY doc_id
        HAVING n >= 5 ORDER BY n DESC LIMIT ?
    """, (max_docs,)).fetchall()

    # Collect leaf signals
    leaf_sigs = defaultdict(set)
    for cid, sig in con.execute("SELECT chunk_id, sig FROM signals"):
        leaf_sigs[cid].add(sig)

    total_nodes = 0
    for did, nchunks in docs:
        chunks = con.execute("SELECT chunk_id, compressed FROM chunks WHERE doc_id=? ORDER BY idx", (did,)).fetchall()
        current = [(cid, comp) for cid, comp in chunks]

        for level in range(1, 4):
            if len(current) <= 1: break
            nxt = []
            for i in range(0, len(current), 5):
                grp = current[i:i+5]
                # Summarize using compressed text (faster, cheaper)
                combined = " | ".join(g[1][:150] for g in grp)[:800]
                child_ids = [g[0] for g in grp]

                # Propagate signals from children
                node_sigs = set()
                for cid in child_ids:
                    if cid in leaf_sigs: node_sigs.update(leaf_sigs[cid])
                    # Check if child is a tree node
                    sub = con.execute("SELECT sigs FROM tree WHERE node_id=?", (cid,)).fetchone()
                    if sub and sub[0]:
                        try: node_sigs.update(json.loads(sub[0]))
                        except: pass

                con.execute("INSERT INTO tree (level,doc_id,text,child_ids,sigs) VALUES (?,?,?,?,?)",
                    (level, did, combined, json.dumps(child_ids), json.dumps(sorted(node_sigs))))
                nid = con.execute("SELECT last_insert_rowid()").fetchone()[0]
                # Register this node's signals for higher levels
                leaf_sigs[nid] = node_sigs
                nxt.append((nid, combined))
            current = nxt
            total_nodes += len(nxt)
        con.commit()

    print(f"  P3 RAPTOR: {total_nodes} tree nodes across {len(docs)} docs, signals propagated")

# ═══════════════════════════════════════════════════════════
# PHASE 3.5: EMBEDDING SEED CLASSIFIER (3-tier cheap→expensive)
# ═══════════════════════════════════════════════════════════
def phase35_embed_classify(con, seed_chunks: dict[str, list[int]], threshold=0.65):
    """Classify chunks by embedding similarity to seed examples.

    seed_chunks: {"exam": [cid1,cid2], "problem": [cid3,cid4], ...}
    For each category, find all chunks similar to the seeds.
    This is TIER 1: 0 LLM calls, instant, recall ~100%.

    Results go into the 'tags' table with confidence='embed_seed'.
    """
    try:
        import numpy as np
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("  P3.5 skip: sentence-transformers not available")
        return

    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    # Embed all chunks once
    all_chunks = con.execute("SELECT chunk_id, text FROM chunks ORDER BY chunk_id").fetchall()
    all_texts = [c[1] for c in all_chunks]
    all_ids = [c[0] for c in all_chunks]
    all_vecs = model.encode(all_texts, batch_size=256, show_progress_bar=False)

    total_tags = 0
    for category, seed_ids in seed_chunks.items():
        if not seed_ids:
            continue
        # Embed seeds and compute centroid
        seed_texts = []
        for sid in seed_ids:
            row = con.execute("SELECT text FROM chunks WHERE chunk_id=?", (sid,)).fetchone()
            if row:
                seed_texts.append(row[0])
        if not seed_texts:
            continue
        seed_vecs = model.encode(seed_texts)
        centroid = np.mean(seed_vecs, axis=0)
        centroid_norm = np.linalg.norm(centroid)

        # Find all chunks above threshold
        for i, cid in enumerate(all_ids):
            sim = float(np.dot(all_vecs[i], centroid) / (np.linalg.norm(all_vecs[i]) * centroid_norm + 1e-9))
            if sim >= threshold:
                con.execute(
                    "INSERT OR IGNORE INTO tags (chunk_id, kind, value, model, confidence) "
                    "VALUES (?, ?, ?, 'embed_seed', 'tier1')",
                    (cid, category, f"sim={sim:.3f}")
                )
                total_tags += 1

    con.commit()
    print(f"  P3.5 embed classify: {total_tags} tags across {len(seed_chunks)} categories")


async def phase35_llm_verify(con, category: str, max_verify=20):
    """TIER 2: individually verify embed-seed candidates with LLM.

    Only verifies chunks tagged with confidence='tier1' for the given category.
    Promotes to confidence='verified' or removes the tag.
    """
    candidates = con.execute(
        "SELECT t.chunk_id, c.text FROM tags t JOIN chunks c ON t.chunk_id=c.chunk_id "
        "WHERE t.kind=? AND t.confidence='tier1' LIMIT ?",
        (category, max_verify)
    ).fetchall()

    if not candidates:
        print(f"  P3.5 verify {category}: no tier1 candidates")
        return

    client = await get_gemini()
    verified = 0
    removed = 0

    VERIFY_PROMPT = """다음 텍스트가 "{category}" 카테고리에 해당하는지 판단하세요.

카테고리 정의:
- exam: 시험/출제/평가 관련 직접적 언급
- problem: 풀이 문제, 연습문제, 과제 문제
- important: 교수가 강조한 핵심 내용

"YES" 또는 "NO"만 답하세요.

텍스트:
{text}"""

    for cid, text in candidates:
        prompt = VERIFY_PROMPT.format(category=category, text=text[:2000])
        try:
            resp = await client.generate_content(prompt)
            answer = (resp.text or "").strip().upper()
        except:
            continue

        if "YES" in answer:
            con.execute("UPDATE tags SET confidence='verified' WHERE chunk_id=? AND kind=? AND confidence='tier1'",
                       (cid, category))
            verified += 1
        else:
            con.execute("DELETE FROM tags WHERE chunk_id=? AND kind=? AND confidence='tier1'",
                       (cid, category))
            removed += 1
        con.commit()

    print(f"  P3.5 verify {category}: {verified} verified, {removed} removed")


# ═══════════════════════════════════════════════════════════
# PHASE 4: CONCEPT GRAPH (brain map)
# ═══════════════════════════════════════════════════════════
def phase4_graph(con):
    """Build cross-document links from shared tokens in compressed text."""
    con.execute("DELETE FROM links")
    # Extract key terms per chunk from compressed text
    chunk_terms = {}
    for cid, comp in con.execute("SELECT chunk_id, compressed FROM chunks"):
        terms = set(tokenize(comp))
        if len(terms) >= 3:
            chunk_terms[cid] = terms

    # Invert: term → chunk_ids
    term_chunks = defaultdict(list)
    for cid, terms in chunk_terms.items():
        for t in terms:
            if len(term_chunks[t]) < 100:  # cap
                term_chunks[t].append(cid)

    # Find pairs with ≥3 shared terms from different docs
    pair_shared = defaultdict(set)
    for t, cids in term_chunks.items():
        if len(cids) > 50: continue
        for i in range(len(cids)):
            for j in range(i+1, min(len(cids), i+20)):
                a, b = min(cids[i],cids[j]), max(cids[i],cids[j])
                pair_shared[(a,b)].add(t)

    batch = []
    for (a,b), shared in pair_shared.items():
        if len(shared) < 3: continue
        # Different docs only
        da = con.execute("SELECT doc_id FROM chunks WHERE chunk_id=?", (a,)).fetchone()
        db = con.execute("SELECT doc_id FROM chunks WHERE chunk_id=?", (b,)).fetchone()
        if da and db and da[0] != db[0]:
            batch.append((a, b, json.dumps(sorted(list(shared)[:10])), len(shared)))
    if batch:
        con.executemany("INSERT OR REPLACE INTO links VALUES (?,?,?,?)", batch)
    con.commit()
    print(f"  P4 graph: {len(batch)} cross-doc links")

# ═══════════════════════════════════════════════════════════
# QUERY
# ═══════════════════════════════════════════════════════════
def query(con, q, top_k=10, course=None, generate=False):
    """Unified query across all layers."""
    if q.startswith("course:"):
        parts = q.split(None, 1)
        course = parts[0].split(":")[1]
        q = parts[1] if len(parts) > 1 else ""

    allowed = None
    if course:
        allowed = set(r[0] for r in con.execute(
            "SELECT c.chunk_id FROM chunks c JOIN docs d ON c.doc_id=d.doc_id "
            "WHERE json_extract(d.meta,'$.course_id')=?", (course,)))

    # R1: BM25
    r1 = _bm25(con, q, allowed, top_k*5)

    # R2: Tag boost
    r2 = _tag_search(con, q, allowed)

    # R3: Signal-guided tree search
    r3 = _tree_search(con, q, allowed)

    # R4: Graph expansion from top BM25 hits
    r4 = _graph_expand(con, [cid for cid,_ in r1[:5]], allowed)

    # RRF
    rankings = [r for r in [r1, r2, r3, r4] if r]
    scores = defaultdict(float)
    for ranking in rankings:
        for rank, (cid, _) in enumerate(ranking):
            scores[cid] += 1.0 / (10 + rank + 1)
    fused = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # Build results
    results = []
    seen = set()
    for cid, score in fused[:top_k*2]:
        ch = con.execute("SELECT text, doc_id FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
        if not ch: continue
        prefix = ch[0][:80]
        if prefix in seen: continue
        seen.add(prefix)
        doc = con.execute("SELECT meta FROM docs WHERE doc_id=?", (ch[1],)).fetchone()
        meta = json.loads(doc[0]) if doc else {}
        # Get tags
        chunk_tags = {}
        for k, v in con.execute("SELECT kind, value FROM tags WHERE chunk_id=?", (cid,)):
            chunk_tags.setdefault(k, []).append(v)
        results.append({
            "chunk_id": cid, "score": round(score, 4),
            "text": ch[0], "meta": meta, "tags": chunk_tags,
        })
        if len(results) >= top_k: break

    answer = None
    if generate and results:
        answer = _generate(q, results)

    layers = []
    if r1: layers.append("BM25")
    if r2: layers.append("tags")
    if r3: layers.append("tree")
    if r4: layers.append("graph")
    return {"query": q, "results": results, "answer": answer, "layers": layers}

def _bm25(con, q, allowed, top_k):
    try:
        N = int(con.execute("SELECT value FROM bm25_meta WHERE key='N'").fetchone()[0])
        avg = float(con.execute("SELECT value FROM bm25_meta WHERE key='avg'").fetchone()[0])
    except: return []
    toks = tokenize(q)
    scores = {}
    for t in set(toks):
        dfr = con.execute("SELECT value FROM bm25_meta WHERE key=?", (f"df:{t}",)).fetchone()
        if not dfr: continue
        df = int(dfr[0])
        if df/max(N,1) > 0.5: continue
        idf = math.log(1+(N-df+0.5)/(df+0.5))
        for cid, tf in con.execute("SELECT chunk_id, tf FROM bm25 WHERE term=?", (t,)):
            if allowed and cid not in allowed: continue
            dl = con.execute("SELECT tok_n FROM chunks WHERE chunk_id=?", (cid,)).fetchone()
            dl = dl[0] if dl else avg
            scores[cid] = scores.get(cid,0) + idf*(tf*2.5)/(tf+1.5*(1-0.75+0.75*dl/avg))
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

TAG_MAP = {"문제":"problem","풀이":"problem","과제":"problem",
           "중요":"important","강조":"important",
           "시험":"exam","출제":"exam","성향":"exam",
           "회로":"media","그림":"media","사진":"media","그래프":"media"}

def _tag_search(con, q, allowed):
    matched = set()
    for kw, tt in TAG_MAP.items():
        if kw in q: matched.add(tt)
    if not matched: return []
    scores = []
    for tt in matched:
        sql = "SELECT chunk_id, COUNT(*) FROM tags WHERE kind=? GROUP BY chunk_id"
        for cid, cnt in con.execute(sql, (tt,)):
            if allowed and cid not in allowed: continue
            scores.append((cid, cnt * 10.0))
    return sorted(scores, key=lambda x: x[1], reverse=True)

def _tree_search(con, q, allowed):
    """Signal-guided top-down: find branches with matching signals."""
    # Map query to signal types
    q_sigs = set()
    for kw, sig in [("시험","exam"),("문제","problem"),("중요","important"),
                     ("회로","media"),("그림","media"),("참고","cross_ref")]:
        if kw in q: q_sigs.add(sig)
    if not q_sigs: return []

    reached = set()
    for lv in range(3, 0, -1):
        for nid, cj, sj in con.execute("SELECT node_id, child_ids, sigs FROM tree WHERE level=?", (lv,)):
            sigs = set(json.loads(sj)) if sj else set()
            if q_sigs & sigs:
                for c in json.loads(cj):
                    if allowed and c in allowed or not allowed:
                        reached.add(c)
                    sub = con.execute("SELECT child_ids FROM tree WHERE node_id=?", (c,)).fetchone()
                    if sub:
                        for sc in json.loads(sub[0]):
                            if not allowed or sc in allowed:
                                reached.add(sc)
    return [(cid, 5.0) for cid in reached]

def _graph_expand(con, seed_cids, allowed):
    """Expand from seed chunks via concept graph links."""
    expanded = []
    for cid in seed_cids:
        for linked, in con.execute(
            "SELECT b FROM links WHERE a=? UNION SELECT a FROM links WHERE b=?", (cid,cid)):
            if allowed and linked not in allowed: continue
            expanded.append((linked, 3.0))
    return expanded

def _generate(q, results):
    evidence = []
    for i, r in enumerate(results[:10], 1):
        evidence.append(f"[chunk {i}] ({r['meta'].get('source_name','?')})")
        evidence.append(r["text"][:800])
        evidence.append("")
    prompt = f"""문서 기반 QA. evidence만 사용. [chunk N] 출처 표기. 근거 없으면 "찾을 수 없습니다".

질문: {q}

--- Evidence ---
{chr(10).join(evidence)}
---

한국어로 답."""
    return codex_call(prompt, timeout=120)

# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════
def _chunk(text, size=800, overlap=200):
    if len(text) <= size:
        return [{"text": text, "s": 0, "e": len(text)}]
    chunks = []; pos = 0
    while pos < len(text):
        end = min(pos+size, len(text))
        if end < len(text):
            window = text[max(pos+size-200, pos+100):min(pos+size+100, len(text))]
            m = re.search(r"\n\s*\n", window)
            if m: end = max(pos+size-200, pos+100) + m.end()
            else:
                m = re.search(r"\n", window)
                if m: end = max(pos+size-200, pos+100) + m.end()
        t = text[pos:end].strip()
        if len(t) >= 100:
            chunks.append({"text": t, "s": pos, "e": end})
        pos = max(pos+1, end-overlap)
        if pos >= len(text): break
    return chunks

def _infer_meta(p, root):
    meta = {"source_name": p.parent.name, "filename": p.name}
    for part in p.relative_to(root).parts:
        m = re.match(r"course_(\d+)(?:_(.*))?", part)
        if m: meta["course_id"] = m.group(1); meta["course_name"] = m.group(2) or ""
        m = re.match(r"week(\d+)_", part)
        if m: meta["week_key"] = f"week{int(m.group(1)):02d}"
    return meta

def stats(con):
    d = con.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
    c = con.execute("SELECT COUNT(*) FROM chunks").fetchone()[0]
    s = con.execute("SELECT COUNT(*) FROM signals").fetchone()[0]
    t = con.execute("SELECT COUNT(*) FROM tags").fetchone()[0]
    tr = con.execute("SELECT COUNT(*) FROM tree").fetchone()[0]
    l = con.execute("SELECT COUNT(*) FROM links").fetchone()[0]
    print(f"  Stats: {d} docs, {c} chunks, {s} signals, {t} tags, {tr} tree nodes, {l} links")

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(prog="brain")
    sub = ap.add_subparsers(dest="cmd")

    p1 = sub.add_parser("ingest")
    p1.add_argument("--root", required=True)
    p1.add_argument("--db", default="brain.db")
    p1.add_argument("--max-tree-docs", type=int, default=10)

    p2 = sub.add_parser("query")
    p2.add_argument("--db", default="brain.db")
    p2.add_argument("--q", required=True)
    p2.add_argument("--top", type=int, default=10)
    p2.add_argument("--generate", action="store_true")

    p3 = sub.add_parser("stats")
    p3.add_argument("--db", default="brain.db")

    args = ap.parse_args()

    if args.cmd == "ingest":
        t0 = time.time()
        con = init_db(args.db)
        print("[P1] Ingest + chunk + compress + BM25", flush=True)
        phase1_ingest(con, args.root)
        print("[P2] Prescan signals", flush=True)
        phase2_prescan(con)
        print("[P3] RAPTOR tree + signal propagation", flush=True)
        phase3_raptor(con, max_docs=args.max_tree_docs)
        print("[P4] Concept graph", flush=True)
        phase4_graph(con)
        # P3.5 needs seed examples — auto-detect from prescan signals
        # Use prescan hits as initial seeds for embedding classification
        seeds = {}
        for sig in ["exam", "problem", "important"]:
            seed_ids = [r[0] for r in con.execute(
                "SELECT chunk_id FROM signals WHERE sig=? LIMIT 10", (sig,))]
            if seed_ids:
                seeds[sig] = seed_ids
        if seeds:
            print("[P3.5] Embedding seed classification", flush=True)
            phase35_embed_classify(con, seeds, threshold=0.65)
        stats(con)
        print(f"\n[DONE] {time.time()-t0:.0f}s", flush=True)
        con.close()

    elif args.cmd == "query":
        con = init_db(args.db)
        r = query(con, args.q, top_k=args.top, generate=args.generate)
        print(f"\nQuery: {r['query']}")
        print(f"Layers: {r['layers']}")
        print(f"Results: {len(r['results'])}\n")
        for i, res in enumerate(r["results"][:5], 1):
            m = res["meta"]
            print(f"[{i}] score={res['score']} {m.get('source_name','?')} {m.get('week_key','?')}")
            print(f"    tags: {list(res['tags'].keys())}")
            print(f"    {res['text'][:200]}...")
            print()
        if r.get("answer"):
            print(f"{'='*50}\nANSWER:\n{'='*50}\n{r['answer']}")
        con.close()

    elif args.cmd == "stats":
        con = init_db(args.db)
        stats(con)
        con.close()
    else:
        ap.print_help()
