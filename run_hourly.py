#!/usr/bin/env python3
"""Signal — Discord-only incremental pipeline.

매 실행:
1) 기존 articles.json(schema v2) 로드 (v1이면 migrate)
2) since = state.last_run_at → Discord export
3) 채팅을 50k chars 청크로 꽉 채워 분할
4) 청크별 LLM 호출 → [no] 또는 [yes]+<data>...
   (프롬프트 입력: 최근 20개 기사 '제목만' + 이 청크 원문)
5) 청크 결과 merge + 제목 Jaccard 중복 제거
6) 3일 초과 기사 drop
7) 새 기사 있으면 분류 호출 1회
   - 규칙: TOP=1(또는 0), MAIN≤6, SIDE 무제한
   - 입력: 규칙 + 최근 3h 결정 로그 + 현재 기사(placement 포함) + 새 기사
   - 검증 실패 → 무한 재시도
8) articles.json 저장 + build_gist.py 호출
"""
from __future__ import annotations
import argparse, json, os, re, subprocess, sys, time, threading, random
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests

ROOT = Path(__file__).parent
ARTICLES_PATH = ROOT / "docs" / "articles.json"
MODEL = "gemma-4-26b-a4b-it"
ENDPOINT_TPL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
KST = timezone(timedelta(hours=9))
CHANNEL_ID = "1365049274068631644"

CHUNK_MAX_CHARS = 80_000
MAX_MAIN = 6
EXPIRY_HOURS = 72
DECISION_LOG_HOURS = 3
TITLES_FOR_DEDUP = 20
KEY_MIN_GAP_S = 3.0
MERGE_ROUNDS = 3
ACTIVE_POOL_LIMIT = 200  # 이 이하면 existing+new 전체 merge, 이상이면 retrieval mode

LOG = lambda m: print(f"[{datetime.now().strftime('%H:%M:%S')}] {m}", flush=True)


# ── Keys ────────────────────────────────────────────────────────

class KeyScheduler:
    def __init__(self, keys):
        self.keys = list(keys)
        self.last_used = {k: 0.0 for k in keys}
        self.lock = threading.Lock()
        self._idx = 0
    def acquire(self):
        with self.lock:
            now = time.time()
            best, bw = None, float("inf")
            for i in range(len(self.keys)):
                k = self.keys[(self._idx + i) % len(self.keys)]
                w = max(0, self.last_used[k] + KEY_MIN_GAP_S - now)
                if w < bw: bw, best = w, k
                if w == 0: break
            if bw > 0: time.sleep(bw)
            self.last_used[best] = time.time()
            self._idx = (self.keys.index(best) + 1) % len(self.keys)
            return best

def load_keys():
    p = Path.home() / ".config" / "legal_evidence_rag" / "keys.env"
    for line in p.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            return [k.strip() for k in line.split("=", 1)[1].split(",") if k.strip()]
    raise RuntimeError("No API keys found")

def call_gemma(prompt, sched, max_tok=8192, temp=0.5, json_mode=False):
    endpoint = ENDPOINT_TPL.format(model=MODEL)
    gen_cfg = {"temperature": temp, "maxOutputTokens": max_tok}
    if json_mode:
        gen_cfg["responseMimeType"] = "application/json"
    # Thinking Burn 방지: Gemma는 thinkingLevel=minimal, Gemini는 high
    is_gemma = "gemma" in MODEL.lower()
    gen_cfg["thinkingConfig"] = {"thinkingLevel": "minimal" if is_gemma else "high"}
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": gen_cfg,
    }
    for attempt in range(20):
        key = sched.acquire()
        try:
            r = requests.post(f"{endpoint}?key={key}", json=body, timeout=240)
        except Exception as e:
            LOG(f"  net err: {e}"); time.sleep(30); continue
        if r.status_code == 429 or r.status_code >= 500:
            LOG(f"  {r.status_code} backoff")
            time.sleep(60 - (time.time() % 60) + 0.5 + random.random())
            continue
        if not r.ok:
            LOG(f"  err {r.status_code}: {r.text[:150]}"); time.sleep(10); continue
        try:
            parts = r.json()["candidates"][0]["content"]["parts"]
            # Gemma는 thought=True 블록을 먼저 반환 — 실제 답변 part만 골라냄
            for p in parts:
                if p.get("thought"): continue
                t = p.get("text", "")
                if t: return t
            # 모든 part가 thought거나 빈 경우
            time.sleep(3); continue
        except Exception:
            time.sleep(3); continue
    raise RuntimeError("API failed after 20 attempts")


# ── Parsers ─────────────────────────────────────────────────────

def parse_envelope(text):
    w = re.search(r'<\s*data\s*>([\s\S]*?)<\s*/\s*data\s*>', text, re.I)
    if not w: raise ValueError("no <data> wrapper")
    return w.group(1)

def parse_chunk_articles(text):
    """Parse priming-continuation: prepend '{"articles":' and try to find balanced JSON.
    Also try raw parse if whole text is JSON."""
    s = text.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()

    # Strategy 1: the response starts with a '[' or '"articles"' etc — try prepending
    # Gemma was primed with '{"articles":' so it should continue with array or value
    candidates = []
    if s and s[0] in '[':
        candidates.append('{"articles":' + s + '}')
        candidates.append('{"articles":' + s.rstrip(',}] ').rstrip() + ']}')
    # Strategy 2: find balanced JSON object anywhere
    start = s.find('{')
    end = s.rfind('}')
    if start != -1 and end > start:
        candidates.append(s[start:end+1])
    # Strategy 3: find articles array in text
    m = re.search(r'"articles"\s*:\s*(\[[\s\S]*?\])', s)
    if m:
        candidates.append('{"articles":' + m.group(1) + '}')

    for cand in candidates:
        try:
            obj = json.loads(cand)
        except Exception:
            # try trimming trailing junk
            try:
                obj = json.loads(re.sub(r',\s*([}\]])', r'\1', cand))
            except Exception:
                continue
        arts = obj.get("articles") if isinstance(obj, dict) else None
        if isinstance(arts, list):
            out = []
            for a in arts:
                if not isinstance(a, dict): continue
                hl = str(a.get("headline", "")).strip().strip('"\'')
                bd = str(a.get("body", "")).strip().strip('"\'')
                if len(hl) <= 4 or len(bd) <= 40: continue
                # 보수적 default: 태그 누락/잘못된 값이면 rumor/low
                cat = str(a.get("category", "")).strip().lower()
                if cat not in ("news", "rumor"): cat = "rumor"
                trust = str(a.get("trust", "")).strip().lower()
                if trust not in ("high", "low"): trust = "low"
                out.append({"headline": hl, "body": bd, "category": cat, "trust": trust})
            return out
    return []

def parse_placement_json(text):
    """Parse {"top": "id"|null, "main": [ids], "side": [ids]}"""
    s = text.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    if start == -1 or end <= start:
        raise ValueError("no JSON object found")
    obj = json.loads(s[start:end+1])
    def as_list(v):
        if v is None or v == "": return []
        if isinstance(v, str): return [v.strip()] if v.strip() else []
        if isinstance(v, list): return [str(x).strip() for x in v if str(x).strip()]
        raise ValueError(f"bad type: {type(v)}")
    return {"top": as_list(obj.get("top")), "main": as_list(obj.get("main")), "side": as_list(obj.get("side"))}


# ── State ───────────────────────────────────────────────────────

def new_id(now: datetime, n: int) -> str:
    return f"art-{now.strftime('%Y%m%d%H%M')}-{n:02d}"

def load_state():
    if not ARTICLES_PATH.exists():
        return {
            "schema_version": 2, "last_run_at": None,
            "generated_at": datetime.now(KST).isoformat(),
            "model": MODEL, "articles": [], "decision_log": [],
        }
    data = json.loads(ARTICLES_PATH.read_text(encoding="utf-8"))
    if data.get("schema_version") == 2:
        return data
    # migrate v1 → v2
    gen_at = data.get("generated_at") or datetime.now(KST).isoformat()
    period_end = (data.get("period") or {}).get("end")
    articles = []
    for a in data.get("articles", []):
        articles.append({
            "id": a.get("id") or f"legacy-{len(articles)+1:03d}",
            "headline": a["headline"],
            "body": a["body"],
            "created_at": gen_at,
            "placement": None,  # 분류 단계에서 채움
            "placed_at": gen_at,
        })
    return {
        "schema_version": 2,
        "last_run_at": period_end,  # 1회차 since = period.end
        "generated_at": gen_at,
        "model": MODEL,
        "articles": articles,
        "decision_log": [],
    }

def save_state(state):
    bak = ARTICLES_PATH.with_suffix(".json.bak")
    if ARTICLES_PATH.exists():
        bak.write_text(ARTICLES_PATH.read_text(encoding="utf-8"), encoding="utf-8")
    ARTICLES_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


# ── Discord ─────────────────────────────────────────────────────

def discord_export(since_iso: str) -> Path:
    """WSL면 기존 discord_export_text_only.py (PowerShell 의존), Linux면 discord_export_linux.py (dotnet DCE)."""
    since = datetime.fromisoformat(since_iso).astimezone(KST)
    after_kst = since.strftime("%Y-%m-%d %H:%M:%S")
    LOG(f"[discord] --after-kst '{after_kst}'")
    # powershell.exe 있으면 WSL 스크립트, 아니면 linux 스크립트
    use_linux = subprocess.run(["which", "powershell.exe"], capture_output=True).returncode != 0
    if use_linux:
        script = ROOT / "discord_export_linux.py"
        cmd = ["python3", str(script), "--channel", CHANNEL_ID, "--after-kst", after_kst]
    else:
        script = ROOT / "discord_export_text_only.py"
        cmd = ["python3", str(script), "--channel", CHANNEL_ID, "--after-kst", after_kst, "--no-upload"]
    LOG(f"  using: {script.name}")
    r = subprocess.run(cmd, capture_output=True, timeout=1800)
    stdout = r.stdout.decode("utf-8", errors="replace") if r.stdout else ""
    stderr = r.stderr.decode("utf-8", errors="replace") if r.stderr else ""
    if stderr:
        for line in stderr.splitlines():
            LOG(f"  [export-stderr] {line}")
    if r.returncode != 0:
        raise RuntimeError(f"Discord export failed: {stderr[-800:]}")
    for line in stdout.split("\n"):
        if line.startswith("final_file="):
            return Path(line.split("=", 1)[1].strip())
    raise RuntimeError(f"final_file not found:\n{stdout[-400:]}")

def read_chat_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    parts = text.split("=" * 62)
    return parts[2] if len(parts) >= 3 else text

MSG_HDR = re.compile(r'(^\[\d{4}\. \d{1,2}\. \d{1,2}\. (?:오전|오후) \d{1,2}:\d{2}\][^\n]*\n)', re.M)

def chunk_by_messages(text: str, max_chars: int = CHUNK_MAX_CHARS) -> list[str]:
    parts = MSG_HDR.split(text)
    blocks = []
    i = 1
    while i < len(parts):
        hdr = parts[i]
        body = parts[i + 1] if i + 1 < len(parts) else ""
        blocks.append(hdr + body)
        i += 2
    chunks, cur = [], ""
    for b in blocks:
        if cur and len(cur) + len(b) > max_chars:
            chunks.append(cur); cur = b
        else:
            cur += b
    if cur.strip(): chunks.append(cur)
    return chunks


# ── Prompts ─────────────────────────────────────────────────────

def prompt_scan_chunk(chunk: str, titles: list[str]) -> str:
    titles_block = "\n".join(f"- {t}" for t in titles) or "(없음)"
    return f"""역할: AI 뉴스 에디터. Discord 채팅에서 '새로운' AI 소식을 기사화하고, 각 기사에 category·trust 태그 부여.

## 출력 규칙
- JSON 객체 하나만. 마크다운 코드펜스 금지. 첫 문자 `{{`, 마지막 `}}`.

## 스키마
{{"articles":[{{"headline":"제목","body":"본문","category":"news|rumor","trust":"high|low"}}]}}

- **category**:
  - "news" = 공식 발표/모델 카드/API 공개/블로그 등 공식 소스 근거 있는 소식
  - "rumor" = 찌라시/루머/미확인 주장/개인 트윗만/스크린샷 유출/"~인 것 같다"
- **trust**:
  - "high" = 여러 소스 일관, 구체적 근거, 공식 출처 포함
  - "low" = 단일 언급, 추측, 농담일 수도 있음, 애매함

## 규칙
- 새 소식 없으면: {{"articles": []}}
- '이미 다룬 기사 제목'에 있는 소식은 재작성 금지
- **찌라시도 빠짐없이 출력** (category=rumor 로 태그 붙여서)
- 본문: 한국어 300~600자. 단정보단 "~라는 소식", "~라고 알려졌다" 같은 전언 형태 선호 (특히 rumor)
- 유저이름·닉네임 언급 금지. 채팅에 실제 나온 내용만.

## 이미 다룬 기사 제목 (최근 {TITLES_FOR_DEDUP}개)
{titles_block}

## Discord 채팅 (이번 청크)
{chunk}

JSON만 출력:"""


def prompt_classify(active: list, new: list, decision_log: list) -> tuple[str, dict]:
    # Build short ID map (1, 2, 3, ...) for prompt clarity
    ordered = active + new
    short2real = {str(i + 1): a["id"] for i, a in enumerate(ordered)}
    real2short = {v: k for k, v in short2real.items()}

    def art_block(a, is_new=False):
        if is_new:
            tag = "[NEW]"
        else:
            tag = f"[{a['placement'] or 'unplaced'} · placed_at={a['placed_at']}]"
        body_brief = (a['body'] or '')[:180].replace('\n', ' ')
        return f"[id={real2short[a['id']]}] {tag} {a['headline']} — {body_brief}"

    log_lines = []
    for entry in decision_log:
        placements = entry.get("placements", {})
        summary = ", ".join(f"{short2real_get(short2real, iid)}→{p}" for iid, p in placements.items())
        log_lines.append(f"- {entry['run_at']} (new={entry.get('new_count',0)})")
    log_block = "\n".join(log_lines) or "(없음)"

    old_block = "\n\n".join(art_block(a) for a in active) or "(없음)"
    new_block = "\n\n".join(art_block(a, is_new=True) for a in new) or "(없음)"

    prompt = f"""당신은 AI 뉴스 편집자입니다. 아래 모든 활성 기사를 TOP/MAIN/SIDE 중 하나로 분류하세요.

## 규칙
- TOP: 최대 1개 (또는 0개). 세계적·기술적 돌파구 급만.
- MAIN: 최대 {MAX_MAIN}개. 중요하지만 TOP은 아닌 것.
- SIDE: 무제한. 그 외 전부.
- **모든 활성 기사가 정확히 한 분류를 받아야 함** (기존 + 새 기사 합쳐서).
- 기존 TOP/MAIN이 새 기사보다 신선·중대하면 유지. 교체 애매하면 새 기사는 SIDE로 밀어넣기.
- 시간별 잡스러운 소식이 큰 발표(신모델 공개, 메이저 업데이트 등)를 밀어내지 않도록 보수적 판단.
- placed_at이 오래되고 새 기사가 명백히 더 중대하면 교체 가능.

## 최근 {DECISION_LOG_HOURS}h 결정 로그 (일관성 참고용)
{log_block}

## 현재 활성 기사
{old_block}

## 이번 배치 새 기사
{new_block}

## 출력 (JSON만, 다른 텍스트 절대 금지)
{{"top": "id" 또는 null, "main": ["id", ...], "side": ["id", ...]}}

- 각 id는 위 목록의 [id=N] 값(문자열 "1", "2"...).
- top이 없으면 null.
- main은 최대 {MAX_MAIN}개.
- 모든 활성 기사(현재+새 기사)가 top/main/side 중 한 곳에만 정확히 1번 나와야 함.

응답은 `{{`로 시작해서 `}}`로 끝. 다른 설명 금지:"""
    return prompt, short2real


def short2real_get(m, k):
    return m.get(k, k)


# ── Validation ──────────────────────────────────────────────────

def validate_placement(p: dict, valid_shorts: set) -> str | None:
    """관대한 검증 + 자동 정리: top>main>side 우선순위로 중복 제거. missing은 side로 기본 배치."""
    # 중복 자동 해결: 같은 id가 여러 그룹에 있으면 top > main > side 우선
    seen = set()
    for k in ("top", "main", "side"):
        p[k] = [x for x in p[k] if not (x in seen or seen.add(x))]
    # top ≤1 강제 (초과분은 main으로)
    if len(p["top"]) > 1:
        overflow = p["top"][1:]
        p["top"] = p["top"][:1]
        p["main"] = list(dict.fromkeys(p["main"] + overflow))
    # main ≤MAX_MAIN 강제 (초과분은 side로)
    if len(p["main"]) > MAX_MAIN:
        overflow = p["main"][MAX_MAIN:]
        p["main"] = p["main"][:MAX_MAIN]
        p["side"] = list(dict.fromkeys(p["side"] + overflow))
    all_ids = p["top"] + p["main"] + p["side"]
    unknown = [i for i in all_ids if i not in valid_shorts]
    if unknown:
        return f"unknown ids: {unknown[:3]}"
    # missing 자동 side로 추가
    missing = valid_shorts - set(all_ids)
    if missing:
        p["side"] = p["side"] + sorted(missing)
    return None


# ── Main ────────────────────────────────────────────────────────

def title_norm(t: str) -> str:
    return re.sub(r'\W+', '', t.lower())

def title_tokens(t: str) -> set:
    """Normalize title to set of tokens (≥2 chars, lowercased)."""
    s = re.sub(r'[^\w가-힣]+', ' ', t.lower())
    return {w for w in s.split() if len(w) >= 2}

def title_similarity(a: str, b: str) -> float:
    """Jaccard of token sets."""
    ta, tb = title_tokens(a), title_tokens(b)
    if not ta or not tb: return 0.0
    return len(ta & tb) / len(ta | tb)

def dedup_articles(articles: list, threshold: float = 0.4) -> list:
    """Greedy dedup: keep longest-body article per cluster."""
    out = []
    for a in sorted(articles, key=lambda x: -len(x.get("body", ""))):
        if any(title_similarity(a["headline"], b["headline"]) >= threshold for b in out):
            continue
        out.append(a)
    return out


# ── LLM-based dedup cluster (body 재작성 없음) ───────────────────

DEDUP_CLUSTER_PROMPT = """아래 기사들 중 **정확히 같은 사실·같은 이벤트**를 전달하는 경우에만 중복으로 판정.

## 중복 판정 엄격 기준 (모두 일치해야 함)
- 같은 회사·같은 제품·같은 날짜/버전
- 같은 사건·같은 발표·같은 행동
- 본질적으로 같은 뉴스 (표현만 다를 뿐)

## 중복 아님 (drop 하지 말 것)
- 같은 회사 다른 제품 (예: Kimi K2.6 vs Kimi 인프라)
- 같은 제품 다른 측면 (예: Opus 4.7 출시 vs Opus 4.7 해커톤)
- 서로 다른 루머·예측 (예: GPT-5.5 출시설 vs GPT-6 예측)
- 새로운 디테일 추가

각 중복 클러스터에서 가장 구체적인 것 1개만 keep, 나머지는 drop.
**제목/본문 재작성 절대 금지** — 원본 id만 keep/drop 결정.
독립 기사는 출력에 포함하지 마세요.

## 기사 (id | 제목 | 본문 일부)
{articles}

## 출력 (JSON만)
{{"clusters": [{{"keep": "id", "drop": ["id","id"]}}, ...]}}

확신 있는 중복만. 없으면 {{"clusters": []}}"""

def dedup_cluster(candidates, sched):
    """LLM으로 중복 클러스터 찾아 drop. body/headline 재작성 없이 원본 보존.
    Returns: (kept_list, dropped_ids_list)"""
    if len(candidates) <= 1:
        return list(candidates), []
    short_to_full = {c["id"].split("-")[-1]: c["id"] for c in candidates}
    lines = []
    for c in candidates:
        s = c["id"].split("-")[-1]
        body = (c["body"] or "").replace("\n", " ")[:250]
        lines.append(f"{s} | {c['headline']} | {body}")
    prompt = DEDUP_CLUSTER_PROMPT.format(articles="\n".join(lines))
    LOG(f"dedup_cluster: {len(candidates)} candidates, prompt {len(prompt):,} chars")

    raw = call_gemma(prompt, sched, max_tok=8192, temp=0.2, json_mode=True)
    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    dropped = set()
    try:
        if start != -1 and end > start:
            obj = json.loads(s[start:end+1])
            for cluster in obj.get("clusters", []) or []:
                for did in cluster.get("drop", []) or []:
                    did = str(did).strip().strip('"\'')
                    if not did: continue
                    stripped = did.lstrip('0') or '0'
                    resolved = short_to_full.get(did) or short_to_full.get(stripped)
                    if resolved:
                        dropped.add(resolved)
    except Exception as e:
        LOG(f"  dedup parse fail: {e}; skipping dedup")
        return list(candidates), []

    kept = [c for c in candidates if c["id"] not in dropped]
    LOG(f"  → kept {len(kept)}, dropped {len(dropped)}")
    return kept, sorted(dropped)


CROSS_DEDUP_PROMPT = """기존 활성 기사와 새 후보 기사를 비교해 **new 중 기존과 정확히 같은 사실·이벤트**만 drop.

## drop 해야 함 (엄격)
- 같은 회사·제품·버전·날짜의 같은 뉴스
- 본질적으로 같은 내용 (표현만 다름)

## drop 금지 (keep)
- 다른 루머·예측 (예: 기존 'Opus 4.7 출시'에 대해 새 'Opus 4.7 벤치마크')
- 다른 측면·새 디테일
- 같은 주제지만 다른 이벤트

**제목/본문 재작성 금지** — 원본 id만 drop 결정.

## 기존 활성 기사 (E prefix)
{existing}

## 새 후보 (N prefix)
{new}

## 출력 (JSON만)
{{"drop_new": ["N-id", ...]}}

확신 있는 중복만. 없으면 {{"drop_new": []}}"""

def cross_existing_dedup(new_candidates, existing_articles, sched):
    """new 중 기존과 '같은 내용'인 것만 drop. 비슷하지만 새 디테일은 keep."""
    if not new_candidates or not existing_articles:
        return list(new_candidates), []
    def fmt_article(prefix, idx, a):
        body = (a.get("body") or "").replace("\n", " ")[:220]
        return f"{prefix}{idx} | {a['headline']} | {body}"
    ex_lines = [fmt_article("E", i+1, a) for i, a in enumerate(existing_articles)]
    nw_lines = [fmt_article("N", i+1, a) for i, a in enumerate(new_candidates)]
    prompt = CROSS_DEDUP_PROMPT.format(existing="\n".join(ex_lines), new="\n".join(nw_lines))
    LOG(f"cross_existing_dedup: {len(existing_articles)} existing vs {len(new_candidates)} new, prompt {len(prompt):,} chars")

    raw = call_gemma(prompt, sched, max_tok=4096, temp=0.2, json_mode=True)
    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    drop_new = set()
    try:
        if start != -1 and end > start:
            obj = json.loads(s[start:end+1])
            for did in obj.get("drop_new", []) or []:
                did = str(did).strip().strip('"\'').upper()
                # N5 → new_candidates[4]
                m = re.match(r'N0*(\d+)$', did)
                if m:
                    idx = int(m.group(1)) - 1
                    if 0 <= idx < len(new_candidates):
                        drop_new.add(new_candidates[idx]["id"])
    except Exception as e:
        LOG(f"  cross dedup parse fail: {e}")
        return list(new_candidates), []
    kept = [c for c in new_candidates if c["id"] not in drop_new]
    LOG(f"  → kept {len(kept)} new (dropped {len(drop_new)} as duplicates of existing)")
    return kept, sorted(drop_new)


# ── Merge loop (consolidation + coverage-patch) ─────────────────

MERGE_ROUND1_PROMPT = """당신은 AI 뉴스 편집장입니다. 아래 candidate 기사들을 검토해 '퍼블릭에 나갈 최종 기사 목록'을 작성하세요.

## 지시
- 중복·유사 내용은 병합: 하나의 최종 기사로 합치고 merged_from에 사용한 candidate id들 모두 기록
- 모순되는 내용은 더 최신·구체적인 쪽으로 정리
- **출시 상태 표현은 보수적으로**: candidate들이 '출시됨/출시 임박/루머/추측'이 엇갈리면 더 약한 표현 사용.
  '공식 출시' 또는 '출시됐다'는 **다수의 구체적 근거**(공식 발표, 모델 카드, 가격 명시, 출시일 명시, 여러 출처 일관된 증언)가 있을 때만.
  애매하면 '출시 임박', '출시 정황 포착', '내부 테스트 중', '루머'로 표현. 본문에서도 "~로 알려졌다", "~라는 소식이 전해졌다" 같은 전언 형태 사용.
- 명백히 가치 없거나 사실성 의심되는 건 discard 배열에 id만
- 각 최종 기사는 400~700자 본문 (한국어). 제목 간결·구체.
- candidate 원문에 없는 사실 지어내지 말 것. 병합은 사실의 합집합.
- merged_from에는 candidate id를 문자열로 (예: "98", "114"). 숫자 앞 0 붙이지 말 것.

## 입력 candidate 기사 (id | 제목 | 본문)
{candidates}

## 출력 (JSON만, 다른 텍스트 금지)
{{"final": [{{"headline": "...", "body": "...", "merged_from": ["id",...]}}], "discard": ["id",...]}}
"""

MERGE_PATCH_PROMPT = """이전 round에서 이미 작성된 최종 기사들이 있습니다. 아래 '언급 안 된' candidate 기사들 중 최종 퍼블릭에 추가할 가치가 있는 것만 가려 새 기사로 쓰세요.

## 이미 확정된 최종 기사 (참고용, 수정 금지)
{existing_finals}

## 언급 안 된 candidate 기사들
{unreferenced}

## 지시
- 이미 확정된 기사와 실질적 중복이면 추가 금지 → discard에 id만
- 완전 새 소식·독립 가치 있는 것만 새 final 기사로 (400~700자)
- 새 기사에는 사용한 candidate id들을 merged_from에 기록
- 추가할 게 없으면 final=[] 로 반환
- merged_from은 문자열 id 배열. 숫자 앞 0 금지.
- **출시 상태 표현은 보수적으로**: 근거 약하면 '출시 임박/루머/추측' 사용. '공식 출시'는 다수의 구체적 근거가 있을 때만.

## 출력 (JSON만)
{{"final": [{{"headline": "...", "body": "...", "merged_from": ["id",...]}}], "discard": ["id",...]}}
"""

def _merge_cand_block(arts):
    lines = []
    for a in arts:
        s = a["id"].split("-")[-1]
        body = (a["body"] or "").replace('\n', ' ')
        lines.append(f"{s} | {a['headline']} | {body}")
    return "\n".join(lines)

def _merge_finals_block(finals):
    lines = []
    for i, f in enumerate(finals, 1):
        lines.append(f"[F{i}] {f['headline']}\n    {f['body'][:300]}")
    return "\n".join(lines) or "(없음)"

def _merge_tolerant_extract(text):
    """수동 JSON 추출. 긴 body의 일부 이스케이프 오류에 관대."""
    finals = []
    i = 0
    while i < len(text):
        m = re.search(r'"headline"\s*:\s*"', text[i:])
        if not m: break
        headline_pos = i + m.start()
        obj_start = text.rfind('{', 0, headline_pos)
        if obj_start == -1:
            i = headline_pos + 1; continue
        depth = 0; in_str = False; esc = False; end_pos = -1
        for j in range(obj_start, len(text)):
            ch = text[j]
            if esc: esc = False; continue
            if ch == '\\' and in_str: esc = True; continue
            if ch == '"': in_str = not in_str; continue
            if in_str: continue
            if ch == '{': depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0: end_pos = j; break
        if end_pos == -1:
            i = headline_pos + 1; continue
        chunk = text[obj_start:end_pos+1]
        hl_m = re.search(r'"headline"\s*:\s*"((?:[^"\\]|\\.)*)"', chunk)
        bd_m = re.search(r'"body"\s*:\s*"((?:[^"\\]|\\.)*)"', chunk, re.S)
        mf_m = re.search(r'"merged_from"\s*:\s*\[([^\]]*)\]', chunk, re.S)
        if hl_m and bd_m:
            try:
                hl = json.loads('"' + hl_m.group(1) + '"')
                bd = json.loads('"' + bd_m.group(1) + '"')
            except Exception:
                hl = hl_m.group(1); bd = bd_m.group(1)
            mf = []
            if mf_m:
                mf = [x.strip().strip('"\'') for x in mf_m.group(1).split(',') if x.strip()]
            finals.append({"headline": hl, "body": bd, "merged_from": mf})
        i = end_pos + 1
    discards = []
    dm = re.search(r'"discard"\s*:\s*\[([^\]]*)\]', text, re.S)
    if dm:
        discards = [x.strip().strip('"\'') for x in dm.group(1).split(',') if x.strip()]
    return {"final": finals, "discard": discards}

def _normalize_merge_ids(id_list, short_to_full):
    """관대한 id 매핑: 0-padding, 공백 허용, 이미 full id면 그대로."""
    lookup = {}
    for k, v in short_to_full.items():
        lookup[k] = v
        lookup[k.lstrip('0') or '0'] = v
    out = []
    seen = set()
    for x in id_list or []:
        x = str(x).strip().strip('"\'')
        if not x: continue
        stripped = x.lstrip('0') or '0'
        v = lookup.get(x) or lookup.get(stripped)
        if v and v not in seen: out.append(v); seen.add(v)
        elif x in short_to_full.values() and x not in seen: out.append(x); seen.add(x)
    return out

def _run_merge_round(candidates, existing_finals, sched, round_num, short_to_full):
    if round_num == 1:
        prompt = MERGE_ROUND1_PROMPT.format(candidates=_merge_cand_block(candidates))
    else:
        prompt = MERGE_PATCH_PROMPT.format(
            existing_finals=_merge_finals_block(existing_finals),
            unreferenced=_merge_cand_block(candidates),
        )
    LOG(f"  merge R{round_num}: {len(candidates)} cands, {len(existing_finals)} exist-finals, prompt={len(prompt):,} chars")
    raw = call_gemma(prompt, sched, max_tok=32768, temp=0.3, json_mode=True)
    s = raw.strip()
    s = re.sub(r'^```(?:json)?\s*|\s*```$', '', s, flags=re.M).strip()
    start = s.find('{'); end = s.rfind('}')
    obj = None
    if start != -1 and end > start:
        try:
            obj = json.loads(s[start:end+1])
        except Exception as e:
            LOG(f"    strict parse fail ({e}); trying tolerant")
    if obj is None:
        obj = _merge_tolerant_extract(raw)
    finals = obj.get("final", []) or []
    discards = obj.get("discard", []) or []
    for f in finals:
        f["merged_from"] = _normalize_merge_ids(f.get("merged_from", []), short_to_full)
    discards = _normalize_merge_ids(discards, short_to_full)
    LOG(f"    → {len(finals)} finals, {len(discards)} discards")
    return {"final": finals, "discard": discards}

def merge_candidates(candidates, sched, rounds=MERGE_ROUNDS, preserve_ids=None):
    """Merge+coverage-patch 루프. candidates → final article objects.

    preserve_ids: 이 id들이 merged_from에 나오면 id/created_at/placement/placed_at 유지 (쌓기)."""
    if not candidates:
        return []
    short_to_full = {c["id"].split("-")[-1]: c["id"] for c in candidates}
    by_id = {c["id"]: c for c in candidates}
    preserve = set(preserve_ids or [])
    now = datetime.now(KST)
    LOG(f"merge_candidates: {len(candidates)} inputs ({len(preserve)} preserve), up to {rounds} rounds")

    finals_raw = []
    discards = set()
    mentioned = set()

    r = _run_merge_round(candidates, [], sched, 1, short_to_full)
    finals_raw.extend(r["final"])
    discards |= set(r["discard"])
    for f in r["final"]:
        mentioned |= set(f["merged_from"])

    for rn in range(2, rounds + 1):
        unref = [c for c in candidates if c["id"] not in mentioned and c["id"] not in discards]
        if not unref:
            LOG(f"  all candidates covered by round {rn-1}")
            break
        r = _run_merge_round(unref, finals_raw, sched, rn, short_to_full)
        finals_raw.extend(r["final"])
        discards |= set(r["discard"])
        for f in r["final"]:
            mentioned |= set(f["merged_from"])

    # Convert to article dicts. 기존 id가 merged_from에 있으면 id/created_at/placement/placed_at 유지.
    result = []
    used_existing_ids = set()
    for i, f in enumerate(finals_raw):
        mf = f.get("merged_from", [])
        # 기존 id 중 merged_from에 포함된 것 (아직 다른 final에 쓰이지 않은 것만)
        existing_refs = [x for x in mf if x in preserve and x not in used_existing_ids]
        if existing_refs:
            # 가장 오래된(=원조) 기존 id를 승계
            anchor = min(existing_refs, key=lambda eid: by_id[eid].get("created_at", now.isoformat()))
            used_existing_ids.add(anchor)
            src = by_id[anchor]
            result.append({
                "id": anchor,
                "headline": f["headline"],
                "body": f["body"],
                "created_at": src.get("created_at", now.isoformat()),
                "placement": src.get("placement"),  # 유지 (classify에서 재검토)
                "placed_at": src.get("placed_at", now.isoformat()),
                "merged_from": mf,
            })
        else:
            # 신규 기사
            src_dates = [by_id[mid].get("created_at") for mid in mf if mid in by_id and by_id[mid].get("created_at")]
            created = max([d for d in src_dates if d]) if src_dates else now.isoformat()
            result.append({
                "id": f"art-{now.strftime('%Y%m%d%H%M')}-m{i+1:02d}",
                "headline": f["headline"],
                "body": f["body"],
                "created_at": created,
                "placement": None,
                "placed_at": now.isoformat(),
                "merged_from": mf,
            })
    still_unref = [c["id"] for c in candidates if c["id"] not in mentioned and c["id"] not in discards]
    LOG(f"  final: {len(result)} articles, {len(discards)} discards, {len(still_unref)} default-dropped "
        f"(preserved {len(used_existing_ids)} existing ids)")
    return result

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="override since ISO")
    ap.add_argument("--chat-file", help="reuse existing Discord export (skip re-export)")
    ap.add_argument("--skip-scan", action="store_true", help="load new articles from cache, skip chunk scan")
    args = ap.parse_args()

    keys = load_keys()
    sched = KeyScheduler(keys)
    LOG(f"loaded {len(keys)} keys")

    state = load_state()
    now = datetime.now(KST)

    since_iso = args.since or state.get("last_run_at") or (now - timedelta(hours=72)).isoformat()
    LOG(f"since = {since_iso}")

    if args.skip_scan:
        cache_path = ROOT / "data" / "new_articles_cache.json"
        new_articles = json.loads(cache_path.read_text(encoding="utf-8"))
        LOG(f"skip-scan: loaded {len(new_articles)} articles from cache")
        _classify_and_save(state, new_articles, now, sched)
        return

    if args.chat_file:
        export_path = Path(args.chat_file)
        LOG(f"[discord] reusing {export_path}")
    else:
        export_path = discord_export(since_iso)
    chat = read_chat_text(export_path)
    chat = chat.strip()
    LOG(f"chat: {len(chat):,} chars")

    if not chat:
        LOG("empty chat — no-op except last_run_at bump")
        state["last_run_at"] = now.isoformat()
        state["generated_at"] = now.isoformat()
        save_state(state)
        subprocess.run(["python3", str(ROOT / "build_gist.py")], check=False)
        return

    chunks = chunk_by_messages(chat)
    LOG(f"{len(chunks)} chunks")

    existing_sorted = sorted(state["articles"], key=lambda a: a["created_at"], reverse=True)
    titles = [a["headline"] for a in existing_sorted[:TITLES_FOR_DEDUP]]

    new_articles = []
    for i, ch in enumerate(chunks):
        LOG(f"[{i+1}/{len(chunks)}] scan chunk ({len(ch):,} chars)")
        raw = call_gemma(prompt_scan_chunk(ch, titles), sched, temp=0.3, json_mode=True)
        arts = parse_chunk_articles(raw)
        for a in arts:
            new_articles.append({
                "id": new_id(now, len(new_articles) + 1),
                "headline": a["headline"],
                "body": a["body"],
                "category": a.get("category", "rumor"),
                "trust": a.get("trust", "low"),
                "created_at": now.isoformat(),
                "placement": None,
                "placed_at": now.isoformat(),
            })
        LOG(f"  → {len(arts)} articles")

    # inter-chunk dedup: first exact title, then Jaccard ≥ 0.4
    seen = set()
    deduped_exact = []
    for a in new_articles:
        k = title_norm(a["headline"])
        if k in seen: continue
        seen.add(k)
        deduped_exact.append(a)
    new_articles = dedup_articles(deduped_exact, threshold=0.4)
    LOG(f"new articles: {len(deduped_exact)} exact-dedup → {len(new_articles)} jaccard-dedup")

    # cache raw deduped (pre-merge) for debug
    cache_path = ROOT / "data" / "new_articles_cache.json"
    cache_path.parent.mkdir(exist_ok=True)
    cache_path.write_text(json.dumps(new_articles, ensure_ascii=False, indent=2), encoding="utf-8")
    LOG(f"cached {len(new_articles)} (pre-merge) → {cache_path}")

    # Step A: intra-batch dedup (new 끼리 중복 제거, body 재작성 X)
    if new_articles:
        new_articles, _ = dedup_cluster(new_articles, sched)
        (ROOT / "data" / "deduped_cache.json").write_text(
            json.dumps(new_articles, ensure_ascii=False, indent=2), encoding="utf-8")

    # Step B: cross-existing dedup (new vs 기존 활성 기사. 같은 내용만 drop)
    if new_articles and state.get("articles"):
        new_articles, _ = cross_existing_dedup(new_articles, state["articles"], sched)

    # Step C: classify (기존 + new 모두 TOP/MAIN/SIDE 배치)
    _classify_and_save(state, new_articles, now, sched)


def _classify_and_save(state, new_articles, now, sched):
    # expire
    before = len(state["articles"])
    state["articles"] = [
        a for a in state["articles"]
        if (now - datetime.fromisoformat(a["created_at"])).total_seconds() < EXPIRY_HOURS * 3600
    ]
    if before != len(state["articles"]):
        LOG(f"expired {before - len(state['articles'])} old articles")

    # 주의: placement를 강제 None으로 리셋하지 않음 (merge가 기존 id 승계한 경우 placement 유지)
    if not new_articles:
        LOG("no new articles — keeping placements")
    else:
        recent_log = [
            e for e in state.get("decision_log", [])
            if (now - datetime.fromisoformat(e["run_at"])).total_seconds() < DECISION_LOG_HOURS * 3600
        ]
        LOG(f"classify: {len(state['articles'])} active + {len(new_articles)} new")

        placement_map_real = None
        attempt = 0
        while True:
            attempt += 1
            prompt, short2real = prompt_classify(state["articles"], new_articles, recent_log)
            valid_shorts = set(short2real.keys())
            raw = call_gemma(prompt, sched, max_tok=16384, temp=0.2, json_mode=True)
            try:
                p = parse_placement_json(raw)
                err = validate_placement(p, valid_shorts)
                if err:
                    raise ValueError(err)
                placement_map_real = {}
                for iid in p["top"]: placement_map_real[short2real[iid]] = "top"
                for iid in p["main"]: placement_map_real[short2real[iid]] = "main"
                for iid in p["side"]: placement_map_real[short2real[iid]] = "side"
                break
            except Exception as e:
                LOG(f"  classify fail ({attempt}): {e}; retry")
                # dump raw for post-mortem on first few failures
                if attempt <= 3:
                    dbg = ROOT / "data" / f"classify_raw_{attempt}.txt"
                    dbg.write_text(raw, encoding="utf-8")
                if attempt >= 8:
                    LOG(f"  giving up classify after {attempt} attempts — fallback: all new → side")
                    placement_map_real = {a["id"]: (a.get("placement") or "side") for a in state["articles"]}
                    for a in new_articles:
                        placement_map_real[a["id"]] = "side"
                    break

        all_articles = state["articles"] + new_articles
        for a in all_articles:
            new_p = placement_map_real.get(a["id"], "side")
            if a.get("placement") != new_p:
                if a.get("placement") is not None:
                    a["placed_at"] = now.isoformat()
                a["placement"] = new_p
        state["articles"] = all_articles

        state["decision_log"] = recent_log + [{
            "run_at": now.isoformat(),
            "new_count": len(new_articles),
            "placements": placement_map_real,
        }]
        counts = {k: sum(1 for v in placement_map_real.values() if v == k) for k in ("top","main","side")}
        LOG(f"placed: top={counts['top']} main={counts['main']} side={counts['side']}")

    state["last_run_at"] = now.isoformat()
    state["generated_at"] = now.isoformat()
    save_state(state)
    LOG("state saved")

    r = subprocess.run(["python3", str(ROOT / "build_gist.py")], capture_output=True, text=True)
    if r.returncode == 0:
        LOG(r.stdout.strip().split("\n")[-1])
    else:
        LOG(f"gist push failed: {r.stderr[-300:]}")


if __name__ == "__main__":
    main()
