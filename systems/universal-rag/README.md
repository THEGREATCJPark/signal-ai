# Universal RAG System

`/path/to/memory_lab/rag/`

## 구조

```
rag/
├── __init__.py      # 패키지 선언
├── store.py         # SQLite 영구 저장소 (docs, chunks, embeddings, BM25, citations)
├── chunker.py       # 800자 overlapping paragraph-snap chunker
├── embedder.py      # paraphrase-multilingual-MiniLM-L12-v2 (384d, 한국어 지원)
├── retriever.py     # BM25 + cosine embedding + RRF(k=10) 하이브리드 검색
├── generator.py     # Gemini 2.5 Pro grounded answer + [chunk N] citation
├── pipeline.py      # CLI 엔트리포인트: ingest / query
├── data/
│   └── rag.db       # 현재 인덱싱된 DB (761 docs, 75,728 chunks, 511MB)
└── README.md        # 이 파일
```

## 사전 조건

```bash
pip install sentence-transformers gemini-webapi aiohttp
```

Gemini 답변 생성을 위해 Firefox에 Google 계정 로그인 필요 (cookies.sqlite에서 `__Secure-1PSID` 자동 추출).

## 사용법

### 1. 인덱싱 (아무 텍스트 코퍼스)

```bash
cd /path/to/memory_lab

# 전체 파이프라인: 문서 수집 → 800자 chunk → BM25 인덱스 → 임베딩
python3 -m rag.pipeline ingest --root /path/to/text/files --db ./rag/data/rag.db
```

- `.md`, `.txt` 파일 자동 수집 (binary, 이미지, 캐시 제외)
- 과목/주차/자료종류 메타데이터 자동 추론 (경로 기반)
- 임베딩 건너뛰기: `--skip-embed`

### 2. 검색만

```bash
python3 -m rag.pipeline query --q "중간고사 시험범위" --top 10
```

- BM25 + 임베딩 cosine similarity → RRF fusion
- top-K 결과에 원문 chunk + 소스 메타데이터 출력

### 3. 검색 + 답변 생성

```bash
python3 -m rag.pipeline query --q "중간고사 시험범위" --top 10 --generate
```

- Gemini 2.5 Pro가 top-K raw chunks를 읽고 구조화된 답변 생성
- 모든 주장에 `[chunk N]` 출처 표기
- 근거 없으면 "정보를 찾을 수 없습니다" 응답

### 4. 과목 필터

```bash
python3 -m rag.pipeline query \
  --q "course:288799 중간시험 기말시험 강의 계획표" \
  --top 10 --generate
```

`course:ID` 접두어로 특정 과목만 검색 (retrieval 단계에서 필터).

## 현재 인덱싱된 코퍼스

- 소스: 로컬 학습자료/문서자료 텍스트 코퍼스
- 761 문서 → 75,728 chunks → 75,728 embeddings
- 8개 자료 묶음

## 검증 결과

인체생리학 `course:288799 중간시험 기말시험 주차별 강의 계획표 날짜`:

> **중간시험:** 4월 22일 [chunk 1, 8, 10]
> **기말시험:** 6월 17일 [chunk 1, 4, 5, 8, 10]
> 주차별 토픽 전체 테이블 + 출처 citation 포함

기초실험 `course:288805 기초실험 중간고사 기말고사 시험 일정 실험 범위`:

> **중간고사:** 8주차(4/21~4/27) — 시험 없음, 실험(6) 진행 [chunk 1, 8, 9]
> **기말고사:** 16주차(6/16~6/22) [chunk 1, 8, 9]
> 실험 범위: 실험(1)~실험(10) 주차별 배치 포함

## 아키텍처 원리

1. **저장 시점 해석 금지** — chunking과 tokenization은 결정적, LLM 호출 없음
2. **다중 검색 신호** — BM25(어휘) + embedding(의미) + RRF(순위 융합)
3. **해석은 질의 시점, raw bytes에서만** — Gemini는 원문 chunk만 읽음
4. **과목 필터는 retrieval 단계** — post-filter 아닌 pre-filter로 정확도 보장
5. **영구 저장** — SQLite DB, 재시작 시 재인덱싱 불필요

## Gemini proxy (별도)

OpenAI SDK로 Gemini 사용하려면:

```bash
python3 /path/to/memory_lab/gemini_proxy.py &
export OPENAI_BASE_URL=http://127.0.0.1:8321/v1
export OPENAI_API_KEY=dummy
```

## 관련 실험 산출물

- `../outputs/FINAL_REPORT.md` — 기록 65K M16 RRF 6/6 실험
- `../exam_test/UNIVERSAL_EXAM_REPORT.md` — 학교 자료 시험 정보 추출
- `../gemini_proxy.py` — Gemini OpenAI-compat proxy
