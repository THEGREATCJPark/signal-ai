<p align="center">
  <img src="assets/logo.png" alt="First Light AI" width="300">
</p>

<h1 align="center">First Light AI</h1>

<p align="center">
  <b>AI 최전방 소식을 가장 먼저, 매일 아침.</b><br>
  자동 수집 → LLM 요약/분류/점수화 → 텔레그램 & X 발행
</p>

<p align="center">
  <a href="https://t.me/firstlight_ai">Telegram</a> · 
  <a href="https://x.com/firstlight_ai">X (Twitter)</a>
</p>

---

## 팀

| 역할 | 담당 | 범위 |
|------|------|------|
| **제품 오너** | CJ | 텔레그램 봇, 메시지 포맷, 채널 운영, 발행 파이프라인, DB/RAG |
| **시스템 오너** | HB | 크롤링 파이프라인, LLM 요약/분류/점수화, 중복 제거, 자동화 |

---

## 아키텍처

```
┌─────────────────────────────────────────────┐
│  크롤러 7종 (1시간 간격)                       │
│  HN · Reddit · arXiv · HuggingFace          │
│  GeekNews · LessWrong · Discord              │
└──────────────┬──────────────────────────────┘
               ↓
     Gemma 4 LLM (요약 · 태깅 · 점수화)
               ↓
┌──────────────┴──────────────────────────────┐
│  SQLite (로컬)  ←→  Supabase (PostgreSQL)    │
│                      + pgvector (벡터 검색)    │
└──────────────┬──────────────────────────────┘
               ↓
     score 기반 자동 트리거 (고득점 → 즉시 발행)
               ↓
┌──────────────┴──────────────┐
│  run_publish.py (발행)       │
├──────────┬──────────────────┤
│ Telegram │  X (Twitter)     │
└──────────┴──────────────────┘
               ↓
     RAG 챗봇 (벡터 검색 → LLM 답변)
```

---

## 진행 현황

### 완료

- [x] 텔레그램 봇 자동 발행 (`bot/telegram_bot.py`)
- [x] X(트위터) OAuth 2.0 자동 발행 (`bot/x_poster.py`)
- [x] 크롤러 7종 구현 (`crawlers/`)
- [x] SQLite DB + FTS5 전문검색 (`db/schema.sql`, `db/ingest.py`)
- [x] Gemma 4 LLM 파이프라인 (`run_full.py`)
- [x] GitHub Actions 자동 발행 워크플로우
- [x] GitHub Pages 다이제스트 배포
- [x] Supabase DB 스키마 + pgvector 설정 (`migrations/`)
- [x] DB 모듈 — articles CRUD, publish_log, embeddings, clusters (`db/`)
- [x] RAG 모듈 — 임베딩, 시맨틱 중복 제거, 벡터 검색, 챗봇 (`rag/`)
- [x] 크롤링 → Supabase 동시 적재 (`db/ingest.py`)
- [x] score 기반 발행 자동 트리거 (`crawlers/run_all.py`)

### 진행 중

- [ ] 1시간 간격 크롤링 스케줄링 적용
- [ ] score threshold 실데이터 기반 튜닝
- [ ] RAG 챗봇 텔레그램 핸들러 연결

### 예정

- [ ] 임베딩 백필 + IVFFlat 인덱스 생성
- [ ] 웹 대시보드 (검색/필터/북마크)
- [ ] 이메일 뉴스레터

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 크롤링 | Python (requests, feedparser, Algolia API, Reddit JSON, GraphQL) |
| LLM | Gemma 4 31B (기사 생성) / Gemini Flash (RAG 챗봇) |
| 봇 | Telegram Bot API (requests 직접 호출) |
| X 포스팅 | X API v2 (OAuth 2.0 + Refresh Token) |
| DB (로컬) | SQLite + FTS5 전문검색 |
| DB (클라우드) | Supabase (PostgreSQL + pgvector) |
| 임베딩 | multilingual-e5-small (sentence-transformers, 384차원) |
| RAG | pgvector 코사인 유사도 검색 + LLM 컨텍스트 주입 |
| 스케줄링 | GitHub Actions (cron + workflow_dispatch 트리거) |
| 배포 | GitHub Pages (다이제스트 HTML) |

---

## 프로젝트 구조

```
signal-ai/
├── crawlers/                   # 크롤링 모듈 (HB)
│   ├── _common.py              # 공통 포맷 + 파일 IO
│   ├── hn.py                   # Hacker News (Algolia API)
│   ├── reddit.py               # Reddit (JSON, 6개 서브레딧)
│   ├── arxiv.py                # arXiv (RSS, cs.AI/CL/LG)
│   ├── hf_trending.py          # HuggingFace (Models + Papers)
│   ├── geeknews.py             # GeekNews (Atom feed)
│   ├── lesswrong.py            # LessWrong (GraphQL API)
│   ├── discord.py              # Discord 채널 파싱
│   └── run_all.py              # 병렬 실행 + score 기반 트리거
│
├── bot/                        # 발행 모듈 (CJ)
│   ├── telegram_bot.py         # 텔레그램 발행
│   ├── x_poster.py             # X 발행 (OAuth 2.0)
│   ├── formatter.py            # 메시지 포맷팅 (HTML)
│   ├── scheduler.py            # 발행 오케스트레이션
│   └── test_publish.py         # 테스트 유틸
│
├── db/                         # DB 모듈
│   ├── schema.sql              # SQLite 스키마 (posts + FTS5)
│   ├── ingest.py               # JSONL → SQLite + Supabase 동시 적재
│   ├── query.py                # SQLite 조회 CLI (stats, search, top)
│   ├── client.py               # Supabase 클라이언트
│   ├── articles.py             # Supabase articles CRUD
│   ├── publish_log.py          # 발행 이력 (DB 기반)
│   ├── embeddings.py           # 벡터 저장/검색 (pgvector)
│   └── clusters.py             # 유사 기사 클러스터
│
├── rag/                        # RAG 파이프라인
│   ├── embedder.py             # 임베딩 생성 (multilingual-e5-small)
│   ├── dedup.py                # 시맨틱 중복 제거
│   ├── search.py               # 벡터 유사도 검색
│   └── chatbot.py              # RAG 챗봇 (질의응답)
│
├── publisher/                  # 발행 상태 관리
│   └── state.py                # JSON/DB 자동 전환 (USE_DB 환경변수)
│
├── migrations/                 # Supabase DB 스키마
│   ├── 001_create_articles.sql
│   ├── 002_create_embeddings.sql
│   ├── 003_create_publish_log.sql
│   ├── 004_create_clusters.sql
│   ├── 005_create_rpc_functions.sql
│   └── 006_create_rls_policies.sql
│
├── scripts/                    # CLI 유틸리티
│   ├── run_publish.py          # 메인 발행 스크립트
│   ├── validate_articles.py    # 기사 JSON 검증
│   ├── migrate.py              # DB 마이그레이션 SQL 출력
│   └── backfill_embeddings.py  # 임베딩 일괄 생성
│
├── run_full.py                 # 전체 파이프라인 (크롤링→DB→LLM→HTML→Gist)
├── run_digest.py               # Discord 다이제스트 파이프라인
├── run_daily.sh                # 일일 자동화 셸 스크립트
│
├── docs/                       # 생성된 기사 / 다이제스트
├── data/                       # 크롤링 데이터 (gitignore)
├── assets/                     # 로고 등 정적 자산
│
├── .github/workflows/
│   ├── daily_publish.yml       # 자동 발행 (cron + 수동 트리거)
│   └── deploy-pages.yml        # GitHub Pages 배포
│
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## 환경 구축

### 1. 의존성 설치

```bash
pip install -r requirements.txt
```

### 2. 환경변수 설정

```bash
cp .env.example .env
# .env 파일에 실제 키 입력
```

### 3. Supabase 테이블 생성

```bash
python scripts/migrate.py
# 출력된 SQL → Supabase SQL Editor에 붙여넣기 → Run
```

### 4. 크롤링 실행

```bash
# 전체 크롤러 병렬 실행
python crawlers/run_all.py

# SQLite + Supabase 동시 적재
python db/ingest.py
```

### 5. 발행

```bash
# Dry-run (미리보기)
python scripts/run_publish.py --dry-run

# 텔레그램만 발행
python scripts/run_publish.py --platform telegram

# 전체 발행
python scripts/run_publish.py --platform both
```

---

## 환경 변수

| 변수 | 설명 | 필수 |
|------|------|------|
| `TELEGRAM_BOT_TOKEN` | BotFather 봇 토큰 | O |
| `TELEGRAM_CHANNEL_ID` | 채널 ID (`@채널명` 또는 `-100...`) | O |
| `X_CLIENT_ID` | X OAuth 2.0 Client ID | O |
| `X_CLIENT_SECRET` | X OAuth 2.0 Client Secret | O |
| `X_REFRESH_TOKEN` | X OAuth 2.0 Refresh Token | O |
| `SUPABASE_URL` | Supabase 프로젝트 URL | O |
| `SUPABASE_KEY` | Supabase anon key | O |
| `USE_DB` | `true` → DB 모드 (published.json 대신 DB) | - |
| `LLM_PROVIDER` | RAG 챗봇 LLM (`gemini` / `openai`) | - |
| `GOOGLE_API_KEY` | Gemini API 키 | - |

---

## 라이선스

Private repo.
