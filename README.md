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
| **제품 오너** | CJ (박찬준) | 텔레그램 봇, 메시지 포맷, 채널 운영, 발행 파이프라인, DB/RAG |
| **시스템 오너** | HB (박형빈) | 크롤링, LLM 요약/분류/점수화, 중복 제거, 자동화 |

---

## 아키텍처

```
크롤러 (HN, Reddit, GitHub, HuggingFace, ...)
    ↓
LLM 파이프라인 (요약 · 태깅 · 점수화)
    ↓
┌─────────────────────────────────┐
│  Supabase (PostgreSQL + pgvector) │
│  articles · embeddings · logs     │
└─────────┬───────────────────────┘
          ↓
    run_publish.py (발행 스크립트)
          ↓
┌──────────────┐    ┌──────────────┐
│   Telegram   │    │  X (트위터)   │
│ First Light  │    │ First Light  │
└──────────────┘    └──────────────┘
          ↓
    RAG 챗봇 (벡터 검색 → LLM 답변)
```

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 크롤링 | Python (requests, BeautifulSoup, feedparser) |
| LLM | GPT-4.1 mini / Gemini Flash |
| 봇 | Telegram Bot API (requests 직접 호출) |
| X 포스팅 | X API v2 (OAuth 2.0 + Refresh Token) |
| DB | Supabase (PostgreSQL + pgvector) |
| 임베딩 | multilingual-e5-small (sentence-transformers) |
| RAG | pgvector 벡터 검색 + LLM 답변 생성 |
| 스케줄링 | GitHub Actions cron (매일 08:00 KST) |

---

## 프로젝트 구조

```
signal-ai/
├── bot/                        # 발행 모듈
│   ├── telegram_bot.py         # 텔레그램 발행
│   ├── x_poster.py             # X 발행 (OAuth 2.0)
│   ├── formatter.py            # 메시지 포맷팅 (HTML)
│   ├── scheduler.py            # 발행 오케스트레이션
│   └── test_publish.py         # 테스트 유틸
│
├── db/                         # DB 모듈 (Supabase)
│   ├── client.py               # Supabase 클라이언트
│   ├── articles.py             # 기사 CRUD
│   ├── publish_log.py          # 발행 이력 (DB 기반)
│   ├── embeddings.py           # 벡터 저장/검색
│   └── clusters.py             # 유사 기사 클러스터
│
├── rag/                        # RAG 파이프라인
│   ├── embedder.py             # 임베딩 생성 (multilingual-e5-small)
│   ├── dedup.py                # 시맨틱 중복 제거
│   ├── search.py               # 벡터 유사도 검색
│   └── chatbot.py              # RAG 챗봇 (질의응답)
│
├── publisher/                  # 상태 관리
│   └── state.py                # JSON/DB 전환 지원
│
├── migrations/                 # DB 스키마
│   ├── 001_create_articles.sql
│   ├── 002_create_embeddings.sql
│   ├── 003_create_publish_log.sql
│   ├── 004_create_clusters.sql
│   └── 005_create_rpc_functions.sql
│
├── scripts/                    # CLI 엔트리포인트
│   ├── run_publish.py          # 메인 발행 스크립트
│   ├── validate_articles.py    # 기사 JSON 검증
│   ├── migrate.py              # DB 마이그레이션 출력
│   └── backfill_embeddings.py  # 임베딩 일괄 생성
│
├── docs/
│   └── articles.json           # 발행할 기사 데이터
│
├── assets/
│   └── logo.png                # First Light AI 로고
│
├── data/
│   └── published.json          # 발행 상태 (JSON 모드)
│
├── .github/workflows/
│   └── daily_publish.yml       # GitHub Actions 자동 발행
│
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## 환경 구축 가이드

### 1. Supabase 프로젝트 생성

1. [supabase.com](https://supabase.com) 가입 → **New Project** 생성
2. 리전: **Northeast Asia (ap-northeast-1)** 또는 **Southeast Asia** 선택
3. 프로젝트 생성 후 **Settings → API**에서 확인:
   - `Project URL` → `.env`의 `SUPABASE_URL`
   - `anon public` 키 → `.env`의 `SUPABASE_KEY`

### 2. DB 테이블 생성

Supabase 대시보드 → **SQL Editor**에서 실행:

```bash
# 마이그레이션 SQL 출력
python scripts/migrate.py
```

출력된 SQL을 SQL Editor에 복사 → **Run** 클릭.

또는 `migrations/` 폴더의 SQL 파일을 001 → 005 순서대로 직접 실행.

### 3. 로컬 환경 설정

```bash
# 의존성 설치
pip install -r requirements.txt

# .env 설정
cp .env.example .env
# .env 파일에 실제 키 입력
```

### 4. 테스트

```bash
# 기사 JSON 검증
python scripts/validate_articles.py

# Dry-run (미리보기, API 호출 없음)
python scripts/run_publish.py --dry-run

# 텔레그램만 발행
python scripts/run_publish.py --platform telegram

# 전체 발행 (Telegram + X)
python scripts/run_publish.py --platform both

# 기사 수 제한
python scripts/run_publish.py --platform x --limit 1

# 강제 재발행
python scripts/run_publish.py --force
```

---

## 환경 변수 / GitHub Secrets

| 변수 | 설명 | 필수 |
|------|------|------|
| `TELEGRAM_BOT_TOKEN` | BotFather에서 발급한 봇 토큰 | O |
| `TELEGRAM_CHANNEL_ID` | 채널 ID (`@채널명` 또는 `-100...` 숫자) | O |
| `X_CLIENT_ID` | X OAuth 2.0 Client ID | O |
| `X_CLIENT_SECRET` | X OAuth 2.0 Client Secret | O |
| `X_REFRESH_TOKEN` | X OAuth 2.0 Refresh Token | O |
| `SUPABASE_URL` | Supabase 프로젝트 URL | O |
| `SUPABASE_KEY` | Supabase anon key | O |
| `USE_DB` | `true`로 설정 시 DB 모드 사용 | - |
| `LLM_PROVIDER` | RAG 챗봇 LLM (`gemini` / `openai`) | - |
| `GOOGLE_API_KEY` | Gemini API 키 | - |
| `OPENAI_API_KEY` | OpenAI API 키 | - |

---

## 크롤링 데이터 규격

크롤러 출력은 아래 JSON 형식을 따라야 합니다:

```json
{
  "id": "src-20260416-001",
  "source": "hackernews",
  "title": "Claude 4.6 Released with Extended Context",
  "url": "https://example.com/article",
  "score": 342,
  "comments": 128,
  "timestamp": "2026-04-16T08:00:00Z",
  "summary": "LLM이 생성한 한국어 요약 (200자 내외)",
  "body": "기사 전체 본문 또는 충분한 텍스트 (RAG 검색용)",
  "tags": ["llm", "claude", "context-window"],
  "media": []
}
```

| 필드 | 타입 | 필수 | 설명 |
|------|------|------|------|
| `id` | string | O | 고유 ID. `{소스}-{날짜}-{번호}` 권장 |
| `source` | string | O | 출처 식별자 (hackernews, reddit, github_trending 등) |
| `title` | string | O | 기사 제목 |
| `url` | string | O | 원문 URL |
| `score` | int | - | 중요도 점수 (HN points, upvotes 등). 기본값 0 |
| `comments` | int | - | 댓글/토론 수. 기본값 0 |
| `timestamp` | string | - | 수집 시각 (ISO 8601). 없으면 현재 시각 |
| `summary` | string | **O** | 한국어 요약 (200자 내외). RAG 검색 품질에 직결 |
| `body` | string | **O** | 전체 본문. RAG 챗봇 답변의 컨텍스트로 사용 |
| `tags` | string[] | - | 분류 태그. 빈 배열 허용 |
| `media` | object[] | - | 첨부 이미지 `[{path: "..."}]`. 빈 배열 허용 |

---

## 라이선스

Private repo.
