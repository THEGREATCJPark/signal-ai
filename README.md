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
| **제품 오너** | CJ | Supabase DB/인프라, GH Secrets, 텔레그램 봇, 메시지 포맷, 발행 파이프라인 |
| **시스템 오너** | HB | 크롤링 파이프라인, LLM 요약/분류/점수화, 중복 제거, 스케줄 자동화 |

---

## 아키텍처 (현재 타겟)

```
┌─────────────────────────────────────────────────┐
│ Local WSL — crawler 7종 (07:00 KST 예정)          │
│ HN · Reddit · arXiv · HuggingFace                │
│ GeekNews · LessWrong · Discord                    │
└──────────────────────┬──────────────────────────┘
                       ↓ upsert (source, source_id)
┌──────────────────────┴──────────────────────────┐
│ Supabase · public.posts (raw, service_role only)│
└──────────────────────┬──────────────────────────┘
                       ↓ get_recent_posts_by_source()
┌──────────────────────┴──────────────────────────┐
│ LLM 파이프라인 (Gemma/Gemini)                     │
│ 요약 · 태깅 · 점수 · 배치(placement/category)      │
└──────────────────────┬──────────────────────────┘
                       ↓ upsert
┌──────────────────────┴──────────────────────────┐
│ Supabase · public.articles (public read)         │
└──────────────────────┬──────────────────────────┘
                       ↓
┌──────────────────────┴──────────────────────────┐
│ GitHub Actions — daily_publish.yml               │
│ 08:00 KST · dry_run/force/limit · both/tg/x      │
├──────────────┬────────────────────┬─────────────┤
│  Telegram    │    X (Twitter)     │ GH Pages    │
└──────────────┴────────────────────┴─────────────┘
                       ↓ idempotent
              Supabase · public.publish_log
```

관측/상태 테이블: `pipeline_state` (JSON state 대체), `ingest_runs` (run당 1행, 옵션).
pg_cron `signal_keepalive_daily`가 매일 03:17 UTC에 `pipeline_state`를 핑해서 무료 플랜 자동정지를 방지.

---

## Supabase 적재 대상

- Project ref: `qyckjkidscpiyrdzqxoc`
- URL: `https://qyckjkidscpiyrdzqxoc.supabase.co`
- Raw ingest target: Table: `public.posts`
- Generated article target: `public.articles`
- Publish idempotency log: `public.publish_log`
- Owner/account: CJ 관리 Supabase 프로젝트. repo에는 Supabase 계정 이메일, 조직명, service role key를 기록하지 않는다.

환경변수 `SUPABASE_URL`이 실제 적재 프로젝트를 결정한다. 쓰기 권한은
`SUPABASE_SERVICE_ROLE_KEY`로만 열리며, service_role key의 소유 Supabase 계정/조직 권한이
곧 실제 적재 권한이다. 로컬 WSL에서는 `.env` 또는 shell env에 위 URL과 service role key가
있어야 `scripts/local_crawl_ingest.py` / `db/supabase_ingest.py`가 `public.posts`로 upsert한다.
GitHub Actions에서는 Repo Secrets의 같은 이름 값이 쓰이지만, crawler workflow는 두지 않는다.

---

## 진행 현황

### 완료
- [x] Supabase 스키마 라이브 적용 — `migrations/001/003/007/008/009/010/011/012` (Supabase migrations 트랙 기록)
- [x] RLS 격리: anon은 `articles`/`publish_log`만 읽기 가능, `posts`/`pipeline_state`/`ingest_runs`는 service_role 전용
- [x] `get_recent_posts_by_source(days, per_source)` RPC — metadata points/score/upvotes/likes/num_comments 중 첫 매치 스코어
- [x] pg_cron keepalive 활성
- [x] 텔레그램 봇 자동 발행 (`bot/telegram_bot.py`)
- [x] X OAuth 2.0 Refresh Token 발행 (`bot/x_poster.py`)
- [x] `publisher/state.py` USE_DB 듀얼 모드 (Supabase `publish_log` 또는 로컬 JSON)
- [x] 크롤러 7종 구현 (`crawlers/`)
- [x] GH Actions `daily_publish.yml` (cron + workflow_dispatch)
- [x] GitHub Pages 다이제스트 배포 (`pages.yml`)
- [x] SQLite → Supabase 백필 스크립트 (`scripts/backfill_sqlite_to_supabase.py`)

### 진행 중
- [x] **로컬 전용 크롤링/적재 경로** — `scripts/local_crawl_ingest.py`가 로컬 WSL에서 crawler 7종 실행 후 Supabase `posts`에 upsert
- [x] **Discord 로컬 전용 적재 경로** — `scripts/local_discord_ingest.py`가 로컬 WSL에서만 Discord crawl 후 Supabase `posts`에 upsert
- [ ] 로컬 적재 스케줄을 Windows Task Scheduler에 붙일지, 기존 08:00 로컬 기사 생성 run 안에 묶을지 결정
- [ ] (선택) 로컬 SQLite 12k rows 백필 실행 결정

### 예정
- [ ] 기사 생성(`run_hourly.py`)을 GH Actions 스케줄로 이관할지 로컬 유지할지 결정
- [ ] 웹 대시보드 (검색/필터/북마크)
- [ ] 이메일 뉴스레터

---

## 기술 스택

| 영역 | 기술 |
|------|------|
| 크롤링 | Python (requests, feedparser, Algolia API, Reddit JSON, GraphQL) |
| LLM | Gemma / Gemini (기사 생성) |
| 봇 | Telegram Bot API (requests 직접 호출) |
| X | X API v2 (OAuth 2.0 + Refresh Token) |
| DB | Supabase PostgreSQL (RLS + pg_cron) |
| 스케줄 | Windows Task Scheduler / GitHub Actions (`daily_publish.yml`) |
| 배포 | GitHub Pages (다이제스트 HTML) |

---

## 프로젝트 구조

```
signal-ai/
├── crawlers/                  # 크롤링 모듈 (HB)
│   ├── _common.py
│   ├── hn.py / reddit.py / arxiv.py / hf_trending.py
│   ├── geeknews.py / lesswrong.py / discord.py
│   ├── run_public.py          # 로컬 public-source subset runner
│   └── run_all.py             # 로컬 전체 실행 + score 트리거
│
├── bot/                       # 발행 모듈
│   ├── telegram_bot.py
│   ├── x_poster.py
│   ├── formatter.py
│   └── scheduler.py
│
├── db/                        # Supabase 액세스 레이어
│   ├── client.py              # Supabase 클라이언트 (service_role)
│   ├── posts.py               # posts upsert (on_conflict source,source_id)
│   ├── articles.py            # articles CRUD + public_state
│   ├── publish_log.py         # 발행 이력 idempotent
│   ├── ingest.py              # JSONL → posts 적재 CLI
│   └── supabase_ingest.py     # 협업용 호환 entrypoint
│
├── publisher/
│   └── state.py               # USE_DB로 published.json ↔ publish_log 스위치
│
├── migrations/                # Supabase 스키마 (라이브와 1:1)
│   ├── 001_create_articles.sql
│   ├── 003_create_publish_log.sql
│   ├── 007_create_posts.sql
│   ├── 008_tighten_rls.sql
│   ├── 009_rpc_recent_posts.sql
│   ├── 010_create_pipeline_state.sql
│   ├── 011_create_ingest_runs.sql
│   └── 012_enable_pg_cron_keepalive.sql
│
├── scripts/
│   ├── run_publish.py         # 발행 메인 엔트리
│   ├── validate_articles.py
│   ├── local_crawl_ingest.py   # 로컬 crawler 7종 → Supabase posts
│   ├── local_discord_ingest.py # 로컬 Discord crawl → Supabase posts
│   └── backfill_sqlite_to_supabase.py
│
├── docs/
│   ├── ingest-spec.md         # 크롤러→Supabase write contract (source of truth)
│   └── ...                    # 다이제스트 정적 산출물
│
├── .github/workflows/
│   ├── daily_publish.yml      # 발행 (cron 23:00 UTC = 08:00 KST)
│   └── deploy-pages.yml       # GH Pages
│
├── run_hourly.py              # 기사 생성 (Discord/LLM)
├── run_full.py / run_digest.py
├── requirements.txt / .env.example / .gitignore
```

---

## 환경 구축

### 1. 의존성
```bash
pip install -r requirements.txt
```

### 2. 환경변수
```bash
cp .env.example .env   # 로컬 개발용
```

GH Actions 운영에서는 Repo Secrets에 다음을 등록 (CJ가 관리):
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`
- `X_CLIENT_ID`, `X_CLIENT_SECRET`, `X_REFRESH_TOKEN`
- 크롤/LLM에 필요한 키 (HB 워크플로우에서 필요 시 추가)

### 3. DB 스키마
이미 라이브 적용됨 (project `qyckjkidscpiyrdzqxoc`). 신규 환경에 다시 세팅하려면 `migrations/` 디렉토리의 SQL을 번호 순서대로 Supabase SQL Editor 또는 MCP `apply_migration`으로 실행.

### 4. 로컬 실행
```bash
# 전체 크롤 + 적재 (JSONL → Supabase posts)
# 전부 로컬 WSL에서 실행한다.
python3 scripts/local_crawl_ingest.py

# Discord만 따로 적재해야 할 때
python3 scripts/local_discord_ingest.py

# (1회성) 기존 SQLite 백필
python scripts/backfill_sqlite_to_supabase.py --db data/signal.db

# 발행 dry-run
python scripts/run_publish.py --dry-run --platform telegram
```

### 5. GH Actions 운영
- **`daily_publish.yml`**: 매일 08:00 KST 자동 발행. 수동 실행 시 `workflow_dispatch`로 `dry_run`/`platform`/`force`/`limit` 지정 가능

크롤링은 전부 로컬 WSL에서만 실행한다. GitHub Actions에는 crawler workflow를
두지 않는다. 로컬에서 `scripts/local_crawl_ingest.py`를 실행하면 모든 crawler가
JSONL을 만들고 Supabase `posts`로 upsert한다.

---

## 협업 룰

- 사람 작업은 `dev` 브랜치에 먼저 올린다.
- `main` 직접 수정/직접 push는 하지 않는다.
- `main` 반영은 `dev`에서 검증 후 PR/merge로 처리한다.

---

## 환경 변수

| 변수 | 설명 | GH Secrets |
|------|------|---|
| `SUPABASE_URL` | `https://qyckjkidscpiyrdzqxoc.supabase.co` | ✅ |
| `SUPABASE_SERVICE_ROLE_KEY` | 쓰기용 (크롤/ingest/state) — 클라 노출 절대 금지 | ✅ |
| `SUPABASE_ANON_KEY` | 정적 사이트 `articles` 읽기용 | ✅ |
| `USE_DB` | `true` → publisher가 `publish_log` 사용 | 설정값 (env 기본 true 권장) |
| `TELEGRAM_BOT_TOKEN` | BotFather 토큰 | ✅ |
| `TELEGRAM_CHANNEL_ID` | 채널 ID (`@...` 또는 `-100...`) | ✅ |
| `X_CLIENT_ID` / `X_CLIENT_SECRET` / `X_REFRESH_TOKEN` | X OAuth 2.0 | ✅ |
| `GOOGLE_API_KEY` (or `GOOGLE_API_KEYS`) | Gemini/Gemma | 생성도 GHA로 옮기면 ✅ |
| `DISCORD_TOKEN` | 로컬 Discord 크롤 전용. **GH Secrets에 올리지 말 것** | ❌ |

---

## 라이선스

Private repo.
