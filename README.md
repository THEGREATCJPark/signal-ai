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
│ Local WSL — crawler 8종 + hourly X watch          │
│ HN · Reddit · arXiv · HuggingFace                │
│ GeekNews · LessWrong · X watch · Discord          │
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
│ manual dry-run · force/limit · both/tg/x         │
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
- [x] X OAuth 1.0a User Context 발행 (`bot/x_poster.py`)
- [x] `publisher/state.py` USE_DB 듀얼 모드 (Supabase `publish_log` 또는 로컬 JSON)
- [x] 크롤러 8종 구현 (`crawlers/`)
- [x] GH Actions `daily_publish.yml` (dev: workflow_dispatch dry-run default)
- [x] GitHub Pages 다이제스트 배포 (`pages.yml`)
- [x] SQLite → Supabase 백필 스크립트 (`scripts/backfill_sqlite_to_supabase.py`)

### 진행 중
- [x] **로컬 전용 크롤링/적재 경로** — `scripts/local_crawl_ingest.py`가 로컬 WSL에서 crawler 8종 실행 후 Supabase `posts`에 upsert
- [x] **Hourly X watch 경로** — `run_x_watch_task.sh`가 Nitter RSS 기반 `x_watch`만 실행하고 Actions secrets handoff로 Supabase `posts`에 upsert
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
| 크롤링 | Python (requests, feedparser, Algolia API, Reddit JSON, GraphQL, Nitter RSS) |
| LLM | Gemma / Gemini (기사 생성) |
| 봇 | Telegram Bot API (requests 직접 호출) |
| X | X API v2 (OAuth 2.0 + Refresh Token) |
| DB | Supabase PostgreSQL (RLS + pg_cron) |
| 스케줄 | Mint cron / GitHub Actions 수동 발행 (`daily_publish.yml`) |
| 배포 | GitHub Pages (다이제스트 HTML) |

---

## 프로젝트 구조

```
signal-ai/
├── crawlers/                  # 크롤링 모듈 (HB)
│   ├── _common.py
│   ├── hn.py / reddit.py / arxiv.py / hf_trending.py
│   ├── geeknews.py / lesswrong.py / x_watch.py / discord.py
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
│   ├── local_crawl_ingest.py   # 로컬 crawler 8종 → Supabase posts
│   ├── local_discord_ingest.py # 로컬 Discord crawl → Supabase posts
│   ├── dispatch_x_watch_handoff.py # X watch → Actions secrets ingest
│   ├── x_watch_gate.py         # hourly X watch catch-up/lock gate
│   └── backfill_sqlite_to_supabase.py
│
├── docs/
│   ├── ingest-spec.md         # 크롤러→Supabase write contract (source of truth)
│   └── ...                    # 다이제스트 정적 산출물
│
├── .github/workflows/
│   ├── daily_publish.yml      # 발행 수동 실행(dry-run 기본)
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
- `X_API_KEY`, `X_API_SECRET`, `X_ACCESS_TOKEN`, `X_ACCESS_TOKEN_SECRET`
- 크롤/LLM에 필요한 키 (HB 워크플로우에서 필요 시 추가)

### 3. DB 스키마
이미 라이브 적용됨 (project `qyckjkidscpiyrdzqxoc`). 신규 환경에 다시 세팅하려면 `migrations/` 디렉토리의 SQL을 번호 순서대로 Supabase SQL Editor 또는 MCP `apply_migration`으로 실행.

### 4. 로컬 실행
```bash
# 전체 크롤 + 적재 (JSONL → Supabase posts)
# 전부 로컬 WSL에서 실행한다.
python3 scripts/local_crawl_ingest.py

# X 계정 watch만 1회 실행하고 Actions secrets로 Supabase 적재
# run_x_watch_task.sh는 이 경로를 hourly gate로 감싼다.
./run_x_watch_task.sh

# Discord만 따로 적재해야 할 때
python3 scripts/local_discord_ingest.py

# (1회성) 기존 SQLite 백필
python scripts/backfill_sqlite_to_supabase.py --db data/signal.db

# 발행 dry-run
python scripts/run_publish.py --dry-run --platform telegram
```

### 5. GH Actions 운영
- **`daily_publish.yml`**: `dev`에서는 수동 실행 전용이며 기본값은 dry-run. `workflow_dispatch`로 `dry_run`/`platform`/`force`/`limit` 지정 가능

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
| `X_API_KEY` / `X_API_SECRET` / `X_ACCESS_TOKEN` / `X_ACCESS_TOKEN_SECRET` | X OAuth 1.0a User Context | ✅ |
| `GOOGLE_API_KEY` (or `GOOGLE_API_KEYS`) | Gemini/Gemma | 생성도 GHA로 옮기면 ✅ |
| `DISCORD_TOKEN` | 로컬 Discord 크롤 전용. **GH Secrets에 올리지 말 것** | ❌ |

---

## 라이선스

Private repo.

---

## X 계정 트리거 검수 플로우

데일리 요약 발행은 그대로 유지하고, 관심 등록한 X 계정의 새 게시글만 빠르게 검수/발행하는 트리거 플로우를 추가했다.

- 계정 목록: `config/x_trigger_accounts.json`
- 스캔: `.github/workflows/x-trigger-scan.yml`
- 검수/승인: `.github/workflows/x-trigger-review.yml`
- 구현: `scripts/x_trigger_scan.py`, `scripts/x_trigger_review.py`
- 운영 문서: `docs/x-trigger-flow.md`

흐름은 다음과 같다.

1. 공식 RSS/changelog, HN, Reddit, arXiv, HuggingFace, GeekNews, Discord ingest를 기본 저비용 레이더로 유지한다.
2. X 계정은 RSSHub 호환 무료 feed bridge로 먼저 확인하고, 실패하면 Nitter RSS mirror로 fallback한다.
3. 전체 관심 계정은 `config/x_trigger_accounts.json`에 `auto`, `core`, `fast`, `scoop`, `oss`, `coding`, `research`, `benchmark` tier로 나누어 둔다.
4. 수동 실행 시 `scope=core|fast|scoop|oss|coding|research|benchmark|all`을 골라 넓게 훑는다.
5. 최초 실행 계정은 최신 tweet id만 기준선으로 저장해 과거 게시글 폭탄을 막는다.
6. 새 게시글이 감지되면 기존 Google/Gemma 계열 요약 API로 한국어 검수 요약을 만든다.
7. GitHub Issue에 `x-trigger`, `needs-review` 라벨을 붙여 검수 카드를 만든다.
8. CJ 또는 HB가 issue 댓글에 `/approve-trigger` 또는 `예`를 남기면 GitHub Actions가 해당 단건을 Telegram/X로 발행한다.
9. `/reject-trigger` 또는 `아니오`를 남기면 발행하지 않고 issue를 닫는다.

크롤링에는 X API secret이 필요 없다. 선택적으로 `X_TRIGGER_FEED_BASE_URLS` repository variable에 RSSHub 호환 base URL을 콤마로 넣을 수 있다. 비워두면 public instance 후보를 순서대로 시도한다. RSSHub가 실패하면 `X_TRIGGER_NITTER_INSTANCES` 또는 `NITTER_INSTANCES`에 지정한 Nitter mirror를 시도한다. 승인자는 `TRIGGER_REVIEWERS` repository variable로 제한할 수 있고, 비워두면 GitHub `OWNER`/`MEMBER`/`COLLABORATOR` 권한의 댓글을 승인으로 인정한다.
