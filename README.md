# First Light AI

Discord chat-to-news automation for AI frontier updates.

This branch contains the local-first production path:

- export new Discord messages since the last successful run
- preserve raw chat locally for downstream DB ingestion
- scan message chunks with Gemma 4 26B
- keep rumors instead of dropping them, tagged as `category=rumor`
- deduplicate only exact same facts/events
- re-rank active articles into `top`, `main`, and `side`
- publish the updated static site assets
- write daily new-article JSON files for other local pipelines

Live preview:

- Main: https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/a9a6b3f417be5221efd2969fe8da85ed/raw/index.html
- Archive: https://htmlpreview.github.io/?https://gist.githubusercontent.com/pineapplesour/a9a6b3f417be5221efd2969fe8da85ed/raw/archive.html

## Runtime

Windows Task Scheduler owns the daily 08:00 KST trigger:

```text
Task name: First Light AI Daily
Schedule: daily at 08:00 KST
Action: wsl.exe -e bash -lc 'cd /home/pineapple/bunjum2/signal && ./run_cron.sh >> /tmp/signal_daily.log 2>&1'
```

The scheduled entrypoint is:

```bash
/home/pineapple/bunjum2/signal/run_cron.sh
```

The wrapper loads only the local `DISCORD_TOKEN` from `discord_export_config.env`, then runs:

```bash
/home/pineapple/miniconda3/bin/python3 run_hourly.py
```

`discord_export_config.env` is intentionally ignored and must never be committed.

## Public Files

- `docs/index.html`: front page
- `docs/archive.html`: full article archive
- `docs/articles.json`: accumulated public article state
- `exports/articles/YYYY-MM-DD.json`: per-day new article export for other local pipelines

## Discord Exporter

The repository includes wrapper code, not the exporter binary:

- `discord_export_linux.py` calls an installed `DiscordChatExporter.Cli`
- `discord_export_text_only.py` supports the Windows/PowerShell path
- `run_hourly.py` chooses the wrapper for the current environment

Install or provide the actual exporter locally. Do not commit binaries, tokens, raw exports, or local DB files.

## Security Boundary

Never commit:

- `discord_export_config.env`
- `*.env`
- `memory/`
- raw Discord exports
- SQLite DB files
- generated DB backups

The old planning document from the previous repository direction is preserved at `docs/legacy/chanjoon-original-plan.md`.

## Supabase Ingest Path (new — cloud-ready)

A parallel path to the local SQLite flow lets crawlers push raw posts to
Supabase so that scheduling can move to GitHub Actions later without changing
the JSONL contract.

- Schema: `migrations/001..012/*.sql` — already applied live on project
  `qyckjkidscpiyrdzqxoc`. Supabase migrations track records each one.
- Tables: `posts` (raw, service_role only), `articles` (public read),
  `publish_log`, `pipeline_state` (service_role), `ingest_runs` (observability).
- Keepalive: pg_cron `signal_keepalive_daily` at 03:17 UTC prevents free-tier
  auto-pause.
- Write contract: `docs/ingest-spec.md` — required fields, scoring keys,
  RLS matrix, and the upsert rule.
- Two entrypoints read the same JSONL:
  - `db/supabase_ingest.py` → Supabase `posts` (needs `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY`)
  - `db/ingest.py` → local SQLite (unchanged; existing cron keeps working)
- One-time backfill: `scripts/backfill_sqlite_to_supabase.py --db data/signal.db`
  (moves `.db` → `.bak` on success).

GitHub Secrets already registered on this repo: `SUPABASE_URL`,
`SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`. Additional Telegram/X/LLM
secrets can be added when the corresponding workflows land.

## Daily Schedule (KST, provisional)

| 시간 | 무엇 | 담당 | 트리거 |
|---|---|---|---|
| 07:00 KST | 크롤 → Supabase `posts` 적재 | HB | GitHub Actions (`on: schedule`) |
| 08:00 KST | Supabase `articles` → Telegram + X 발행 | CJ | GitHub Actions (`daily_publish.yml`) |

두 시간 모두 임시. 적재/발행 품질 지켜보면서 최적 시간은 추후 조정.

## 협업 룰

- **출시 전 (현재)**: `main`에 직접 push해서 빠르게 이터레이션. 자동 발행 커밋(`chore: publish First Light AI ...`)과 사람 작업 모두 main 직행 OK.
- **출시 후**: 모든 **사람 작업(크롤러 변경, 워크플로우 추가/수정, 스키마 변경, 발행 포맷 수정 등)**은 먼저 `dev`에 푸시 → dev에서 돌려보고 문제 없으면 `main`으로 PR/머지. 로컬 cron/스케줄이 만들어내는 **자동 발행 커밋은 계속 `main` 직행**.

즉, 출시 이후에는 사람 손이 닿는 변경만 `dev → main` 게이트를 통과.
