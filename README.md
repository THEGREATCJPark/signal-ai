# AI 최전방 뉴스

AI frontier news pipeline for daily publishing plus fast X-trigger review.

`main` is the production branch. The old `dev`/PR #14 line contains useful
experiments, but this branch now carries the selected production pieces:
daily digest publishing, local-only crawler handoff, Supabase ingestion, and
GitHub Issue based trigger approval.

## Production Flow

| Time (KST) | Flow | Runtime |
| --- | --- | --- |
| 07:00 | Local public/Discord crawl, bundle handoff to GitHub Actions, Supabase `posts` ingest | Local WSL + `.github/workflows/local-crawl-handoff.yml` |
| 08:00 | Daily article generation from local Discord pipeline | Local WSL `run_cron_task.sh` |
| 20:42 | Sync generated articles to Supabase, then publish daily article content to Telegram and X | `.github/workflows/daily_publish.yml` |
| Hourly `:00` | Scan watched X accounts and open review issues | `.github/workflows/x-trigger-scan.yml` |
| On issue comment | Approve/reject a trigger candidate and publish if approved | `.github/workflows/x-trigger-review.yml` |

Daily publish posts the article content only. It does not prepend any brand
banner. X daily publishing uses the same per-article posting path as approved
trigger publishing, so each selected article is posted and tracked separately.

## X Trigger Pipeline

The trigger path uses free public feed crawling only. It scans
`config/x_trigger_accounts.json`, summarizes new high-signal posts with the
same AI tooling used by the existing pipeline, then creates a GitHub Issue for
human review.

Official high-signal launch/model/API posts are also auto-published immediately
after the review issue is created. The issue is kept as the audit record and is
marked `trigger-auto-published` when the automatic publish succeeds.

Review comments:

- `yes`, `approve`, `승인`, `/approve`: publish to the configured platform
- `no`, `reject`, `거절`, `/reject`: close without publishing

Publishing uses OAuth 1.0a X credentials only:

- `X_API_KEY`
- `X_API_SECRET`
- `X_ACCESS_TOKEN`
- `X_ACCESS_TOKEN_SECRET`

OAuth 2.0 secrets may remain registered for legacy/manual experiments, but posting does not use them:
`X_CLIENT_ID`, `X_CLIENT_SECRET`, `X_REFRESH_TOKEN`.

## Watched X Accounts

The requested priority groups are included in `config/x_trigger_accounts.json`:

- early detection: `testingcatalog`, `btibor91`
- OpenAI: `OpenAI`, `OpenAIDevs`, `sama`, `gdb`, `polynoamial`
- Anthropic/Claude: `AnthropicAI`, `claudeai`, `ClaudeDevs`, `DarioAmodei`, `alexalbert__`
- Google/Gemini: `GoogleDeepMind`, `GoogleAI`, `GeminiApp`, `demishassabis`, `JeffDean`
- benchmarks: `arena`, `ArtificialAnlys`, `METR_Evals`
- interpretation/practice: `karpathy`, `simonw`
- fast signal, scoop, and open-source/local-model accounts from the expanded watchlist

Scheduled trigger scans run with `SCHEDULED_SCOPE=all`, so the full configured
watchlist is checked hourly. The default feed mode is `nitter-first`: try Nitter
RSS, then RSSHub-compatible free feed mirrors. Manual runs can still choose a
narrower scope.

## Local Crawler Handoff

Crawler execution stays local. GitHub Actions receives only a short-lived
bundle URL and performs the Supabase upsert with repository secrets.

Operational dependency: the local WSL machine that owns `run_cron_task.sh` and
`run_local_crawl_handoff_task.sh` must be awake for fresh daily collection and
article generation. GitHub Actions can still publish and run X trigger scans
without that machine, but the daily report will only be as fresh as the latest
pushed `docs/articles.json` and synced Supabase `public_state`.

Local commands:

```bash
./run_cron_task.sh
./run_local_crawl_handoff_task.sh
python scripts/local_crawl_ingest.py --skip-crawl data/crawled/example.jsonl
python scripts/local_discord_ingest.py --skip-crawl data/crawled/discord-example.jsonl
```

Useful environment overrides:

- `AI_FRONTIER_PYTHON`: Python interpreter for local shell wrappers
- `AI_FRONTIER_PUBLISH_BRANCH`: branch used by the local publisher commit/push path
- `AI_FRONTIER_PUBLISH_SOURCE`: default source for `scripts/run_publish.py`
- `DISCORD_EXPORT_CONFIG`: local path to `discord_export_config.env`

`discord_export_config.env`, raw exports, local DB files, and token-bearing
files must stay untracked.

## GitHub Secrets And Variables

Required secrets:

- Supabase: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`
- Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`
- X OAuth 1.0a: `X_API_KEY`, `X_API_SECRET`, `X_ACCESS_TOKEN`, `X_ACCESS_TOKEN_SECRET`
- LLM: `GOOGLE_API_KEY` or `GOOGLE_API_KEYS`; trigger scan uses `GEMINI_API_KEYS_CJ`
- Local handoff: `LOCAL_CRAWL_BUNDLE_URL`, `LOCAL_CRAWL_BATCH_SIZE`

Useful variables:

- `X_TRIGGER_FEED_MODE`
- `X_TRIGGER_FEED_BASE_URLS`
- `X_TRIGGER_NITTER_INSTANCES`
- `NITTER_INSTANCES`
- `TRIGGER_REVIEWERS`
- `TRIGGER_PUBLISH_PLATFORM`

## Important Files

- `.github/workflows/daily_publish.yml`: 20:42 KST Telegram/X daily publish
- `.github/workflows/x-trigger-scan.yml`: hourly watched-account scan
- `.github/workflows/x-trigger-review.yml`: issue comment approval handler
- `.github/workflows/local-crawl-handoff.yml`: local bundle ingestion into Supabase
- `scripts/x_trigger_scan.py`: feed crawling, summarization, issue creation
- `scripts/x_trigger_review.py`: approval parsing and trigger publishing
- `scripts/dispatch_local_crawl_handoff.py`: local tunnel and workflow trigger
- `db/supabase_ingest.py`: JSONL to Supabase `posts`

## Verification

```bash
python -m unittest discover -s tests
python -m compileall -q bot crawlers db scripts tests run_hourly.py discord_export_linux.py
git diff --check
```

The operational target is simple: `main` should be safe to run as production,
while `dev` remains a legacy/experimental reference rather than the deployment
source of truth.
