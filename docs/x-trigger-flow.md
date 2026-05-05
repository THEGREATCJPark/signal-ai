# X Trigger Flow

AI 최전방 뉴스 keeps the daily digest on the existing schedule and adds an hourly review path for watched X accounts.

## Flow

1. `X Trigger Scan` runs hourly at minute 17 with the conservative `auto` scope.
2. The scanner uses Nitter RSS by default. `X_TRIGGER_FEED_MODE` can explicitly select `rsshub`, `nitter-first`, or `rsshub-first`, but the MVP scheduled path is Nitter-based and does not require `X_BEARER_TOKEN`.
3. Manual `workflow_dispatch` can scan broader scopes: `core`, `fast`, `scoop`, `oss`, `coding`, `research`, `benchmark`, or `all`. Use `all` only for manual review/backfill runs.
4. `scripts/x_trigger_scan.py` reads `config/x_trigger_accounts.json`, deduplicates handles case-insensitively, fetches RSS items from Nitter mirrors, and stores cursors in Supabase `pipeline_state` under `x_trigger_state`.
5. On first run for an account, the scanner records the latest tweet as the baseline and does not create review issues. Manual `backfill=true` creates cards for the latest posts.
6. Replies and retweets are excluded from trigger issues by default. Nitter titles beginning with `R to` are replies; titles beginning with `RT by` are retweets.
7. For each new post, the scanner summarizes it in Korean with the Google/Gemma style API used by the article pipeline. If the model key is missing or fails, it falls back to a literal summary from the tweet text.
8. The scanner opens a GitHub Issue labeled `x-trigger` and `needs-review`. The issue body contains account metadata, source URL, timestamps, Korean summary, original tweet text, recommended publication text, and a hidden payload used by the approval workflow.
9. A reviewer comments `/approve-trigger`, `yes`, `예`, `approve`, or `승인` to publish. They comment `/reject-trigger`, `no`, `아니오`, `reject`, or `거절` to reject.
10. `X Trigger Review` handles the issue comment. One approved comment from an allowed reviewer publishes through Telegram and/or the official X API path, adds a terminal label, and closes the issue.

## Review Platform Choice

GitHub Issues is the source of truth for approval. Telegram inline buttons would require a public webhook or polling service to receive callback queries, while GitHub issue comments are already a native Actions trigger. The scanner can still send an optional Telegram notification with the issue link by setting `TRIGGER_REVIEW_TELEGRAM_CHAT_ID`.

## Guardrails

The default scheduled X scan is intentionally modest to avoid hammering public Nitter instances:

- Scope: `auto`
- Accounts: `OpenAI`, `OpenAIDevs`, `ChatGPTapp`, `AnthropicAI`, `GoogleDeepMind`
- Schedule: hourly at minute 17
- Results: `max_results=1`
- Feed mode: `nitter`

Broader account groups are manual by default:

| Scope | Included tiers | Use case |
| --- | --- | --- |
| `auto` | `auto` | Always-on low-cost official sensor |
| `core` | `auto`, `core` | Priority model labs, benchmark, and practitioner accounts |
| `fast` | `auto`, `core`, `fast` | Early community signal sweep |
| `scoop` | `auto`, `core`, `scoop` | Company/internal reporting sweep |
| `oss` | `auto`, `core`, `oss` | Open-source/local-model sweep |
| `coding` | `auto`, `core`, `coding` | AI coding and agent product sweep |
| `research` | `auto`, `core`, `research` | Researcher and AGI discourse sweep |
| `benchmark` | `auto`, `core`, `benchmark` | Benchmark and eval sweep |
| `all` | every tier | Manual broad scan only |

Do not raise the scheduled scope or `max_results` against public Nitter/RSSHub instances. Add reliable Nitter mirrors with `X_TRIGGER_NITTER_INSTANCES` before broader automated scans.

## Required Settings

The scheduled scan and `issue_comment` approval workflows must exist on the repository default branch before GitHub can run them from cron or issue comments. If `dev` is not the default branch, merge this workflow set to the default branch before relying on automated scans or comment approvals.

Secrets:

- `GOOGLE_API_KEY` or `GOOGLE_API_KEYS`: summary model key(s).
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`: cursor state and publish log.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`: Telegram publishing.
- `X_CLIENT_ID`, `X_CLIENT_SECRET`, `X_REFRESH_TOKEN`: official X API publishing.

Optional variables:

- `TRIGGER_REVIEWERS`: comma-separated GitHub usernames allowed to approve. If empty, GitHub `OWNER`, `MEMBER`, and `COLLABORATOR` commenters are allowed.
- `TRIGGER_PUBLISH_PLATFORM`: `telegram`, `x`, or `both`. Defaults to `both`.
- `X_TRIGGER_FEED_MODE`: `nitter`, `rsshub`, `nitter-first`, or `rsshub-first`. Defaults to `nitter`.
- `X_TRIGGER_NITTER_INSTANCES` or `NITTER_INSTANCES`: comma-separated Nitter instance URLs. Defaults to `https://nitter.net`.
- `X_TRIGGER_FEED_BASE_URLS`: comma-separated RSSHub-compatible base URLs for explicit RSSHub modes.
- `X_TRIGGER_FEED_TIMEOUT_SECONDS`: per-instance feed timeout. Defaults to `15`.
- `X_TRIGGER_FEED_RETRIES`: attempts per feed URL. Defaults to `2`.
- `X_TRIGGER_PER_ACCOUNT_DELAY_SECONDS`: delay between accounts. Defaults to `1.5`.

Optional secret:

- `TRIGGER_REVIEW_TELEGRAM_CHAT_ID`: private Telegram chat or group that receives review issue links.

## Manual Commands

Dry-run scan without saving state:

```bash
python scripts/x_trigger_scan.py --accounts config/x_trigger_accounts.json --scope auto --dry-run
```

Manual priority sweep:

```bash
python scripts/x_trigger_scan.py --accounts config/x_trigger_accounts.json --scope core --max-results 1
```

Manual broad sweep:

```bash
python scripts/x_trigger_scan.py --accounts config/x_trigger_accounts.json --scope all --max-results 1
```

Create review issues for the latest visible posts even if cursors are empty:

```bash
python scripts/x_trigger_scan.py --accounts config/x_trigger_accounts.json --scope core --max-results 1 --backfill
```
