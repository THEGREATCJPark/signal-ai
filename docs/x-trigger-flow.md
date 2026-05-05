# X Trigger Flow

First Light AI keeps the daily digest on the existing schedule and adds a fast path for watched X accounts.

## Flow

1. Free and low-cost sources remain the main early-warning system: official blogs, changelogs, RSS feeds, HN, Reddit, arXiv, HuggingFace, GeekNews, and Discord ingest.
2. `X Trigger Scan` uses free RSSHub-compatible X account feeds first, then falls back to Nitter RSS when the RSSHub bridges fail. It does not call the official X API and does not require `X_BEARER_TOKEN`.
3. Manual `workflow_dispatch` can scan broader scopes: `core`, `fast`, `scoop`, `oss`, `coding`, `research`, `benchmark`, or `all`.
4. `scripts/x_trigger_scan.py` reads `config/x_trigger_accounts.json`, fetches RSS items from X account feed bridges or Nitter mirrors, and stores cursors in Supabase `pipeline_state` under `x_trigger_state`.
5. On first run for an account, the scanner records the latest tweet as the baseline and does not create review issues. Manual `backfill=true` creates cards for the latest posts.
6. For each new post, the scanner summarizes it with the same Google/Gemma style API used by the article pipeline. If the model key is missing or fails, it falls back to a literal summary from the tweet text.
7. The scanner opens a GitHub Issue labeled `x-trigger` and `needs-review`. The issue body contains the review summary, source URL, original tweet text, and a hidden payload used by the approval workflow.
8. CJ or HB reviews the issue. Comment `/approve-trigger`, `yes`, or `예` to publish. Comment `/reject-trigger`, `no`, or `아니오` to reject.
9. `X Trigger Review` handles the issue comment. One approved comment from an allowed reviewer publishes the single trigger article through GitHub Actions and closes the issue.

## Review Platform Choice

GitHub Issues is the source of truth for approval. Telegram inline buttons would require a public webhook or polling service to receive callback queries, while GitHub issue comments are already a native Actions trigger. The scanner can still send an optional Telegram notification with the issue link by setting `TRIGGER_REVIEW_TELEGRAM_CHAT_ID`.

## Free Feed Guardrails

The default scheduled X scan is intentionally modest to avoid hammering public RSS bridge and Nitter instances:

- Scope: `auto`
- Accounts: `OpenAI`, `OpenAIDevs`, `ChatGPTapp`, `AnthropicAI`, `GoogleDeepMind`
- Schedule: every 4 hours
- Results: `max_results=1`

This keeps X as a fast but bounded signal. Broader account groups are manual by default:

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

Do not raise the scheduled scope or `max_results` against public RSSHub/Nitter instances. Self-host RSSHub and set `X_TRIGGER_FEED_BASE_URLS` if broader automated scans become necessary.

## Required Settings

The scheduled scan and `issue_comment` approval workflows must exist on the repository default branch before GitHub can run them from cron or issue comments. If `dev` is not the default branch, merge this workflow set to the default branch before relying on automated scans or comment approvals.

Secrets:

- `GOOGLE_API_KEY` or `GOOGLE_API_KEYS`: summary model key(s).
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`: cursor state and publish log.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`: Telegram publishing.
- `X_CLIENT_ID`, `X_CLIENT_SECRET`, `X_REFRESH_TOKEN`: X publishing.

Optional variables:

- `TRIGGER_REVIEWERS`: comma-separated GitHub usernames allowed to approve. If empty, GitHub `OWNER`, `MEMBER`, and `COLLABORATOR` commenters are allowed.
- `TRIGGER_PUBLISH_PLATFORM`: `telegram`, `x`, or `both`. Defaults to `both`.
- `X_TRIGGER_FEED_BASE_URLS`: comma-separated RSSHub-compatible base URLs. Defaults to public instances; self-hosted RSSHub is recommended for reliability.
- `X_TRIGGER_NITTER_INSTANCES` or `NITTER_INSTANCES`: comma-separated Nitter instance URLs used after RSSHub-compatible feeds fail.

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

Approve from a GitHub Issue comment:

```text
/approve-trigger
```

Reject from a GitHub Issue comment:

```text
/reject-trigger
```
