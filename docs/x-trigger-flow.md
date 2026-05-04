# X Trigger Flow

First Light AI keeps the daily digest on the existing schedule and adds a fast path for watched X accounts.

## Flow

1. `X Trigger Scan` runs every hour at `:17` and `:47`, or manually through `workflow_dispatch`.
2. `scripts/x_trigger_scan.py` reads `config/x_trigger_accounts.json`, fetches recent original posts from X, and stores per-account cursors in Supabase `pipeline_state` under `x_trigger_state`.
3. On first run for an account, the scanner records the latest tweet as the baseline and does not create review issues. Manual `backfill=true` creates cards for the latest posts.
4. For each new post, the scanner summarizes it with the same Google/Gemma style API used by the article pipeline. If the model key is missing or fails, it falls back to a literal summary from the tweet text.
5. The scanner opens a GitHub Issue labeled `x-trigger` and `needs-review`. The issue body contains the review summary, source URL, original tweet text, and a hidden payload used by the approval workflow.
6. CJ or HB reviews the issue. Comment `/approve-trigger`, `yes`, or `예` to publish. Comment `/reject-trigger`, `no`, or `아니오` to reject.
7. `X Trigger Review` handles the issue comment. One approved comment from an allowed reviewer publishes the single trigger article through GitHub Actions and closes the issue.

## Review Platform Choice

GitHub Issues is the source of truth for approval. Telegram inline buttons would require a public webhook or polling service to receive callback queries, while GitHub issue comments are already a native Actions trigger. The scanner can still send an optional Telegram notification with the issue link by setting `TRIGGER_REVIEW_TELEGRAM_CHAT_ID`.

## Required Settings

The scheduled scan and `issue_comment` approval workflows must exist on the repository default branch before GitHub can run them from cron or issue comments. If `dev` is not the default branch, merge this workflow set to the default branch before relying on automated scans or comment approvals.

Secrets:

- `X_BEARER_TOKEN`: read-only X API bearer token for scanning watched account timelines.
- `GOOGLE_API_KEY` or `GOOGLE_API_KEYS`: summary model key(s).
- `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`: cursor state and publish log.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`: Telegram publishing.
- `X_CLIENT_ID`, `X_CLIENT_SECRET`, `X_REFRESH_TOKEN`: X publishing.

Optional variables:

- `TRIGGER_REVIEWERS`: comma-separated GitHub usernames allowed to approve. If empty, GitHub `OWNER`, `MEMBER`, and `COLLABORATOR` commenters are allowed.
- `TRIGGER_PUBLISH_PLATFORM`: `telegram`, `x`, or `both`. Defaults to `both`.

Optional secret:

- `TRIGGER_REVIEW_TELEGRAM_CHAT_ID`: private Telegram chat or group that receives review issue links.

## Manual Commands

Dry-run scan without saving state:

```bash
python scripts/x_trigger_scan.py --accounts config/x_trigger_accounts.json --dry-run
```

Create review issues for the latest visible posts even if cursors are empty:

```bash
python scripts/x_trigger_scan.py --accounts config/x_trigger_accounts.json --backfill
```

Approve from a GitHub Issue comment:

```text
/approve-trigger
```

Reject from a GitHub Issue comment:

```text
/reject-trigger
```
