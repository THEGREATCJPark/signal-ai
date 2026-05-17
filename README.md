# Signal AI

AI frontier news trigger pipeline for publishing important X-sourced AI news to
Telegram and X.

`main` is the production branch. GitHub scheduled workflows only run the code
that has been committed and pushed to `main`; local edits do not affect
automation until they are shipped.

Daily article posting is not the active product direction right now. Daily
workflow files and helper scripts may still exist in the repository as legacy or
manual utilities, but the current operating model is the X trigger pipeline.

## Current Operating Model

| Time | Flow | Runtime |
| --- | --- | --- |
| Hourly `:00` | Scan watched X accounts, score candidates, create review issues or auto-publish high-confidence items | `.github/workflows/x-trigger-scan.yml` |
| On issue comment | Approve/reject a trigger candidate and publish if approved | `.github/workflows/x-trigger-review.yml` |
| Manual | Publish selected reviewed trigger issues | `.github/workflows/manual-trigger-issue-publish.yml` |
| Manual | Smoke-test one Telegram publish path | `.github/workflows/manual-publish-smoke.yml` |

The active automation goal is simple: catch important AI news from watched X
accounts, avoid low-signal noise, avoid duplicate stories, and publish only
high-confidence items automatically.

## X Trigger Pipeline

The trigger path scans `config/x_trigger_accounts.json` with public X feed
sources, summarizes new posts, scores them with `scripts/x_trigger_scoring.py`,
and then either skips, queues for review, or publishes.

Candidate scoring is v2:

- Score is 0-100.
- Breakdown: `source`, `event`, `impact`, `freshness`, `evidence`, `publish_fit`.
- The issue body includes the score, decision, confidence, event type,
  `story_key`, breakdown, penalties, hard blocks, and reasons.
- `score_trigger_candidate(candidate)` remains the compatibility API that
  returns only the integer score.
- `score_trigger_candidate_detail(candidate)` returns the full scoring detail.

Current decision defaults:

| Decision | Meaning |
| --- | --- |
| `auto_both` | Publish to Telegram and X automatically after creating an audit issue |
| `auto_telegram_review_x` | Publish to Telegram only; leave X for review |
| `review` | Create a GitHub issue for human review |
| `watch` | Low-signal watch item; skipped unless low-signal issue creation is enabled |
| `drop` | Skip by default |

Automatic X + Telegram publishing requires:

- `score >= 60`
- `confidence == verified`
- no `hard_blocks`

Rumor, inference, market commentary, personal anecdotes, event promos, weak AI
relevance, simple official requotes from secondary accounts, and duplicates can
block automatic X publishing even when the numeric score is high.

## Duplicate Protection

The X trigger pipeline deduplicates by story, not just by tweet id.

`build_story_key(candidate)` prefers an official or quoted source status id when
available. Otherwise it builds a stable key from normalized entities and event
terms, such as `openai:chatgpt:personal-finance` or `v0:browser-use`.

The scanner checks these state containers:

- `published_story_keys`
- `recent_story_keys`
- `story_keys`

If a matching `story_key` already exists, scoring applies:

- penalty: `duplicate_story_key`, `-45`
- hard block: `duplicate_story_key`
- automatic X + Telegram publishing is blocked even if the candidate has an
  explicit high score

When a trigger auto-publish succeeds, the scanner records the `story_key` in
`published_story_keys`.

## Trigger Issues And Labels

GitHub Issues remain the audit and review surface. The scanner labels issues
based on the v2 decision:

- `x-trigger`
- `needs-review`
- `trigger-watch`
- `trigger-review`
- `trigger-auto-candidate`
- `trigger-low-signal`
- `trigger-hard-blocked`
- `trigger-auto-published`
- `trigger-approved`
- `trigger-rejected`

By default, `drop` and `watch` candidates do not create issues. Set
`TRIGGER_CREATE_LOW_SIGNAL_ISSUES=true` to keep them for debugging.

Review comments:

- `yes`, `approve`, `/approve`, `/approve-trigger`: publish to the configured review platform
- `no`, `reject`, `/reject`, `/reject-trigger`: close without publishing

## Watched X Accounts

The requested priority groups are included in `config/x_trigger_accounts.json`:

- early detection: `testingcatalog`, `btibor91`
- OpenAI: `OpenAI`, `OpenAIDevs`, `sama`, `gdb`, `polynoamial`
- Anthropic/Claude: `AnthropicAI`, `claudeai`, `ClaudeDevs`, `DarioAmodei`, `alexalbert__`
- Google/Gemini: `GoogleDeepMind`, `GoogleAI`, `GeminiApp`, `demishassabis`, `JeffDean`
- benchmarks: `arena`, `ArtificialAnlys`, `METR_Evals`
- interpretation/practice: `karpathy`, `simonw`
- fast signal, scoop, open-source, local-model, and AI coding accounts from the expanded watchlist

Scheduled trigger scans run with `SCHEDULED_SCOPE=all`, so the full configured
watchlist is checked hourly. The default feed mode is `nitter-first`: try Nitter
RSS, then RSSHub-compatible free feed mirrors.

## Publishing Credentials

Telegram publishing requires:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHANNEL_ID`

X posting supports OAuth 1.0a first:

- `X_API_KEY`
- `X_API_SECRET`
- `X_ACCESS_TOKEN`
- `X_ACCESS_TOKEN_SECRET`

If OAuth 1.0a credentials are absent or rejected, posting can use OAuth 2.0 user
context refresh credentials:

- `X_CLIENT_ID`
- `X_CLIENT_SECRET` optional for public clients
- `X_REFRESH_TOKEN`

Rotated OAuth 2.0 refresh tokens are saved to Supabase `pipeline_state` when
Supabase service credentials are available.

## GitHub Secrets And Variables

Required secrets:

- Supabase: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_ANON_KEY`
- Telegram: `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHANNEL_ID`
- X OAuth 1.0a: `X_API_KEY`, `X_API_SECRET`, `X_ACCESS_TOKEN`, `X_ACCESS_TOKEN_SECRET`
- X OAuth 2.0 fallback: `X_CLIENT_ID`, `X_CLIENT_SECRET`, `X_REFRESH_TOKEN`
- LLM for trigger summaries: `GEMINI_API_KEYS_CJ`

Useful variables:

- `X_TRIGGER_FEED_MODE`
- `X_TRIGGER_FEED_BASE_URLS`
- `X_TRIGGER_NITTER_INSTANCES`
- `NITTER_INSTANCES`
- `TRIGGER_AUTO_PUBLISH_SCORE_THRESHOLD` default `60`
- `TRIGGER_REVIEW_SCORE_THRESHOLD` default `65`
- `TRIGGER_MIN_ISSUE_SCORE` default `45`
- `TRIGGER_AUTO_PUBLISH_PLATFORM` default `both`
- `TRIGGER_CREATE_LOW_SIGNAL_ISSUES` default `false`
- `TRIGGER_REVIEWERS`
- `TRIGGER_PUBLISH_PLATFORM` default `both` for issue approvals
- `TRIGGER_REVIEW_TELEGRAM_CHAT_ID`

## Important Files

- `scripts/x_trigger_scoring.py`: v2 scoring, story keys, decisions, duplicate hard blocks
- `scripts/x_trigger_scan.py`: feed crawling, summarization, scoring, issue creation, trigger auto-publish
- `scripts/x_trigger_review.py`: approval parsing and manual trigger publishing
- `scripts/publish_trigger_issues.py`: manual publish of selected trigger issues
- `bot/x_poster.py`: X text fitting, OAuth 1.0a posting, OAuth 2.0 fallback, trigger post rendering
- `.github/workflows/x-trigger-scan.yml`: hourly watched-account scan
- `.github/workflows/x-trigger-review.yml`: issue comment approval handler
- `.github/workflows/manual-trigger-issue-publish.yml`: manual publish of selected trigger issues
- `.github/workflows/manual-publish-smoke.yml`: manual smoke publish path

## Verification

```bash
python -m pytest -q
python -m unittest discover -s tests
python -m compileall -q bot crawlers db scripts tests run_hourly.py discord_export_linux.py
git diff --check
```

Operational target: `main` should be safe to run as the X trigger production
pipeline.
