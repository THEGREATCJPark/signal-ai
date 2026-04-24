# Ingest Spec — Supabase-only pipeline

Source of truth for how crawlers hand data to Supabase. Matches the live schema
in project `qyckjkidscpiyrdzqxoc` as of migrations 001–012.

## Two-layer model

| Layer | Table | Who writes | Who reads |
|---|---|---|---|
| Raw source | `posts` | crawlers (service_role) | pipeline only (service_role) |
| Generated public | `articles` | LLM pipeline | site/tooling (anon read OK) |
| Publish log | `publish_log` | publisher | site/tooling (anon read OK) |
| Automation state | `pipeline_state` | pipeline | pipeline only |
| Observability | `ingest_runs` | crawlers | pipeline only |

Never mix raw crawler/Discord content into `articles`. Raw rows always land in
`posts` first. Article generation reads from `posts` via
`get_recent_posts_by_source(days, per_source)`.

## `posts` write contract

Required fields per row:

| field | type | notes |
|---|---|---|
| `source` | text | e.g. `hackernews`, `reddit`, `discord`, `twitter` |
| `source_id` | text | stable per-source id — HN item id, Reddit t3_xxx, Discord message id |
| `content` | text | the text payload |
| `timestamp` | timestamptz | when the post was authored at the source |

Optional fields:

| field | type | notes |
|---|---|---|
| `source_url` | text | permalink |
| `author` | text | handle/username at the source |
| `parent_id` | text | for replies/threads — reference the parent `source_id` |
| `metadata` | jsonb | free-form; see scoring keys below |
| `fetched_at` | timestamptz | defaults to `now()` if omitted |

### Uniqueness + upsert

`(source, source_id)` is a `UNIQUE` constraint. Every write must be an upsert:

```python
supabase.table("posts").upsert(rows, on_conflict="source,source_id").execute()
```

Batch size: 500 rows per upsert call. Rows missing any required field must be
skipped at client side (increment `rows_skipped` on the run log).

### Scoring keys in `metadata`

`get_recent_posts_by_source` extracts a numeric score by checking these keys in
order — the first that parses as a number wins:

1. `points`
2. `score`
3. `upvotes`
4. `likes`
5. `num_comments`

Put the source's native popularity signal under one of those keys and it will
rank. Anything else is free-form.

## `articles` write contract

Generated public items only. Key columns: `id`, `source`, `title`, `body`,
`summary`, `placement`, `category`, `trust`, `tags[]`, `raw_json`,
`generated_at`, `updated_at`. `source` defaults to `first_light_ai`.

## `publish_log` write contract

One row per (`article_id`, `platform`). The `UNIQUE (article_id, platform)`
constraint makes republish attempts idempotent — use upsert on conflict.

## `pipeline_state` — JSON state replacement

Key/value JSONB store. Replaces all local state files
(`digest_state.json`, `headline_state.json`, `articles.json`, last-run markers).
Read/write via service_role only.

Example keys in use:

- `keepalive_last_ping` — written by the pg_cron job every 03:17 UTC
- other pipeline checkpoints: one key per domain concern

## `ingest_runs` — observability

Optional for HB's crawlers today, but cheap to adopt. Pattern:

```python
# Start
run = supabase.table("ingest_runs").insert({
    "source": "hackernews",
    "status": "running",
}).execute()
run_id = run.data[0]["id"]

try:
    rows = crawl()  # list[dict]
    result = supabase.table("posts").upsert(rows, on_conflict="source,source_id").execute()
    supabase.table("ingest_runs").update({
        "status": "success",
        "finished_at": "now()",
        "rows_read": len(rows),
        "rows_inserted": len(result.data),
    }).eq("id", run_id).execute()
except Exception as e:
    supabase.table("ingest_runs").update({
        "status": "error",
        "finished_at": "now()",
        "error_message": str(e)[:2000],
    }).eq("id", run_id).execute()
    raise
```

Useful read queries:

```sql
-- Latest run per source
select distinct on (source) source, status, rows_inserted, finished_at
from ingest_runs
order by source, started_at desc;

-- Failures in the last 24h
select source, started_at, error_message
from ingest_runs
where status = 'error' and started_at > now() - interval '1 day'
order by started_at desc;
```

## Execution

Crawler entrypoint (current):

```bash
python3 db/ingest.py data/crawled/*.jsonl
```

`db/ingest.py` reads JSONL files with the fields above and upserts via
`db/posts.py:upsert_posts(rows)`. No write path should bypass this module.

## Adding a new field to a source

1. Open a migration in `migrations/NNN_*.sql`; apply via Supabase migrations
   (MCP `apply_migration` or CLI).
2. Update `db/posts.py` / crawler to emit the new field in JSONL.
3. Update this spec.

Order matters: schema first, then data. Do not emit unknown columns — the
client will 400.

## RLS summary

| Table | anon SELECT | service_role |
|---|---|---|
| `articles` | allowed | full access |
| `publish_log` | allowed | full access |
| `posts` | denied (`42501`) | full access |
| `pipeline_state` | denied | full access |
| `ingest_runs` | denied | full access |

Public reads on `articles`/`publish_log` are intentional — that is what the
static site consumes. Everything else is service_role only.

## Keepalive

`pg_cron` job `signal_keepalive_daily` runs at `17 3 * * *` (03:17 UTC) and
pings `pipeline_state`. Prevents Supabase free-tier auto-pause. No GitHub
Actions fallback needed.
