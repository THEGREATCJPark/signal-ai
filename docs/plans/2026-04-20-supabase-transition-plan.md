# Supabase Transition Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Move crawler/source storage and generated article state to Supabase while preserving the current First Light AI article generation, design, archive, and daily public JSON behavior.

**Architecture:** Supabase becomes the canonical database for source `posts`, generated `articles`, and `publish_log`. The current static files remain public render artifacts for GitHub Pages, but they are generated from DB-backed state rather than treated as canonical storage. Local SQLite is only used once as a backfill source and then archived to `.bak`.

**Tech Stack:** Python 3, `supabase-py`, Postgres/Supabase SQL migrations, existing `run_hourly.py`, existing GitHub Pages static deploy.

---

### Task 1: Bring In Supabase Skeleton Without Reverting First Light UI

**Files:**
- Create: `requirements.txt`
- Create: `db/__init__.py`
- Create: `db/client.py`
- Create: `db/articles.py`
- Create: `db/publish_log.py`
- Create: `publisher/__init__.py`
- Create: `publisher/state.py`
- Create: `.env.example`
- Modify: `.gitignore`

**Steps:**
1. Add a failing test that imports DB helper modules and asserts service/anon key selection behavior.
2. Implement `get_client(service: bool = False)` with `SUPABASE_SERVICE_ROLE_KEY` for writes and `SUPABASE_ANON_KEY` for reads.
3. Keep current `docs/index.html`, root `index.html`, `run_hourly.py`, and public article JSON intact.
4. Run targeted tests.

### Task 2: Add Posts Table/RLS/RPC Migrations

**Files:**
- Create: `migrations/001_create_articles.sql`
- Create: `migrations/003_create_publish_log.sql`
- Create: `migrations/007_create_posts.sql`
- Create: `migrations/008_tighten_rls.sql`
- Create: `migrations/009_rpc_recent_posts.sql`

**Steps:**
1. Add tests that inspect migration SQL for required columns, unique constraints, indexes, RLS, grants, and RPC shape.
2. Create `posts` as source/raw storage compatible with existing SQLite `posts`.
3. Keep `articles` as generated-news storage with `raw_json` to preserve all existing article fields.
4. Create `get_recent_posts_by_source(days, per_source)` for `run_full.py` context queries.

### Task 3: Replace SQLite Ingest With Supabase Posts Upsert

**Files:**
- Rewrite: `db/ingest.py`
- Create: `db/posts.py`

**Steps:**
1. Add tests for JSONL row normalization and service-role failure when write key is missing.
2. Upsert to `posts` with `on_conflict="source,source_id"` in batches.
3. Store `metadata` as JSON object, not a string.
4. Remove SQLite connection/schema code from ingest.

### Task 4: Make `run_hourly.py` DB-Backed Without Changing Public Output

**Files:**
- Modify: `run_hourly.py`
- Test: `tests/test_run_hourly_exports.py`

**Steps:**
1. Add tests that `load_state()` reads generated article state from `db.articles`.
2. Add tests that `_classify_and_save()` writes generated article state to Supabase before public artifact export.
3. Keep `save_state()` writing static artifacts for Pages compatibility.
4. Ensure no age-based expiration returns.

### Task 5: Backfill Existing SQLite/Data JSON Without Loss

**Files:**
- Create: `scripts/backfill_sqlite_to_supabase.py`
- Create: `scripts/backfill_articles_to_supabase.py`

**Steps:**
1. Add tests around SQLite row conversion and article JSON conversion.
2. Backfill `data/signal.db.posts` to Supabase `posts`.
3. Backfill current `articles.json` 174 generated articles to Supabase `articles`.
4. Verify counts and sample hashes before moving `data/signal.db` to a timestamped `.bak`.

### Task 6: Remove SQLite/GitHub State Storage

**Files:**
- Delete: `db/schema.sql`
- Delete: `db/query.py`
- Remove if tracked: `data/published.json`
- Modify: `.github/workflows/daily_publish.yml`

**Steps:**
1. Add tests/grep checks that `sqlite3.connect` and `data/published.json` state usage are gone.
2. Remove workflow commit step for published state only.
3. Keep Pages public artifact deployment path until the frontend directly reads Supabase.

### Task 7: Verify End-to-End

**Commands:**
- `python3 -m unittest discover -s tests`
- `python3 -m py_compile run_hourly.py run_full.py db/client.py db/articles.py db/posts.py db/ingest.py db/publish_log.py publisher/state.py`
- `git diff --check`
- Local diff secret scan
- Supabase migration apply/backfill/count checks when MCP has project access or when `SUPABASE_URL`/service key are present.

**Expected:** Existing article generation/design remains functionally the same, current 174 articles remain recoverable, raw posts are preserved in Supabase, and GitHub Pages still updates from generated public artifacts.
