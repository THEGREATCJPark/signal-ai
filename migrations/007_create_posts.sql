-- Raw source post store.
-- This mirrors the old SQLite posts table and keeps crawler/Discord input
-- separate from generated public articles.

create table if not exists public.posts (
  id bigserial primary key,
  source text not null,
  source_id text not null,
  source_url text,
  author text,
  content text not null,
  "timestamp" timestamptz not null,
  parent_id text,
  metadata jsonb not null default '{}'::jsonb,
  fetched_at timestamptz not null default now(),
  unique (source, source_id)
);

create index if not exists idx_posts_source_timestamp
  on public.posts (source, "timestamp" desc);

create index if not exists idx_posts_fetched_at
  on public.posts (fetched_at desc);

create index if not exists idx_posts_parent_id
  on public.posts (parent_id);
