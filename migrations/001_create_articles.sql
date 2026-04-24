-- Generated First Light AI article store.
-- articles are LLM-generated public news items, not raw crawler/Discord posts.

create table if not exists public.articles (
  id text primary key,
  source text not null default 'first_light_ai',
  title text not null,
  url text,
  score integer not null default 0,
  comments integer not null default 0,
  summary text,
  body text,
  tags text[] not null default '{}',
  raw_json jsonb not null default '{}'::jsonb,
  crawled_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  placement text,
  placed_at timestamptz,
  category text,
  trust text,
  generated_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists idx_articles_source_created_at
  on public.articles (source, created_at desc);

create index if not exists idx_articles_placement_created_at
  on public.articles (placement, created_at desc);

create index if not exists idx_articles_raw_json_gin
  on public.articles using gin (raw_json);
