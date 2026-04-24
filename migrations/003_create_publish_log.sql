-- Public artifact/publisher idempotency log.

create table if not exists public.publish_log (
  id bigserial primary key,
  article_id text not null references public.articles(id) on delete cascade,
  platform text not null,
  published_at timestamptz not null default now(),
  message_id text,
  metadata jsonb not null default '{}'::jsonb,
  unique (article_id, platform)
);

create index if not exists idx_publish_log_platform_published_at
  on public.publish_log (platform, published_at desc);
