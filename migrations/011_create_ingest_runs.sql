-- Per-run observability for the ingest pipeline.
-- Crawlers insert one row per run; pipeline dashboards/keepalive can read counts+latest.

create table if not exists public.ingest_runs (
  id bigserial primary key,
  source text not null,
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  status text not null default 'running' check (status in ('running','success','error')),
  rows_read integer not null default 0,
  rows_inserted integer not null default 0,
  rows_skipped integer not null default 0,
  error_message text,
  metadata jsonb not null default '{}'::jsonb
);

create index if not exists idx_ingest_runs_source_started
  on public.ingest_runs (source, started_at desc);

create index if not exists idx_ingest_runs_status_started
  on public.ingest_runs (status, started_at desc);

alter table public.ingest_runs enable row level security;

drop policy if exists "ingest_runs service role only" on public.ingest_runs;
revoke all on public.ingest_runs from anon;
revoke all on public.ingest_runs from authenticated;
grant all on public.ingest_runs to service_role;
