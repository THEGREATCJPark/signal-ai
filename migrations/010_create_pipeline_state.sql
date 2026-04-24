-- Private JSON state for local automation.
-- This stores the complete articles.json-compatible state so no public history,
-- daily summary, decision log, or last_run_at value is lost.

create table if not exists public.pipeline_state (
  key text primary key,
  value jsonb not null,
  updated_at timestamptz not null default now()
);

alter table public.pipeline_state enable row level security;

drop policy if exists "pipeline_state service role only" on public.pipeline_state;
revoke all on public.pipeline_state from anon;
revoke all on public.pipeline_state from authenticated;
grant all on public.pipeline_state to service_role;
