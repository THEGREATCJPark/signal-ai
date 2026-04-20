-- Keep generated public tables permissive for the current site/tooling, but
-- block anon/authenticated access to raw posts and private pipeline state.

alter table public.articles enable row level security;
alter table public.publish_log enable row level security;
alter table public.posts enable row level security;

drop policy if exists "articles all access" on public.articles;
create policy "articles all access"
  on public.articles
  for all
  using (true)
  with check (true);

drop policy if exists "publish_log all access" on public.publish_log;
create policy "publish_log all access"
  on public.publish_log
  for all
  using (true)
  with check (true);

drop policy if exists "posts service role only" on public.posts;
revoke all on public.posts from anon;
revoke all on public.posts from authenticated;
grant all on public.posts to service_role;
