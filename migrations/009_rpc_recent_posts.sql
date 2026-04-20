-- Recent top-N raw posts by source for article generation context.

create or replace function public.get_recent_posts_by_source(
  days integer default 3,
  per_source integer default 15
)
returns table (
  source text,
  source_id text,
  source_url text,
  author text,
  content text,
  "timestamp" timestamptz,
  parent_id text,
  metadata jsonb,
  fetched_at timestamptz,
  score numeric
)
language sql
security definer
set search_path = public
as $$
  with scored as (
    select
      p.source,
      p.source_id,
      p.source_url,
      p.author,
      p.content,
      p."timestamp",
      p.parent_id,
      p.metadata,
      p.fetched_at,
      coalesce(
        case when p.metadata->>'points' ~ '^-?[0-9]+(\.[0-9]+)?$' then (p.metadata->>'points')::numeric end,
        case when p.metadata->>'score' ~ '^-?[0-9]+(\.[0-9]+)?$' then (p.metadata->>'score')::numeric end,
        case when p.metadata->>'upvotes' ~ '^-?[0-9]+(\.[0-9]+)?$' then (p.metadata->>'upvotes')::numeric end,
        case when p.metadata->>'likes' ~ '^-?[0-9]+(\.[0-9]+)?$' then (p.metadata->>'likes')::numeric end,
        case when p.metadata->>'num_comments' ~ '^-?[0-9]+(\.[0-9]+)?$' then (p.metadata->>'num_comments')::numeric end,
        0
      ) as score
    from public.posts p
    where p."timestamp" >= now() - make_interval(days => greatest(days, 0))
  ),
  ranked as (
    select
      p.*,
      row_number() over (
        partition by p.source
        order by p.score desc, p."timestamp" desc
      ) as rn
    from scored p
  )
  select
    ranked.source,
    ranked.source_id,
    ranked.source_url,
    ranked.author,
    ranked.content,
    ranked."timestamp",
    ranked.parent_id,
    ranked.metadata,
    ranked.fetched_at,
    ranked.score
  from ranked
  where ranked.rn <= greatest(per_source, 1)
  order by ranked.source, ranked.score desc, ranked."timestamp" desc;
$$;

revoke all on function public.get_recent_posts_by_source(integer, integer) from anon;
revoke all on function public.get_recent_posts_by_source(integer, integer) from authenticated;
grant execute on function public.get_recent_posts_by_source(integer, integer) to service_role;
