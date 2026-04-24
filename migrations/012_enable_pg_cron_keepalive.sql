-- Enable pg_cron and register a daily keepalive ping so Supabase does not
-- auto-pause the project after 7 days of inactivity on the free tier.
-- The job writes a timestamp to pipeline_state; both tables stay touched.

create extension if not exists pg_cron;

do $$
begin
  if exists (select 1 from cron.job where jobname = 'signal_keepalive_daily') then
    perform cron.unschedule('signal_keepalive_daily');
  end if;
end $$;

select cron.schedule(
  'signal_keepalive_daily',
  '17 3 * * *',
  $$ insert into public.pipeline_state (key, value)
       values ('keepalive_last_ping', jsonb_build_object('at', now()))
     on conflict (key) do update
       set value = excluded.value,
           updated_at = now(); $$
);
