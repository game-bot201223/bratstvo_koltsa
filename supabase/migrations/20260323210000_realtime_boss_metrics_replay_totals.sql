do $$
begin
  if exists (
    select 1
    from information_schema.table_constraints
    where table_schema='public'
      and table_name='realtime_boss_metrics'
      and constraint_name='realtime_boss_metrics_metric_kind_check'
  ) then
    alter table public.realtime_boss_metrics
      drop constraint realtime_boss_metrics_metric_kind_check;
  end if;
end $$;

alter table public.realtime_boss_metrics
  add constraint realtime_boss_metrics_metric_kind_check
  check (
    metric_kind in (
      'boss_update_apply_live',
      'boss_update_apply_replay',
      'duplicate_event_dropped',
      'seq_gap_detected',
      'replay_empty',
      'replay_catchup_depth',
      'replay_requested_total',
      'replay_served_events_total'
    )
  );
