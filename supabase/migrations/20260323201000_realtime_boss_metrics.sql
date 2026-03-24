create table if not exists public.realtime_boss_metrics (
  id bigserial primary key,
  metric_kind text not null check (
    metric_kind in (
      'boss_update_apply_live',
      'boss_update_apply_replay',
      'duplicate_event_dropped',
      'seq_gap_detected'
    )
  ),
  metric_value integer not null check (metric_value >= 0),
  created_at timestamptz not null default now()
);

create index if not exists realtime_boss_metrics_kind_created_at_idx
  on public.realtime_boss_metrics (metric_kind, created_at);
