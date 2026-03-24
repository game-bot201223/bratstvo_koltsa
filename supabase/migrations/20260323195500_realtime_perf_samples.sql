create table if not exists public.realtime_perf_samples (
  id bigserial primary key,
  metric_kind text not null check (metric_kind in ('save_ack', 'boss_update')),
  value_ms integer not null check (value_ms >= 0),
  created_at timestamptz not null default now()
);

create index if not exists realtime_perf_samples_kind_created_at_idx
  on public.realtime_perf_samples (metric_kind, created_at);
