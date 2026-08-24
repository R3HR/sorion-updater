-- Captain-Ausnahmen ("besondere Anweisungen an den Bot").
-- Beispiel 23.08.: Es gibt nicht genug Torhueter, um den Player-Cap auf der
-- GK-Position einzuhalten -> Position fuer diesen Step vom Cap ausgenommen.
--
-- kind:
--   position_exempt  value = Position ('Goalkeeper','Defender','Midfielder','Forward')
--   player_exempt    value = player_slug
--   cap              value = abweichende Cap-Zahl fuer diesen Step
--   excuse           value = manager_slug (keine -5 fuers fehlende Lineup)
-- step_id NULL = gilt fuer alle Steps (Dauerregel), sonst nur fuer diesen Step.
create table if not exists public.squad_overrides (
  id bigint generated always as identity primary key,
  step_id text,
  kind text not null check (kind in ('position_exempt','player_exempt','cap','excuse')),
  value text not null,
  note text,
  created_at timestamptz not null default now()
);
create index if not exists squad_overrides_step_idx on public.squad_overrides (step_id, kind);
alter table public.squad_overrides enable row level security;
