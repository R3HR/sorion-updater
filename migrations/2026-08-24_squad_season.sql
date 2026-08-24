-- Saison-Stand des Handpicked Leaderboards (Spezifikation: docs/SQUAD_LEADERBOARD.md).
-- Geseedet aus Jonas' History (Runden 1-21, verifiziert: 0 Abweichungen).
create table if not exists public.squad_season_state (
  manager_slug text primary key,
  display_name text,
  points int not null default 0,
  score_sum numeric not null default 0,
  rounds_played int not null default 0,
  joined_at_round int,
  unexcused_missed int not null default 0,
  seeded_through_round int,
  updated_at timestamptz not null default now()
);
alter table public.squad_season_state enable row level security;

-- Rundenergebnisse ab dem API-Tracking, damit der Stand nachvollziehbar fortgeschrieben wird.
create table if not exists public.squad_rounds (
  round_no int,
  step_id text,
  manager_slug text,
  score numeric,
  placement int,
  placement_points int,
  stage_bonus int,
  penalty int not null default 0,
  penalty_reason text,
  round_points int,
  played_on date,
  primary key (step_id, manager_slug)
);
alter table public.squad_rounds enable row level security;
