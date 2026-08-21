-- Squad-Manager (Sorion IDEA-003): Tabellen + pg_cron-Poller
-- Alle Tabellen: RLS aktiv OHNE Policies => nur Service-Role (Edge Function squad-poll).
-- UI-Zugriff kommt spaeter ueber eine eigene Function/RPC, NICHT per Direkt-Read.

-- 1) Sorare-Tokens (1 Zeile). Kein Supabase-Secret, weil Sorare Refresh-Tokens
--    bei Benutzung rotiert — die Function schreibt hier die frischen Werte zurueck.
create table if not exists public.squad_tokens (
  id int primary key default 1 check (id = 1),
  access_token text not null,
  refresh_token text not null,
  updated_at timestamptz not null default now()
);
alter table public.squad_tokens enable row level security;

-- 2) Roh-Snapshots des offenen Steps, je Poll eine Zeile. TTL 24 h
--    (squad-poll loescht am Ende jedes Laufs alles Aeltere).
create table if not exists public.squad_snapshots (
  id bigint generated always as identity primary key,
  taken_at timestamptz not null default now(),
  step_id text not null,
  step_state text not null,
  target int,
  lineups jsonb not null
);
create index if not exists squad_snapshots_taken_at_idx on public.squad_snapshots (taken_at);
alter table public.squad_snapshots enable row level security;

-- 3) Dauerhaftes Aufstellungs-Log: je Step x Manager x Spieler.
--    first_seen_at = wann der Poller die Kombination erstmals sah (Reihenfolge fuer
--    den Player-Cap: "wer war der 5.?"). removed_at = Spieler wieder rausgenommen.
--    Aufloesung = Poll-Intervall (15 min). Achtung: fuer Steps, die schon VOR dem
--    ersten Poller-Lauf fertig waren, ist first_seen_at bedeutungslos (= Backfill).
create table if not exists public.squad_lineup_log (
  step_id text not null,
  manager_slug text not null,
  player_slug text not null,
  player_name text,
  card_slug text,
  skin text,
  captain boolean not null default false,
  score numeric,
  locked boolean not null default false,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  removed_at timestamptz,
  primary key (step_id, manager_slug, player_slug)
);
create index if not exists squad_lineup_log_player_idx on public.squad_lineup_log (step_id, player_slug);
alter table public.squad_lineup_log enable row level security;

-- 4) Punkte + Ranking je Step x Manager (Basis fuer das Durchschnitts-Leaderboard).
create table if not exists public.squad_step_scores (
  step_id text not null,
  manager_slug text not null,
  manager_nickname text,
  ranking int,
  score numeric,
  aasm_state text,
  step_state text,
  target int,
  updated_at timestamptz not null default now(),
  primary key (step_id, manager_slug)
);
alter table public.squad_step_scores enable row level security;

-- 5) Poller alle 15 Minuten via pg_cron -> pg_net -> Edge Function squad-poll.
--    Der Platzhalter __SQUAD_CRON_SECRET__ wird beim Einspielen durch den echten
--    Wert ersetzt (Klartext NICHT einchecken; liegt bei Jonas / in Supabase-Secrets).
create extension if not exists pg_cron;
create extension if not exists pg_net;

select cron.schedule(
  'squad-poll-15min',
  '*/15 * * * *',
  $$
  select net.http_post(
    url := 'https://jxhdlcpdupmkpsoytzes.supabase.co/functions/v1/squad-poll',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'X-Cron-Secret', '__SQUAD_CRON_SECRET__'
    ),
    body := '{"action":"poll"}'::jsonb
  );
  $$
);
