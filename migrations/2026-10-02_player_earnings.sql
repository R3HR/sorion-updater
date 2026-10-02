-- Top-Verdiener je Liga: welche Spieler erwirtschaften Cash bzw. Essence (02.10.2026, Wunsch Jonas)
--
-- ENTSCHEIDUNGEN (Jonas 02.10.):
--   1. Zurechnung nach PUNKTEANTEIL: Gewinnt eine Aufstellung 50 USD und ein Spieler holte
--      80 von 400 Punkten, bekommt er 10 USD zugerechnet.
--   2. Erst nur CASH (Vollerhebung existiert seit 09.09.). Essence folgt spaeter, dafuer
--      braucht es eine Vollerhebung aller Essence-Raenge (~11.500 Seiten fuer die Saison).
--   3. Eine Tabelle auf rewards.html mit Schalter Cash/Essence und Liga-Dropdown.
--   4. Cash ist konsequent Pro, gleiches Gate wie die Cash-Spalten (leaderboard_cash,
--      Kostprobe ueber feature_access.preview_value).
--
-- GRANULAR je Spieltag x Leaderboard x Spieler (nicht je Aufstellung): verdichtet beim
-- Einlesen, idempotent ersetzbar je Leaderboard-Woche, und spaeter fuer "letzte N Spieltage"
-- nutzbar. Rohaufstellungen werden NICHT gespeichert (DB 654 MB).

create table if not exists public.player_earnings (
  fixture_slug     text        not null,
  leaderboard_slug text        not null,
  competition      text        not null,
  rarity           text        not null,
  reward_kind      text        not null check (reward_kind in ('cash', 'essence')),
  player_slug      text        not null,
  player_name      text,
  lineups          int         not null,   -- Gewinner-Aufstellungen mit diesem Spieler
  points           numeric     not null,   -- seine Punkte in diesen Aufstellungen
  earned           numeric     not null,   -- zugerechneter Gewinn (USD bzw. Essence)
  synced_at        timestamptz not null default now(),
  primary key (fixture_slug, leaderboard_slug, reward_kind, player_slug)
);
create index if not exists player_earnings_lookup_idx
  on public.player_earnings (competition, rarity, reward_kind);

alter table public.player_earnings enable row level security;
revoke all on public.player_earnings from anon, authenticated;

-- Fuer die Bestandspruefung des Syncs: welche Leaderboard-Wochen sind schon erfasst
-- (sonst muesste der Sync zehntausende Zeilen seitenweise lesen).
create or replace view public.player_earnings_weeks as
  select distinct fixture_slug, leaderboard_slug, reward_kind from public.player_earnings;
revoke all on public.player_earnings_weeks from anon, authenticated;

-- RPC: Top N einer Liga. Gesperrt = Zeilen kommen, aber ohne Namen und Zahlen (der Server
-- verraet nichts, die Seite blurrt nur).
create or replace function public.top_earners(
  p_competition text, p_rarity text, p_kind text default 'cash', p_limit int default 25)
returns table (pos int, player_slug text, player_name text, club text,
               lineups int, gameweeks int, points numeric, earned numeric, locked boolean)
language sql stable security definer set search_path = public as $fn$
  with g as (
    select public.has_feature('leaderboard_cash') as ok,
           (select f.preview_value from public.feature_access f
             where f.feature_key = 'leaderboard_cash') as preview
  ),
  gate as (
    select (p_kind <> 'cash' or g.ok or p_competition is not distinct from g.preview) as o from g
  ),
  agg as (
    select e.player_slug, max(e.player_name) as player_name,
           sum(e.lineups)::int as lineups, count(distinct e.fixture_slug)::int as gameweeks,
           sum(e.points) as points, sum(e.earned) as earned
    from public.player_earnings e
    where e.competition = p_competition and e.rarity = p_rarity and e.reward_kind = p_kind
    group by e.player_slug
    order by sum(e.earned) desc
    limit least(greatest(p_limit, 1), 50)
  ),
  ranked as (select row_number() over (order by earned desc)::int as rnk, a.* from agg a)
  select r.rnk,
         case when gate.o then r.player_slug end,
         case when gate.o then r.player_name end,
         case when gate.o then (select c.team_name from public.card_prices c
                                 where c.player_slug = r.player_slug and c.team_name is not null
                                 limit 1) end,
         case when gate.o then r.lineups end,
         case when gate.o then r.gameweeks end,
         case when gate.o then round(r.points, 1) end,
         case when gate.o then round(r.earned, 2) end,
         not gate.o
  from ranked r cross join gate
  order by r.rnk
$fn$;
revoke execute on function public.top_earners(text, text, text, int) from public;
grant execute on function public.top_earners(text, text, text, int) to anon, authenticated;
