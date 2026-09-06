-- ═══════════════════════════════════════════════════════════════════════════
-- Was kostet eine Aufstellung, die Geld bzw. Essence gewonnen hat?
-- (06.09.2026, Wunsch Jonas)
--
-- GRUNDLAGE: Sorare gibt die Aufstellungen der Platzierten heraus
-- (so5RankingsPaginated -> so5Lineup -> so5Appearances mit Spieler und Karte).
-- Bepreist werden sie mit UNSEREM FMV zu heutigen Kursen — die nuetzliche
-- Frage ist "was muesste ich heute zahlen, um so ein Team zu stellen",
-- nicht der historische Anschaffungspreis.
--
-- NUR DIESE SAISON (Ansage Jonas): Spielmechaniken aendern sich je Saison,
-- aeltere Aufstellungen taugen nicht als Kostenmassstab. Der Sync
-- (tools/sync-lineup-costs.mjs) filtert entsprechend ab GW1 (31.07.2026).
--
-- Gewertet werden nur vollstaendig bepreiste Aufstellungen (5 von 5 Karten),
-- sonst waere die Summe systematisch zu niedrig.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.lineup_costs (
  fixture_slug     text        not null,
  leaderboard_slug text        not null,
  ranking          int         not null,
  score            numeric,
  cost_eur         numeric,                 -- Summe der FMV der 5 Karten
  cards_total      int,
  cards_priced     int,                     -- davon mit bekanntem FMV
  reward_kind      text,                    -- cash | essence | none
  priced_at        timestamptz not null default now(),
  primary key (fixture_slug, leaderboard_slug, ranking)
);
create index if not exists idx_lineup_costs_lb
  on public.lineup_costs (leaderboard_slug, reward_kind);

-- Nicht oeffentlich lesbar: die Auswertung laeuft ueber die RPC unten,
-- damit dieselbe Gate-Logik wie bei den Punkte-Schwellen greift.
alter table public.lineup_costs enable row level security;
revoke all on public.lineup_costs from anon, authenticated;

-- ── RPC erweitern: Team-Kosten je Leaderboard-Woche ───────────────────────
-- Median statt Mittelwert: einzelne Sammler-Aufstellungen mit sehr teuren
-- Karten wuerden den Schnitt sonst nach oben ziehen.
-- Die CASH-Kosten haengen am selben Feature-Schalter wie die Cash-Schwelle
-- ('leaderboard_cash'); die Essence-Kosten bleiben frei. Umstellen ohne
-- Deploy: update feature_access set min_tier = 'free' where feature_key = ...
-- Rueckgabetyp aendert sich (zwei neue Spalten) -> create or replace reicht NICHT
-- (42P13). Erst droppen, dann neu anlegen; der SQL-Editor laeuft als EINE
-- Transaktion, die Funktion ist also nie wirklich weg. Gleiche Falle wie bei
-- market_facets am 25.08.
drop function if exists public.leaderboard_thresholds();
create function public.leaderboard_thresholds()
returns table (
  fixture_slug text, fixture_name text, start_date date,
  competition text, rarity text, lineups int, top_score numeric,
  cash_rank int, cash_score numeric,
  essence_rank int, essence_score numeric,
  cash_cost numeric, essence_cost numeric,
  cash_locked boolean
)
language sql stable security definer set search_path = public as $fn$
  with g as (select public.has_feature('leaderboard_cash') as ok),
  lc as (
    select l.fixture_slug, l.leaderboard_slug,
           percentile_cont(0.5) within group (order by l.cost_eur)
             filter (where l.reward_kind = 'cash')    as cash_cost,
           percentile_cont(0.5) within group (order by l.cost_eur)
             filter (where l.reward_kind = 'essence') as essence_cost
    from public.lineup_costs l
    where l.cost_eur is not null
      and l.cards_priced = l.cards_total       -- nur vollstaendig bepreiste
      and l.cards_total >= 5
    group by 1, 2
  )
  select r.fixture_slug, r.fixture_name, r.start_date,
         r.competition, r.rarity, r.lineups, r.top_score,
         r.cash_rank,
         case when g.ok then r.cash_score end,
         r.essence_rank, r.essence_score,
         case when g.ok then round(lc.cash_cost::numeric, 2) end,
         round(lc.essence_cost::numeric, 2),
         not g.ok
  from public.reward_thresholds r
  cross join g
  left join lc on lc.fixture_slug = r.fixture_slug
              and lc.leaderboard_slug = r.leaderboard_slug
  order by r.start_date
$fn$;
revoke execute on function public.leaderboard_thresholds() from public;
grant execute on function public.leaderboard_thresholds() to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Direkt nach dem Einspielen sind die Kosten leer; sie kommen mit dem Sync:
--   railway run -s "Updater Limited" node tools/sync-lineup-costs.mjs
select competition, rarity, cash_cost, essence_cost, cash_locked
from public.leaderboard_thresholds()
where cash_cost is not null or essence_cost is not null
limit 10;
