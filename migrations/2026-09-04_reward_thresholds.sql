-- ═══════════════════════════════════════════════════════════════════════════
-- Reward-Schwellen je Leaderboard und Spieltag  (04.09.2026, Wunsch Jonas)
--
-- FRAGE: "Wie viele Punkte braucht man im Schnitt fuer Geld / Essence auf
-- einem Leaderboard?" — Sorare liefert fuer abgeschlossene Spieltage je
-- Leaderboard die Preisstufen INKLUSIVE der Rankings an den Stufengrenzen
-- (rewardsConfig.ranking[].toSo5Ranking.score). Der Sync-Job
-- tools/sync-reward-thresholds.mjs zieht das je Spieltag (nur diese Saison,
-- nur Wettbewerbs-Leaderboards, keine Arena/PvP/Cap-Raeume) und schreibt je
-- Leaderboard+Spieltag EINE Zeile mit den abgeleiteten Schwellen; die
-- Rohstufen bleiben als JSON dabei.
--
-- Oeffentlich lesbar: aggregierte Spieldaten ohne Personenbezug — Grundlage
-- der Seite rewards.html. Schreiben nur per Service-Key (Sync-Job).
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.reward_thresholds (
  fixture_slug      text        not null,
  leaderboard_slug  text        not null,
  game_week         int,                 -- Sorare gameWeek (fortlaufend)
  season_game_week  int,                 -- Spieltag innerhalb der Saison
  fixture_name      text,                -- "Game Week 10"
  start_date        date,
  competition       text        not null,-- "EFL Championship", "Bundesliga", "Champion" ...
  rarity            text        not null,-- limited | rare | super_rare | unique
  lineups           int,
  rewarded_lineups  int,
  top_score         numeric,
  cash_rank         int,                 -- letzter Rang mit Cash
  cash_score        numeric,             -- Score dieses Rangs = Cash-Schwelle
  essence_rank      int,
  essence_score     numeric,
  card_rank         int,
  card_score        numeric,
  prize_pool_usd    numeric,             -- Summe usdAmount der Stufen (Naeherung)
  tiers             jsonb,               -- Rohstufen [{from,to,scoreFrom,scoreTo,usd,essence,cards}]
  synced_at         timestamptz not null default now(),
  primary key (fixture_slug, leaderboard_slug)
);
create index if not exists idx_reward_thresholds_comp on public.reward_thresholds (competition, rarity, start_date);

alter table public.reward_thresholds enable row level security;
drop policy if exists reward_thresholds_read on public.reward_thresholds;
create policy reward_thresholds_read on public.reward_thresholds
  for select to anon, authenticated using (true);
grant select on public.reward_thresholds to anon, authenticated;

-- ── Verifikation (nach dem ersten Sync-Lauf) ──────────────────────────────
-- select competition, rarity, count(*) as spieltage,
--        round(avg(cash_score)) as cash_avg, round(avg(essence_score)) as essence_avg
-- from public.reward_thresholds group by 1,2 order by 1,2;
