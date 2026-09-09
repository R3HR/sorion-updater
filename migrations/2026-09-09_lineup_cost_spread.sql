-- Team-Kosten: billigstes Gewinner-Team und guenstigstes Fuenftel (09.09.2026, Wunsch Jonas)
--
-- WARUM: Seit der Vollerhebung aller Cash-Gewinner liegen je Leaderboard ALLE bezahlten
-- Aufstellungen vor. Erst dadurch sind diese beiden Zahlen ueberhaupt ehrlich:
--   cash_cost_min = die billigste Aufstellung, die tatsaechlich Geld bekam
--   cash_cost_p20 = das guenstigste Fuenftel, also: so billig war jede fuenfte Gewinner-Elf
-- Der Median allein verschweigt die Spanne. Beispiel MLS Limited: Median 79 EUR, aber jede
-- zehnte Gewinner-Aufstellung kam mit 17 EUR aus.
--
-- BEWUSST NUR FUER CASH. Bei Essence holen wir weiterhin nur Spitzenfeld und Grenzseite
-- (Vollerhebung waere 7.209 Seiten statt 316). Ein "billigstes Team" aus einer Stichprobe,
-- die die Mitte auslaesst, waere keine Aussage ueber das Feld, sondern ueber die Stichprobe.
-- Deshalb behaelt Essence nur den Median.

drop function if exists public.leaderboard_thresholds();
create function public.leaderboard_thresholds()
returns table (
  fixture_slug text, fixture_name text, start_date date,
  competition text, rarity text, lineups int, top_score numeric,
  cash_rank int, cash_score numeric,
  essence_rank int, essence_score numeric,
  cash_cost numeric, essence_cost numeric,
  cash_cost_min numeric, cash_cost_p20 numeric,
  cash_locked boolean
)
language sql stable security definer set search_path = public as $fn$
  with g as (
    select public.has_feature('leaderboard_cash') as ok,
           (select f.preview_value from public.feature_access f
             where f.feature_key = 'leaderboard_cash') as preview
  ),
  lc as (
    select l.fixture_slug, l.leaderboard_slug,
           percentile_cont(0.5) within group (order by l.cost_eur)
             filter (where l.reward_kind = 'cash')    as cash_cost,
           percentile_cont(0.5) within group (order by l.cost_eur)
             filter (where l.reward_kind = 'essence') as essence_cost,
           percentile_cont(0.2) within group (order by l.cost_eur)
             filter (where l.reward_kind = 'cash')    as cash_p20,
           min(l.cost_eur) filter (where l.reward_kind = 'cash') as cash_min
    from public.lineup_costs l
    where l.cost_eur is not null
      and l.cards_priced = l.cards_total
      and l.cards_total >= 5
    group by 1, 2
  ),
  j as (
    select r.*, lc.cash_cost as lc_cash, lc.essence_cost as lc_ess,
           lc.cash_p20 as lc_p20, lc.cash_min as lc_min, g.ok, g.preview
    from public.reward_thresholds r
    cross join g
    left join lc on lc.fixture_slug = r.fixture_slug
                and lc.leaderboard_slug = r.leaderboard_slug
  ),
  o as (
    select j.*,
           ( j.ok
             or j.competition is not distinct from j.preview
             or (j.cash_score is null and j.lc_cash is null)
           ) as open
    from j
  )
  select o.fixture_slug, o.fixture_name, o.start_date,
         o.competition, o.rarity, o.lineups, o.top_score,
         o.cash_rank,
         case when o.open then o.cash_score end,
         o.essence_rank, o.essence_score,
         case when o.open then round(o.lc_cash::numeric, 2) end,
         round(o.lc_ess::numeric, 2),
         case when o.open then round(o.lc_min::numeric, 2) end,
         case when o.open then round(o.lc_p20::numeric, 2) end,
         not o.open
  from o
  order by o.start_date
$fn$;
revoke execute on function public.leaderboard_thresholds() from public;
grant execute on function public.leaderboard_thresholds() to anon, authenticated;

-- Verifikation: min <= p20 <= median, und alle drei haengen am selben Schalter.
select competition, rarity,
       round(avg(cash_cost_min)) as billigstes,
       round(avg(cash_cost_p20)) as guenstigstes_fuenftel,
       round(avg(cash_cost))     as median
from public.leaderboard_thresholds()
where cash_cost is not null
group by competition, rarity
order by median
limit 8;
