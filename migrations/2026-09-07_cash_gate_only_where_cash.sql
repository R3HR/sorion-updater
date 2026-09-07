-- Kein Schloss, wo es nichts zu holen gibt (Ansage Jonas 07.09.2026)
--
-- BEFUND: In Champion, Under 23 und All Star zahlt Sorare Geld NUR in Unique.
-- In Limited, Rare und Super Rare gibt es dort ueberhaupt keine Cash-Preise
-- (geprueft ueber alle Spieltage dieser Saison: 0 von 3 bzw. 0 von 10 GW).
-- Bisher stand in diesen Zeilen trotzdem ein Schloss. Das war doppelt falsch:
-- Es versprach Zahlen, die es nicht gibt, und es liess das Pro-Feature wertloser
-- aussehen, als es ist.
--
-- REGEL statt Namensliste: Eine Zeile ist gesperrt, wenn sie Cash-Daten HAT und
-- das Konto sie nicht sehen darf. Hat sie keine, ist sie offen und zeigt "no cash".
-- Damit bleibt es richtig, wenn Sorare die Preisstruktur aendert oder ein neuer
-- Wettbewerb dazukommt. Die Kostprobe (preview_value) bleibt unveraendert daneben.

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
             filter (where l.reward_kind = 'essence') as essence_cost
    from public.lineup_costs l
    where l.cost_eur is not null
      and l.cards_priced = l.cards_total
      and l.cards_total >= 5
    group by 1, 2
  ),
  j as (
    select r.*, lc.cash_cost as lc_cash, lc.essence_cost as lc_ess, g.ok, g.preview
    from public.reward_thresholds r
    cross join g
    left join lc on lc.fixture_slug = r.fixture_slug
                and lc.leaderboard_slug = r.leaderboard_slug
  ),
  o as (
    select j.*,
           ( j.ok                                                -- Pro-Konto
             or j.competition is not distinct from j.preview     -- Kostprobe
             or (j.cash_score is null and j.lc_cash is null)     -- nichts zu verbergen
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
         not o.open
  from o
  order by o.start_date
$fn$;
revoke execute on function public.leaderboard_thresholds() from public;
grant execute on function public.leaderboard_thresholds() to anon, authenticated;

-- ── Verifikation ──────────────────────────────────────────────────────────
-- Erwartung anonym: Champion/Under 23/All Star sind in unique GESPERRT und in
-- limited/rare/super_rare OFFEN (ohne Cash-Zahlen); English League Players ist
-- ueberall offen MIT Zahlen; alle uebrigen sind gesperrt.
select competition, rarity,
       count(*)                as zeilen,
       count(cash_score)       as cash_sichtbar,
       bool_and(cash_locked)   as gesperrt
from public.leaderboard_thresholds()
where competition in ('Champion','Under 23','All Star','English League Players','LALIGA EA SPORTS')
group by competition, rarity
order by competition, rarity;
