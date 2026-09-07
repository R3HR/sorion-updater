-- Kostprobe: EIN Wettbewerb zeigt die Cash-Spalten offen (Ansage Jonas 07.09.2026)
--
-- WARUM: Ein komplett verschlossenes Feature laesst sich nicht beurteilen. Wer den
-- teuersten Wettbewerb offen sieht, versteht, was hinter dem Schloss steckt, und
-- kann entscheiden, ob ihm das etwas wert ist. Bewusst der TEUERSTE: dort ist der
-- Abstand zwischen "Punkte reichen" und "Team bezahlbar" am groessten, also genau
-- die Information, die den Rest der Tabelle interessant macht.
--
-- WIE: Eine Konfigurationszeile, kein Code. Der freigegebene Wettbewerb laesst sich
-- ohne Deploy wechseln oder ganz abschalten (preview_value = null).

alter table public.feature_access
  add column if not exists preview_value text;

comment on column public.feature_access.preview_value is
  'Kostprobe: ein Wert (z. B. Wettbewerbsname), fuer den das Feature auch ohne Rang offen ist. null = keine Kostprobe.';

update public.feature_access
   set preview_value = 'English League Players', updated_at = now()
 where feature_key = 'leaderboard_cash';

-- ── RPC: Sperre jetzt ZEILENWEISE ─────────────────────────────────────────
-- cash_locked wandert vom Konto auf die Zeile. Die Seite blurrt danach je Zeile,
-- nicht mehr die ganze Spalte.
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
  s as (
    select r.*, (g.ok or r.competition is not distinct from g.preview) as open
    from public.reward_thresholds r cross join g
  )
  select s.fixture_slug, s.fixture_name, s.start_date,
         s.competition, s.rarity, s.lineups, s.top_score,
         s.cash_rank,
         case when s.open then s.cash_score end,
         s.essence_rank, s.essence_score,
         case when s.open then round(lc.cash_cost::numeric, 2) end,
         round(lc.essence_cost::numeric, 2),
         not s.open
  from s
  left join lc on lc.fixture_slug = s.fixture_slug
              and lc.leaderboard_slug = s.leaderboard_slug
  order by s.start_date
$fn$;
revoke execute on function public.leaderboard_thresholds() from public;
grant execute on function public.leaderboard_thresholds() to anon, authenticated;

-- ── Verifikation ──────────────────────────────────────────────────────────
-- Anonym aufgerufen muss GENAU der Kostproben-Wettbewerb Zahlen zeigen:
select competition,
       count(*)                                     as zeilen,
       count(cash_score)                            as cash_sichtbar,
       count(cash_cost)                             as kosten_sichtbar,
       bool_and(cash_locked)                        as gesperrt
from public.leaderboard_thresholds()
group by competition
order by cash_sichtbar desc, competition;
