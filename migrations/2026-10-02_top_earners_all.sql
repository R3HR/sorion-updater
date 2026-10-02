-- Top-Verdiener: ALLE Gewinner statt nur Top 25 (02.10.2026, Wunsch Jonas)
--
-- Die Seite zeigt weiter zuerst 25, laedt auf Knopfdruck aber die ganze Liste. Dafuer:
--   * p_limit bis 2000 (Cash-Listen haben im Mittel 184, hoechstens 1.160 Spieler;
--     Essence-Listen werden deutlich laenger, dort zeigt die Seite "top 2000 of N").
--   * neue Spalte `total`: wie viele Spieler die Liste insgesamt hat.
--   * GESPERRT bleibt es bei 25 Platzhaltern, egal welches Limit kommt. Sonst
--     schickte der Server bis zu 2000 leere Zeilen.
-- Rueckgabetyp aendert sich (neue Spalte), deshalb DROP vorher (Falle 42P13).

drop function if exists public.top_earners(text, text, text, int);
create function public.top_earners(
  p_competition text, p_rarity text, p_kind text default 'cash', p_limit int default 25)
returns table (pos int, player_slug text, player_name text, club text,
               lineups int, gameweeks int, points numeric, earned numeric,
               total int, locked boolean)
language sql stable security definer set search_path = public as $fn$
  with g as (
    select public.has_feature('leaderboard_cash') as ok,
           (select f.preview_value from public.feature_access f
             where f.feature_key = 'leaderboard_cash') as preview
  ),
  gate as (
    select o, case when o then least(greatest(p_limit, 1), 2000) else 25 end as lim
    from (select (p_kind <> 'cash' or g.ok or p_competition is not distinct from g.preview) as o from g) x
  ),
  agg as (
    select e.player_slug, max(e.player_name) as player_name,
           mode() within group (order by e.club) as club,
           sum(e.lineups)::int as lineups, count(distinct e.fixture_slug)::int as gameweeks,
           sum(e.points) as points, sum(e.earned) as earned
    from public.player_earnings e
    where e.competition = p_competition and e.rarity = p_rarity and e.reward_kind = p_kind
    group by e.player_slug
  ),
  ranked as (
    select row_number() over (order by earned desc, player_slug)::int as rnk,
           count(*) over ()::int as total, a.*
    from agg a
  )
  select r.rnk,
         case when gate.o then r.player_slug end,
         case when gate.o then r.player_name end,
         case when gate.o then coalesce(r.club,
           (select c.team_name from public.card_prices c
             where c.player_slug = r.player_slug and c.team_name is not null
             order by c.updated_at desc nulls last limit 1)) end,
         case when gate.o then r.lineups end,
         case when gate.o then r.gameweeks end,
         case when gate.o then round(r.points, 1) end,
         case when gate.o then round(r.earned, 2) end,
         r.total,
         not gate.o
  from ranked r cross join gate
  where r.rnk <= gate.lim
  order by r.rnk
$fn$;
revoke execute on function public.top_earners(text, text, text, int) from public;
grant execute on function public.top_earners(text, text, text, int) to anon, authenticated;
