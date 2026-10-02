-- Top-Verdiener: Verein AUS DEM SPIEL statt heutiger Verein (02.10.2026, BUG-049)
--
-- BEFUND (Jonas: "Ligen durcheinander geworfen"): Die Verdienste waren richtig zugeordnet,
-- aber die Vereinsspalte kam aus card_prices, also dem HEUTIGEN Verein, und dort per
-- `limit 1` aus irgendeiner Kartenzeile. 32 von 805 Spielern der Top-Listen hatten dort
-- widerspruechliche Vereine. Ayase Ueda punktete fuer Feyenoord in der Eredivisie, stand
-- aber mit LOSC Lille in der Eredivisie-Liste: Er ist nach seinen Punkten gewechselt.
--
-- FIX: Der Sync speichert je Woche den Verein, fuer den der Spieler in der Aufstellung
-- angetreten ist (`So5Appearance.anyTeam`). Die Liste zeigt den Verein, mit dem er in
-- diesem Wettbewerb am haeufigsten gepunktet hat. card_prices nur noch als Rueckfall,
-- und dann die zuletzt aktualisierte Zeile statt irgendeiner.

alter table public.player_earnings add column if not exists club text;

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
           mode() within group (order by e.club) as club,
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
         case when gate.o then coalesce(r.club,
           (select c.team_name from public.card_prices c
             where c.player_slug = r.player_slug and c.team_name is not null
             order by c.updated_at desc nulls last limit 1)) end,
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
