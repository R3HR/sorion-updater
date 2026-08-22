-- ═══════════════════════════════════════════════════════════════════════════
-- Markt-Aggregate als Materialized Views  (22.08.2026) — der richtige Fix
-- nach INC-005 (der Cache-Waermer vom 21.08. war der falsche).
--
-- PROBLEM: market_leagues/market_facets aggregieren LIVE ueber ~122k Zeilen
-- card_prices — warm 0,4 s, kalt 3-3,5 s, unter IO-Drosselung Minuten. Jeder
-- erste Besucher nach Cache-Verfall zahlte den Kaltstart (leere Abschnitte).
--
-- LOESUNG: Die Aggregate werden ZWEIMAL TAEGLICH in Materialized Views
-- geschrieben; die RPCs lesen nur noch daraus (wenige hundert Zeilen,
-- Millisekunden, immer — egal ob kalt oder warm). Signaturen und Rueckgabe-
-- formen der RPCs bleiben IDENTISCH, das Frontend wird nicht angefasst.
--
-- LAST-BILANZ: 2 volle Aggregat-Laeufe pro Tag (statt 864 beim Waermer und
-- statt 1 pro Besucher vorher). Refresh mit ADVISORY LOCK gegen Ueberlappung
-- — die Lehre aus INC-005 (pg_cron startet sonst blind den naechsten Lauf).
-- Zeiten 06:15 + 14:00 UTC: ausserhalb der Updater-Fenster (22-04, 16-20),
-- nach Harvester (05:30) und Accuracy-Snapshot (05:45), vor Rosters (07:00).
--
-- Frische: Liga-Summen und Club-Listen aendern sich im Tagesrhythmus —
-- zweimal taeglich ist fuer ein Ranking und fuer Filterlisten voellig ok.
--
-- Waehrend eines Refresh (Sekunden) blockiert die MV kurz lesende RPCs;
-- der Client wiederholt mit Backoff und der Wachhund heilt nach 9 s — ein
-- Besucher exakt in dem Moment sieht schlimmstenfalls eine kurze Verzoegerung.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Die Erstbefuellung macht die zwei
-- Aggregat-Laeufe einmalig — am besten ausserhalb der Updater-Fenster.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Liga-Summen je Eligibility x Rarity x Liga(+Land) ───────────────────
drop materialized view if exists public.mv_market_leagues;
create materialized view public.mv_market_leagues as
select cp.eligibility,
       cp.scarcity,
       cp.league_name,
       cp.league_country,
       round(sum(cp.fmv)::numeric, 2) as total_fmv,
       count(*)::int                  as cards
from public.card_prices cp
where cp.fmv is not null and cp.league_name is not null
group by cp.eligibility, cp.scarcity, cp.league_name, cp.league_country
with data;
create index idx_mv_market_leagues_key on public.mv_market_leagues (eligibility, scarcity);

-- ── 2) Filter-Facetten (Club / Liga / Position) je Eligibility ─────────────
drop materialized view if exists public.mv_market_facets;
create materialized view public.mv_market_facets as
select t.eligibility, t.kind, t.val as value, t.extra, t.cards
from (
  select cp.eligibility,
         case
           when grouping(cp.team_name)   = 0 then 'club'
           when grouping(cp.league_name) = 0 then 'league'
           else                                   'position'
         end::text                                                as kind,
         coalesce(cp.team_name, cp.league_name, cp.position)::text as val,
         cp.league_country::text                                  as extra,
         count(*)::int                                            as cards
  from public.card_prices cp
  where cp.fmv is not null
  group by grouping sets (
    (cp.eligibility, cp.team_name),
    (cp.eligibility, cp.league_name, cp.league_country),
    (cp.eligibility, cp.position)
  )
) t
where t.val is not null
with data;
create index idx_mv_market_facets_key on public.mv_market_facets (eligibility);

-- MVs bewusst NICHT an anon/authenticated freigeben — nur die RPCs lesen sie
revoke all on public.mv_market_leagues from public, anon, authenticated;
revoke all on public.mv_market_facets  from public, anon, authenticated;

-- ── 3) RPCs lesen nur noch aus den MVs (Signaturen unveraendert) ───────────
create or replace function public.market_leagues(
  p_elig     text default 'in_season',
  p_scarcity text default null
)
returns table (league_name text, league_country text, total_fmv numeric, cards int)
language sql stable security definer set search_path = public as $$
  select m.league_name,
         m.league_country,
         round(sum(m.total_fmv)::numeric, 2),
         sum(m.cards)::int
  from public.mv_market_leagues m
  where m.eligibility = p_elig
    and (p_scarcity is null or m.scarcity = p_scarcity)
  group by m.league_name, m.league_country
  order by 3 desc
$$;

create or replace function public.market_facets(p_elig text default 'in_season')
returns table (kind text, value text, extra text, cards int)
language sql stable security definer set search_path = public as $$
  select m.kind, m.value, m.extra, m.cards
  from public.mv_market_facets m
  where m.eligibility = p_elig
$$;

revoke execute on function public.market_leagues(text,text) from public;
revoke execute on function public.market_facets(text)       from public;
grant  execute on function public.market_leagues(text,text) to anon, authenticated;
grant  execute on function public.market_facets(text)       to anon, authenticated;

-- ── 4) Refresh mit Ueberlappungsschutz ─────────────────────────────────────
create or replace function public.refresh_market_aggregates()
returns text
language plpgsql security definer set search_path = public as $$
begin
  -- Laeuft noch ein Refresh, wird dieser Aufruf uebersprungen statt gestapelt
  if not pg_try_advisory_lock(8472001) then
    return 'skipped: refresh already running';
  end if;
  begin
    refresh materialized view public.mv_market_leagues;
    refresh materialized view public.mv_market_facets;
  exception when others then
    perform pg_advisory_unlock(8472001);
    raise;
  end;
  perform pg_advisory_unlock(8472001);
  return 'ok';
end $$;
revoke all on function public.refresh_market_aggregates() from public;

do $$
begin
  create extension if not exists pg_cron;
  perform cron.schedule('refresh_market_aggregates', '15 6,14 * * *',
                        'select public.refresh_market_aggregates()');
exception when others then
  raise notice 'pg_cron nicht verfuegbar (%): bitte melden', sqlerrm;
end $$;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- select count(*) from mv_market_leagues;   -> einige hundert Zeilen
-- select kind, count(*) from mv_market_facets group by 1;
-- select * from market_leagues('in_season', null) limit 5;   -> in Millisekunden
-- select jobname, schedule from cron.job order by jobname;
--   -> fmv_accuracy_daily · price_history_rollup_weekly · refresh_market_aggregates
--      (+ squad-poll der anderen Session). KEIN warm_market_aggregates mehr!
-- Am Folgetag: select jobname, status, end_time-start_time
--              from cron.job_run_details order by start_time desc limit 10;
