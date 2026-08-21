-- ═══════════════════════════════════════════════════════════════════════════
-- Cache-Waermer fuer die Markt-Aggregate  (21.08.2026)
--
-- BEFUND (Jonas: "Seite laedt oefter nicht vollstaendig, Refresh hilft"):
-- market_leagues/market_facets brauchen KALT 3-3,5 s und reissen dann den
-- statement_timeout — der jeweils ERSTE Besucher nach Cache-Verfall bekommt
-- eine unvollstaendige Seite. Der manuelle Refresh half, weil schon der
-- gescheiterte Versuch den DB-Cache waermte.
--
-- FIX AN DER WURZEL: Ein pg_cron-Job ruft die Aggregate alle 10 Minuten auf.
-- Damit bleiben ihre Seiten dauerhaft im Cache und KEIN Besucher zahlt mehr
-- den Kaltstart. Kosten: 6 leichte Abfragen alle 10 Minuten (~400 ms warm),
-- laeuft als Datenbank-Rolle — kein API-Verbrauch, kein Egress.
-- (Ergaenzt die Client-Seite, die seit 20.08. mit Backoff wiederholt und per
-- Wachhund fehlende Abschnitte selbst nachlaedt.)
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS" (Lektion BUG-018)!
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.warm_market_aggregates()
returns void
language plpgsql security definer set search_path = public as $$
begin
  -- Ergebnisse sind egal — es geht nur darum, die Seiten in den Cache zu lesen
  perform * from public.market_leagues('in_season', null);
  perform * from public.market_leagues('classic',   null);
  perform * from public.market_facets('in_season');
  perform * from public.market_facets('classic');
  perform * from public.market_overview('in_season');
  perform * from public.market_overview('classic');
end $$;
revoke all on function public.warm_market_aggregates() from public;

do $$
begin
  create extension if not exists pg_cron;
  perform cron.schedule('warm_market_aggregates', '*/10 * * * *',
                        'select public.warm_market_aggregates()');
exception when others then
  raise notice 'pg_cron nicht verfuegbar (%): bitte melden', sqlerrm;
end $$;

-- Einmal sofort waermen
select public.warm_market_aggregates();

-- ── Verifikation ───────────────────────────────────────────────────────────
-- select jobname, schedule from cron.job order by jobname;
--   -> fmv_accuracy_daily · price_history_rollup_weekly · warm_market_aggregates
