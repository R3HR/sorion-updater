-- Nachtrag zu 2026-07-30_analytics.sql (30.07.)
--
-- Befund im Live-Test: Die Auswertungs-RPCs waren ohne Login noch AUFRUFBAR
-- (Antwort: leere Liste, HTTP 200). Ursache: Postgres vergibt EXECUTE auf neue
-- Funktionen automatisch an die Rolle PUBLIC — `revoke ... from anon` allein
-- entfernt dieses Standardrecht NICHT (gleiche Klasse wie der price_history-Fall,
-- wo `enable RLS` ohne `revoke select` nicht gereicht hat).
--
-- Daten waren nie exponiert (is_analytics_admin() filtert im WHERE), aber der
-- Aufruf soll gar nicht erst durchgehen.

revoke execute on function public.analytics_daily(text,int)      from public;
revoke execute on function public.analytics_pages(text,int)      from public;
revoke execute on function public.analytics_events_top(text,int) from public;
revoke execute on function public.analytics_sources(text,int)    from public;
revoke execute on function public.analytics_prune()              from public;
revoke execute on function public.snapshot_market_daily(date)    from public;

-- Nur eingeloggte Nutzer duerfen aufrufen; der Admin-Gate im WHERE entscheidet dann,
-- wer tatsaechlich Zahlen sieht.
grant execute on function public.analytics_daily(text,int)      to authenticated;
grant execute on function public.analytics_pages(text,int)      to authenticated;
grant execute on function public.analytics_events_top(text,int) to authenticated;
grant execute on function public.analytics_sources(text,int)    to authenticated;

-- Optional: die 2 Testereignisse aus der Verifikation vom 30.07. entfernen.
-- (Nur ausfuehren, solange noch kein echter Traffic gezaehlt wurde.)
-- delete from public.analytics_events where day = current_date;

-- Verifikation danach (mit publishable Key, also ohne Login):
--   curl -X POST .../rest/v1/rpc/analytics_pages -d '{"p_site":"sorion","p_days":30}'
--   -> erwartet: 401/404 "permission denied for function", NICHT mehr []
-- Und der Beacon muss weiter funktionieren:
--   curl -X POST .../functions/v1/track -d '{"site":"sorion","path":"/"}'  -> {"ok":true}
