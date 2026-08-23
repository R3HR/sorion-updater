-- ═══════════════════════════════════════════════════════════════════════════
-- SEC-004: Interne Funktionen anonym aufrufbar  (22.08.2026)
--
-- BEFUND (Herz-und-Nieren-Pruefung 22.08., mit dem anonymen Schluessel):
--   warm_market_aggregates()    -> HTTP 204 (LIEF! die INC-005-Last, fuer jeden ausloesbar)
--   snapshot_fmv_accuracy()     -> 200 (Voll-Perzentil + DELETE > 90 Tage)
--   snapshot_market_daily()     -> 500/57014 (lief an, Vollaggregat)
--   refresh_market_aggregates() -> 500/57014 (lief an, 2 Vollaggregate)
--   price_history_rollup()      -> 500/57014 (lief an — LOESCHT Zeilen)
--   analytics_prune()           -> 200 (DELETE, derzeit 0 Zeilen)
--   purge_manager_data()        -> 401 (ok — hier hatte die Migration es richtig gemacht)
--
-- URSACHE: Supabase vergibt EXECUTE auf neue Funktionen per DEFAULT PRIVILEGES
-- explizit an anon/authenticated. "revoke ... from public" (das stand in den
-- Migrationen) entfernt diese EXPLIZITEN Grants nicht. Jede Security-Definer-
-- Funktion ohne eigenes "revoke from anon, authenticated" ist damit ein
-- oeffentlicher Endpunkt — ein Angreifer (oder ein Crawler) koennte die
-- IO-Drossel der Nano-Instanz jederzeit reproduzieren (INC-005 per Knopfdruck)
-- und den Rollup Zeilen loeschen lassen.
--
-- FIX: (1) Waermer-Funktion komplett droppen. (2) Alle internen Funktionen
-- explizit fuer anon+authenticated sperren. (3) DEFAULT PRIVILEGES aendern,
-- damit KUENFTIGE Funktionen privat starten — oeffentliche RPCs bekommen ihr
-- "grant execute ... to anon, authenticated" ohnehin ausdruecklich (so machen
-- es alle Markt-RPC-Migrationen). WICHTIG fuer alle Sessions: Eine neue RPC,
-- die das Frontend anonym aufrufen soll, braucht ab jetzt ZWINGEND ihren
-- expliziten Grant — sonst 401.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Waermer-Funktion entsorgen (Job ist seit 22.08. unscheduled) ────────
drop function if exists public.warm_market_aggregates();

-- ── 2) Interne Funktionen explizit sperren ─────────────────────────────────
revoke all on function public.snapshot_market_daily(date)      from public, anon, authenticated;
revoke all on function public.snapshot_fmv_accuracy(date)      from public, anon, authenticated;
revoke all on function public.refresh_market_aggregates()      from public, anon, authenticated;
revoke all on function public.price_history_rollup(int)        from public, anon, authenticated;
revoke all on function public.analytics_prune()                from public, anon, authenticated;
-- (purge_manager_data ist bereits dicht; pg_cron + Edge Functions laufen als
--  postgres/service_role und sind von den Revokes nicht betroffen)

-- ── 3) Kuenftige Funktionen starten privat ─────────────────────────────────
alter default privileges in schema public revoke execute on functions from anon, authenticated;
alter default privileges for role postgres in schema public revoke execute on functions from anon, authenticated;

-- ── Verifikation: was darf anon JETZT noch ausfuehren? ─────────────────────
-- Erwartete Liste (die bewussten oeffentlichen RPCs):
--   market_overview, market_leagues, market_facets, market_move, player_history,
--   is_analytics_admin  (+ ggf. Squad-RPCs der anderen Session, die das
--   Frontend anonym braucht — alles andere auf der Liste ist ein Befund)
select p.proname, pg_get_function_identity_arguments(p.oid) as args
from pg_proc p
join pg_namespace n on n.oid = p.pronamespace
where n.nspname = 'public'
  and has_function_privilege('anon', p.oid, 'EXECUTE')
order by 1;
