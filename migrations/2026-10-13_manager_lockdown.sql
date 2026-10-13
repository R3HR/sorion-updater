-- SEC-006 Schritt 2b — direkten Tabellenzugriff auf manager_* entziehen
-- ⚠️ ERST ausfuehren, wenn 2026-10-13_manager_rpc.sql gelaufen UND das neue
--    portfolio.html live ist (es ruft die RPCs auf). Sonst bleibt das Portfolio leer.
--
-- Schliesst den Bulk-Abzug: 73.865 + 35.295 Zeilen waren per publishable Key
-- am Stueck abrufbar, und die geplante Pro-Funktion "Manager suchen" war gratis.

drop policy if exists "manager_cards_read"  on public.manager_cards;
drop policy if exists "manager_trades_read" on public.manager_trades;
drop policy if exists "manager_sync_read"   on public.manager_sync;

revoke all on public.manager_cards, public.manager_trades, public.manager_sync
  from anon, authenticated;

-- Kontrolle: muss ueberall false liefern
select c.relname as tabelle,
       has_table_privilege('anon','public.'||c.relname,'SELECT') as anon_select
from pg_class c join pg_namespace n on n.oid=c.relnamespace
where n.nspname='public'
  and c.relname in ('manager_cards','manager_trades','manager_sync')
order by c.relname;
