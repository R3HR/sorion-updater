-- SEC-006 — RLS-Lockdown, Schritt 1 (ungefährlich: nichts davon liest ein Frontend)
-- Im Supabase Dashboard → SQL Editor ausführen.
--
-- Anlass: Supabase-Advisor meldete `rls_disabled_in_public` für fmv_versions.
-- Die Prüfung förderte Gravierenderes zutage (siehe docs/INCIDENTS.md SEC-006):
--   • pool_cache war für JEDEN beschreibbar (Policies hiessen "Service only",
--     erlaubten per `true` aber alle Rollen)
--   • mehrere interne Tabellen waren oeffentlich lesbar
-- Hinweis: service_role umgeht RLS vollstaendig -> Harvester, Railway-Updater
-- und Edge Functions laufen unveraendert weiter.

-- ── 1) fmv_versions: RLS war komplett AUS (die gemeldete Tabelle) ──────────
alter table public.fmv_versions enable row level security;
alter table public.fmv_versions force row level security;
revoke all on public.fmv_versions from anon, authenticated;

-- ── 2) pool_cache: Schreibzugriff fuer alle entfernen ─────────────────────
-- Die beiden Policies erlaubten trotz ihres Namens jedem INSERT/UPDATE.
drop policy if exists "Service only insert" on public.pool_cache;
drop policy if exists "Service only update" on public.pool_cache;
revoke insert, update, delete on public.pool_cache from anon, authenticated;
-- "Read all" (SELECT) bleibt bestehen: der Pool ist nicht personenbezogen.
-- Optional spaeter auch sperren - kein Frontend liest ihn direkt (nur get-pool
-- ueber den Service-Key).

-- ── 3) Interne Auswertungen: nicht oeffentlich ────────────────────────────
drop policy if exists "fmv_accuracy_public_read"  on public.fmv_accuracy;
drop policy if exists "fmv_accuracy_daily_read"   on public.fmv_accuracy_daily;
drop policy if exists "market_daily_read"         on public.market_daily;
revoke all on public.fmv_accuracy, public.fmv_accuracy_daily, public.market_daily
  from anon, authenticated;

-- ── 4) card_prices: Lesen bleibt gewollt, Schreiben nie ───────────────────
-- RLS verhindert Schreiben zwar schon (nur SELECT-Policy), aber die Grants
-- standen offen - defense in depth.
revoke insert, update, delete on public.card_prices from anon, authenticated;

-- ── 5) Kontrolle ──────────────────────────────────────────────────────────
select c.relname as tabelle,
       c.relrowsecurity as rls_aktiv,
       (select count(*) from pg_policies p
         where p.schemaname='public' and p.tablename=c.relname) as policies,
       has_table_privilege('anon', c.oid, 'SELECT') as anon_select,
       has_table_privilege('anon', c.oid, 'INSERT') as anon_insert
from pg_class c join pg_namespace n on n.oid=c.relnamespace
where n.nspname='public'
  and c.relname in ('fmv_versions','pool_cache','fmv_accuracy','fmv_accuracy_daily',
                    'market_daily','card_prices')
order by c.relname;
