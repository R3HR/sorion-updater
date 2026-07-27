-- KORREKTUR (27.07.): Nach dem ersten Lockdown war price_history per publishable Key
-- weiterhin am Stueck lesbar (Live-Test: 1.589.335 Zeilen erreichbar). `enable row level
-- security` allein hat nicht gegriffen -> es existierte noch ein anon-SELECT-Grant und/oder
-- eine permissive Policy. Dieser Block macht den Zugriff sicher dicht (idempotent).
-- Die RPC public.player_history bleibt unberuehrt (SECURITY DEFINER, laeuft als Owner)
-- und liefert die Sparklines weiter. Der Railway-Updater schreibt mit dem Service-Key weiter.

-- 1) RLS wirklich aktiv (auch fuer Tabellen-Owner erzwingen)
alter table public.price_history enable row level security;
alter table public.price_history force  row level security;

-- 2) ALLE bestehenden Policies auf price_history entfernen (eine davon erlaubte evtl. anon-Lesen)
do $$
declare p record;
begin
  for p in
    select policyname from pg_policies
    where schemaname = 'public' and tablename = 'price_history'
  loop
    execute format('drop policy if exists %I on public.price_history', p.policyname);
  end loop;
end $$;

-- 3) Der Hammer: direktes SELECT-Recht der API-Rollen entziehen.
--    Damit kann weder anon noch authenticated die Tabelle direkt lesen,
--    egal wie RLS/Policies stehen. PostgREST bietet die Tabelle dann nicht mehr an.
revoke select on public.price_history from anon, authenticated;

-- 4) RPC sicher weiter aufrufbar halten
grant execute on function public.player_history(text, text, text, int) to anon, authenticated;

-- ── Verifikation danach (mit publishable Key) ────────────────────────────────
-- direkter Bulk-Zugriff -> muss jetzt scheitern/leer sein:
--   curl .../rest/v1/price_history?select=id&limit=5      => [] oder 401/permission denied
-- spielergenau via RPC -> muss weiter Daten liefern:
--   curl -X POST .../rest/v1/rpc/player_history \
--     -d '{"p_slug":"will-dennis","p_scarcity":"limited","p_elig":"in_season","p_days":30}'
