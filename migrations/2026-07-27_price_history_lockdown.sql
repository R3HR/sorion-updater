-- price_history gegen Bulk-Abgriff sperren (27.07.)
-- Ziel: Der oeffentliche publishable Key soll die komplette Zeitreihe (~1,6 Mio Zeilen)
-- NICHT mehr am Stueck herunterladen koennen. Spielergenaue Sparkline-Abfragen bleiben
-- ueber eine SECURITY-DEFINER-RPC moeglich. Der Railway-Updater schreibt mit dem
-- Service-Key weiter (Service-Rolle umgeht RLS).

-- 1) Direkten Tabellenzugriff fuer anon/authenticated dichtmachen.
alter table public.price_history enable row level security;
-- (Bewusst KEINE SELECT-Policy fuer anon/authenticated -> direkter REST-Zugriff liefert nichts.
--  Falls frueher versehentlich eine offene Policy existierte, hier entfernen:)
drop policy if exists "price_history_anon_read" on public.price_history;

-- 2) Spielergenaue Lesefunktion (genau ein Slug, begrenztes Fenster).
create or replace function public.player_history(
  p_slug     text,
  p_scarcity text,
  p_elig     text default null,
  p_days     int  default 30
)
returns table (price numeric, recorded_at date)
language sql
stable
security definer
set search_path = public
as $$
  select ph.price, ph.recorded_at
  from public.price_history ph
  where ph.player_slug = p_slug
    and ph.scarcity    = p_scarcity
    and (p_elig is null or ph.eligibility = p_elig)
    and ph.recorded_at >= (current_date - make_interval(days => least(greatest(coalesce(p_days,30), 1), 120)))
  order by ph.recorded_at asc
$$;

-- 3) Nur die RPC ist von aussen aufrufbar, die Tabelle nicht.
revoke all on function public.player_history(text, text, text, int) from public;
grant execute on function public.player_history(text, text, text, int) to anon, authenticated;

-- Verifikation nach dem Ausfuehren:
--   direkter Bulk-Zugriff  -> leer/deny:
--     curl .../rest/v1/price_history?select=id&limit=1  (mit publishable Key)  => []
--   spielergenau via RPC   -> Daten:
--     curl -X POST .../rest/v1/rpc/player_history -d '{"p_slug":"will-dennis","p_scarcity":"limited","p_elig":"in_season","p_days":30}'
