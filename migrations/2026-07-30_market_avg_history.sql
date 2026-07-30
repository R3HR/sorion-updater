-- Aggregat-RPC fuer die 7d-Chips an den AVG-FMV-Boxen (30.07.)
-- Problem: Die Chips zeigten den MEDIAN der Einzelkarten-7d-Aenderungen — ein anderer
-- Wert als die Veraenderung des angezeigten Durchschnitts (Avg stieg 2,22->4,52 €,
-- Chip zeigte -1,4%; Rare-Median war konstant 0,0%). Wirkte "random"/falsch.
-- Fix: Chip = avg FMV heute vs. avg FMV vor N Tagen, direkt aus price_history.
-- price_history ist seit 27.07. gegen Direktzugriff gesperrt (BUG-012 Oekosystem) —
-- daher diese SECURITY-DEFINER-RPC, die NUR Tages-AGGREGATE liefert (avg + count,
-- keine Einzelspieler-Daten -> kein Scrape-Risiko).

create or replace function public.market_avg_history(
  p_scarcity text,
  p_elig     text,
  p_days     int default 10
)
returns table (day date, avg_price numeric, n int)
language sql
stable
security definer
set search_path = public
as $$
  select ph.recorded_at as day,
         round(avg(ph.price)::numeric, 4) as avg_price,
         count(*)::int as n
  from public.price_history ph
  where ph.scarcity    = p_scarcity
    and ph.eligibility = p_elig
    and ph.recorded_at >= (current_date - make_interval(days => least(greatest(coalesce(p_days,10), 1), 120)))
  group by ph.recorded_at
  order by ph.recorded_at asc
$$;

revoke all on function public.market_avg_history(text, text, int) from public;
grant execute on function public.market_avg_history(text, text, int) to anon, authenticated;

-- Verifikation (mit publishable Key):
--   curl -X POST .../rest/v1/rpc/market_avg_history \
--     -H "Content-Type: application/json" \
--     -d '{"p_scarcity":"limited","p_elig":"in_season","p_days":10}'
--   -> eine Zeile pro Tag: {day, avg_price, n}
