-- KORREKTUR v2 (30.07.) — ersetzt market_avg_history aus derselben Session.
--
-- Warum die erste Fassung falsch war (mit Echtdaten belegt):
-- `market_avg_history` lieferte pro Tag den Durchschnitt ALLER an dem Tag geschriebenen
-- price_history-Zeilen. price_history bekommt aber taeglich nur die Teilmenge, die der
-- Updater in dem Lauf abgearbeitet hat (ein voller Sweep dauert Tage) -> n schwankte
-- 537..5102 und der "Tagesdurchschnitt" sprang 6,38 -> 25,96 €. Ein Vergleich zweier
-- solcher Tage misst die Stichprobe, nicht den Markt. Zusaetzlich lief die Aggregation
-- fuer 'limited' in den statement_timeout.
--
-- v2: GEPAARTER Korb-Vergleich. Nur Spieler, die in BEIDEN Zeitfenstern einen Preis
-- haben, werden verglichen (je Spieler das Fenster-Mittel). Ergebnis = wertgewichtete
-- Veraenderung desselben Korbs -> passt zur Aussage "Avg FMV hat sich um X % bewegt"
-- und ist gegen Rotation/Neuzugaenge immun. Zwei enge Fenster (je 3 Tage) halten die
-- Datenmenge klein und die Laufzeit unter dem Timeout.

-- Index aus dem Nachtrag (idempotent, falls noch nicht vorhanden)
create index if not exists idx_price_history_scarcity_elig_day
  on public.price_history (scarcity, eligibility, recorded_at);

drop function if exists public.market_avg_history(text, text, int);

create or replace function public.market_move(
  p_scarcity text,
  p_elig     text,
  p_days     int default 7
)
returns table (pct numeric, players int, days_gap int)
language sql
stable
security definer
set search_path = public
as $$
  with g as (select least(greatest(coalesce(p_days,7), 4), 60) as d),
  -- Fenster "jetzt": die letzten 3 Tage
  now_w as (
    select ph.player_slug, avg(ph.price) as p
    from public.price_history ph, g
    where ph.scarcity = p_scarcity and ph.eligibility = p_elig
      and ph.recorded_at >= current_date - 2
    group by ph.player_slug
  ),
  -- Fenster "vorher": 3 Tage um den Referenzpunkt (heute - d)
  ref_w as (
    select ph.player_slug, avg(ph.price) as p
    from public.price_history ph, g
    where ph.scarcity = p_scarcity and ph.eligibility = p_elig
      and ph.recorded_at >= current_date - g.d - 1
      and ph.recorded_at <= current_date - g.d + 1
    group by ph.player_slug
  ),
  paired as (
    select n.p as now_p, r.p as ref_p
    from now_w n join ref_w r using (player_slug)
  )
  select round((((sum(now_p) - sum(ref_p)) / nullif(sum(ref_p), 0)) * 100)::numeric, 1) as pct,
         count(*)::int as players,
         (select d from g)::int as days_gap
  from paired
  having count(*) >= 100   -- unter 100 gepaarten Spielern keine Aussage
$$;

revoke all on function public.market_move(text, text, int) from public;
grant execute on function public.market_move(text, text, int) to anon, authenticated;

-- Verifikation (soll <1 s antworten, eine Zeile oder leer):
--   curl -X POST .../rest/v1/rpc/market_move -H "Content-Type: application/json" \
--     -d '{"p_scarcity":"limited","p_elig":"in_season","p_days":7}'
--   -> [{"pct":-2.4,"players":8123,"days_gap":7}]  (leer = zu wenig gepaarte Daten)
