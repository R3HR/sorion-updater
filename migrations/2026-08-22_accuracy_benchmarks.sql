-- ═══════════════════════════════════════════════════════════════════════════
-- Gegenprobe: Floor und Durchschnitt gegen FMV messen  (22.08.2026)
--
-- ANLASS (Jonas): "Wir tracken die Genauigkeit des FMV — wie akkurat ist der
-- Floor Price im Vergleich?" Bisher protokolliert fmv_accuracy beim Verkauf
-- NUR den FMV. Ein sauberer Vergleich war damit unmoeglich.
--
-- Ab jetzt wandern die beiden Alternativ-Schaetzer mit in dieselbe Zeile:
--   floor_est      = der Floor, den wir zum Schaetzzeitpunkt anzeigten
--   avg_sales_est  = der einfache Durchschnitt der letzten 10 Verkaeufe
-- Beide stammen aus DERSELBEN Zeile wie fmv_est, also aus dem Stand VOR dem
-- Verkauf — kein Leakage, exakt dieselbe Messlatte wie beim FMV.
--
-- Damit laesst sich die Aussage auf accuracy.html ("FMV ist besser als Floor
-- oder simpler Durchschnitt") erstmals mit eigenen Daten belegen — oder
-- widerlegen. Beides ist ein Gewinn.
--
-- Vorschau aus card_prices (22.08., naeherungsweise, MIT Leakage-Vorbehalt —
-- deshalb ueberhaupt diese Migration): FMV Median-Abweichung 31,0 %, Floor
-- 33,2 %, Avg Sales 20,4 %. Auffaellig: bei 63,5 % der Karten liegt der FMV
-- exakt auf dem Deckel Floor x 1,05; diese Gruppe weicht 37,7 % ab, die
-- ungedeckelte nur 18,6 %.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Reine Spaltenerweiterung, kein
-- Datenverlust; Altzeilen behalten NULL in den neuen Spalten.
-- ═══════════════════════════════════════════════════════════════════════════

alter table public.fmv_accuracy add column if not exists floor_est     numeric;
alter table public.fmv_accuracy add column if not exists avg_sales_est numeric;

-- Auswertung: alle drei Schaetzer nebeneinander, gleiche Messlatte.
-- Zeilen ohne den jeweiligen Schaetzer fallen fuer DIESEN heraus (n je Spalte).
create or replace function public.accuracy_benchmark(p_days int default 30)
returns table (
  eligibility  text,
  scarcity     text,
  estimator    text,    -- 'fmv' | 'floor' | 'avg_sales'
  samples      int,
  median_abs   numeric, -- Median der absoluten Abweichung in %
  bias         numeric, -- Median der SIGNIERTEN Abweichung (+ = Verkauf teurer)
  within_20pct numeric  -- Anteil der Verkaeufe, die naeher als 20 % lagen
)
language sql stable security definer set search_path = public as $$
  with base as (
    select eligibility, scarcity, sale_price, fmv_est, floor_est, avg_sales_est
    from public.fmv_accuracy
    where created_at > now() - make_interval(days => p_days)
      and hours_gap < 48
      and sale_price > 0
  ),
  lang as (
    select eligibility, scarcity, 'fmv'::text as estimator, sale_price, fmv_est as est from base where fmv_est > 0
    union all
    select eligibility, scarcity, 'floor',      sale_price, floor_est     from base where floor_est > 0
    union all
    select eligibility, scarcity, 'avg_sales',  sale_price, avg_sales_est from base where avg_sales_est > 0
  )
  select l.eligibility, l.scarcity, l.estimator,
         count(*)::int,
         round((percentile_cont(0.5) within group (order by abs((l.sale_price - l.est) / l.est * 100)))::numeric, 1),
         round((percentile_cont(0.5) within group (order by      (l.sale_price - l.est) / l.est * 100))::numeric, 1),
         round(100.0 * count(*) filter (where abs((l.sale_price - l.est) / l.est * 100) <= 20) / count(*), 1)
  from lang l
  group by l.eligibility, l.scarcity, l.estimator
  having count(*) >= 10
  order by l.eligibility, l.scarcity,
           case l.estimator when 'fmv' then 1 when 'floor' then 2 else 3 end
$$;

-- Oeffentlich lesbar: die Aussage gehoert auf die Accuracy-Seite, sobald
-- genug Daten da sind (fmv_accuracy ist ohnehin oeffentlich lesbar).
revoke all on function public.accuracy_benchmark(int) from public;
grant execute on function public.accuracy_benchmark(int) to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Direkt nach dem Einspielen leer (Altzeilen haben keine Vergleichswerte).
-- Nach dem naechsten Updater-Fenster fuellt es sich:
--   select * from accuracy_benchmark(30);
-- Erwartet: je Eligibility x Scarcity drei Zeilen (fmv / floor / avg_sales).
