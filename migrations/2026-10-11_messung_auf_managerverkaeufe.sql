-- ═══════════════════════════════════════════════════════════════════════════
-- Genauigkeitsmessung auf dieselbe Grundlage stellen wie den FMV  (11.10.2026)
--
-- BEFUND (Jonas: "wieso berechnest du das nochmal, wir messen das doch permanent"):
-- Wir messen permanent, aber nicht das, was die Formel optimiert. Seit v3.5
-- rechnet der FMV NUR mit Manager-Verkaeufen (TokenOffer). Beide Messungen
-- zaehlten dagegen ALLE Verkaufsarten, und snapshot_fmv_accuracy nahm fuer den
-- Bias zusaetzlich den MITTELWERT.
--
-- Rare in-season, Verkaeufe seit 05.10., derselbe Zeitraum:
--   alle Arten, Mittelwert (so lief es): +33,8 %
--   alle Arten, Median:                   +5,0 %
--   nur Manager, Median (Formelbasis):    -5,1 %
-- Der Mittelwert wird von Ausreissern getragen: groesste Einzelabweichung
-- +9.324 %, 221 Verkaeufe ueber +300 %. Die Zahl auf accuracy.html war damit
-- oeffentlich sichtbar falsch (dort standen fuer Rare in-season +25 bis +42 %).
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Tages-Snapshot: Median statt Mittelwert, nur Manager-Verkaeufe ──────
create or replace function public.snapshot_fmv_accuracy(p_day date default current_date)
returns int
language plpgsql
security definer
set search_path to 'public'
as $$
declare n int;
begin
  insert into public.fmv_accuracy_daily (day, eligibility, scarcity, samples, median_abs_delta, bias)
  select p_day, eligibility, scarcity,
         count(*)::int,
         round((percentile_cont(0.5) within group (order by abs(delta_pct)))::numeric, 1),
         -- Median, nicht Mittelwert: ein einzelner Verkauf zum Zehnfachen des
         -- Werts verschob den Mittelwert frueher um zweistellige Prozentpunkte.
         round((percentile_cont(0.5) within group (order by delta_pct))::numeric, 1)
  from public.fmv_accuracy
  where created_at > now() - interval '3 days'
    and hours_gap < 48
    and deal_type = 'TokenOffer'   -- dieselbe Grundlage wie der FMV (v3.5+)
  group by eligibility, scarcity
  on conflict (day, eligibility, scarcity) do update
    set samples          = excluded.samples,
        median_abs_delta = excluded.median_abs_delta,
        bias             = excluded.bias;
  get diagnostics n = row_count;

  -- Putzdienst unveraendert: Rohdaten > 90 Tage raus (Backtest-Reserve).
  -- card_sales_weekly haelt die Marktpreise dauerhaft fest (07.10.).
  delete from public.fmv_accuracy where created_at < now() - interval '90 days';
  return n;
end $$;

-- ── 2) Schaetzer-Vergleich ebenfalls auf Manager-Verkaeufe ─────────────────
create or replace function public.accuracy_benchmark(p_days integer default 30)
returns table(eligibility text, scarcity text, estimator text, samples integer,
              median_abs numeric, bias numeric, within_20pct numeric)
language sql
stable
security definer
set search_path to 'public'
as $$
  with base as (
    select eligibility, scarcity, sale_price, fmv_est, floor_est, avg_sales_est
    from public.fmv_accuracy
    where created_at > now() - make_interval(days => p_days)
      and hours_gap < 48
      and sale_price > 0
      and deal_type = 'TokenOffer'   -- nur echte Marktpreise, wie im FMV
  ),
  lang as (
    select eligibility, scarcity, 'fmv'::text as estimator, sale_price, fmv_est as est from base where fmv_est > 0
    union all
    select eligibility, scarcity, 'floor',     sale_price, floor_est     from base where floor_est > 0
    union all
    select eligibility, scarcity, 'avg_sales', sale_price, avg_sales_est from base where avg_sales_est > 0
  )
  select l.eligibility, l.scarcity, l.estimator,
         count(*)::int,
         round((percentile_cont(0.5) within group (order by abs((l.sale_price - l.est) / l.est * 100)))::numeric, 1),
         round((percentile_cont(0.5) within group (order by      (l.sale_price - l.est) / l.est * 100))::numeric, 1),
         round(100.0 * count(*) filter (where abs((l.sale_price - l.est) / l.est * 100) <= 20) / count(*), 1)
  from lang l
  group by l.eligibility, l.scarcity, l.estimator
  having count(*) >= 10
  order by l.eligibility, l.scarcity, case l.estimator when 'fmv' then 1 when 'avg_sales' then 2 else 3 end
$$;

-- ── 3) Tageswerte neu berechnen, soweit Rohdaten noch da sind ──────────────
do $$
declare d date;
begin
  for d in select generate_series(date '2026-09-08', current_date, interval '1 day')::date loop
    delete from public.fmv_accuracy_daily where day = d;
    insert into public.fmv_accuracy_daily (day, eligibility, scarcity, samples, median_abs_delta, bias)
    select d, eligibility, scarcity, count(*)::int,
           round((percentile_cont(0.5) within group (order by abs(delta_pct)))::numeric, 1),
           round((percentile_cont(0.5) within group (order by delta_pct))::numeric, 1)
    from public.fmv_accuracy
    where sale_at >= d - 2 and sale_at < d + 1 and deal_type = 'TokenOffer'
    group by eligibility, scarcity;
  end loop;
end $$;

select to_char(day,'DD.MM.') as tag, scarcity, eligibility, samples, median_abs_delta, bias
from public.fmv_accuracy_daily
where scarcity='rare' and day >= current_date - 4 order by day desc, eligibility;
