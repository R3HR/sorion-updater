-- ═══════════════════════════════════════════════════════════════════════════
-- accuracy.html laedt die Genauigkeitstabelle nicht  (17.10.2026)
--
-- ZWEI URSACHEN, gemessen von aussen mit dem anon-Schluessel:
--   1. GET fmv_accuracy_daily  -> 401 "permission denied for table"
--      RLS ist an, es gibt aber WEDER eine Policy NOCH ein select-Recht.
--      Die Tabelle enthaelt nur Aggregate (Tag, Segment, Proben, Median, Bias),
--      also nichts Schuetzenswertes. Die Seite ist oeffentlich und zeigt genau das.
--   2. POST rpc/accuracy_benchmark ohne Argument -> 500, statement timeout nach 3 s.
--      Der Standard ist 30 Tage; die Edge Function fragt mit 3 Tagen und ist in
--      0,37 s durch, der Fallback der Seite lief aber in den anon-Timeout.
-- ═══════════════════════════════════════════════════════════════════════════

grant select on public.fmv_accuracy_daily to anon, authenticated;
drop policy if exists fmv_accuracy_daily_read on public.fmv_accuracy_daily;
create policy fmv_accuracy_daily_read on public.fmv_accuracy_daily for select using (true);

-- Der Filter auf die Verkaufsart (seit 11.10.) hat keinen Index. Beides zusammen
-- macht den 30-Tage-Lauf teuer.
create index if not exists fmv_accuracy_deal_created_idx
  on public.fmv_accuracy (deal_type, created_at);

-- Standard auf 7 Tage: das ist der Zeitraum, den die Seite auch anzeigt, und er
-- bleibt sicher unter dem 3-Sekunden-Limit fuer anonyme Aufrufe.
create or replace function public.accuracy_benchmark(p_days integer default 7)
returns table(eligibility text, scarcity text, estimator text, samples integer,
              median_abs numeric, bias numeric, within_20pct numeric)
language sql stable security definer set search_path to 'public' as $$
  with base as (
    select eligibility, scarcity, sale_price, fmv_est, floor_est, avg_sales_est
    from public.fmv_accuracy
    where created_at > now() - make_interval(days => least(greatest(p_days, 1), 30))
      and hours_gap < 48 and sale_price > 0 and deal_type = 'TokenOffer'
  ),
  lang as (
    select eligibility, scarcity, 'fmv'::text as estimator, sale_price, fmv_est as est from base where fmv_est > 0
    union all select eligibility, scarcity, 'floor',     sale_price, floor_est     from base where floor_est > 0
    union all select eligibility, scarcity, 'avg_sales', sale_price, avg_sales_est from base where avg_sales_est > 0
  )
  select l.eligibility, l.scarcity, l.estimator, count(*)::int,
         round((percentile_cont(0.5) within group (order by abs((l.sale_price - l.est) / l.est * 100)))::numeric, 1),
         round((percentile_cont(0.5) within group (order by      (l.sale_price - l.est) / l.est * 100))::numeric, 1),
         round(100.0 * count(*) filter (where abs((l.sale_price - l.est) / l.est * 100) <= 20) / count(*), 1)
  from lang l group by l.eligibility, l.scarcity, l.estimator having count(*) >= 10
  order by l.eligibility, l.scarcity, case l.estimator when 'fmv' then 1 when 'avg_sales' then 2 else 3 end
$$;
revoke all on function public.accuracy_benchmark(integer) from public;
grant execute on function public.accuracy_benchmark(integer) to anon, authenticated;

select 'fertig' as status;
