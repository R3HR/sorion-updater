-- SECHSTE Schnittkante: FMV v3.9 (staerkere Floor-Gewichte), erste Berechnung 18.10.2026.
create or replace function public.market_move(p_scarcity text, p_elig text, p_days int default 7)
returns table (pct numeric, players int, days_gap int)
language sql stable security definer set search_path = public as $$
  with cuts as (select d from (values
      (date '2026-08-22'), (date '2026-08-26'), (date '2026-09-12'),
      (date '2026-10-05'), (date '2026-10-12'), (date '2026-10-18')
    ) as c(d)),
  d as (select least(greatest(coalesce(p_days,7), 2), 90) as gap),
  cur as (select * from public.market_daily
          where scarcity = p_scarcity and eligibility = p_elig and avg_fmv is not null and n > 100
          order by day desc limit 1),
  ref as (select md.* from public.market_daily md, cur, d
          where md.scarcity = p_scarcity and md.eligibility = p_elig
            and md.avg_fmv is not null and md.n > 100 and md.day <= cur.day - d.gap
            and not exists (select 1 from cuts c where (cur.day >= c.d) <> (md.day >= c.d))
          order by md.day desc limit 1)
  select round((((cur.avg_fmv - ref.avg_fmv) / nullif(ref.avg_fmv, 0)) * 100)::numeric, 1),
         least(cur.n, ref.n), (cur.day - ref.day)::int
  from cur, ref where abs(cur.n - ref.n)::numeric / greatest(ref.n, 1) < 0.25
$$;
revoke all on function public.market_move(text, text, int) from public, anon, authenticated;
grant execute on function public.market_move(text, text, int) to anon, authenticated;
insert into public.fmv_versions (version, live_from, note)
values ('v3.9', date '2026-10-18', 'Floor-Gewichte nachgezogen; limited in-season bekommt kleinen Anteil')
on conflict (version) do nothing;
select case when pg_get_functiondef(p.oid) like '%2026-10-18%' then 'Kante 18.10. gesetzt' else 'FEHLT' end as status
from pg_proc p join pg_namespace n on n.oid=p.pronamespace where n.nspname='public' and p.proname='market_move';
