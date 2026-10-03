-- ═══════════════════════════════════════════════════════════════════════════
-- VIERTE Schnittkante: FMV v3.7 (Trendnachfuehrung), erste Berechnung 05.10.2026
--
-- v3.7 laesst den Wert einem laufenden Trend schneller folgen (Prinzip 8). Die
-- angezeigten Werte verschieben sich dadurch einmalig, ohne dass sich der Markt
-- bewegt haette. Der 7-Tage-Chip darf keine Snapshots ueber die Umstellung hinweg
-- vergleichen.
--
-- WARUM 05.10.: Die Updater sind Cron-Jobs (22:00-05:00 UTC), der Tages-Snapshot
-- entsteht um 05:30 UTC. Laeuft der Push in der Nacht vom 04. auf den 05.10. mit,
-- ist der Snapshot vom 05.10. der erste mit v3.7-Werten.
-- !! WIRD SPAETER GEPUSHT, MUSS DAS DATUM MITWANDERN !!
--
-- Kanten bisher: v3.2 22.08., v3.3 26.08., v3.5 12.09. (v3.4 und v3.6 haben nie
-- einen eigenen Snapshot erzeugt, siehe INC-009).
-- ═══════════════════════════════════════════════════════════════════════════

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
  with cuts as (select d from (values
      (date '2026-08-22'), (date '2026-08-26'), (date '2026-09-12'), (date '2026-10-05')
    ) as c(d)),
  d as (select least(greatest(coalesce(p_days,7), 2), 90) as gap),
  cur as (
    select * from public.market_daily
    where scarcity = p_scarcity and eligibility = p_elig and avg_fmv is not null and n > 100
    order by day desc limit 1
  ),
  ref as (
    select md.* from public.market_daily md, cur, d
    where md.scarcity = p_scarcity and md.eligibility = p_elig
      and md.avg_fmv is not null and md.n > 100
      and md.day <= cur.day - d.gap
      and not exists (
        select 1 from cuts c
        where (cur.day >= c.d) <> (md.day >= c.d)
      )
    order by md.day desc limit 1
  )
  select round((((cur.avg_fmv - ref.avg_fmv) / nullif(ref.avg_fmv, 0)) * 100)::numeric, 1),
         least(cur.n, ref.n),
         (cur.day - ref.day)::int
  from cur, ref
  where abs(cur.n - ref.n)::numeric / greatest(ref.n, 1) < 0.25
$$;

revoke all on function public.market_move(text, text, int) from public, anon, authenticated;
grant execute on function public.market_move(text, text, int) to anon, authenticated;

select case when pg_get_functiondef(p.oid) like '%2026-10-05%' then 'Kante 05.10. gesetzt' else 'FEHLT' end as status
from pg_proc p join pg_namespace n on n.oid=p.pronamespace
where n.nspname='public' and p.proname='market_move';
