-- ═══════════════════════════════════════════════════════════════════════════
-- Schutz vor einem SCHEIN-Marktsprung nach der FMV-Umstellung auf v3.4
-- (kuerzere Halbwertszeit bei hoher Verkaufsdichte, Freigabe Jonas, Deploy 07.09.2026)
--
-- HINTERGRUND: Mit v3.4 gewichten liquide Karten die juengsten Verkaeufe
-- staerker. Ihre angezeigten Werte verschieben sich dadurch einmalig (Backtest:
-- Median 23,9 -> 23,2 %, Bias +1,7 -> +1,9 %). Der Markt hat sich dabei NICHT
-- bewegt, nur unsere Rechnung. Der 7-Tage-Chip darf deshalb keine Snapshots
-- ueber die Umstellung hinweg vergleichen.
--
-- DRITTE Schnittkante: v3.2 (22.08.), v3.3 (26.08.), v3.4 (08.09.). Deploy ist der
-- 07.09.; der Snapshot dieses Tages entstand aber schon um 05:30 UTC und ist noch
-- v3.3. Der erste v3.4-Snapshot faellt daher auf den 08.09. (Erst-Fassung hatte
-- faelschlich den 07.09. und sperrte den Chip sofort.) Beide Snapshots eines
-- Vergleichs muessen auf derselben Seite JEDER
-- Kante liegen. Der Chip ist damit fuer p_days Tage leer ("leer statt falsch")
-- und greift danach von selbst wieder.
--
-- REIHENFOLGE: erst diese Migration, dann lib/fmv.mjs pushen.
-- AUSFUEHREN: per CLI (npx supabase db query --linked --file ...).
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
  -- Formel-Schnittkanten: v3.2 22.08., v3.3 26.08., v3.4 08.09.
  with cuts as (select d from (values (date '2026-08-22'), (date '2026-08-26'), (date '2026-09-08')) as c(d)),
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
      -- BEIDE Snapshots muessen auf derselben Seite JEDER Umstellung liegen
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

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Heute (07.09., vor dem ersten v3.4-Snapshot) muss noch ein Ergebnis kommen:
select 'limited/in_season' as segment, * from public.market_move('limited','in_season',7)
union all
select 'rare/in_season', * from public.market_move('rare','in_season',7);
-- Ab dem 08.09.-Snapshot leer, bis wieder 7 Tage v3.4-Snapshots existieren.
-- Leer ist dort das RICHTIGE Ergebnis (die UI blendet den Chip aus).
