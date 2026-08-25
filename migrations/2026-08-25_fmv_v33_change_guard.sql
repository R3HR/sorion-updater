-- ═══════════════════════════════════════════════════════════════════════════
-- Schutz vor einem SCHEIN-Marktsprung nach der FMV-Umstellung auf v3.3
-- (bedingter SELL_CAP — Faktoren-Analyse 25.08.2026)
--
-- ⚠️ VOR DEM AUSFUEHREN: Das Cut-Datum unten ('2026-08-26') auf den
--    TATSAECHLICHEN v3.3-Deploy-Tag setzen. Reihenfolge: erst diese Migration
--    (SQL-Editor, "ohne RLS"), DANN lib/fmv.mjs ersetzen und pushen.
--
-- HINTERGRUND: Mit v3.3 faellt der Deckel fuer liquide Karten weg — deren
-- angezeigte Werte steigen einmalig (Backtest: Bias +22 % -> +2 %, d. h. die
-- Werte ruecken im Schnitt um diese Groessenordnung nach oben). Der Markt hat
-- sich dabei NICHT bewegt, nur unsere Rechnung — exakt die v3.2-Situation vom
-- 22.08. Der 7d-Chip darf deshalb keine Snapshots ueber die Umstellung hinweg
-- vergleichen.
--
-- NEU ggue. der v3.2-Sperre: ZWEI Schnittkanten (22.08. UND v3.3-Tag). Beide
-- Snapshots eines Vergleichs muessen auf derselben Seite JEDER Kante liegen.
-- Der Chip ist damit ab dem Deploy fuer p_days Tage leer ("leer statt falsch")
-- und greift danach von selbst wieder.
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
  -- Formel-Schnittkanten: v3.2 am 22.08., v3.3 am Deploy-Tag (ANPASSEN!)
  with cuts as (select d from (values (date '2026-08-22'), (date '2026-08-26')) as c(d)),
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
-- select * from market_move('limited','in_season',7);
--   -> Am Deploy-Tag noch ein Ergebnis (beide Snapshots vor der Kante).
--   -> Ab dem ersten v3.3-Snapshot leer, bis wieder p_days Tage v3.3-Snapshots
--      existieren. Leer ist hier das RICHTIGE Ergebnis (UI blendet den Chip aus).
-- Erinnerung aus dem HANDOFF: nach jeder kuenftigen Formelaenderung dieselbe
-- Sperre auf das neue Datum ziehen und den Backtest neu laufen lassen.
