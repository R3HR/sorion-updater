-- ═══════════════════════════════════════════════════════════════════════════
-- Schutz vor einem SCHEIN-Marktsprung nach der FMV-Umstellung  (22.08.2026)
--
-- HINTERGRUND: Mit FMV v3.2 (SELL_CAP 1,05 -> 1,50, FLOOR_BLEND 0,35 -> 0,00)
-- steigen alle geschaetzten Werte auf einen Schlag um grob 20 %. Der Markt hat
-- sich dabei NICHT bewegt — nur unsere Rechnung.
--
-- PROBLEM: Der 7-Tage-Bewegungs-Chip an den Avg-FMV-Boxen vergleicht zwei
-- market_daily-Snapshots. Ein Snapshot von VOR der Umstellung gegen einen von
-- DANACH ergaebe "+20 % Marktbewegung" — glatt falsch, und genau die Sorte
-- Zahl, die am 30.07. schon einmal als "wirkt random" auffiel.
--
-- LOESUNG: market_move vergleicht nur noch Snapshots, die auf derselben Seite
-- der Umstellung liegen. Fuer die naechsten 7 Tage liefert der Chip fuer
-- In-Season/Classic-Vergleiche ueber die Grenze hinweg KEIN Ergebnis — die UI
-- blendet ihn dann aus (so gebaut: "leer statt falsch"). Danach greift er
-- automatisch wieder, ohne weiteres Zutun.
--
-- Die 24h/7d-Prozente je Karte (change_24h/change_7d aus price_history) zeigen
-- denselben Einmal-Effekt; sie waschen sich nach 24 h bzw. 7 Tagen von selbst
-- aus. Bewusst NICHT unterdrueckt: die Rangfolge der Movers bleibt brauchbar,
-- weil der Sprung alle Karten gleich trifft.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Danach lib/fmv.mjs deployen (Push).
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
  -- Tag der Formelumstellung: Snapshots davor und danach sind nicht vergleichbar
  with cut as (select date '2026-08-22' as d),
  d as (select least(greatest(coalesce(p_days,7), 2), 90) as gap),
  cur as (
    select * from public.market_daily
    where scarcity = p_scarcity and eligibility = p_elig and avg_fmv is not null and n > 100
    order by day desc limit 1
  ),
  ref as (
    select md.* from public.market_daily md, cur, d, cut
    where md.scarcity = p_scarcity and md.eligibility = p_elig
      and md.avg_fmv is not null and md.n > 100
      and md.day <= cur.day - d.gap
      -- BEIDE Snapshots muessen auf derselben Seite der Umstellung liegen
      and ((cur.day >= cut.d and md.day >= cut.d) or (cur.day < cut.d and md.day < cut.d))
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
--   -> HEUTE noch ein Ergebnis (beide Snapshots vor der Umstellung).
--   -> AB MORGEN (erster Snapshot mit v3.2) leer, bis der 29.08. erreicht ist.
--   Leer ist hier das RICHTIGE Ergebnis; die UI zeigt den Chip dann nicht an.
