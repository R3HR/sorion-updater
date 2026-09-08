-- ═══════════════════════════════════════════════════════════════════════════
-- VIERTE Schnittkante: FMV v3.5 (nur Manager-Verkaeufe), Deploy 08.09.2026
--
-- HINTERGRUND: v3.5 laesst Auktionen und Sofortkauf aus der Bewertung fallen
-- (Vorgabe Jonas). Die angezeigten Werte verschieben sich dadurch einmalig,
-- ohne dass sich der Markt bewegt hat. Der 7-Tage-Chip darf keine Snapshots
-- ueber die Umstellung hinweg vergleichen.
--
-- WARUM 09.09. UND NICHT 08.09.: Die Updater sind Cron-Jobs (22:00-05:00 UTC).
-- Der Snapshot vom 08.09. entstand um 05:30 UTC und traegt bereits v3.4-Werte
-- (v3.4 ging am 07.09. live). Laeuft der v3.5-Push heute vor 22:00 UTC, ist der
-- erste Snapshot mit v3.5-Werten der vom 09.09.
--
-- !! WIRD SPAETER GEPUSHT, MUSS DAS DATUM MITWANDERN !! Sonst vergleicht der
-- Chip v3.4 gegen v3.5. Kanten bisher: v3.2 22.08., v3.3 26.08., v3.4 08.09.
--
-- REIHENFOLGE: erst diese Migration, dann lib/fmv.mjs pushen.
-- AUSFUEHREN: npx supabase db query --linked --file migrations/2026-09-08_fmv_v35_change_guard.sql
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
  -- Formel-Schnittkanten: v3.2 22.08., v3.3 26.08., v3.4 08.09., v3.5 09.09.
  with cuts as (select d from (values
      (date '2026-08-22'), (date '2026-08-26'), (date '2026-09-08'), (date '2026-09-09')
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
-- Vor dem 09.09.-Snapshot aendert diese Kante nichts (alle vorhandenen Snapshots
-- liegen davor). Ab dem 09.09. ist der Chip fuer p_days Tage leer — das ist gewollt.
select 'limited/in_season' as segment, * from public.market_move('limited','in_season',7)
union all
select 'rare/in_season', * from public.market_move('rare','in_season',7);
