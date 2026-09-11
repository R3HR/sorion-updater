-- ═══════════════════════════════════════════════════════════════════════════
-- DRITTE Schnittkante: FMV v3.5 (nur Manager-Verkaeufe), erste Berechnung 12.09.2026
--
-- WICHTIG, Befund vom 11.09. (INC-009): v3.4 und v3.5 haben bis heute NIE eine
-- Karte berechnet. Die Preisaktualisierung stand vom 06. bis 11.09. still, weil
-- ein '//' im GraphQL-Query jede Abfrage ungueltig machte. Die angezeigten Werte
-- stammen deshalb alle noch aus v3.3. Es gibt also keinen v3.4-Uebergang in den
-- Daten, und die frueher geplante Kante 08.09. ist gegenstandslos.
--
-- WARUM 12.09.: Die Updater sind Cron-Jobs (22:00-05:00 UTC), der Tages-Snapshot
-- entsteht um 05:30 UTC. Laeuft der reparierte Updater in der Nacht vom 11. auf
-- den 12.09., ist der Snapshot vom 12.09. der erste mit v3.5-Werten.
--
-- !! WIRD DER FIX SPAETER GEPUSHT, MUSS DAS DATUM MITWANDERN !! Eine zu frueh
-- gesetzte Kante sperrt den Chip nur unnoetig ("leer statt falsch"), eine
-- fehlende zeigt den Versionssprung als Marktbewegung. Deshalb im Zweifel setzen.
--
-- Der Sprung wird gross: v3.3 rechnete mit allen Verkaufsarten, v3.5 nur mit
-- Manager-Verkaeufen, und die Werte sind zusaetzlich 5 Tage alt.
--
-- AUSFUEHREN: npx supabase db query --linked --file "<absoluter Pfad>"
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
  -- Formel-Schnittkanten: v3.2 22.08., v3.3 26.08., v3.5 12.09.
  with cuts as (select d from (values
      (date '2026-08-22'), (date '2026-08-26'), (date '2026-09-12')
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
