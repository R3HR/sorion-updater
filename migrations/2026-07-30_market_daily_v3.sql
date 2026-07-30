-- ============================================================================
-- v3 (30.07.) — DIE maßgebliche Migration für die Avg-FMV-Bewegung.
-- Ersetzt die beiden Fehlversuche desselben Tages (market_avg_history, market_move v2).
--
-- Kernproblem: `price_history` ist KEIN Tages-Snapshot des Markts, sondern ein
-- Änderungs-Log — eine Zeile entsteht nur, wenn der Updater sie in dem Lauf anfasst
-- (voller Sweep dauert Tage). Jeder "Tagesdurchschnitt" daraus ist eine rotierende
-- Stichprobe (n schwankte 537…5102, Avg sprang 6,38…25,96 €) → nicht vergleichbar.
--
-- Richtige Lösung (Vorgabe Jonas: realer Wert, tagesaktuell):
-- `card_prices` hält für JEDE (player, scarcity, eligibility) den aktuellen FMV —
-- das ist der echte Gesamtmarkt und genau die Basis der angezeigten Avg-Zahl.
-- Also: 1× täglich diesen Vollmarkt-Durchschnitt in eine winzige Tabelle schreiben
-- und Tag-gegen-Tag vergleichen. Vollmarkt vs. Vollmarkt, konsistent mit der UI,
-- und die Leseabfrage kostet nichts (paar Hundert Zeilen statt 1,6 Mio).
-- ============================================================================

-- ── 1) Snapshot-Tabelle (winzig: 6 Zeilen/Tag) ──────────────────────────────
create table if not exists public.market_daily (
  day         date    not null,
  scarcity    text    not null,
  eligibility text    not null,
  avg_fmv     numeric,
  median_fmv  numeric,
  n           int,
  primary key (day, scarcity, eligibility)
);

alter table public.market_daily enable row level security;
drop policy if exists market_daily_read on public.market_daily;
create policy market_daily_read on public.market_daily for select to anon, authenticated using (true);
grant select on public.market_daily to anon, authenticated;

-- ── 2) Tages-Snapshot aus card_prices (der "reale Wert") ────────────────────
-- Idempotent: mehrfacher Aufruf am selben Tag überschreibt den Tageswert.
create or replace function public.snapshot_market_daily(p_day date default current_date)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare rows_written int;
begin
  insert into public.market_daily (day, scarcity, eligibility, avg_fmv, median_fmv, n)
  select p_day,
         cp.scarcity,
         coalesce(cp.eligibility, 'in_season'),
         round(avg(cp.fmv)::numeric, 4),
         round((percentile_cont(0.5) within group (order by cp.fmv))::numeric, 4),
         count(*)::int
  from public.card_prices cp
  where cp.fmv is not null
  group by cp.scarcity, coalesce(cp.eligibility, 'in_season')
  on conflict (day, scarcity, eligibility) do update
    set avg_fmv = excluded.avg_fmv,
        median_fmv = excluded.median_fmv,
        n = excluded.n;
  get diagnostics rows_written = row_count;
  return rows_written;
end $$;

revoke all on function public.snapshot_market_daily(date) from public;
-- Nur der Service-Key (Harvester-Cron) darf schreiben — anon/authenticated ausdrücklich NICHT.

-- ── 3) Heutigen Snapshot sofort setzen (damit der Vergleich morgen greift) ───
select public.snapshot_market_daily(current_date);

-- ── 4) Rückwirkende Auffüllung: Vollmarkt je Tag aus price_history ──────────
-- Für jeden Tag D wird pro Spieler der ZULETZT bekannte Preis <= D genommen
-- (Carry-Forward). Das rekonstruiert einen Vollmarkt-Stand — dieselbe Logik, nach
-- der auch card_prices "aktuell" ist — statt nur der an D geschriebenen Teilmenge.
-- Damit funktioniert der 7d-Chip sofort und nicht erst in einer Woche.
create index if not exists idx_price_history_group_player_day
  on public.price_history (scarcity, eligibility, player_slug, recorded_at desc);

insert into public.market_daily (day, scarcity, eligibility, avg_fmv, n)
select d.day, g.sc, g.el,
       round(avg(x.price)::numeric, 4),
       count(*)::int
from (select generate_series(current_date - 14, current_date - 1, interval '1 day')::date as day) d
cross join (values ('limited','in_season'), ('limited','classic'),
                   ('rare','in_season'),    ('rare','classic'),
                   ('super_rare','in_season'), ('super_rare','classic')) g(sc, el)
cross join lateral (
  select distinct on (ph.player_slug) ph.price
  from public.price_history ph
  where ph.scarcity = g.sc
    and ph.eligibility = g.el
    and ph.recorded_at <= d.day
    and ph.recorded_at >= d.day - 45      -- veraltete Preise nicht endlos mitschleppen
  order by ph.player_slug, ph.recorded_at desc
) x
group by 1, 2, 3
on conflict (day, scarcity, eligibility) do update
  set avg_fmv = excluded.avg_fmv, n = excluded.n;

-- ── 5) Lese-RPC für die UI (Signatur bleibt: pct / players / days_gap) ──────
-- Vergleicht den neuesten Snapshot mit dem nächstgelegenen Snapshot <= (neuester - p_days).
-- Verlangt vergleichbare Stichprobengrößen (±25 %), damit ein unvollständiger Tag
-- niemals als Marktbewegung durchgeht.
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
  with d as (select least(greatest(coalesce(p_days,7), 2), 90) as gap),
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
    order by md.day desc limit 1
  )
  select round((((cur.avg_fmv - ref.avg_fmv) / nullif(ref.avg_fmv, 0)) * 100)::numeric, 1),
         cur.n,
         (cur.day - ref.day)::int
  from cur, ref
  where abs(cur.n - ref.n)::numeric / greatest(ref.n, 1) < 0.25
$$;

revoke all on function public.market_move(text, text, int) from public;
grant execute on function public.market_move(text, text, int) to anon, authenticated;

-- Alte Fehlversuch-Funktion entfernen
drop function if exists public.market_avg_history(text, text, int);

-- ── Verifikation ────────────────────────────────────────────────────────────
-- a) Snapshots vorhanden?
--    select day, scarcity, eligibility, avg_fmv, n from market_daily order by day desc limit 12;
-- b) RPC (soll sofort antworten):
--    curl -X POST .../rest/v1/rpc/market_move -H "Content-Type: application/json" \
--      -d '{"p_scarcity":"limited","p_elig":"in_season","p_days":7}'
--    -> [{"pct":-2.4,"players":18320,"days_gap":7}]   (leer = noch kein vergleichbarer Vortag)
