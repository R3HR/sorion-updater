-- ═══════════════════════════════════════════════════════════════════════════
-- (a) fmv_accuracy wiederherstellen (INC-004)  +
-- (b) "Players Tracked" in den Tages-Snapshot verlegen (BUG-017, Teil 2)  +
-- (c) redundanten Index entsorgen                              (20.08.2026)
--
-- (a) Die Tabelle wurde ausserhalb der Migrationen geloescht (INC-004, nicht
--     beabsichtigt — Entscheidung Jonas 20.08.: Accuracy soll weiter getrackt
--     werden). Exakt die Definition vom 22.07.; der Updater erkennt die
--     Tabelle per Probe von selbst wieder und loggt ab dem naechsten Lauf.
--     Die Historie seit 22.07. ist verloren; die 30-Tage-Anzeige fuellt sich
--     von selbst (UI zeigt erst ab 10 Samples je Zelle etwas an).
--
-- (b) Die Players-Tracked-Zaehlung riss auch MIT idx_cp_visible die 3-s-Grenze
--     (gemessen 20.08.: 500 nach 3,5 s): auf der IO-gedrosselten Nano-Instanz
--     ist selbst ein Index-Scan ueber ~73k Eintraege zu teuer, weil die stark
--     beschriebene Tabelle staendig Heap-Nachschlaege erzwingt. Daher: die
--     Zahl wandert als visible_n in den Tages-Snapshot (gleiche Definition
--     wie bisher: FMV ODER Floor ODER Sale vorhanden — Kontinuitaet der
--     angezeigten Zahl). market_overview liefert sie mit; das Frontend
--     braucht KEINE Live-Zaehlung mehr.
--
-- (c) idx_card_prices_elig_scarcity_fmv (partiell, WHERE fmv is not null):
--     letzter Nutzer war die Live-Aggregation in market_overview — die gibt
--     es seit dem Snapshot-Umbau nicht mehr. market_facets/market_leagues
--     fahren mit dem Voll-Index idx_cp_elig_sc_fmv_all. Weg damit: spart
--     Platz und vor allem Schreiblast (der Updater pflegt ~150 Zeilen/min).
-- ═══════════════════════════════════════════════════════════════════════════

-- ── (a) fmv_accuracy + View, Definition vom 22.07. ─────────────────────────
create table if not exists public.fmv_accuracy (
  id          bigint generated always as identity primary key,
  player_slug text not null,
  scarcity    text not null,
  eligibility text not null default 'in_season',
  fmv_est     numeric not null,   -- unser FMV VOR dem Verkauf (kein Leakage)
  sale_price  numeric not null,
  delta_pct   numeric not null,   -- signiert: (sale - fmv) / fmv * 100
  est_at      timestamptz,
  sale_at     timestamptz not null,
  hours_gap   numeric,
  created_at  timestamptz not null default now()
);
create index if not exists fmv_accuracy_created_idx on public.fmv_accuracy (created_at);
create index if not exists fmv_accuracy_elig_idx    on public.fmv_accuracy (eligibility, scarcity);
alter table public.fmv_accuracy enable row level security;
drop policy if exists "fmv_accuracy_public_read" on public.fmv_accuracy;
create policy "fmv_accuracy_public_read" on public.fmv_accuracy for select using (true);
grant select on public.fmv_accuracy to anon, authenticated;

create or replace view public.fmv_accuracy_stats with (security_invoker = true) as
select eligibility, scarcity,
       count(*)::int as samples,
       round((percentile_cont(0.5) within group (order by abs(delta_pct)))::numeric, 1) as median_abs_delta,
       round(avg(delta_pct)::numeric, 1) as bias
from public.fmv_accuracy
where created_at > now() - interval '30 days' and hours_gap < 48
group by 1, 2;
grant select on public.fmv_accuracy_stats to anon, authenticated;

-- ── (b) visible_n in den Snapshot ──────────────────────────────────────────
alter table public.market_daily add column if not exists visible_n int;

-- Gleiche avg/median/n-Semantik wie vorher (Aggregate ignorieren NULL-fmv),
-- nur die Grundmenge ist jetzt "sichtbar" und count(*) darueber = visible_n.
create or replace function public.snapshot_market_daily(p_day date default current_date)
returns int
language plpgsql
security definer
set search_path = public
as $$
declare rows_written int;
begin
  insert into public.market_daily (day, scarcity, eligibility, avg_fmv, median_fmv, n, visible_n)
  select p_day,
         cp.scarcity,
         coalesce(cp.eligibility, 'in_season'),
         round(avg(cp.fmv)::numeric, 4),
         round((percentile_cont(0.5) within group (order by cp.fmv))::numeric, 4),
         count(cp.fmv)::int,
         count(*)::int
  from public.card_prices cp
  where cp.fmv is not null or cp.floor_price is not null or cp.sale_1 is not null
  group by cp.scarcity, coalesce(cp.eligibility, 'in_season')
  on conflict (day, scarcity, eligibility) do update
    set avg_fmv    = excluded.avg_fmv,
        median_fmv = excluded.median_fmv,
        n          = excluded.n,
        visible_n  = excluded.visible_n;
  get diagnostics rows_written = row_count;
  return rows_written;
end $$;
revoke all on function public.snapshot_market_daily(date) from public;

-- market_overview bekommt eine 6. Spalte -> Rueckgabetyp aendert sich -> drop
-- (ohne CASCADE: unerwartete Abhaengigkeiten sollen laut scheitern).
drop function if exists public.market_overview(text);
create function public.market_overview(p_elig text default 'in_season')
returns table (scarcity text, cards int, avg_fmv numeric, median_fmv numeric, median_as_of date, visible_n int)
language sql stable security definer set search_path = public as $$
  select distinct on (md.scarcity)
         md.scarcity, md.n, round(md.avg_fmv, 2), md.median_fmv, md.day, md.visible_n
  from public.market_daily md
  where md.eligibility = p_elig
  order by md.scarcity, md.day desc
$$;
revoke execute on function public.market_overview(text) from public;
grant  execute on function public.market_overview(text) to anon, authenticated;

-- Heutigen Snapshot sofort fuellen (laeuft als Editor-Rolle ohne anon-Timeout;
-- ab morgen pflegt ihn der Harvester um 05:30 wie gehabt)
select public.snapshot_market_daily(current_date);

-- ── (c) redundanten Index entsorgen ────────────────────────────────────────
drop index if exists public.idx_card_prices_elig_scarcity_fmv;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- select * from market_overview('in_season');
--   -> 3 Zeilen, visible_n gefuellt (Summe ~22k), day = heute
-- select count(*) from fmv_accuracy;   -> 0 (fuellt sich ab dem naechsten Updater-Lauf)
