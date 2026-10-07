-- ═══════════════════════════════════════════════════════════════════════════
-- Dauerhaftes Marktgedaechtnis: Wochenwerte je Karte  (07.10.2026)
--
-- PROBLEM: snapshot_fmv_accuracy loescht Einzelverkaeufe aelter als 90 Tage,
-- price_history_rollup duennt unseren FMV nach 90 Tagen auf einen Punkt je Woche
-- aus. In einem Jahr koennten wir also sagen, was wir DACHTEN, aber nicht mehr,
-- was eine Karte tatsaechlich gekostet hat. Genau das ist aber der einzige
-- formelunabhaengige Beleg: unser FMV hat ueber ein Jahr mehrere Versionen
-- (v3.2 bis v3.7), echte Verkaufspreise haben keine.
--
-- LOESUNG: je Karte und Kalenderwoche eine Zeile mit den Eckdaten der echten
-- Verkaeufe. Nur rund 14.500 Karten werden pro Woche gehandelt, das sind etwa
-- 750.000 Zeilen im Jahr. Die Rohverkaeufe (100.000/Woche) duerfen weiter nach
-- 90 Tagen verschwinden.
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.card_sales_weekly (
  week          date    not null,          -- Montag der Kalenderwoche
  player_slug   text    not null,
  scarcity      text    not null,
  eligibility   text    not null,
  mgr_n         int     not null default 0, -- Verkaeufe zwischen Managern
  mgr_median    numeric,
  mgr_min       numeric,
  mgr_max       numeric,
  all_n         int     not null default 0, -- alle Verkaufsarten, als Kontext
  all_median    numeric,
  updated_at    timestamptz not null default now(),
  primary key (week, player_slug, scarcity, eligibility)
);

comment on table public.card_sales_weekly is
  'Dauerhaftes Marktgedaechtnis: tatsaechliche Verkaufspreise je Karte und Woche. Formelunabhaengig, wird NICHT aufgeraeumt.';

create index if not exists card_sales_weekly_card_idx
  on public.card_sales_weekly (player_slug, scarcity, eligibility, week);

-- Zugriff bleibt vorerst intern (wie price_history gegen Bulk-Abgriff geschuetzt).
alter table public.card_sales_weekly enable row level security;
revoke all on public.card_sales_weekly from anon, authenticated;

-- Verdichtung. Idempotent: derselbe Zeitraum darf beliebig oft laufen.
create or replace function public.rollup_card_sales_weekly(p_days int default 4)
returns int
language plpgsql
security definer
set search_path to 'public'
as $$
declare n int;
begin
  insert into public.card_sales_weekly as w
    (week, player_slug, scarcity, eligibility, mgr_n, mgr_median, mgr_min, mgr_max, all_n, all_median, updated_at)
  select date_trunc('week', sale_at)::date,
         player_slug, scarcity, eligibility,
         count(*) filter (where deal_type = 'TokenOffer'),
         round(percentile_cont(0.5) within group (order by sale_price)
               filter (where deal_type = 'TokenOffer')::numeric, 2),
         round(min(sale_price) filter (where deal_type = 'TokenOffer')::numeric, 2),
         round(max(sale_price) filter (where deal_type = 'TokenOffer')::numeric, 2),
         count(*),
         round(percentile_cont(0.5) within group (order by sale_price)::numeric, 2),
         now()
  from public.fmv_accuracy
  where sale_at >= current_date - p_days and sale_price > 0
  group by 1, 2, 3, 4
  on conflict (week, player_slug, scarcity, eligibility) do update
    set mgr_n = excluded.mgr_n, mgr_median = excluded.mgr_median,
        mgr_min = excluded.mgr_min, mgr_max = excluded.mgr_max,
        all_n = excluded.all_n, all_median = excluded.all_median,
        updated_at = now();
  get diagnostics n = row_count;
  return n;
end $$;

-- Formelstaende dokumentieren: ohne das vergleicht ein Jahresvergleich auch
-- unsere eigenen Versionen miteinander. Die Schnittkanten standen bisher nur
-- im Code von market_move.
create table if not exists public.fmv_versions (
  version    text primary key,
  live_from  date not null,
  note       text
);
revoke all on public.fmv_versions from anon;
grant select on public.fmv_versions to anon, authenticated;

insert into public.fmv_versions (version, live_from, note) values
  ('v3.2', date '2026-08-22', 'Floor nur noch als Notbremse, kein Mischpartner'),
  ('v3.3', date '2026-08-26', 'Sicherheitsdeckel nur bei duenner oder alter Basis'),
  ('v3.5', date '2026-09-12', 'Nur Manager-Verkaeufe; v3.4 und v3.6 haben nie einen eigenen Snapshot erzeugt (INC-009)'),
  ('v3.7', date '2026-10-05', 'Trendnachfuehrung bei einseitiger Verkaufslage')
on conflict (version) do nothing;

-- ── Nachtragen, was noch da ist (seit 17.08.) ──────────────────────────────
select public.rollup_card_sales_weekly(400) as nachgetragene_zeilen;
