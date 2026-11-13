-- ═══════════════════════════════════════════════════════════════════════════
-- Marktvolumen aus Sorares Verkaufshistorie (13.11.2026, Freigabe Jonas)
--
-- WARUM: Jonas: "Unsere Nutzer koennen nicht bis November 2027 warten." Sorare hat
-- eine blaetterbare, vollstaendige Historie:
--   anyPlayer(slug) { tokenPrices(rarity, seasonEligibility, since, first: 50, after) }
-- Getestet: Lamine Yamal Limited 13.11.2025-13.11.2026 = 1.848 Verkaeufe, lueckenlos,
-- mit Verkaufsart; seasonEligibility gilt zum VERKAUFSZEITPUNKT (bis Juli 2026 sind
-- die 2025er Karten In-Season, ab August die 2026er).
--
-- EINE QUELLE FUER ALLES: Vergangenheit UND laufende Tage kommen aus derselben
-- Abfrage. Zwei Methoden aneinanderzustueckeln haette an der Naht einen Sprung
-- erzeugt, der wie ein Markttrend aussieht. Sorares Historie ist sofort vollstaendig,
-- der 7-Tage-Verzug bei Classic entfaellt. market_volume_daily (Zaehlung im Updater)
-- wird von der Seite nicht mehr gelesen.
--
-- LAEUFT AUF RAILWAY IN HAEPPCHEN (Jonas kann den Laptop nicht laufen lassen):
-- Ein Cron-Dienst startet alle 15 Minuten tools/market-volume-history.mjs, das einige
-- Minuten arbeitet und endet. Der Fortschritt liegt hier:
--   market_volume_jobs         ein Auftrag = Datumsbereich + eingefrorene Spielerliste
--   market_volume_job_players  die Spielerliste (fest, damit Pakete stabil bleiben)
--   market_volume_batches      Pakete zu 100 Spielern: reserviert / erledigt
--   market_volume_hist         Tagessummen je Auftrag (chunk = Auftragsname)
-- mv_claim_batch() reserviert ein Paket (SKIP LOCKED, Reservierung verfaellt nach
-- 15 Minuten). mv_complete_batch() addiert das Ergebnis UND setzt den Erledigt-Vermerk
-- in EINER Transaktion und lehnt ein bereits erledigtes Paket ab. Ein abgebrochener
-- Lauf verliert also nur Zeit, und nichts wird doppelt gezaehlt.
--
-- AUFTRAEGE (Datumsbereiche ueberschneiden sich nicht; Summe = Volumen):
--   recent          [04.09.2026, 13.11.2026)   zuerst: Woche, Monat, Monatsvergleich
--   day:JJJJ-MM-TT  ein Tag ab 13.11.2026, angelegt erst nach Tagesende
--   year            [13.11.2025, 04.09.2026)   danach: Rest des Jahres
--
-- AUSFUEHREN: npx supabase db query --remote --yes --file "<absoluter Pfad>"
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.market_volume_hist (
  chunk       text          not null,               -- Auftragsname
  day         date          not null,               -- Verkaufstag, Europe/Berlin
  eligibility text          not null,               -- in_season | classic (zum Verkaufszeitpunkt)
  scarcity    text          not null,               -- limited | rare | super_rare
  deal_type   text          not null,               -- TokenOffer | TokenAuction | TokenPrimaryOffer
  sales       integer       not null,
  volume      numeric(14,2) not null,               -- EUR
  primary key (chunk, day, eligibility, scarcity, deal_type)
);
create index if not exists market_volume_hist_day_idx on public.market_volume_hist (day, eligibility);

create table if not exists public.market_volume_jobs (
  job        text        primary key,
  range_from date        not null,                  -- erster Tag (einschliesslich)
  range_to   date        not null,                  -- letzter Tag (ausschliesslich)
  priority   integer     not null,                  -- kleiner = zuerst
  players    integer     not null,
  batches    integer     not null,
  final      boolean     not null default false,    -- alle Pakete erledigt
  created_at timestamptz not null default now(),
  final_at   timestamptz
);

create table if not exists public.market_volume_job_players (
  job         text    not null,
  idx         integer not null,
  player_slug text    not null,
  primary key (job, idx)
);

create table if not exists public.market_volume_batches (
  job        text        not null,
  batch      integer     not null,
  claimed_at timestamptz,
  done_at    timestamptz,
  calls      integer,
  sales      integer,
  primary key (job, batch)
);

alter table public.market_volume_hist        enable row level security;
alter table public.market_volume_jobs        enable row level security;
alter table public.market_volume_job_players enable row level security;
alter table public.market_volume_batches     enable row level security;
revoke all on public.market_volume_hist, public.market_volume_jobs,
              public.market_volume_job_players, public.market_volume_batches
  from anon, authenticated;

-- ── Auftrag anlegen (einmalig; Spielerliste wird eingefroren) ──────────────
-- Spieler = alle mit mindestens einem bekannten Verkauf in card_prices, sortiert.
create or replace function public.mv_create_job(
  p_job text, p_from date, p_to date, p_priority int, p_batch_size int default 100)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare n integer; b integer;
begin
  if exists (select 1 from public.market_volume_jobs where job = p_job) then
    return 0;
  end if;
  insert into public.market_volume_job_players (job, idx, player_slug)
  select p_job, (row_number() over (order by player_slug))::int - 1, player_slug
  from (select distinct player_slug from public.card_prices where sale_1 is not null) x;
  get diagnostics n = row_count;
  b := ceil(n::numeric / p_batch_size)::int;
  insert into public.market_volume_batches (job, batch)
  select p_job, g from generate_series(0, b - 1) g;
  insert into public.market_volume_jobs (job, range_from, range_to, priority, players, batches)
  values (p_job, p_from, p_to, p_priority, n, b);
  return b;
end
$$;

-- ── Naechstes offenes Paket reservieren ────────────────────────────────────
-- Liefert Auftrag, Datumsbereich und die Spieler des Pakets; leer, wenn alles erledigt
-- oder gerade von einem anderen Lauf reserviert ist.
create or replace function public.mv_claim_batch(p_batch_size int default 100)
returns table (job text, batch int, range_from date, range_to date, players text[])
language plpgsql
security definer
set search_path = public
as $$
declare r record;
begin
  select b.job, b.batch, j.range_from, j.range_to into r
  from public.market_volume_batches b
  join public.market_volume_jobs j on j.job = b.job
  where b.done_at is null
    and (b.claimed_at is null or b.claimed_at < now() - interval '15 minutes')
  order by j.priority, j.range_from, b.batch
  limit 1
  for update of b skip locked;
  if not found then return; end if;
  update public.market_volume_batches
     set claimed_at = now()
   where market_volume_batches.job = r.job and market_volume_batches.batch = r.batch;
  return query
    select r.job, r.batch, r.range_from, r.range_to,
           array(select p.player_slug from public.market_volume_job_players p
                 where p.job = r.job and p.idx >= r.batch * p_batch_size
                   and p.idx < (r.batch + 1) * p_batch_size order by p.idx);
end
$$;

-- ── Paket abschliessen: addieren + Vermerk in EINER Transaktion ────────────
create or replace function public.mv_complete_batch(
  p_job text, p_batch int, p_calls int, p_rows jsonb)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare done timestamptz; n integer; s integer;
begin
  select done_at into done from public.market_volume_batches
  where job = p_job and batch = p_batch for update;
  if not found then raise exception 'Paket %/% unbekannt', p_job, p_batch; end if;
  if done is not null then return -1; end if;              -- schon erledigt: nichts addieren

  insert into public.market_volume_hist as h (chunk, day, eligibility, scarcity, deal_type, sales, volume)
  select p_job, (r->>'day')::date, r->>'eligibility', r->>'scarcity', r->>'deal_type',
         (r->>'sales')::int, round((r->>'volume')::numeric, 2)
  from jsonb_array_elements(coalesce(p_rows, '[]'::jsonb)) r
  where (r->>'sales')::int > 0
  on conflict (chunk, day, eligibility, scarcity, deal_type) do update
    set sales = h.sales + excluded.sales, volume = h.volume + excluded.volume;
  get diagnostics n = row_count;

  select coalesce(sum((r->>'sales')::int), 0) into s from jsonb_array_elements(coalesce(p_rows, '[]'::jsonb)) r;
  update public.market_volume_batches
     set done_at = now(), calls = p_calls, sales = s
   where job = p_job and batch = p_batch;

  if not exists (select 1 from public.market_volume_batches where job = p_job and done_at is null) then
    update public.market_volume_jobs set final = true, final_at = now() where job = p_job;
    delete from public.market_volume_job_players where job = p_job;   -- Liste nicht mehr noetig
  end if;
  return n;
end
$$;

revoke all on function public.mv_create_job(text, date, date, int, int) from public, anon, authenticated;
revoke all on function public.mv_claim_batch(int) from public, anon, authenticated;
revoke all on function public.mv_complete_batch(text, int, int, jsonb) from public, anon, authenticated;
grant execute on function public.mv_create_job(text, date, date, int, int) to service_role;
grant execute on function public.mv_claim_batch(int) to service_role;
grant execute on function public.mv_complete_batch(text, int, int, jsonb) to service_role;

-- ── Lesen: die Seite liest ab jetzt NUR die Historie ──────────────────────
-- Signatur und Rueckgabe unveraendert. `complete` = der Tag liegt in einem fertigen
-- Auftrag. Tage eines laufenden Auftrags kommen als Teilsumme mit complete = false
-- (die Seite schraffiert sie). `start_day` = fruehester Tag eines fertigen Auftrags.
create or replace function public.market_volume(
  p_elig     text,
  p_scarcity text default 'all',
  p_days     int  default 56
)
returns table (day date, sales int, volume numeric, mgr_sales int, mgr_volume numeric,
               complete boolean, start_day date)
language sql
stable
security definer
set search_path = public
as $$
  with t as (select (now() at time zone 'Europe/Berlin')::date as today),
       n as (select least(greatest(coalesce(p_days, 56), 7), 800) as days),
       fin as (select range_from, range_to from public.market_volume_jobs where final),
       agg as (
         select h.day,
                sum(h.sales)  as s,
                sum(h.volume) as v,
                sum(h.sales)  filter (where h.deal_type = 'TokenOffer') as ms,
                sum(h.volume) filter (where h.deal_type = 'TokenOffer') as mv
         from public.market_volume_hist h
         where h.eligibility = p_elig
           and (coalesce(p_scarcity, 'all') = 'all' or h.scarcity = p_scarcity)
           and h.day >= (select today from t) - (select days from n)
         group by h.day
       )
  select a.day,
         a.s::int,
         round(a.v, 2),
         coalesce(a.ms, 0)::int,
         round(coalesce(a.mv, 0), 2),
         exists (select 1 from fin where a.day >= fin.range_from and a.day < fin.range_to),
         (select min(range_from) from fin)
  from agg a
  order by a.day
$$;

revoke all on function public.market_volume(text, text, int) from public, anon, authenticated;
grant execute on function public.market_volume(text, text, int) to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
select 'funktionen' as pruefung, string_agg(proname, ', ' order by proname) as wert
from pg_proc p join pg_namespace ns on ns.oid = p.pronamespace
where ns.nspname = 'public'
  and proname in ('mv_create_job', 'mv_claim_batch', 'mv_complete_batch', 'market_volume');
