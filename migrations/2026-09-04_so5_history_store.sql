-- ═══════════════════════════════════════════════════════════════════════════
-- SO5-Historie dauerhaft speichern  (04.09.2026)
--
-- WARUM: Bisher holt die Edge Function jede Gameweek bei jedem Besuch neu von
-- Sorare (nur Memory-/Browser-Cache). Das begrenzt die Reichweite auf ~14
-- Monate, weil das API-Kontingent (200/min, geteilt mit den Preis-Updatern)
-- sonst reisst. Mit eigener Ablage wird jede (Manager, Gameweek) **weltweit
-- genau einmal** geholt — danach kommen alle Besucher sofort dran.
--
-- ZWEITER, GROESSERER NUTZEN (Ziel Jonas): Wir koennen kuenftig fuer JEDE
-- Karte ausweisen, was sie **jemals** erspielt hat — ueber alle Manager
-- hinweg. Dafuer liegt neben den Roh-Aufstellungen eine Zeile je Karte und
-- Aufstellung mit dem bereits verteilten Ertrag.
--
-- SCHREIBLAST (Lehre INC-005/006): Abgeschlossene Gameweeks sind
-- UNVERAENDERLICH — jede Zeile wird genau einmal geschrieben und danach nur
-- noch gelesen. Kein Cron, keine periodische Last, keine Updates im Betrieb.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

-- Neuanlage: die erste Fassung hatte einen zu schwachen Schluessel (siehe
-- Kommentar unten). Tabellen sind frisch/leer, deshalb einfach ersetzen.
drop table if exists public.so5_card_earnings;
drop table if exists public.so5_lineups;

-- ── 1) Aufstellungen (Rohdaten, eine Zeile je Manager/GW/Wettbewerb) ───────
-- lineup_id: Sorares eigene Aufstellungs-ID. NOETIG, weil ein Manager im
-- SELBEN Wettbewerb mehrere Aufstellungen haben kann (jr3hr hatte in GW9 zwei
-- "Bundesliga – Limited") — ohne sie kollidiert der Schluessel und der ganze
-- Stapel-Insert scheitert.
create table if not exists public.so5_lineups (
  lineup_id      text        not null,
  manager_slug   text        not null,
  fixture_slug   text        not null,
  leaderboard    text        not null,
  ranking        int,
  ranking_ratio  int,                    -- Perzentil (Top x %)
  score          numeric,
  rewards        jsonb       not null default '[]'::jsonb,
  players        jsonb       not null default '[]'::jsonb,
  fetched_at     timestamptz not null default now(),
  primary key (lineup_id)
);
create index if not exists idx_so5_lineups_manager on public.so5_lineups (manager_slug, fixture_slug);

-- ── 2) Ertrag je KARTE und Aufstellung (die Auswertungs-Ebene) ────────────
-- Der Ertrag ist hier bereits nach Punkteanteil verteilt (Regel IDEA-001:
-- nur zaehlende Spieler, Anteil = Punkte / Summe). Damit sind spaetere
-- Fragen ("was hat diese Karte jemals gebracht?", "welche Karte ist die
-- ertragreichste ueberhaupt?") eine simple Summe statt einer JSON-Wanderung.
create table if not exists public.so5_card_earnings (
  card_slug      text        not null,
  lineup_id      text        not null,
  manager_slug   text        not null,
  fixture_slug   text        not null,
  leaderboard    text        not null,
  player_name    text,
  card_rarity    text,
  card_serial    int,
  card_season    int,
  score          numeric,                -- Punkte dieser Karte
  share          numeric,                -- Anteil an der Aufstellung (0..1)
  essence        jsonb       not null default '{}'::jsonb,  -- {"limited":123.4,"rare":5.6}
  currency       jsonb       not null default '{}'::jsonb,  -- {"LIMITED_XP":2000}
  cash_eur       numeric     not null default 0,
  primary key (card_slug, lineup_id)
);
create index if not exists idx_so5_earn_card    on public.so5_card_earnings (card_slug);
create index if not exists idx_so5_earn_manager on public.so5_card_earnings (manager_slug);

-- ── 3) Zugriff: oeffentlich LESBAR, Schreiben nur per Service-Key ─────────
-- Die Daten sind bei Sorare ohnehin oeffentlich; geschrieben wird
-- ausschliesslich von der Edge Function so5-results.
alter table public.so5_lineups        enable row level security;
alter table public.so5_card_earnings  enable row level security;
drop policy if exists so5_lineups_read on public.so5_lineups;
drop policy if exists so5_earn_read    on public.so5_card_earnings;
create policy so5_lineups_read on public.so5_lineups
  for select to anon, authenticated using (true);
create policy so5_earn_read on public.so5_card_earnings
  for select to anon, authenticated using (true);
grant select on public.so5_lineups       to anon, authenticated;
grant select on public.so5_card_earnings to anon, authenticated;
revoke insert, update, delete on public.so5_lineups       from anon, authenticated;
revoke insert, update, delete on public.so5_card_earnings from anon, authenticated;

-- ── 4) Was ist fuer diesen Manager schon gespeichert? ─────────────────────
-- Die Function fragt das ab, um nur die FEHLENDEN Gameweeks bei Sorare zu holen.
create or replace function public.so5_known_fixtures(p_manager text)
returns table (fixture_slug text)
language sql stable security definer set search_path = public as $$
  select distinct l.fixture_slug from public.so5_lineups l
  where l.manager_slug = lower(p_manager)
$$;
revoke all on function public.so5_known_fixtures(text) from public;
grant execute on function public.so5_known_fixtures(text) to anon, authenticated;

-- ── 5) Ertrag je Karte fuer einen Manager (das Portfolio liest genau das) ──
create or replace function public.so5_earnings_by_card(p_manager text)
returns table (
  card_slug   text,
  player_name text,
  lineups     int,
  essence     jsonb,
  currency    jsonb,
  cash_eur    numeric
)
language sql stable security definer set search_path = public as $$
  with e as (
    select * from public.so5_card_earnings where manager_slug = lower(p_manager)
  ),
  ess as (   -- JSONB-Summen je Karte
    select e.card_slug, k.key, sum((k.value)::numeric) as v
    from e, lateral jsonb_each_text(e.essence) k group by 1, 2
  ),
  cur as (
    select e.card_slug, k.key, sum((k.value)::numeric) as v
    from e, lateral jsonb_each_text(e.currency) k group by 1, 2
  )
  select e.card_slug,
         max(e.player_name),
         count(*)::int,
         coalesce((select jsonb_object_agg(key, round(v, 1)) from ess where ess.card_slug = e.card_slug), '{}'::jsonb),
         coalesce((select jsonb_object_agg(key, round(v, 0)) from cur where cur.card_slug = e.card_slug), '{}'::jsonb),
         round(sum(e.cash_eur), 2)
  from e group by e.card_slug
$$;
revoke all on function public.so5_earnings_by_card(text) from public;
grant execute on function public.so5_earnings_by_card(text) to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Direkt nach der Migration leer; fuellt sich, sobald jemand seine Historie
-- oeffnet (die Edge Function schreibt beim Abruf mit).
select count(*) as lineups from public.so5_lineups;
select count(*) as card_rows from public.so5_card_earnings;
