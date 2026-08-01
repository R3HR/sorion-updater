-- Marktseite serverseitig (02.08.) — Egress von ~15 MB auf ~50 KB pro Besuch
--
-- PROBLEM: Die Marktseite laedt ALLE bewerteten Zeilen (104.755) in den Browser,
-- um dort zu filtern, zu sortieren und Aggregate zu rechnen. Das sind ~15 MB gzip
-- und 199 HTTP-Anfragen PRO BESUCH — bei 5 GB Free-Kontingent reichen das fuer
-- rund 340 Aufrufe im Monat. Gemessen 02.08.
--
-- LOESUNG: Die Seite braucht pro Ansicht nur 50 Zeilen plus ein paar Aggregate.
-- Filtern/Sortieren/Blaettern uebernimmt PostgREST direkt auf card_prices; die
-- Aggregate liefern die drei Funktionen hier.
--
-- Alle Funktionen sind LESEND und liefern nur Aggregate — keine personenbezogenen
-- Daten, daher wie die Markttabelle oeffentlich lesbar.

-- ── 1) Liga-Ranking (ersetzt die Client-Aggregation ueber alle Zeilen) ──────
-- Gruppiert nach LAND + Liga: Sorare nennt AT und DE beide "Bundesliga".
create or replace function public.market_leagues(
  p_elig     text default 'in_season',
  p_scarcity text default null            -- null = alle Rarities
)
returns table (league_name text, league_country text, total_fmv numeric, cards int)
language sql stable security definer set search_path = public as $$
  select cp.league_name,
         cp.league_country,
         round(sum(cp.fmv)::numeric, 2) as total_fmv,
         count(*)::int                  as cards
  from public.card_prices cp
  where cp.fmv is not null
    and cp.league_name is not null
    and coalesce(cp.eligibility, 'in_season') = p_elig
    and (p_scarcity is null or cp.scarcity = p_scarcity)
  group by cp.league_name, cp.league_country
  order by 3 desc
$$;

-- ── 2) Filter-Listen fuer die Auswahlfelder (Clubs, Ligen, Positionen) ──────
-- Klein und gut cachebar; ersetzt das Ableiten aus dem Vollbestand im Browser.
create or replace function public.market_facets(p_elig text default 'in_season')
returns table (kind text, value text, extra text, cards int)
language sql stable security definer set search_path = public as $$
  select 'club'::text, cp.team_name, null::text, count(*)::int
  from public.card_prices cp
  where cp.fmv is not null and cp.team_name is not null
    and coalesce(cp.eligibility,'in_season') = p_elig
  group by cp.team_name
  union all
  select 'league', cp.league_name, cp.league_country, count(*)::int
  from public.card_prices cp
  where cp.fmv is not null and cp.league_name is not null
    and coalesce(cp.eligibility,'in_season') = p_elig
  group by cp.league_name, cp.league_country
  union all
  select 'position', cp.position, null::text, count(*)::int
  from public.card_prices cp
  where cp.fmv is not null and cp.position is not null
    and coalesce(cp.eligibility,'in_season') = p_elig
  group by cp.position
$$;

-- ── 3) Kennzahlen der Kopfzeile (ersetzt das Zaehlen im Browser) ────────────
create or replace function public.market_overview(p_elig text default 'in_season')
returns table (scarcity text, cards int, avg_fmv numeric, median_fmv numeric)
language sql stable security definer set search_path = public as $$
  select cp.scarcity,
         count(*)::int,
         round(avg(cp.fmv)::numeric, 2),
         round((percentile_cont(0.5) within group (order by cp.fmv))::numeric, 2)
  from public.card_prices cp
  where cp.fmv is not null
    and coalesce(cp.eligibility,'in_season') = p_elig
  group by cp.scarcity
$$;

-- ── Rechte ─────────────────────────────────────────────────────────────────
revoke execute on function public.market_leagues(text,text)  from public;
revoke execute on function public.market_facets(text)        from public;
revoke execute on function public.market_overview(text)      from public;
grant  execute on function public.market_leagues(text,text)  to anon, authenticated;
grant  execute on function public.market_facets(text)        to anon, authenticated;
grant  execute on function public.market_overview(text)      to anon, authenticated;

-- ── Index fuer die serverseitige Tabelle (Filter + Sortierung) ─────────────
-- Die Tabelle sortiert standardmaessig nach fmv desc und filtert nach
-- eligibility + scarcity. Ohne Index waere das ein Full-Scan je Seitenaufruf.
create index if not exists idx_card_prices_elig_scarcity_fmv
  on public.card_prices (eligibility, scarcity, fmv desc nulls last)
  where fmv is not null;

-- Verifikation:
--   select * from market_overview('in_season');
--   select * from market_leagues('in_season','limited') limit 5;
--   select kind, count(*) from market_facets('in_season') group by 1;
