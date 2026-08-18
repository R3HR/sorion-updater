-- ═══════════════════════════════════════════════════════════════════════════
-- Marktseite serverseitig: RPCs schnell machen  (18.08.2026)
--
-- BEFUND (gemessen 18.08.): market_overview, market_leagues und market_facets
-- liefen ALLE in den statement_timeout (4,0-5,2 s, HTTP 57014). Die Marktseite
-- konnte deshalb nie auf Server-Aggregation umgestellt werden und laedt weiter
-- ~15 MB pro Besuch.
--
-- URSACHE: Jede Funktion filterte mit
--     coalesce(cp.eligibility, 'in_season') = p_elig
-- Ein Funktionsaufruf auf der Spalte macht den Index
--     idx_card_prices_elig_scarcity_fmv (eligibility, scarcity, fmv desc)
-- unbenutzbar -> Full Scan ueber ~122.000 Zeilen bei JEDEM Aufruf.
-- Das coalesce schuetzte dabei vor einem Fall, den es nicht gibt: geprueft am
-- 18.08. hat die Tabelle **0 Zeilen mit eligibility IS NULL** (der Schluessel
-- ist slug x scarcity x eligibility, der Updater setzt die Spalte immer).
--
-- Ohne coalesce trifft der Index: dieselbe Filterung zaehlte 19.866 Zeilen in
-- 915 ms, mit Sortierung + Limit in 523 ms.
--
-- ZWEITE URSACHE (nur market_overview): percentile_cont(0.5) sortiert je
-- Gruppe ALLE Zeilen. Der Median kommt jetzt aus dem Tagessnapshot
-- market_daily (dort wird er ohnehin taeglich berechnet) statt live.
-- Das ersetzt die nie eingespielte Migration 2026-08-02_market_overview_fix.sql
-- -- diese Datei hier gilt, die alte kann ignoriert werden.
--
-- ACHTUNG Semantik: Ohne coalesce wuerden Zeilen mit eligibility IS NULL nicht
-- mehr mitgezaehlt. Das ist heute wirkungslos (0 Zeilen) und beabsichtigt --
-- eine Zeile ohne Eligibility waere ein Datenfehler, kein In-Season-Fall.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Ligen-Ranking ───────────────────────────────────────────────────────
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
    and cp.eligibility = p_elig
    and (p_scarcity is null or cp.scarcity = p_scarcity)
  group by cp.league_name, cp.league_country
  order by 3 desc
$$;

-- ── 2) Filter-Listen (Club / Liga / Position) ──────────────────────────────
-- EIN Tabellendurchlauf statt drei. Vorher war es dreimal dasselbe union all
-- ueber dieselben Zeilen. Gemessen 18.08.: ein Scan kostet warm ~250 ms, KALT
-- aber ~2,4 s -- bei den aktuellen Besucherzahlen ist der Cache meistens kalt,
-- drei Scans liefen also zuverlaessig in den 3-Sekunden-Timeout.
-- grouping sets erledigt alle drei Gruppierungen in einem Durchlauf.
--   grouping(x) = 0  ->  x gehoert zur aktuell aktiven Gruppierung
-- Spalten, die in KEINER aktiven Gruppierung stehen, sind in der Zeile null --
-- daher waehlt das coalesce automatisch den richtigen Wert aus.
-- Zeilen ohne Club/Liga/Position bilden eine null-Gruppe und fliegen aussen
-- raus; das entspricht exakt dem frueheren "... is not null" im where.
-- FALLBACK: Sollte grouping sets Aerger machen, steht die union-all-Fassung in
-- migrations/2026-08-02_market_server_side.sql -- dann dort nur das coalesce
-- durch "cp.eligibility = p_elig" ersetzen.
create or replace function public.market_facets(p_elig text default 'in_season')
returns table (kind text, value text, extra text, cards int)
language sql stable security definer set search_path = public as $$
  select t.kind, t.val, t.extra, t.cards
  from (
    select case
             when grouping(cp.team_name)   = 0 then 'club'
             when grouping(cp.league_name) = 0 then 'league'
             else                                   'position'
           end::text                                                as kind,
           coalesce(cp.team_name, cp.league_name, cp.position)::text as val,
           cp.league_country::text                                  as extra,
           count(*)::int                                            as cards
    from public.card_prices cp
    where cp.fmv is not null
      and cp.eligibility = p_elig
    group by grouping sets (
      (cp.team_name),
      (cp.league_name, cp.league_country),
      (cp.position)
    )
  ) t
  where t.val is not null
$$;

-- ── 3) Kennzahlen der Kopfzeile ────────────────────────────────────────────
-- Median aus market_daily statt percentile_cont live. median_as_of macht
-- sichtbar, von wann der Median stammt -- Avg ist live, Median vom Snapshot.
-- DROP noetig: die bisherige Fassung gibt VIER Spalten zurueck, diese hier
-- fuenf (median_as_of kam dazu). "create or replace" darf den Rueckgabetyp
-- nicht aendern (42P13). Ohne CASCADE -- haengt wider Erwarten etwas an der
-- Funktion, soll das laut scheitern statt still mitgerissen zu werden.
-- Die Rechte gehen beim Drop verloren und werden unten neu gesetzt.
drop function if exists public.market_overview(text);

create function public.market_overview(p_elig text default 'in_season')
returns table (scarcity text, cards int, avg_fmv numeric, median_fmv numeric, median_as_of date)
language sql stable security definer set search_path = public as $$
  with live as (
    select cp.scarcity,
           count(*)::int                    as cards,
           round(avg(cp.fmv)::numeric, 2)   as avg_fmv
    from public.card_prices cp
    where cp.fmv is not null
      and cp.eligibility = p_elig
    group by cp.scarcity
  ),
  snap as (
    select distinct on (md.scarcity) md.scarcity, md.median_fmv, md.day
    from public.market_daily md
    where md.eligibility = p_elig and md.median_fmv is not null
    order by md.scarcity, md.day desc
  )
  select l.scarcity, l.cards, l.avg_fmv, s.median_fmv, s.day
  from live l left join snap s on s.scarcity = l.scarcity
$$;

-- ── Rechte ─────────────────────────────────────────────────────────────────
-- WICHTIG: Postgres vergibt EXECUTE automatisch an PUBLIC. "revoke from anon"
-- allein reicht NICHT -- dieselbe Fehlerklasse wie beim price_history-Lockdown
-- und beim Analytics-Hardening. Deshalb erst von public entziehen.
-- market_overview wurde oben neu angelegt (Drop wegen geaendertem
-- Rueckgabetyp) und hat daher NOCH KEINE Rechte ausser dem PUBLIC-Default --
-- die Zeilen unten sind dort also Pflicht, nicht Vorsichtsmassnahme.
revoke execute on function public.market_leagues(text,text)  from public;
revoke execute on function public.market_facets(text)        from public;
revoke execute on function public.market_overview(text)      from public;
grant  execute on function public.market_leagues(text,text)  to anon, authenticated;
grant  execute on function public.market_facets(text)        to anon, authenticated;
grant  execute on function public.market_overview(text)      to anon, authenticated;

-- ── Verifikation (im SQL-Editor ausfuehren) ────────────────────────────────
-- 1) Plan pruefen: es MUSS "Index Scan using idx_card_prices_elig_scarcity_fmv"
--    auftauchen, kein "Seq Scan on card_prices":
--      explain analyze select * from market_overview('in_season');
-- 2) Ergebnisse:
--      select * from market_overview('in_season');
--      select * from market_leagues('in_season','limited') limit 5;
--      select kind, count(*) from market_facets('in_season') group by 1;
