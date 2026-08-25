-- ═══════════════════════════════════════════════════════════════════════════
-- Club-Facetten lernen ihre Liga  (25.08.2026)
--
-- ZIEL (Jonas): Ist eine Liga gewaehlt, soll die Club-Suche nur noch Clubs
-- DIESER Liga vorschlagen. Dazu muessen die Club-Zeilen der Facetten-MV
-- wissen, in welcher Liga der Club spielt.
--
-- AENDERUNG: mv_market_facets gruppiert Club-Zeilen jetzt nach
-- (eligibility, team_name, league_name, league_country) und bekommt die neue
-- Spalte `league`. Ein Club mit Karten in mehreren Ligen (waehrend der
-- Selbstheilung nach dem Liga-Fix vom 25.08. moeglich) liefert mehrere
-- Zeilen — das Frontend fasst sie zusammen. Liga- und Positions-Zeilen
-- unveraendert; `extra` traegt bei Club-Zeilen jetzt das Liga-Land.
--
-- RPC: market_facets braucht die neue Rueckgabespalte `league` → Rueckgabetyp
-- aendert sich → DROP + CREATE (create or replace kann das nicht). Das alte
-- Frontend ignoriert die Zusatzspalte, das neue vertraegt ihr Fehlen — die
-- Reihenfolge von Migration und UI-Deploy ist damit egal.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Laeuft als eine Transaktion.
-- ═══════════════════════════════════════════════════════════════════════════

drop materialized view if exists public.mv_market_facets;
create materialized view public.mv_market_facets as
select t.eligibility, t.kind, t.val as value, t.extra, t.league, t.cards
from (
  select cp.eligibility,
         case
           when grouping(cp.team_name)   = 0 then 'club'
           when grouping(cp.league_name) = 0 then 'league'
           else                                   'position'
         end::text                                                 as kind,
         coalesce(cp.team_name, cp.league_name, cp.position)::text as val,
         cp.league_country::text                                   as extra,
         case when grouping(cp.team_name) = 0
              then cp.league_name end::text                        as league,
         count(*)::int                                             as cards
  from public.card_prices cp
  where cp.fmv is not null
  group by grouping sets (
    (cp.eligibility, cp.team_name, cp.league_name, cp.league_country),
    (cp.eligibility, cp.league_name, cp.league_country),
    (cp.eligibility, cp.position)
  )
) t
where t.val is not null
with data;
create index idx_mv_market_facets_key on public.mv_market_facets (eligibility);

-- Wie bei der Erstanlage: nur die RPC liest die MV, niemand direkt
revoke all on public.mv_market_facets from public, anon, authenticated;

-- Rueckgabetyp aendert sich → drop + create + Rechte NEU setzen (SEC-004:
-- Default Privileges vergeben nichts mehr automatisch — Grant ist Pflicht,
-- sonst ist die Marktseite fuer anonyme Besucher leer!)
drop function if exists public.market_facets(text);
create function public.market_facets(p_elig text default 'in_season')
returns table (kind text, value text, extra text, league text, cards int)
language sql stable security definer set search_path = public as $$
  select m.kind, m.value, m.extra, m.league, m.cards
  from public.mv_market_facets m
  where m.eligibility = p_elig
$$;
revoke execute on function public.market_facets(text) from public;
grant  execute on function public.market_facets(text) to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Club-Zeilen tragen jetzt ihre Liga (league darf bei manchen null sein —
-- Clubs, denen Sorare keine domesticLeague zuordnet, z. B. Leicester):
select kind, count(*) as zeilen, count(league) as mit_liga
from public.mv_market_facets group by kind order by kind;

-- Stichprobe England: Premier-League-Clubs getrennt von Championship-Clubs
select value, league, extra, cards from public.market_facets('in_season')
where kind = 'club' and extra = 'gb-eng'
order by league, cards desc limit 25;
