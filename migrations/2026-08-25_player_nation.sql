-- ═══════════════════════════════════════════════════════════════════════════
-- Spieler-Nationalitaet als Filter  (25.08.2026)
--
-- ZIEL (Jonas): Spieler nach Nation filtern, kombinierbar mit der Liga —
-- Beispiel: Sota Kitano (jp) bei RB Salzburg soll unter "Japaner in der
-- oesterreichischen Bundesliga" auftauchen.
--
-- WICHTIG: player_nation ist die NATIONALITAET des Spielers (Sorare:
-- anyPlayer.country.code) — nicht zu verwechseln mit league_country (Land
-- der LIGA). Kitano: player_nation='jp', league_country='at'.
--
-- BEFUELLUNG: Der Updater schreibt die Nation bei jeder Beruehrung (Code
-- deployt mit demselben Push); In-Season ist damit nach ~1 Tag gefuellt,
-- Classic nach ~3-4 Tagen. Bis dahin zeigt der Nation-Filter nur die schon
-- gefuellten Spieler — die Facette waechst von selbst.
--
-- Diese Migration ERSETZT mv_market_facets erneut (viertes Facetten-Set
-- 'nation'). Sie ist eigenstaendig lauffaehig: Wer die Vorgaenger-Migration
-- 2026-08-25_facets_club_league.sql schon ausgefuehrt hat, kann diese einfach
-- danach ausfuehren; wer nicht, bekommt hier beides (Club-Liga + Nation).
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

alter table public.card_prices add column if not exists player_nation text;

-- Kleiner Teilindex fuer den eq-Filter. Schreiblast-Abwaegung (Lehre INC-006):
-- die Nationalitaet aendert sich praktisch nie — nach der Erstbefuellung
-- verursacht der Index bei Preis-Updates keine zusaetzlichen Schreibvorgaenge
-- (HOT-Updates bleiben moeglich, da die indizierte Spalte unveraendert bleibt).
create index if not exists idx_cp_nation on public.card_prices (player_nation)
  where player_nation is not null;

-- ── Facetten-MV: viertes Set 'nation' ──────────────────────────────────────
drop materialized view if exists public.mv_market_facets;
create materialized view public.mv_market_facets as
select t.eligibility, t.kind, t.val as value, t.extra, t.league, t.cards
from (
  select cp.eligibility,
         case
           when grouping(cp.team_name)   = 0 then 'club'
           when grouping(cp.league_name) = 0 then 'league'
           when grouping(cp.position)    = 0 then 'position'
           else                                   'nation'
         end::text                                                 as kind,
         coalesce(cp.team_name, cp.league_name, cp.position,
                  cp.player_nation)::text                          as val,
         cp.league_country::text                                   as extra,
         case when grouping(cp.team_name) = 0
              then cp.league_name end::text                        as league,
         count(*)::int                                             as cards
  from public.card_prices cp
  where cp.fmv is not null
  group by grouping sets (
    (cp.eligibility, cp.team_name, cp.league_name, cp.league_country),
    (cp.eligibility, cp.league_name, cp.league_country),
    (cp.eligibility, cp.position),
    (cp.eligibility, cp.player_nation)
  )
) t
where t.val is not null
with data;
create index idx_mv_market_facets_key on public.mv_market_facets (eligibility);

revoke all on public.mv_market_facets from public, anon, authenticated;

-- RPC unveraendert zur Facetten-Migration von heute (kind/value/extra/league/
-- cards) — hier idempotent neu angelegt, damit diese Datei allein genuegt.
-- SEC-004: Default Privileges vergeben nichts — Grant ist Pflicht.
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
-- Direkt nach der Migration ist 'nation' noch leer/duenn (Spalte frisch) —
-- die Zeilen kommen mit dem naechsten Updater-Zyklus + refresh (09:20 UTC).
select kind, count(*) as zeilen, sum(cards) as karten
from public.mv_market_facets group by kind order by kind;
