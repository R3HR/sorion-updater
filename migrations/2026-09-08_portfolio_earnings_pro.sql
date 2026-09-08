-- ═══════════════════════════════════════════════════════════════════════════
-- PRO-FEATURE: "Earned in lineups" im Portfolio  (08.09.2026, Wunsch Jonas)
--
-- BETRIFFT die zwei Kaesten in portfolio.html:
--   1. Statistik-Kachel "EARNED IN LINEUPS" (Gesamtsumme des Managers)
--   2. Karten-Modal "EARNED IN LINEUPS (N GAMEWEEKS)" (Preisgeld/Essence/XP je Karte)
--
-- GRUNDREGEL (HANDOFF, Pro-Features): Ein Blur ist kein Schutz. Der Wert darf
-- den Server gar nicht erst verlassen. Deshalb: Tabellen sperren, die RPC
-- liefert nur bei has_feature() Zeilen.
--
-- EHRLICHE GRENZE: Die Rohdaten je Spieltag bleiben ueber die Edge Function
-- `so5-results` erreichbar, weil die Gameweek-Seite sie braucht (dort stehen
-- die Gewinne je Spieler einer einzelnen Aufstellung, bewusst frei). Wer 119
-- Spieltage einzeln abruft, kann die Summe selbst bilden. Geschuetzt ist also
-- die fertige Lebenszeit-Auswertung, nicht das Rohmaterial. Soll auch das
-- zu, muesste `so5-results` fuer den Manager-Modus ebenfalls gated werden -
-- das wuerde die freie Gameweek-Ansicht treffen (Entscheidung Jonas, offen).
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Feature eintragen ───────────────────────────────────────────────────
-- Verschiebbar ohne Deploy:
--   update feature_access set min_tier='free' where feature_key='portfolio_earnings';
insert into public.feature_access (feature_key, min_tier, label, preview_value)
values ('portfolio_earnings', 'pro',
        'Portfolio: Ertraege aus Aufstellungen (Preisgeld, Essence, XP)',
        '≈€184.22')
on conflict (feature_key) do nothing;

-- ── 2) Datenquelle sperren ─────────────────────────────────────────────────
-- Nur die Edge Function `so5-results` liest diese Tabellen, und die nutzt den
-- Service-Key (SUPABASE_SERVICE_ROLE_KEY) - sie ist davon nicht betroffen.
-- Geprueft am 08.09.: kein Frontend liest sie direkt.
drop policy if exists so5_earn_read    on public.so5_card_earnings;
drop policy if exists so5_lineups_read on public.so5_lineups;
revoke select on public.so5_card_earnings from anon, authenticated;
revoke select on public.so5_lineups       from anon, authenticated;

-- ── 3) Das Tor ─────────────────────────────────────────────────────────────
-- Rueckgabetyp bleibt UNVERAENDERT (sonst 42P13). Gesperrt = keine Zeilen.
-- Die UI fragt zusaetzlich has_feature('portfolio_earnings') fuer den Hinweis.
create or replace function public.so5_earnings_by_card(p_manager text)
returns table(card_slug text, player_name text, lineups integer, essence jsonb, currency jsonb, cash_eur numeric)
language sql
stable
security definer
set search_path to 'public'
as $function$
  with allowed as (select public.has_feature('portfolio_earnings') as ok),
  e as (
    select * from public.so5_card_earnings
    where manager_slug = lower(p_manager)
      and (select ok from allowed)          -- gesperrt: gar keine Zeilen
  ),
  ess as (
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
$function$;

-- so5_known_fixtures bleibt offen: sie nennt nur, welche Spieltage erfasst
-- sind, und enthaelt keinerlei Betraege.

revoke all on function public.so5_earnings_by_card(text) from public, anon, authenticated;
grant execute on function public.so5_earnings_by_card(text) to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
select 'feature'   as pruefung, feature_key, min_tier from public.feature_access where feature_key='portfolio_earnings';
select 'zeilen fuer nicht-pro (erwartet 0)' as pruefung, count(*) from public.so5_earnings_by_card('jr3hr');
select 'tabellenrechte anon (erwartet leer)' as pruefung, grantee, privilege_type
from information_schema.role_table_grants
where table_schema='public' and table_name in ('so5_card_earnings','so5_lineups')
  and grantee in ('anon','authenticated') and privilege_type='SELECT';
