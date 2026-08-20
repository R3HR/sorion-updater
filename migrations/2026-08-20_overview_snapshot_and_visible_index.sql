-- ═══════════════════════════════════════════════════════════════════════════
-- Hero-Boxen robust machen: Snapshot statt Live-Aggregat + Sichtbarkeits-Index
-- (20.08.2026 — BUG-017)
--
-- BEFUND: Mit dem ersten echten Traffic (19./20.08.) zeigten "Players Tracked"
-- und beide Avg-FMV-Boxen wieder "—". Gemessen: market_overview kalt 3,49 s ->
-- 500 (57014), die Players-Tracked-Zaehlung kalt 3,61 s -> 500. Beide lagen
-- gestern knapp UNTER der Grenze — unter Last ist der Cache umkaempfter und
-- der jeweils erste Besucher reisst die 3-s-Grenze.
--
-- FIX A — market_overview liest NUR NOCH den Tages-Snapshot market_daily
-- (6 Zeilen/Tag, taeglich 05:30 vom Harvester geschrieben) statt live ueber
-- ~20k card_prices-Zeilen zu mitteln. Nebeneffekt: Der angezeigte Avg ist
-- damit EXAKT deckungsgleich mit der Basis der 7d-Bewegungs-Chips
-- (market_move vergleicht dieselben Snapshots) — bisher war er "live" und
-- konnte tagsueber leicht abweichen. Rueckgabeform unveraendert (5 Spalten),
-- das Frontend braucht keine Aenderung.
--
-- FIX B — Teil-Index EXAKT auf der Sichtbarkeitsregel der Marktseite
-- (fmv ODER floor ODER sale vorhanden). Damit werden die Trefferzahl-
-- Zaehlungen (Players Tracked, Pagination) zu Index-Only-Scans statt
-- Heap-Durchlaeufen ueber 122k Zeilen.
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.market_overview(p_elig text default 'in_season')
returns table (scarcity text, cards int, avg_fmv numeric, median_fmv numeric, median_as_of date)
language sql stable security definer set search_path = public as $$
  select distinct on (md.scarcity)
         md.scarcity,
         md.n,
         round(md.avg_fmv, 2),
         md.median_fmv,
         md.day
  from public.market_daily md
  where md.eligibility = p_elig
  order by md.scarcity, md.day desc
$$;

-- Rechte unveraendert (gleiche Signatur -> Grants bleiben), zur Sicherheit:
revoke execute on function public.market_overview(text) from public;
grant  execute on function public.market_overview(text) to anon, authenticated;

create index if not exists idx_cp_visible
  on public.card_prices (eligibility, scarcity)
  where fmv is not null or floor_price is not null or sale_1 is not null;

-- ── Verifikation (SQL-Editor) ──────────────────────────────────────────────
-- select * from market_overview('in_season');
--   -> 3 Zeilen mit dem heutigen day; Laufzeit einstellige Millisekunden.
-- explain analyze
--   select count(*) from public.card_prices
--   where eligibility='in_season'
--     and (fmv is not null or floor_price is not null or sale_1 is not null);
--   -> "Index Only Scan using idx_cp_visible", kein Seq Scan.
