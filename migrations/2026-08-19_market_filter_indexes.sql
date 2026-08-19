-- ═══════════════════════════════════════════════════════════════════════════
-- Filter-Indizes fuer die serverseitige Marktseite  (19.08.2026)
--
-- BEFUND: Nach dem Server-Umbau (19.08.) brachen Tabellenladen und Liga-Filter
-- sporadisch mit "error" ab. Gemessen: die Standard-Abfrage (Seite 1, Limited
-- in-season, sortiert nach FMV) braucht KALT 3,65 s — knapp ueber dem
-- statement_timeout der anon-Rolle. Warm 0,66 s. Der Fehler trifft also den
-- jeweils ersten Besucher nach Cache-Verfall.
--
-- URSACHE: Jede Abfrage ist ein Volldurchlauf ueber ~122k Zeilen, denn
--  (a) die Sichtbarkeitsregel or(fmv|floor|sale_1 not null) wird vom partiellen
--      Index idx_card_prices_elig_scarcity_fmv (WHERE fmv is not null) NICHT
--      abgedeckt — sein Praedikat ist nicht impliziert, Postgres darf ihn
--      nicht verwenden. Selbst die Startseite scannt daher alles.
--  (b) league_name/league_country, team_name und die Namenssuche haben
--      GAR KEINEN Index.
--
-- PRINZIP fuer kuenftige Filter (Nation, Alter, ...): jede Spalte, nach der
-- die Marktseite filtert oder sortiert, braucht hier ihr Register — sonst
-- Volldurchlauf und ab kaltem Cache Timeout.
-- ═══════════════════════════════════════════════════════════════════════════

-- 1) VOLL-Index fuer die Standardansicht (ohne fmv-Praedikat!).
--    Liefert die Zeilen bereits in Sortierreihenfolge; die or()-Sichtbarkeit
--    wird pro Zeile geprueft — die ersten 50 haben ohnehin FMV, die Abfrage
--    liest also ~50 Zeilen statt 122.000.
--    Der bestehende partielle Index bleibt: die Markt-RPCs (market_overview
--    etc.) filtern exakt auf "fmv is not null" und fahren mit dem kleineren
--    Index besser.
create index if not exists idx_cp_elig_sc_fmv_all
  on public.card_prices (eligibility, scarcity, fmv desc nulls last);

-- 2) Liga-Filter (Name + Land, siehe Bundesliga DE/AT)
create index if not exists idx_cp_league
  on public.card_prices (league_name, league_country)
  where league_name is not null;

-- 3) Vereins-Filter (exakte Auswahl aus der Vorschlagsliste)
create index if not exists idx_cp_team
  on public.card_prices (team_name)
  where team_name is not null;

-- 4) Movers (Top/Flop nach 7d) — laufen bei JEDEM Seitenaufruf
create index if not exists idx_cp_elig_sc_ch7
  on public.card_prices (eligibility, scarcity, change_7d desc nulls last)
  where change_7d is not null;

-- 5) Namens-/Vereinssuche: ilike '%...%' kann kein B-Baum beantworten,
--    dafuer gibt es Trigramm-Indizes (pg_trgm ist bei Supabase verfuegbar).
create extension if not exists pg_trgm;
create index if not exists idx_cp_name_trgm
  on public.card_prices using gin (player_name gin_trgm_ops);
create index if not exists idx_cp_team_trgm
  on public.card_prices using gin (team_name gin_trgm_ops)
  where team_name is not null;

-- Schreibkosten: der Updater aendert ~150 Zeilen/min — sechs zusaetzliche
-- Index-Pflegen dabei sind vernachlaessigbar.

-- ── Verifikation (im SQL-Editor) ───────────────────────────────────────────
-- explain analyze
--   select id from public.card_prices
--   where eligibility='in_season' and scarcity='limited'
--     and (fmv is not null or floor_price is not null or sale_1 is not null)
--   order by fmv desc nulls last, id asc limit 50;
-- => es MUSS "Index Scan using idx_cp_elig_sc_fmv_all" erscheinen,
--    kein "Seq Scan on card_prices".
