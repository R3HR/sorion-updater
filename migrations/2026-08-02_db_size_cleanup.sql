-- DRINGEND (02.08.): Datenbank ueber dem Limit (531 MB / 500 MB, Supabase Free).
-- Supabase drosselt in dem Zustand Schreibzugriffe — das traefe die Preis-Updater.
--
-- Schritt 1: Zwei Indizes entfernen, die ich am 30./31.07. angelegt habe und die
-- inzwischen NICHT mehr gebraucht werden. Auf 1,6 Mio Zeilen kostet jeder von
-- ihnen zweistellige MB.
--   * idx_price_history_scarcity_elig_day  -> war fuer market_avg_history(), diese
--     Funktion wurde durch market_daily ersetzt und existiert nicht mehr
--   * idx_price_history_group_player_day   -> war NUR fuer die einmalige
--     Carry-Forward-Auffuellung der Marktbewegung, die ist laengst gelaufen
-- Die Sparklines (player_history) laufen weiter ueber den Unique-Index, der mit
-- player_slug beginnt.

drop index if exists public.idx_price_history_scarcity_elig_day;
drop index if exists public.idx_price_history_group_player_day;

-- Schritt 2: Platz wirklich freigeben (VACUUM FULL sperrt die Tabelle kurz —
-- ausserhalb der Cron-Fenster ausfuehren, dauert bei 1,6 Mio Zeilen ~1 Min).
vacuum full public.price_history;
analyze public.price_history;

-- ── Diagnose: was belegt den Platz wirklich? Bitte ausfuehren und mir das
--    Ergebnis zeigen, dann planen wir den naechsten Schritt gezielt. ─────────
-- select relname as tabelle,
--        pg_size_pretty(pg_total_relation_size(c.oid))                as gesamt,
--        pg_size_pretty(pg_relation_size(c.oid))                      as daten,
--        pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) as indizes,
--        (select count(*) from pg_index i where i.indrelid = c.oid)   as anzahl_indizes
-- from pg_class c join pg_namespace n on n.oid = c.relnamespace
-- where n.nspname = 'public' and c.relkind = 'r'
-- order by pg_total_relation_size(c.oid) desc
-- limit 12;
