-- DB-Groesse: zwei verwaiste Indizes auf price_history entfernen (02.08.)
--
-- Anlass: Datenbank ueber dem Free-Limit (531 MB / 500 MB). In dem Zustand drosselt
-- Supabase Schreibzugriffe — das traefe die Preis-Updater.
--
-- SICHERHEIT: Hier wird KEINE Zeile geloescht. `drop index` entfernt nur eine
-- Suchhilfe, die aus der Tabelle abgeleitet ist — die Daten selbst bleiben
-- unveraendert. Es gibt in dieser Datei kein delete, kein truncate, kein drop table.
--
-- Beide Indizes stammen vom 30./31.07. und haben ihren Zweck verloren:
--   * idx_price_history_scarcity_elig_day  — war fuer market_avg_history();
--     diese Funktion wurde durch market_daily ersetzt und ist geloescht
--     (live geprueft 02.08.: rpc/market_avg_history -> HTTP 404)
--   * idx_price_history_group_player_day   — war NUR fuer die einmalige
--     Carry-Forward-Auffuellung der Marktbewegung; die ist laengst gelaufen
--
-- Was WEITER funktioniert (live geprueft 02.08., beide unabhaengig von diesen Indizes):
--   * player_history() (Sparklines) nutzt den Unique-Index, der mit player_slug beginnt
--   * market_move() liest market_daily, nicht price_history
--   * calcChanges() im Updater filtert ebenfalls ueber player_slug
--
-- KEIN `vacuum full`: Der Speicher eines Index wird beim Loeschen sofort frei.
-- vacuum full wuerde nur die Tabelle verdichten (bei geloeschten Zeilen — hier
-- keine) und dabei price_history ~1 Minute EXKLUSIV sperren; Updater und
-- Sparklines liefen so lange ins Leere. Nicht noetig, daher weggelassen.

-- ── Vorher: welche Indizes existieren? ──────────────────────────────────────
select indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) as groesse
from pg_indexes
where schemaname = 'public' and tablename = 'price_history'
order by pg_relation_size(indexname::regclass) desc;

-- ── Aufraeumen ──────────────────────────────────────────────────────────────
drop index if exists public.idx_price_history_scarcity_elig_day;
drop index if exists public.idx_price_history_group_player_day;

-- ── Nachher: Kontrolle (die beiden duerfen nicht mehr auftauchen) ───────────
select indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) as groesse
from pg_indexes
where schemaname = 'public' and tablename = 'price_history'
order by pg_relation_size(indexname::regclass) desc;

-- ── Diagnose: was belegt die 531 MB wirklich? (nur lesend) ──────────────────
select relname as tabelle,
       pg_size_pretty(pg_total_relation_size(c.oid)) as gesamt,
       pg_size_pretty(pg_relation_size(c.oid))       as daten,
       pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) as indizes,
       (select count(*) from pg_index i where i.indrelid = c.oid) as anzahl_indizes
from pg_class c join pg_namespace n on n.oid = c.relnamespace
where n.nspname = 'public' and c.relkind = 'r'
order by pg_total_relation_size(c.oid) desc
limit 12;
