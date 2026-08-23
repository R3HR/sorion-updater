-- ═══════════════════════════════════════════════════════════════════════════
-- IO-Diagnose: Wer verbraucht das Disk-IO-Budget?  (23.08.2026, NUR LESEND)
--
-- ANLASS: Zweite "depleting its Disk IO Budget"-Warnung binnen zwei Tagen.
-- Der Waermer (INC-005) ist seit 22.08. tot, die DB antwortet aktuell normal
-- (240-270 ms) — es ist also kein akuter Ausfall, sondern ein Budget, das ueber
-- den Tag leergezogen wird.
--
-- HAUPTVERDACHT (selbstkritisch): Auf card_prices sind diese Woche viele
-- Indizes dazugekommen (BUG-016 am 19.08., SEC-004 am 22.08.) — darunter ZWEI
-- GIN-Trigramm-Indizes fuer die Namenssuche. Lesen wurde dadurch schnell, aber
-- jeder Index kostet SCHREIB-IO, und der Updater aendert ~100.000 Zeilen/Tag
-- (11 h Cron-Fenster). GIN-Indizes sind dabei besonders teuer. Zusaetzlich
-- verhindern viele Indizes die guenstigen HOT-Updates von Postgres.
--
-- Diese Datei aendert NICHTS. Sie beantwortet drei Fragen; bitte die Ausgaben
-- zurueckschicken, dann kommt eine gezielte Aufraeum-Migration.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Welche Indizes auf card_prices werden ueberhaupt benutzt? ───────────
-- idx_scan = 0 (oder sehr niedrig) bei grossem Index = Kandidat zum Loeschen.
select i.indexrelname                              as index_name,
       pg_size_pretty(pg_relation_size(i.indexrelid)) as groesse,
       i.idx_scan                                  as benutzungen,
       i.idx_tup_read                              as gelesene_eintraege
from pg_stat_user_indexes i
join pg_class t on t.oid = i.relid
where t.relname = 'card_prices'
order by i.idx_scan asc, pg_relation_size(i.indexrelid) desc;

-- ── 2) Wie teuer sind die Schreibvorgaenge? ────────────────────────────────
-- n_tup_upd  = Zeilen-Updates gesamt
-- n_tup_hot_upd = davon die GUENSTIGEN (kein Index musste angefasst werden)
-- Ist der HOT-Anteil niedrig (<50 %), kosten die Updates unnoetig viel IO.
select relname,
       n_tup_upd                                   as updates,
       n_tup_hot_upd                               as davon_hot,
       case when n_tup_upd > 0
            then round(100.0 * n_tup_hot_upd / n_tup_upd, 1) end as hot_prozent,
       n_dead_tup                                  as tote_zeilen,
       last_autovacuum, last_autoanalyze
from pg_stat_user_tables
where relname in ('card_prices','price_history','fmv_accuracy','analytics_events',
                  'manager_cards','squad_snapshots','squad_lineup_log')
order by n_tup_upd desc nulls last;

-- ── 3) Laufen die Cron-Jobs sauber? (Lehre aus INC-005) ───────────────────
select jobname, status, start_time,
       end_time - start_time                        as dauer
from cron.job_run_details
order by start_time desc
limit 20;

-- ── 4) Welche Jobs sind ueberhaupt geplant? ────────────────────────────────
select jobid, jobname, schedule, active from cron.job order by jobname;
