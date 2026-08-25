-- ═══════════════════════════════════════════════════════════════════════════
-- INC-006 — Notfall-Wiederanlauf  (25.08.2026)
--
-- LAGE: Zweiter Totalausfall binnen drei Tagen. REST/Auth antworteten nicht
-- (Zeitueberschreitung), Edge Functions liefen weiter (brauchen die DB-Platte
-- nicht). Railway ist NICHT die Ursache: alle Dienste 0/1 running, Updater-
-- Fenster bereits auf 22-23 + 0-4 gekuerzt. Die Last kommt von innen:
-- pg_cron + Schreibverstaerkung durch zwoelf Indizes auf card_prices.
--
-- ═══ SCHRITT 1 — Dashboard: "Restart project" ═══ (bereits erledigt)
-- ═══ SCHRITT 2 — diese Datei, "ohne RLS" ═══
--   Nimmt ALLE wiederkehrende Last weg, damit sich das IO-Budget erholt.
--   Bewusst radikal: erst Stille herstellen, dann einzeln zurueckholen.
--
-- KORREKTUR ggue. der ersten Fassung: cron.job_run_details hat in dieser
-- pg_cron-Version KEINE Spalte jobname — nur jobid. Daher Join auf cron.job.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Beleg: welcher Job laeuft wie lange? ────────────────────────────────
select j.jobname,
       d.status,
       d.start_time,
       coalesce(d.end_time, now()) - d.start_time as dauer
from cron.job_run_details d
left join cron.job j on j.jobid = d.jobid
where d.start_time > now() - interval '24 hours'
order by d.start_time desc
limit 40;

-- Laeuft gerade noch etwas? (Ueberlappung = die INC-005-Todesspirale)
select j.jobname, d.start_time, now() - d.start_time as laeuft_seit
from cron.job_run_details d
left join cron.job j on j.jobid = d.jobid
where d.status = 'running'
order by d.start_time;

select jobid, jobname, schedule, active from cron.job order by jobname;

-- ── 2) ALLE Jobs stilllegen ────────────────────────────────────────────────
-- Nicht loeschen, nur deaktivieren: Definition bleibt erhalten.
update cron.job set active = false;

-- ── 3) Nur das Noetigste zurueck ───────────────────────────────────────────
-- Tages-Snapshot der Accuracy: Grundlage der Hero-Zahlen, billig (6 Zeilen).
update cron.job set active = true where jobname = 'fmv_accuracy_daily';

-- Markt-Aggregate nur noch EINMAL taeglich (statt 06:15 + 14:00), in der
-- ruhigsten Stunde: nach dem Morgen-Pulk, weit vor dem Abend-Fenster.
update cron.job set schedule = '20 9 * * *', active = true
where jobname = 'refresh_market_aggregates';

-- Wochen-Rollup bleibt (nur montags, haelt price_history klein).
update cron.job set active = true where jobname = 'price_history_rollup_weekly';

-- BEWUSST AUS, mit Jonas zu klaeren:
--   squad-poll-*  → 15-Minuten-Takt ist derselbe Ueberlappungs-Mechanismus wie
--                   der Waermer bei INC-005. Gehoert der anderen Session.
--                   Vorschlag: stuendlich + Advisory-Lock gegen Ueberlappung.

select jobid, jobname, schedule, active from cron.job order by jobname;

-- ── 4) Diagnose der EIGENTLICHEN Ursache (nur lesend) ─────────────────────
-- a) Welche Indizes auf card_prices werden benutzt? benutzungen = 0 bei
--    grossem Index = purer Schreib-Ballast, Kandidat zum Loeschen.
select i.indexrelname as index_name,
       pg_size_pretty(pg_relation_size(i.indexrelid)) as groesse,
       i.idx_scan as benutzungen
from pg_stat_user_indexes i
join pg_class t on t.oid = i.relid
where t.relname = 'card_prices'
order by i.idx_scan asc, pg_relation_size(i.indexrelid) desc;

-- b) Wie teuer sind die Updates? Niedriger HOT-Anteil = jeder Update fasst
--    Indizes an = viel IO.
select relname, n_tup_upd as updates, n_tup_hot_upd as davon_hot,
       case when n_tup_upd > 0 then round(100.0*n_tup_hot_upd/n_tup_upd,1) end as hot_prozent,
       n_dead_tup as tote_zeilen, last_autovacuum
from pg_stat_user_tables
where relname in ('card_prices','price_history','squad_snapshots','squad_lineup_log','fmv_accuracy')
order by n_tup_upd desc nulls last;
