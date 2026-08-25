-- ═══════════════════════════════════════════════════════════════════════════
-- INC-006 — Notfall-Wiederanlauf  (25.08.2026)
--
-- LAGE: Zweiter Totalausfall binnen drei Tagen. REST/Auth antworten nicht
-- (Zeitueberschreitung), Edge Functions leben (brauchen die DB-Platte nicht).
-- Railway ist NICHT die Ursache: alle Dienste 0/1 running, Updater-Fenster
-- bereits auf 22-23 + 0-4 gekuerzt, naechster Lauf in 13 h.
-- Die Last kommt von innen: pg_cron + Schreibverstaerkung durch Indizes.
--
-- ═══ SCHRITT 1 — ZUERST im Dashboard, BEVOR diese Datei laeuft ═══
--   Supabase → Project Settings → General → "Restart project"
--   Ohne Neustart antwortet der SQL-Editor gar nicht.
--   (Das allein hat INC-005 geloest; es fuellt aber NICHT das IO-Budget auf.)
--
-- ═══ SCHRITT 2 — diese Datei, "ohne RLS" ═══
--   Nimmt ALLE wiederkehrende Last weg, damit sich das Budget erholt.
--   Bewusst radikal: erst Stille herstellen, dann einzeln zurueckholen.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Was laeuft ueberhaupt und wie lange? (Beleg fuer die Ursache) ───────
select jobname, status, start_time, end_time - start_time as dauer
from cron.job_run_details
where start_time > now() - interval '24 hours'
order by start_time desc
limit 40;

select jobid, jobname, schedule, active from cron.job order by jobname;

-- ── 2) ALLE Jobs stilllegen ────────────────────────────────────────────────
-- Nicht loeschen, nur deaktivieren: so bleibt die Definition erhalten und
-- einzelne Jobs koennen gezielt wieder scharf gestellt werden.
update cron.job set active = false;

-- ── 3) Nur das Noetigste zurueck ───────────────────────────────────────────
-- Der Tages-Snapshot ist die Grundlage der Hero-Zahlen und billig (6 Zeilen).
update cron.job set active = true where jobname = 'fmv_accuracy_daily';

-- Die Markt-Aggregate NUR EINMAL taeglich statt zweimal, und in die ruhigste
-- Stunde (09:20 UTC — nach dem Morgen-Pulk, weit vor dem Abend-Fenster).
update cron.job set schedule = '20 9 * * *', active = true
where jobname = 'refresh_market_aggregates';

-- Wochen-Rollup bleibt (laeuft nur montags, haelt die Tabelle klein).
update cron.job set active = true where jobname = 'price_history_rollup_weekly';

-- BEWUSST AUS und mit Jonas zu klaeren:
--   squad-poll-*  → 15-Minuten-Takt ist derselbe Ueberlappungs-Mechanismus wie
--                   der Waermer bei INC-005. Gehoert der anderen Session;
--                   Vorschlag: auf stuendlich, mit Advisory-Lock.

select jobid, jobname, schedule, active from cron.job order by jobname;

-- ── 4) Diagnose fuer die eigentliche Ursache (nur lesend) ─────────────────
-- Schreibverstaerkung: viele Indizes = viel IO je Zeilen-Update.
select i.indexrelname as index_name,
       pg_size_pretty(pg_relation_size(i.indexrelid)) as groesse,
       i.idx_scan as benutzungen
from pg_stat_user_indexes i
join pg_class t on t.oid = i.relid
where t.relname = 'card_prices'
order by i.idx_scan asc, pg_relation_size(i.indexrelid) desc;

select relname, n_tup_upd as updates, n_tup_hot_upd as davon_hot,
       case when n_tup_upd > 0 then round(100.0*n_tup_hot_upd/n_tup_upd,1) end as hot_prozent,
       n_dead_tup as tote_zeilen, last_autovacuum
from pg_stat_user_tables
where relname in ('card_prices','price_history','squad_snapshots','squad_lineup_log')
order by n_tup_upd desc nulls last;
