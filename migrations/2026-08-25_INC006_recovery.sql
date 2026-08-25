-- ═══════════════════════════════════════════════════════════════════════════
-- INC-006 — Notfall-Wiederanlauf  (25.08.2026)
--
-- LAGE: Zweiter Totalausfall binnen drei Tagen. Railway ist NICHT die Ursache
-- (alle Dienste 0/1 running, Updater-Fenster bereits auf 6 h gekuerzt). Die
-- Last kommt von innen: pg_cron + Schreibverstaerkung durch zwoelf Indizes
-- auf card_prices. Projekt-Neustart erledigt, DB antwortet wieder (~250 ms).
--
-- ZWEI KORREKTUREN ggue. den ersten Fassungen (beide live gelernt):
--   1. cron.job_run_details hat KEINE Spalte jobname, nur jobid → Join.
--   2. cron.job ist fuer die Editor-Rolle NICHT beschreibbar (42501) →
--      Jobs werden ueber cron.unschedule()/cron.schedule() gesteuert, nicht
--      per UPDATE. unschedule LOESCHT den Job; deshalb sichert Block 1 unten
--      alle Definitionen in der Ausgabe, BEVOR etwas entfernt wird.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Bitte die Ausgabe von Block 1 und 4
-- aufheben bzw. zurueckschicken.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) SICHERUNG + BELEG: alle Job-Definitionen im Klartext ───────────────
-- Diese Ausgabe ist die Wiederherstellungs-Grundlage. Bitte aufheben!
select jobid, jobname, schedule, active, command
from cron.job
order by jobname;

-- Welcher Job lief wie lange? Ueberlappung = die INC-005-Todesspirale.
select j.jobname,
       d.status,
       d.start_time,
       coalesce(d.end_time, now()) - d.start_time as dauer
from cron.job_run_details d
left join cron.job j on j.jobid = d.jobid
where d.start_time > now() - interval '24 hours'
order by d.start_time desc
limit 40;

-- Laeuft gerade noch etwas?
select j.jobname, d.start_time, now() - d.start_time as laeuft_seit
from cron.job_run_details d
left join cron.job j on j.jobid = d.jobid
where d.status = 'running'
order by d.start_time;

-- ── 2) ALLE Jobs abschalten ────────────────────────────────────────────────
-- Ueber die Funktion statt per UPDATE (Rechte!). Fehlschlaege einzelner Jobs
-- brechen den Lauf nicht ab — sie werden nur gemeldet.
do $$
declare r record;
begin
  for r in select jobname, command from cron.job loop
    begin
      raise notice 'stoppe % → %', r.jobname, r.command;
      perform cron.unschedule(r.jobname);
    exception when others then
      raise notice '  KONNTE % NICHT STOPPEN: %', r.jobname, sqlerrm;
    end;
  end loop;
end $$;

-- ── 3) Nur das Noetigste zurueck ───────────────────────────────────────────
-- Tages-Snapshot der Accuracy: Grundlage der Hero-Zahlen, billig (6 Zeilen).
select cron.schedule('fmv_accuracy_daily', '45 5 * * *',
                     'select public.snapshot_fmv_accuracy()');

-- Markt-Aggregate nur noch EINMAL taeglich (statt 06:15 + 14:00), in der
-- ruhigsten Stunde: nach dem Morgen-Pulk, weit vor dem Abend-Fenster.
select cron.schedule('refresh_market_aggregates', '20 9 * * *',
                     'select public.refresh_market_aggregates()');

-- Wochen-Rollup: laeuft nur montags, haelt price_history klein.
select cron.schedule('price_history_rollup_weekly', '30 6 * * 1',
                     'select public.price_history_rollup(90)');

-- BEWUSST NICHT zurueckgeholt — mit Jonas / der Squad-Session zu klaeren:
--   squad-poll-*  → 15-Minuten-Takt ist derselbe Ueberlappungs-Mechanismus,
--                   der bei INC-005 die Drossel am Leben hielt. Die genaue
--                   Definition steht in der Ausgabe von Block 1.
--                   Vorschlag: stuendlich + Advisory-Lock.

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

-- b) Wie teuer sind die Updates? Niedriger HOT-Anteil = jedes Update fasst
--    Indizes an = viel Schreib-IO.
select relname, n_tup_upd as updates, n_tup_hot_upd as davon_hot,
       case when n_tup_upd > 0 then round(100.0*n_tup_hot_upd/n_tup_upd,1) end as hot_prozent,
       n_dead_tup as tote_zeilen, last_autovacuum
from pg_stat_user_tables
where relname in ('card_prices','price_history','squad_snapshots','squad_lineup_log','fmv_accuracy')
order by n_tup_upd desc nulls last;
