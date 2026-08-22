-- ═══════════════════════════════════════════════════════════════════════════
-- INC-005 — Wiederanlauf nach Totalausfall (Disk-IO-Budget erschöpft)
-- SOFORT nach dem Projekt-Neustart im SQL-Editor ausführen ("ohne RLS"),
-- bevor pg_cron den Cache-Wärmer erneut startet (er feuert alle 10 Minuten).
-- ═══════════════════════════════════════════════════════════════════════════

-- ── SCHRITT 1: Last abstellen ─────────────────────────────────────────────
-- Haupttreiber: 6 Vollaggregate über card_prices, alle 10 Min, rund um die Uhr.
select cron.unschedule('warm_market_aggregates');

-- Squad-Poller vorübergehend pausieren (klein, aber jetzt zählt jede Abfrage).
-- Wird nach der Stabilisierung wieder eingeschaltet.
select cron.unschedule('squad-poll-15min');

-- ── SCHRITT 2: Bestandsaufnahme ───────────────────────────────────────────
-- Welche Jobs gibt es überhaupt noch?
select jobid, jobname, schedule, active from cron.job order by jobname;

-- Haben sich Läufe aufgestaut? (Todesspiralen-Nachweis: viele 'running')
select jobname, status, start_time, end_time
from cron.job_run_details
order by start_time desc
limit 30;

-- ── SCHRITT 3: Beweis, wer das IO gefressen hat ───────────────────────────
-- Erst ausführen, wenn die DB wieder antwortet. shared_blks_read = Plattenlesen.
select calls,
       round((total_exec_time / 1000)::numeric, 1) as sekunden_gesamt,
       shared_blks_read,
       left(query, 90) as abfrage
from pg_stat_statements
order by shared_blks_read desc
limit 15;
