-- ═══════════════════════════════════════════════════════════════════════════
-- Accuracy-Seite: Tages-Snapshot statt Live-Rechnung pro Besucher (20.08.2026)
--
-- ENTSCHEIDUNG Jonas: tagesaktuell, nicht monatlich. Aber die Seite rechnete
-- die Perzentile bei JEDEM Besuch live ueber die Rohtabelle — heute 15k Zeilen
-- (schnell), in 30 Tagen Hunderttausende: exakt das Timeout-Muster aus
-- BUG-015/017. Daher dasselbe Muster wie market_daily: ein naechtlicher Job
-- schreibt 6 Zeilen, die Seite liest nur noch diese.
--
-- Eingebauter Putzdienst: Rohdaten aelter als 90 Tage fliegen raus (30 braucht
-- die Anzeige, 90 behalten wir als Reserve fuer den FMV-Backtest). Ohne das
-- waechst fmv_accuracy um ~50-80 MB/Monat — der 225-MB-Stand von heute waere
-- in einem Quartal wieder aufgefressen.
--
-- Nebeneffekt: die Tages-Zeilen ergeben einen ACCURACY-VERLAUF (wird die
-- Formel besser?) — 6 Zeilen/Tag, praktisch gratis.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS" (Lektion BUG-018)!
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.fmv_accuracy_daily (
  day              date    not null,
  eligibility      text    not null,
  scarcity         text    not null,
  samples          int,
  median_abs_delta numeric,
  bias             numeric,
  primary key (day, eligibility, scarcity)
);
alter table public.fmv_accuracy_daily enable row level security;
drop policy if exists fmv_accuracy_daily_read on public.fmv_accuracy_daily;
create policy fmv_accuracy_daily_read on public.fmv_accuracy_daily
  for select to anon, authenticated using (true);
grant select on public.fmv_accuracy_daily to anon, authenticated;

-- Gleiche Aggregation wie die View fmv_accuracy_stats (30d rollierend, gap<48h)
create or replace function public.snapshot_fmv_accuracy(p_day date default current_date)
returns int
language plpgsql security definer set search_path = public as $$
declare n int;
begin
  insert into public.fmv_accuracy_daily (day, eligibility, scarcity, samples, median_abs_delta, bias)
  select p_day, eligibility, scarcity,
         count(*)::int,
         round((percentile_cont(0.5) within group (order by abs(delta_pct)))::numeric, 1),
         round(avg(delta_pct)::numeric, 1)
  from public.fmv_accuracy
  where created_at > now() - interval '30 days' and hours_gap < 48
  group by eligibility, scarcity
  on conflict (day, eligibility, scarcity) do update
    set samples          = excluded.samples,
        median_abs_delta = excluded.median_abs_delta,
        bias             = excluded.bias;
  get diagnostics n = row_count;

  -- Putzdienst im selben Job: Rohdaten > 90 Tage raus
  delete from public.fmv_accuracy where created_at < now() - interval '90 days';
  return n;
end $$;
revoke all on function public.snapshot_fmv_accuracy(date) from public;

-- Taeglich 05:45 UTC (vor dem Harvester-Snapshot-Umfeld, ausserhalb der
-- Updater-Fenster). Fallback-sicher, falls pg_cron fehlt.
do $$
begin
  create extension if not exists pg_cron;
  perform cron.schedule('fmv_accuracy_daily', '45 5 * * *',
                        'select public.snapshot_fmv_accuracy()');
exception when others then
  raise notice 'pg_cron nicht verfuegbar (%): bitte melden', sqlerrm;
end $$;

-- Heutigen Snapshot sofort schreiben, damit die Seite direkt umschalten kann
select public.snapshot_fmv_accuracy(current_date);

-- ── Verifikation ───────────────────────────────────────────────────────────
-- select * from public.fmv_accuracy_daily order by day desc, scarcity, eligibility;
--   -> bis zu 6 Zeilen fuer heute
-- select jobname, schedule from cron.job;
--   -> 'fmv_accuracy_daily' (45 5 * * *) UND 'price_history_rollup_weekly' (30 6 * * 1)
