-- SORION Migration 2026-07-22 (2) — FMV-Accuracy-Tracking
-- Im Supabase Dashboard → SQL Editor ausführen.
-- Loggt jede Abweichung zwischen VORHER geschätztem FMV und tatsächlichem Verkauf.

create table if not exists fmv_accuracy (
  id          bigint generated always as identity primary key,
  player_slug text not null,
  scarcity    text not null,
  eligibility text not null default 'in_season',
  fmv_est     numeric not null,   -- unser FMV VOR dem Verkauf (kein Leakage)
  sale_price  numeric not null,   -- tatsächlicher Verkaufspreis
  delta_pct   numeric not null,   -- signiert: (sale - fmv) / fmv * 100
  est_at      timestamptz,        -- wann der FMV geschätzt wurde
  sale_at     timestamptz not null,
  hours_gap   numeric,            -- Abstand Schätzung → Verkauf (zum Filtern)
  created_at  timestamptz not null default now()
);

create index if not exists fmv_accuracy_created_idx on fmv_accuracy (created_at);
create index if not exists fmv_accuracy_elig_idx on fmv_accuracy (eligibility, scarcity);

alter table fmv_accuracy enable row level security;
drop policy if exists "fmv_accuracy_public_read" on fmv_accuracy;
create policy "fmv_accuracy_public_read" on fmv_accuracy for select using (true);
-- Schreiben nur via service_role (Updater) — braucht keine Policy.

-- ── Auswertungs-Beispiel (später im SQL Editor oder als View):
-- select eligibility, scarcity,
--        count(*)                                                as samples,
--        round(percentile_cont(0.5) within group (order by abs(delta_pct))::numeric, 2) as median_abs_delta,
--        round(avg(delta_pct)::numeric, 2)                       as bias
-- from fmv_accuracy
-- where created_at > now() - interval '30 days' and hours_gap < 48
-- group by 1, 2 order by 1, 2;

-- View für die UI-Anzeige (RLS der Basistabelle greift via security_invoker)
create or replace view fmv_accuracy_stats with (security_invoker = true) as
select eligibility, scarcity,
       count(*)::int as samples,
       round((percentile_cont(0.5) within group (order by abs(delta_pct)))::numeric, 1) as median_abs_delta,
       round(avg(delta_pct)::numeric, 1) as bias
from fmv_accuracy
where created_at > now() - interval '30 days' and hours_gap < 48
group by 1, 2;
