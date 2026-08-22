-- Anpfiffzeit je aufgestellter Karte (lockedAt aus der Sorare-API).
-- Wichtig fuer die Claim-Frist: ein Lineup ist gesperrt, sobald die ERSTE Karte
-- darin anpfeift. Wer tauschen soll, braucht davor noch Zeit zum Umstellen.
alter table public.squad_lineup_log add column if not exists locked_at timestamptz;
create index if not exists squad_lineup_log_locked_at_idx on public.squad_lineup_log (step_id, manager_slug, locked_at);
