-- Squad-Manager: Bonus je aufgestellter Karte mitschreiben.
-- Zweck (Jonas 22.08.): Bei Cap-Konflikten sichtbar machen, WIE VIEL Bonus
-- die jeweilige Version bringt (Holo/Shiny/Standard, XP-Level, Collection).
-- power = Gesamtfaktor der Karte laut Sorare, bonus = Bonus der Aufstellung.
alter table public.squad_lineup_log add column if not exists power numeric;
alter table public.squad_lineup_log add column if not exists bonus numeric;
alter table public.squad_lineup_log add column if not exists grade int;
alter table public.squad_lineup_log add column if not exists xp int;
