-- skin_bonus: Bonusfaktor der Kartenversion (STANDARD 1.0, SHINY 1.05, HOLO 1.1, ...)
-- Genau der Wert, auf den die Squad-Cap-Regel abzielt. powerBreakdown waere
-- fachlich schoener, sprengt aber Sorares Query-Komplexitaetslimit (30.000).
alter table public.squad_lineup_log add column if not exists skin_bonus numeric;
