-- SORION Migration 2026-07-06
-- Im Supabase Dashboard → SQL Editor ausführen.

-- 1. Zeitbasierte Prozent-Änderungen (BUG-004): werden vom Update-Script
--    aus price_history berechnet und hier abgelegt, damit die UI danach
--    sortieren kann ohne die History zu laden.
alter table card_prices add column if not exists change_24h numeric;
alter table card_prices add column if not exists change_7d  numeric;

-- 2. Vorbereitung InSeason/Classic-Trennung (HANDOFF TODO #1).
--    Bestandsdaten sind alle in_season; Classic-Zeilen kommen später dazu.
alter table card_prices add column if not exists eligibility text not null default 'in_season';

-- HINWEIS für den Classic-Ausbau (noch NICHT ausführen, erst Constraint-Namen prüfen):
-- Der Unique-Constraint auf (player_slug, scarcity) muss dann um eligibility
-- erweitert werden, ebenso price_history um eine eligibility-Spalte und
-- der Unique-Key (player_slug, scarcity, recorded_at).
