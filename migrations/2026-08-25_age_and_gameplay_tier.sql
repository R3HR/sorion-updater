-- ═══════════════════════════════════════════════════════════════════════════
-- Spieleralter + Gameplay-Tier fuer neue Filter  (25.08.2026)
--
-- ZIEL (Jonas): Auf der Marktseite nach Spieleralter und FMV filtern
-- (Doppel-Slider wie bei Sorare). FMV braucht keine Migration (Spalte
-- existiert). Fuers Alter nehmen wir auf Jonas' Wunsch direkt das
-- age-Feld der Sorare-API (Ganzzahl) statt eines Geburtsdatums.
--
-- VERALTET DAS ALTER? Praktisch nein: Der Updater schreibt es bei JEDER
-- Beruehrung neu — In-Season taeglich, Classic alle ~3-4 Tage. Ein
-- Geburtstag ist also spaetestens nach einem Zyklus nachgezogen.
--
-- BEFUELLUNG: Code im selben Push; In-Season nach ~1 Tag voll. Der
-- Alters-Slider erscheint im UI erst, wenn Daten vorhanden sind.
--
-- KEINE MV-Aenderung noetig — der Filter arbeitet direkt auf card_prices.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

alter table public.card_prices add column if not exists player_age int;

-- Teilindex fuer die Bereichsabfrage. Schreiblast (Lehre INC-006): das Alter
-- aendert sich nur am Geburtstag — Preis-Updates lassen die Spalte unveraendert,
-- HOT-Updates bleiben moeglich; der Index kostet im Alltag praktisch nichts.
create index if not exists idx_cp_age on public.card_prices (player_age)
  where player_age is not null;

-- ── Gameplay-Tier (Sterne-Klassifizierung des SPIELERS) ────────────────────
-- Sorare anyPlayer.gameplayTier: GOAT (5*) / STAR (4*) / IMPACT / ROSTER /
-- DNP, kann auch null sein. NICHT zu verwechseln mit unserer tier-Spalte
-- (CardQuality TIER_0..TIER_5, Karten-Qualitaet aus dem Seed).
alter table public.card_prices add column if not exists gameplay_tier text;
create index if not exists idx_cp_gameplay_tier on public.card_prices (gameplay_tier)
  where gameplay_tier is not null;

-- ── Verifikation (direkt nach Migration: 0 befuellt — waechst per Updater) ─
select count(*) as gesamt, count(player_age) as mit_alter,
       count(gameplay_tier) as mit_gameplay_tier
from public.card_prices;
