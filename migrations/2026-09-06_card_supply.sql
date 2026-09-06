-- ═══════════════════════════════════════════════════════════════════════════
-- Kartenbestand je Zeile erfassen  (06.09.2026, IDEA-006)
--
-- FUND: Sorares `anyPlayer { cardSupply { season { startYear } limited rare
-- superRare unique } }` liefert die TATSAECHLICHE Stueckzahl je Spieler, Saison
-- und Rarity. Kostet keinen zusaetzlichen API-Aufruf (ein Feld mehr im
-- bestehenden Query von update-scarcity.mjs).
--
-- MODELL: Jede card_prices-Zeile ist bereits (player, scarcity, eligibility).
-- Deshalb genuegt EINE Zahl je Zeile:
--   eligibility = in_season -> Stueckzahl der AKTUELLEN Saison (hoechstes startYear)
--   eligibility = classic   -> Summe aller frueheren Saisons
-- Beispiel Kobel Limited: in_season 212, classic 3.182 (5 Vorsaisons).
--
-- WOZU: (1) Knappheit als moeglicher FMV-Faktor — erst messen, dann entscheidet
-- Jonas ueber die Formel; (2) "212 von 1.000 gepraegt" im Karten-Detail;
-- (3) Marktkapitalisierung als Nebenprodukt (siehe WETTBEWERB.md).
--
-- ALTLAST: `available_supply` ist verwaist (kein Skript schreibt sie, Classic 0 %,
-- In-Season 23-28 %). Wird NICHT weiterverwendet; die neue Spalte heisst `supply`.
-- Die alte Spalte bleibt vorerst stehen, damit nichts bricht, was sie noch liest.
--
-- SCHREIBLAST (Lehre INC-005/006): der Updater schreibt `supply` im selben
-- UPDATE mit, das er ohnehin je Zeile ausfuehrt. Kein Cron, keine Zusatzlast.
--
-- AUSFUEHREN: per CLI (npx supabase db query --linked --file ...).
-- ═══════════════════════════════════════════════════════════════════════════

alter table public.card_prices
  add column if not exists supply           int,          -- Stueckzahl fuer genau diese Zeile
  add column if not exists supply_season    int,          -- Saison, aus der in_season stammt (startYear)
  add column if not exists supply_updated_at timestamptz; -- wann zuletzt von Sorare geholt

-- Fuer die Marktkapitalisierung und Knappheits-Auswertungen
create index if not exists idx_card_prices_supply on public.card_prices (eligibility, scarcity, supply);

comment on column public.card_prices.supply is
  'Tatsaechliche Stueckzahl (Sorare cardSupply): bei in_season die aktuelle Saison, bei classic die Summe der frueheren. Quelle: update-scarcity.mjs.';

-- ── Verifikation ───────────────────────────────────────────────────────────
select count(*) as zeilen,
       count(supply) as mit_supply,
       count(available_supply) as alte_spalte
from public.card_prices;
