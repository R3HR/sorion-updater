# SORION — Bug-Archiv

> Format: ID · Titel · Symptom · Ursache · Fix · Status
> Sicherheitslücken & Crashes gehören nach [INCIDENTS.md](INCIDENTS.md).

---

## BUG-001 — FMV massiv über Marktpreis (Maza 7,15 € vs. Markt ~5 €)

- **Symptom:** Spieler wurden mit FMV weit über dem Preis angezeigt, zu dem Karten tatsächlich angeboten/verkauft wurden.
- **Ursache (3-fach):**
  1. `player.floor_price ?? sorted[0]?.eur` — Nullish-Coalescing fror den Floor beim ersten gesetzten Wert für immer ein
  2. Floor kam aus letzten *Sales* statt aus aktiven *Listings*
  3. Sale-Gewichte zu flach: Sales 6–20 hatten zusammen ~40 % Gewicht → alte teure Verkäufe dominierten bei illiquiden Karten
- **Fix:** Floor live aus `lowestPriceAnyCard.publicMinPrices` (ein kombinierter GraphQL-Call); FMV v3 mit Zeit-Decay statt Index-Gewichten (`lib/fmv.mjs`)
- **Status:** ✅ behoben 2026-07-06 (Commits `c7f2f22`, FMV v3)

## BUG-002 — Floor Price = letzter Verkauf statt aktuelles Angebot

- **Symptom:** „Floor" in UI entsprach nicht dem günstigsten Kaufangebot auf Sorare.
- **Ursache:** Scripts kannten nur `tokenPrices` (Sales). Kein Query auf aktive Listings.
- **Fix:** `anyPlayer(slug).lowestPriceAnyCard(inSeason: true, rarity).publicMinPrices.eurCents`, Fallback günstigster letzter Sale.
- **Status:** ✅ behoben 2026-07-06

## BUG-003 — Drei konkurrierende FMV-Formeln

- **Symptom:** FMV eines Spielers sprang je nachdem, welcher Codepfad zuletzt schrieb.
- **Ursache:** Formel dupliziert in `update-*.mjs` (Railway), `add-missing-players` (alte Gewichte, min(sales) als Floor) und `update-prices` (ganz andere Gewichte, sorarehoops-Daten).
- **Fix:** Eine Formel in `lib/fmv.mjs`. `add-missing-players` berechnet nichts mehr (Slim-Insert mit `updated_at=epoch`). `update-prices` schreibt kein FMV mehr (nur noch Tier/Pool-Daten) — prüfen ob Function überhaupt noch gebraucht wird.
- **Status:** 🟡 Code fertig 2026-07-06 — Edge-Function-Deploy durch Jonas offen

## BUG-004 — 24h-/7d-Prozente ohne Zeitbezug

- **Symptom:** „24h %" verglich Sale 1 mit Sale 2, „7d %" Sale 1 mit Sale 5 — bei illiquiden Karten sind das ggf. Wochen alte Verkäufe. Faktisch falsche Zahlen.
- **Ursache:** UI rechnete aus `sale_1/sale_2/sale_5` statt aus `price_history` (die existiert und täglich befüllt wird).
- **Fix:** Update-Script berechnet `change_24h`/`change_7d` aus `price_history` (FMV heute vs. vor 1/7 Tagen) und speichert sie in `card_prices`. UI liest nur noch die Spalten. Migration: `migrations/2026-07-06_add_change_columns.sql`.
- **Status:** 🟡 Code fertig 2026-07-06 — SQL-Migration durch Jonas offen

## BUG-005 — updated_at wird auch bei Fehlschlag gesetzt

- **Symptom:** Spieler, deren API-Abruf dauerhaft scheitert (falscher Slug, keine Daten), verschwinden unbemerkt ans Queue-Ende. Keine Sichtbarkeit über tote Einträge.
- **Ursache:** Fail-Pfad schreibt `updated_at = now()` ohne Fehlerzähler.
- **Fix (vorgeschlagen):** Spalte `fail_count int default 0`; bei Fehlschlag inkrementieren, bei Erfolg auf 0. UI/Query kann dann `fail_count > 5` als tot markieren.
- **Status:** 🔴 offen

## BUG-006 — Hardcodiertes Jahr „2025" in Pool-URLs

- **Symptom (erwartet):** Tier-/Pool-Daten brechen beim Saisonwechsel August 2026.
- **Ursache:** `update-pool`/`update-prices` fetchen `footballRewardPool2025<Rarity>.json` von sorarehoops.vercel.app — Jahr fest verdrahtet, Dritt-Site-Abhängigkeit.
- **Fix (vorgeschlagen):** Jahr dynamisch bestimmen oder Tier-Daten direkt aus Sorare-API beziehen.
- **Status:** 🔴 offen — muss vor August gelöst sein
