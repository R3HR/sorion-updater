// ═══════════════════════════════════════════════════════════════════════════
// VORSCHLAG v3.3 (25.08.2026) — VOLLSTAENDIGER ERSATZ fuer lib/fmv.mjs.
// NICHT automatisch eingebaut: Jonas kopiert diese Datei nach lib/fmv.mjs,
// fuehrt VORHER migrations/2026-08-25_fmv_v33_change_guard.sql aus (Cut-Datum
// dort auf den tatsaechlichen Deploy-Tag setzen!) und pusht dann selbst.
// Einzige Aenderung ggue. v3.2: der SELL_CAP ist BEDINGT (siehe Prinzip 3).
// ═══════════════════════════════════════════════════════════════════════════
//
// SORION FMV v3.3 — "sellable FMV"
// Ziel: Wer eine Karte zum FMV listet, soll sie zu diesem Preis auch verkaufen können.
//
// Prinzipien:
// 1. Zeit-Decay statt Index-Gewichten: ein Sale von vor 2 Stunden zählt fast voll,
//    einer von vor 2 Wochen fast nichts — unabhängig davon, wie viele Sales dazwischen liegen.
// 2. Floor (günstigstes AKTIVES Listing) nur als Notbremse nach unten, NICHT als
//    Mischpartner (FLOOR_BLEND 0 seit v3.2, Backtest 22.08.).
// 3. BEDINGTER Sicherheitsdeckel (v3.3, 25.08.2026): Der Cap Floor × 1,50
//    greift nur noch, wenn die Sales-Basis DÜNN oder ALT ist (< 3 qualifizierte
//    Sales im Fenster ODER jüngster Sale älter als die Halbwertszeit). Bei
//    frischer, liquider Basis zählt der Sales-Wert pur.
//    Beleg (Faktoren-Analyse 25.08., docs/FMV_FAKTOREN_ANALYSE.md): Der immer
//    aktive Cap war die Hauptursache der Unterschätzung im Billigsegment
//    (FMV < 2 €: Verkäufe +23…+75 % über FMV; bei gedeckelten Werten Bias
//    +35…+71 pp, bei ungedeckelten ~0). Walk-Forward über 11.917 Verkäufe /
//    774 Karten: Median-Fehler 33,3 → 23,9 %, Bias +22,2 → +1,7 %,
//    ±20-%-Quote 34 → 44 % — je Rarity: limited in-season 33,3 → 24,5 %,
//    limited classic 31,4 → 22,4 %, rare in-season 35,7 → 24,1 %.
//    Die Schutzfunktion gegen Absurditäten (BUG-010: Fantasie-Preise bei
//    dünner Datenlage) bleibt exakt erhalten — bei dünner/alter Basis rechnet
//    v3.3 identisch zu v3.2.

const HALF_LIFE_DAYS = 3;    // Gewicht eines Sales halbiert sich alle 3 Tage
const MAX_AGE_DAYS   = 21;   // Sales älter als 3 Wochen werden ignoriert
const FLOOR_BLEND    = 0.00; // v3.2: kein Floor-Anteil mehr (Backtest 22.08.)
const SELL_CAP       = 1.50; // Sicherheitsdeckel — seit v3.3 nur bei dünner/alter Basis
const CAP_MIN_SALES  = 3;    // v3.3: unter 3 Sales im Fenster gilt die Basis als dünn

// Classic-Markt ist träge: längeres Fenster + langsamerer Decay, sonst bleiben
// illiquide Alt-Karten ohne jeden Wert (BUG: ~15 % der Classic-Zeilen null)
export const CLASSIC_PROFILE = { halfLifeDays: 14, maxAgeDays: 90 };

/**
 * @param {{date: string, eur: number}[]} sales   letzte Verkäufe, neueste zuerst
 * @param {number|null} floorPrice                günstigstes aktives Listing (EUR)
 * @param {number} [now]                          Zeitstempel (ms), default Date.now()
 * @param {{halfLifeDays?: number, maxAgeDays?: number}} [opts]  Markt-Profil
 * @returns {number|null} FMV in EUR oder null wenn keinerlei Daten
 */
export function calculateFMV(sales, floorPrice, now = Date.now(), opts = {}) {
  const halfLife = opts.halfLifeDays ?? HALF_LIFE_DAYS;
  const maxAge   = opts.maxAgeDays   ?? MAX_AGE_DAYS;
  let entries = (sales || [])
    .filter(s => s && s.eur > 0)
    .map(s => {
      const ageDays = Math.max(0, (now - new Date(s.date).getTime()) / 86400000);
      return { v: s.eur, w: Math.pow(0.5, ageDays / halfLife), age: ageDays };
    })
    .filter(e => e.age <= maxAge);

  // v3.3: Zustand der Sales-Basis VOR dem Trimmen festhalten — er entscheidet,
  // ob der Sicherheitsdeckel unten überhaupt greift.
  const nWindow   = entries.length;
  const newestAge = nWindow ? Math.min(...entries.map(e => e.age)) : Infinity;

  // Ausreißer trimmen (je 1× höchster und niedrigster Wert), erst ab 5 Datenpunkten
  if (entries.length >= 5) {
    entries.sort((a, b) => a.v - b.v);
    entries = entries.slice(1, -1);
  }

  const hasFloor = typeof floorPrice === 'number' && floorPrice > 0;

  // Kein qualifizierter Sale → KEIN FMV. Ein Listing allein ist ein Wunschpreis,
  // kein Marktpreis (BUG-010: Fantasie-Listing 731 € wurde zum FMV, Sales lagen bei 2,94 €).
  if (!entries.length) return null;

  const totalW     = entries.reduce((s, e) => s + e.w, 0);
  const salesValue = entries.reduce((s, e) => s + e.v * e.w, 0) / totalW;

  if (!hasFloor) return salesValue;

  // Floor darf den FMV nur nach UNTEN ziehen: Liegt das günstigste Listing über
  // dem Sales-Wert, verkauft man durch Unterbieten zum Sales-Wert — ein hoher
  // Ask hebt die Verkäuflichkeit nicht.
  if (floorPrice >= salesValue) return salesValue;

  const blended = FLOOR_BLEND * floorPrice + (1 - FLOOR_BLEND) * salesValue;

  // v3.3: Deckel nur bei dünner ODER alter Basis. Bei ≥3 Sales, deren jüngster
  // frischer als eine Halbwertszeit ist, ist der Sales-Wert selbst die beste
  // Schätzung — der Backtest zeigt, dass der immer aktive Deckel dort nur
  // systematisch unterschätzt (Verkäufe finden regelmäßig weit über dem
  // billigsten Listing statt: andere Serials, andere Jahrgänge, gezielter Kauf).
  const thinOrStale = nWindow < CAP_MIN_SALES || newestAge > halfLife;
  return thinOrStale ? Math.min(blended, floorPrice * SELL_CAP) : blended;
}
