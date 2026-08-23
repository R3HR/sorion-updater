// SORION FMV v3 — "sellable FMV"
// Ziel: Wer eine Karte zum FMV listet, soll sie zu diesem Preis auch verkaufen können.
//
// Prinzipien:
// 1. Zeit-Decay statt Index-Gewichten: ein Sale von vor 2 Stunden zählt fast voll,
//    einer von vor 2 Wochen fast nichts — unabhängig davon, wie viele Sales dazwischen liegen.
//    Damit reguliert sich Liquidität selbst: illiquide Karten stützen sich automatisch
//    stärker auf den Floor.
// 2. Floor (günstigstes AKTIVES Listing) nur als Notbremse nach unten, NICHT als
//    Mischpartner. Bis 22.08. flossen 35 % Floor in jeden Wert ein (FLOOR_BLEND) —
//    der Backtest zeigte, dass das den Wert nur nach unten zieht, ohne die
//    Treffsicherheit zu verbessern.
// 3. Sicherheitsdeckel statt Sellability-Cap (geändert 22.08.2026, v3.2):
//    Die alte Annahme lautete „solange ein Angebot unter deinem Preis steht,
//    verkauft deins nicht" → FMV ≤ Floor × 1,05. Der Walk-Forward-Backtest über
//    153 Karten / 2.433 Vorhersagen (tools/fmv-backtest.mjs) hat diese Annahme
//    WIDERLEGT: Verkäufe finden regelmäßig deutlich über dem billigsten Listing
//    statt (andere Seriennummer, anderer Jahrgang, Käufer will genau diese Karte).
//    Ergebnis alt (1,05 / 0,35): Median-Abweichung 26,4 %, Verzerrung +21,2 % —
//    76 % aller Werte klebten am Deckel. Neu (1,50 / 0,00): 17,6 % / +4,1 %.
//    Der Deckel bleibt bei 1,50 als Riegel gegen Absurditäten (vgl. BUG-010:
//    ein Fantasie-Listing machte einmal 731 € aus einem 2,94-€-Markt).

const HALF_LIFE_DAYS = 3;    // Gewicht eines Sales halbiert sich alle 3 Tage
const MAX_AGE_DAYS   = 21;   // Sales älter als 3 Wochen werden ignoriert
const FLOOR_BLEND    = 0.00; // v3.2: kein Floor-Anteil mehr (Backtest 22.08.)
const SELL_CAP       = 1.50; // v3.2: Sicherheitsdeckel, vorher 1,05 (Backtest 22.08.)

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

  // FLOOR_BLEND steht auf 0 → der Wert kommt rein aus den Verkäufen; der Deckel
  // greift nur noch, wenn der Sales-Wert mehr als 50 % über dem Floor liegt.
  const blended = FLOOR_BLEND * floorPrice + (1 - FLOOR_BLEND) * salesValue;
  return Math.min(blended, floorPrice * SELL_CAP);
}
