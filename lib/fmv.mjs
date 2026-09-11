// ═══════════════════════════════════════════════════════════════════════════
// FMV v3.6 (11.09.2026) — Manager-Verkaeufe zuerst, mit Rueckfallebene (Prinzip 7).
// NICHT automatisch eingebaut: Jonas kopiert diese Datei nach lib/fmv.mjs,
// fuehrt VORHER migrations/2026-08-25_fmv_v33_change_guard.sql aus (Cut-Datum
// dort auf den tatsaechlichen Deploy-Tag setzen!) und pusht dann selbst.
// Einzige Aenderung ggue. v3.4: Auktionen und Sofortkauf fliegen aus der
// Bewertung (siehe Prinzip 0). Die Rechnung selbst ist unveraendert.
// ═══════════════════════════════════════════════════════════════════════════
//
// SORION FMV v3.5 — "sellable FMV"
// Ziel: Wer eine Karte zum FMV listet, soll sie zu diesem Preis auch verkaufen können.
//
// Prinzipien:
// 0. NUR MANAGER-VERKAEUFE (v3.5, eingebaut 08.09.2026, Vorgabe Jonas, bindend).
//    Sorares eigene Maerkte sind keine Marktpreise: auf Auktion (TokenAuction)
//    und Sofortkauf (TokenPrimaryOffer) gelten Gutscheine mit bis zu 50 %
//    Rabatt, dazu Zugaben wie Essence und Wheel-Tickets. Nur der Handel
//    zwischen Managern (TokenOffer) ist echter Zweitmarkt.
//    Beleg (docs/2026-09-07_ZWEITMARKT_FILTER.md, 7.029 Walk-Forward-Ziele,
//    gemessen gegen Manager-Verkaeufe): Median gesamt 26,3 -> 24,4 %,
//    limited in-season 30,5 -> 26,0 %, limited classic unveraendert.
//    Der Bias dreht von -0,6 auf +3,2 %: die Sorare-Maerkte haben den Wert
//    bisher nach unten gezogen, genau wie es die Rabatte erwarten lassen.
//    Preis: 42 % aller Datenpunkte fallen weg. Deshalb Prinzip 5.
//    WICHTIG: Prinzip 0 gilt fuer den WERT. Ob eine Karte ueberhaupt rege
//    gehandelt wird, bezeugt auch eine Auktion — siehe Prinzip 6.
//    Und wenn es GAR KEINE Manager-Verkaeufe gibt, zaehlen sie ersatzweise
//    doch — umgerechnet, siehe Prinzip 7.
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
// v3.4: Je mehr Verkäufe im Fenster liegen, desto kürzer die effektive Halbwertszeit.
// hlEff = halfLife / (1 + nWindow / DENSITY_DIVISOR). Bei 5 Verkäufen also die halbe,
// bei 10 ein Drittel der Grundhalbwertszeit. Grund: Wo viel gehandelt wird, bewegt sich
// der Preis schnell, und ein Mittel über alte Verkäufe hinkt hinterher.
// Backtest 06.09. (11.917 Walk-Forward-Ziele, docs/2026-09-06_V34_BACKTEST.md):
// Median gesamt 23,9 -> 23,2 %, damit erstmals besser als der Last-5-Schnitt (23,7 %);
// Bias +1,9 %; Sprunghöhe 5,5 % und damit ruhiger als der Wettbewerb (6,1 %).
// Stärkere Divisoren (3 oder 2) trafen minimal besser, sprangen aber deutlich mehr.
const DENSITY_DIVISOR = 5;
// v3.5, Prinzip 0: Nur diese Verkaufsart ist Zweitmarkt.
const DEAL_MANAGER = 'TokenOffer';
// v3.5, Prinzip 5: Der Filter kostet Abdeckung, vor allem bei Rare in-season
// (19 % der Karten haetten gar keinen Wert mehr). Statt fremde Verkaufsarten
// zurueckzuholen, schauen wir bei duenner Basis WEITER ZURUECK — es bleiben
// echte Marktpreise, nur aeltere. Messung docs/2026-09-07_ZWEITMARKT_FALLBACK.md:
// Median ueberall gleich oder besser, Bias durchgehend niedriger (+3,2 -> +2,6 %),
// Karten ohne Wert bei rare in-season 19 -> 15 %. Aeltere Verkaeufe wiegen durch
// den Zeit-Decay ohnehin wenig, und der Deckel unten greift dann meist mit.
const WINDOW_STRETCH = 3;
// Prinzip 7 (v3.6, 11.09.2026, Vorgabe Jonas): "bei so wenig Verkaeufen muessen wir
// zwangslaeufig die Auktionen und Sofortkaeufe mit einbeziehen".
// Anlass: Aleix Garcia rare/in-season hatte EINEN Manager-Verkauf in 25 Tagen, dazu
// 17 Auktionen. v3.5 gab solchen Karten gar keinen Wert.
//
// Die Ruckfallebene greift NUR, wenn sich kein einziger Manager-Verkauf findet, auch
// im gedehnten Fenster nicht. Dann werden Auktion und Sofortkauf umgerechnet, denn sie
// sind nicht nur anders bepreist, sondern oft andere Ware: Sorare versteigert frische
// Karten, Manager geben aeltere ab. Gemessen (11.09., 798 Karten): Auktionen liegen im
// Median 70 % ueber dem Manager-Preis DERSELBEN Karte, Sofortkaeufe ueber 130 %.
//
// Die Faktoren sind NICHT der Kehrwert dieses Aufschlags: voll umgerechnet (0,59/0,42)
// kippt die Schaetzung ins Gegenteil (Bias +37 %, Median ±47 %). Gewaehlt ist die im
// Backtest beste Stufe (docs/2026-09-11_FALLBACK_BACKTEST.md, 7.302 Ziele):
// Median gesamt ±24,5 % wie v3.5, aber KEINE Karte ohne Wert mehr (v3.5: 3 %, bei
// rare in-season 14 %). In den Faellen, wo v3.5 nichts liefert, sinkt der Bias von
// -21,6 % (roh beigemischt, also deutlich zu hoch geschaetzt) auf -5,6 %.
const FALLBACK_FACTOR = { TokenAuction: 0.85, TokenPrimaryOffer: 0.70 };

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
  // v3.4: ZWEISTUFIG. Erst die Basis bestimmen (wie viele Verkäufe liegen im
  // Fenster?), dann gewichten — denn die Anzahl legt die Halbwertszeit fest.
  const priced = (sales || []).filter(s => s && s.eur > 0);

  // v3.5: Auktionen und Sofortkauf raus. Absicherung: Liefert die Quelle GAR
  // KEINE Verkaufsart (altes Datenformat, Feld von Sorare entfernt), wird nicht
  // gefiltert — sonst verloeren Karten stumm ihren Wert, statt dass es auffaellt.
  const hasDealInfo = priced.some(s => s.deal);
  const usable = hasDealInfo ? priced.filter(s => s.deal === DEAL_MANAGER) : priced;

  // scale=true rechnet fremde Verkaufsarten auf Manager-Niveau um (Prinzip 7).
  const inWindow = (list, maxAgeDays, scale = false) => list
    .map(s => ({
      v: s.eur * (scale ? (FALLBACK_FACTOR[s.deal] ?? 1) : 1),
      age: Math.max(0, (now - new Date(s.date).getTime()) / 86400000),
    }))
    .filter(e => e.age <= maxAgeDays);

  // v3.5: Erst das normale Fenster. Reicht die Basis nicht, weiter zurueckschauen.
  let raw = inWindow(usable, maxAge);
  if (raw.length < CAP_MIN_SALES && WINDOW_STRETCH > 1) {
    const wider = inWindow(usable, maxAge * WINDOW_STRETCH);
    if (wider.length > raw.length) raw = wider;
  }

  // Prinzip 7: Kein einziger Manager-Verkauf, auch nicht im gedehnten Fenster?
  // Dann lieber ein umgerechneter Wert als gar keiner. Sobald EIN Manager-Verkauf
  // vorliegt, bleibt es bei ihm allein: er ist der echte Marktpreis.
  if (!raw.length && hasDealInfo) {
    raw = inWindow(priced, maxAge, true);
    if (raw.length < CAP_MIN_SALES && WINDOW_STRETCH > 1) {
      const wider = inWindow(priced, maxAge * WINDOW_STRETCH, true);
      if (wider.length > raw.length) raw = wider;
    }
  }

  // Dichte für die Halbwertszeit (v3.4): zählt die tatsächlich verwendeten Punkte.
  const nWindow = raw.length;

  // Prinzip 6 (v3.5): Der Sicherheitsdeckel unten fragt nicht nach dem Wert,
  // sondern ob die Karte überhaupt lebhaft gehandelt wird — dafür zählt JEDE
  // Verkaufsart. Eine Auktion von gestern beweist einen aktiven Markt, auch
  // wenn ihr Preis (Prinzip 0) nicht in den Wert einfließen darf.
  // Ohne das rutschten Karten allein durch den Filter unter die Schwelle und
  // wurden gedeckelt: bei rare in-season 23 -> 33 % der Fälle, ihr Wert dort im
  // Median halbiert und die Treffgenauigkeit von ±32,9 auf ±44,2 % verschlechtert.
  // Mit Prinzip 6 (Messung 07.09.): rare in-season ±27,0 -> ±24,2 %, gesamt
  // ±24,7 -> ±24,3 %, Bias +2,9 -> +2,3 %.
  const liqWindow = inWindow(priced, maxAge);
  const nLiquidity = Math.max(nWindow, liqWindow.length);
  const newestAge = Math.min(
    nWindow ? Math.min(...raw.map(e => e.age)) : Infinity,
    liqWindow.length ? Math.min(...liqWindow.map(e => e.age)) : Infinity,
  );

  // v3.4: effektive Halbwertszeit. Der Deckel unten prüft weiterhin gegen die
  // GRUND-Halbwertszeit, damit sich sein Verhalten nicht mitverschiebt.
  const halfLifeEff = halfLife / (1 + nWindow / DENSITY_DIVISOR);

  let entries = raw.map(e => ({ ...e, w: Math.pow(0.5, e.age / halfLifeEff) }));

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
  const thinOrStale = nLiquidity < CAP_MIN_SALES || newestAge > halfLife;
  return thinOrStale ? Math.min(blended, floorPrice * SELL_CAP) : blended;
}
