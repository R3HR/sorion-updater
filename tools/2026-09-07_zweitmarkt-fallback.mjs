// ═══════════════════════════════════════════════════════════════════════════
// Nur Manager-Verkaeufe: was tun, wenn die Basis zu duenn wird?  (07.09.2026)
//
// AUSGANGSLAGE (docs/2026-09-07_ZWEITMARKT_FILTER.md): Die Vorgabe "nur
// TokenOffer" ist bei Limited ein Gewinn, kostet bei rare/in_season aber 19 %
// der Karten ihre Schaetzung und druckt dort die Genauigkeit.
//
// FRAGE: Faengt eine Fensterverlaengerung das auf? Also: zu wenige
// Manager-Verkaeufe in 21 Tagen -> schau weiter zurueck, aber weiterhin NUR
// auf Manager-Verkaeufe. Die Vorgabe bleibt damit unangetastet; wir nehmen
// aeltere echte Marktpreise statt fremder Verkaufsarten.
//
// Offline, keine API-Aufrufe. Nutzt den Datensatz vom 07.09.
//   node tools/2026-09-07_zweitmarkt-fallback.mjs
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DATA = join(dirname(fileURLToPath(import.meta.url)), 'analysis-out', '2026-09-07_deal-type-data.json');
const d = JSON.parse(readFileSync(DATA, 'utf8'));
const DAY = 86400000;
const PROF = el => el === 'classic' ? { hl: 14, ma: 90 } : { hl: 3, ma: 21 };
const MIN_SALES = 3;          // ab hier gilt die Basis als tragfaehig (wie CAP_MIN_SALES)

// FMV wie v3.4, mit waehlbarem Filter und waehlbarem Fenster.
// stretch = Faktor, um den das Fenster waechst, wenn unter MIN_SALES Verkaeufe
// darin liegen. 1 = keine Verlaengerung.
function fmv(sales, now, p, onlyOffer, floor, stretch = 1) {
  const pick = maxAge => sales
    .filter(s => s.eur > 0 && (!onlyOffer || s.deal === 'TokenOffer'))
    .map(s => ({ v: s.eur, age: Math.max(0, (now - new Date(s.date).getTime()) / DAY) }))
    .filter(x => x.age <= maxAge);

  let raw = pick(p.ma);
  let stretched = false;
  if (raw.length < MIN_SALES && stretch > 1) {
    const wider = pick(p.ma * stretch);
    if (wider.length > raw.length) { raw = wider; stretched = true; }
  }
  const n = raw.length;
  if (!n) return null;

  const newest = Math.min(...raw.map(x => x.age));
  const hlEff = p.hl / (1 + n / 5);                      // v3.4, unveraendert
  let e = raw.map(x => ({ ...x, w: Math.pow(0.5, x.age / hlEff) }));
  if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }
  const tw = e.reduce((s, x) => s + x.w, 0);
  const sv = e.reduce((s, x) => s + x.v * x.w, 0) / tw;
  // Sicherheitsdeckel wie v3.3/v3.4: duenne oder alte Basis -> gegen Floor deckeln
  const val = (!(floor > 0) || floor >= sv) ? sv
            : (n < MIN_SALES || newest > p.hl) ? Math.min(sv, floor * 1.5) : sv;
  return { v: val, n, stretched };
}

const VAR = {
  'A heute (alle Arten)':        (h, now, p, fl) => fmv(h, now, p, false, fl, 1),
  'B nur Manager':               (h, now, p, fl) => fmv(h, now, p, true,  fl, 1),
  'C nur Manager, Fenster x2':   (h, now, p, fl) => fmv(h, now, p, true,  fl, 2),
  'D nur Manager, Fenster x3':   (h, now, p, fl) => fmv(h, now, p, true,  fl, 3),
  'E nur Manager, Fenster x5':   (h, now, p, fl) => fmv(h, now, p, true,  fl, 5),
};
const KEYS = Object.keys(VAR);

const rows = [];
for (const r of d.sample) {
  const p = PROF(r.eligibility);
  const s = r.sales;
  for (let t = 0; t < s.length - 3; t++) {
    if (s[t].deal !== 'TokenOffer') continue;            // nur echte Marktpreise als Ziel
    const now = new Date(s[t].date).getTime();
    const hist = s.slice(t + 1);
    const fl = r.floor_price ?? 0;
    const est = {};
    for (const k of KEYS) est[k] = VAR[k](hist, now, p, fl);
    if (!est[KEYS[0]] || est[KEYS[0]].v <= 0) continue;  // A bestimmt die Zielmenge
    rows.push({
      seg: `${r.scarcity}/${r.eligibility}`,
      dev: Object.fromEntries(KEYS.map(k => [k, est[k] && est[k].v > 0 ? (s[t].eur - est[k].v) / est[k].v * 100 : null])),
      stretched: Object.fromEntries(KEYS.map(k => [k, est[k]?.stretched ?? false])),
    });
  }
}

const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };
const out = [];
const say = t => { console.log(t); out.push(t); };
const segs = [...new Set(rows.map(r => r.seg))].sort();

say(`# Nur Manager-Verkaeufe: Fensterverlaengerung statt Datenverlust (${new Date().toISOString().slice(0, 16)} UTC)\n`);
say('Anschluss an `2026-09-07_ZWEITMARKT_FILTER.md`. Die Vorgabe (nur TokenOffer) steht fest.');
say('Hier geht es nur darum, wie mit Karten umzugehen ist, deren Manager-Basis zu duenn wird.');
say(`Fremde Verkaufsarten kommen in KEINER Variante zurueck, es wird nur weiter zurueckgeschaut.\n`);
say(`Zielmenge: ${rows.length} Walk-Forward-Ziele, Ziel ist immer ein Manager-Verkauf.`);
say('Median = mittlere absolute Abweichung, kleiner ist besser. Bias positiv = zu niedrig geschaetzt.\n');

for (const seg of [...segs, 'ALLE']) {
  const dd = seg === 'ALLE' ? rows : rows.filter(r => r.seg === seg);
  if (dd.length < 30) continue;
  say(`\n## ${seg} (n=${dd.length})\n`);
  say('| Variante | Median | Bias | ohne Schaetzung | davon Fenster gedehnt |');
  say('|---|---|---|---|---|');
  const refPure = dd.filter(r => r.dev[KEYS[1]] != null);
  for (const k of KEYS) {
    const have = dd.filter(r => r.dev[k] != null);
    if (!have.length) continue;
    const gone = ((dd.length - have.length) / dd.length * 100).toFixed(0);
    const str = (have.filter(r => r.stretched[k]).length / have.length * 100).toFixed(0);
    const m = med(have.map(r => Math.abs(r.dev[k])));
    const b = med(have.map(r => r.dev[k]));
    say(`| ${k} | ±${m.toFixed(1)} % | ${(b > 0 ? '+' : '') + b.toFixed(1)} % | ${gone} % | ${str} % |`);
  }
  // Kernfrage: die Ziele, die "B nur Manager" gar nicht schaetzen kann.
  const rescued = dd.filter(r => r.dev[KEYS[1]] == null && r.dev['C nur Manager, Fenster x2'] != null);
  if (rescued.length >= 20) {
    const mC = med(rescued.map(r => Math.abs(r.dev['C nur Manager, Fenster x2'])));
    const mA = med(rescued.filter(r => r.dev[KEYS[0]] != null).map(r => Math.abs(r.dev[KEYS[0]])));
    say(`\nBei den ${rescued.length} Zielen, die B gar nicht schaetzen koennte, trifft C auf ±${mC.toFixed(1)} %`);
    say(`(heutiger Stand mit allen Arten dort: ±${mA.toFixed(1)} %).`);
  }
}

writeFileSync('docs/2026-09-07_ZWEITMARKT_FALLBACK.md', out.join('\n') + '\n');
console.log('\nBericht: docs/2026-09-07_ZWEITMARKT_FALLBACK.md');
