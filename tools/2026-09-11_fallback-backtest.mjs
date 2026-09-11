// ═══════════════════════════════════════════════════════════════════════════
// v3.6-Kandidaten: Was tun, wenn Manager-Verkaeufe fehlen?  (11.09.2026)
//
// VORGABE Jonas (11.09.): "bei so wenig verkaeufen muessen wir zwangslaeufig die
// Auktionen und Sofortkaeufe mit einbeziehen". Anlass: Aleix Garcia rare/in-season
// hat EINEN Manager-Verkauf in 25 Tagen; v3.5 gibt solchen Karten gar keinen Wert.
//
// ABER: Fremde Verkaufsarten sind nicht nur anders bepreist, sie sind oft andere
// Ware (Sorare versteigert frische Karten, Manager geben alte ab). Gemessen liegen
// Auktionen im Median 70 % ueber dem Manager-Preis derselben Karte, Sofortkaeufe
// ueber 130 %. Roh beigemischt heben sie den FMV also kraeftig an.
//
// Gemessen wird IMMER gegen Manager-Verkaeufe: nur die sind echte Marktpreise.
//   node tools/2026-09-11_fallback-backtest.mjs
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DATA = join(dirname(fileURLToPath(import.meta.url)), 'analysis-out', '2026-09-07_deal-type-data.json');
const d = JSON.parse(readFileSync(DATA, 'utf8'));
const DAY = 86400000;
const PROF = el => el === 'classic' ? { hl: 14, ma: 90 } : { hl: 3, ma: 21 };
const MIN = 3;                      // ab so vielen Verkaeufen gilt die Basis als tragfaehig
const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };

// Kern wie v3.5. `mix` entscheidet, welche Verkaeufe mit welchem Faktor zaehlen.
function fmv(sales, now, p, floor, mix) {
  const priced = sales.filter(s => s.eur > 0);
  const pick = (list, maxAge) => list
    .map(s => ({ v: s.eur * (mix.factor[s.deal] ?? 1), age: Math.max(0, (now - new Date(s.date).getTime()) / DAY) }))
    .filter(e => e.age <= maxAge);
  const mgr = priced.filter(s => s.deal === 'TokenOffer');

  // Stufe 1: nur Manager-Verkaeufe, normales Fenster, dann gedehnt (v3.5)
  let raw = pick(mgr, p.ma);
  if (raw.length < MIN) { const w = pick(mgr, p.ma * 3); if (w.length > raw.length) raw = w; }
  // Stufe 2: reicht das nicht, kommen die fremden Arten dazu (umgerechnet)
  let used = 'manager';
  if (raw.length < mix.need) {
    const all = pick(priced, p.ma);
    if (all.length > raw.length) { raw = all; used = 'gemischt'; }
    if (raw.length < MIN) { const w = pick(priced, p.ma * 3); if (w.length > raw.length) { raw = w; used = 'gemischt'; } }
  }
  const n = raw.length;
  if (!n) return null;
  const liq = pick(priced, p.ma);
  const nLiq = Math.max(n, liq.length);
  const newest = Math.min(Math.min(...raw.map(x => x.age)), liq.length ? Math.min(...liq.map(x => x.age)) : Infinity);
  const hlEff = p.hl / (1 + n / 5);
  let e = raw.map(x => ({ ...x, w: Math.pow(0.5, x.age / hlEff) }));
  if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }
  const tw = e.reduce((s, x) => s + x.w, 0);
  const sv = e.reduce((s, x) => s + x.v * x.w, 0) / tw;
  const thin = nLiq < MIN || newest > p.hl;
  const val = (!(floor > 0) || floor >= sv) ? sv : (thin ? Math.min(sv, floor * 1.5) : sv);
  return { v: val, used };
}

const ROH  = { TokenAuction: 1,    TokenPrimaryOffer: 1 };
const GEM  = { TokenAuction: 0.59, TokenPrimaryOffer: 0.42 };   // Kehrwert der gemessenen Aufschlaege
const MILD = { TokenAuction: 0.75, TokenPrimaryOffer: 0.60 };
const M85 = { TokenAuction: 0.85, TokenPrimaryOffer: 0.70 };
const M90 = { TokenAuction: 0.90, TokenPrimaryOffer: 0.80 };
const V = {
  'v3.3 (heute live, alles roh)':        { factor: ROH,  need: 99 },   // need 99 = immer mischen
  'v3.5 (nur Manager)':                  { factor: ROH,  need: 0 },    // need 0 = nie mischen
  'A Fallback roh, wenn <1 Manager':     { factor: ROH,  need: 1 },
  'B Fallback roh, wenn <3 Manager':     { factor: ROH,  need: 3 },
  'F nur-wenn-noetig, mild 0,75/0,60':   { factor: MILD, need: 1 },
  'G nur-wenn-noetig, 0,85/0,70':        { factor: M85,  need: 1 },
  'H nur-wenn-noetig, 0,90/0,80':        { factor: M90,  need: 1 },
  'I nur-wenn-noetig, voll umgerechnet': { factor: GEM,  need: 1 },
};
const K = Object.keys(V);

const rows = [];
for (const r of d.sample) {
  const p = PROF(r.eligibility), s = r.sales, fl = r.floor_price ?? 0;
  for (let t = 0; t < s.length - 3; t++) {
    if (s[t].deal !== 'TokenOffer') continue;        // Ziel ist immer ein echter Marktpreis
    const now = new Date(s[t].date).getTime(), hist = s.slice(t + 1);
    const est = {};
    for (const k of K) est[k] = fmv(hist, now, p, fl, V[k]);
    if (!est[K[0]] || est[K[0]].v <= 0) continue;    // v3.3 bestimmt die Zielmenge
    rows.push({
      seg: `${r.scarcity}/${r.eligibility}`, sale: s[t].eur,
      dev: Object.fromEntries(K.map(k => [k, est[k] && est[k].v > 0 ? (s[t].eur - est[k].v) / est[k].v * 100 : null])),
      used: Object.fromEntries(K.map(k => [k, est[k]?.used ?? null])),
      rettung: est['v3.5 (nur Manager)'] == null,   // hier versagt der reine Filter
    });
  }
}

const out = [];
const say = t => { console.log(t); out.push(t); };
say(`# v3.6: Auktionen und Sofortkaeufe als Rueckfallebene (${new Date().toISOString().slice(0, 16)} UTC)\n`);
say(`Zielmenge: ${rows.length} Walk-Forward-Ziele, Ziel ist immer ein Manager-Verkauf.`);
say('Median = mittlere absolute Abweichung, kleiner ist besser. Bias positiv = zu niedrig geschaetzt.\n');

const block = (label, sel) => {
  const dd = rows.filter(sel);
  if (dd.length < 40) return;
  say(`\n## ${label} (n=${dd.length})\n`);
  say('| Variante | Median | Bias | ohne Wert | gemischt gerechnet |');
  say('|---|---|---|---|---|');
  for (const k of K) {
    const have = dd.filter(r => r.dev[k] != null);
    if (!have.length) continue;
    const dev = have.map(r => r.dev[k]);
    const mix = have.filter(r => r.used[k] === 'gemischt').length / have.length * 100;
    say(`| ${k} | ±${med(dev.map(Math.abs)).toFixed(1)} % | ${(med(dev) > 0 ? '+' : '') + med(dev).toFixed(1)} % | ${((dd.length - have.length) / dd.length * 100).toFixed(0)} % | ${mix.toFixed(0)} % |`);
  }
};
block('GESAMT', () => true);
for (const seg of [...new Set(rows.map(r => r.seg))].sort()) block(seg, r => r.seg === seg);
block('NUR die Faelle, in denen v3.5 gar nichts liefert', r => r.rettung);

writeFileSync('docs/2026-09-11_FALLBACK_BACKTEST.md', out.join('\n') + '\n');
console.log('\nBericht: docs/2026-09-11_FALLBACK_BACKTEST.md');
