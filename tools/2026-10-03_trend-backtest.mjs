// ═══════════════════════════════════════════════════════════════════════════
// v3.7-Kandidaten: folgt der FMV einem fallenden Markt schnell genug?  (03.10.2026)
//
// BEFUND (fmv_accuracy, 21 Tage, nur Manager-Verkaeufe): Lagen die letzten DREI
// Verkaeufe einer Karte unter unserem FMV, liegt der naechste im Median 11,3 %
// darunter, in 72 % der Faelle. Umgekehrt +8,0 %, wenn alle drei darueber lagen.
// Der Wert hinkt also beiden Richtungen hinterher. Jonas: "Karten haben viel Wert,
// fallen aber immer weiter."
//
// GEPRUEFT wird, ob eine Korrektur die SCHAETZUNG verbessert, nicht ob der Effekt
// existiert (Lehre Spezialedition 12.09.: echt, aber ohne Gewinn im Backtest).
//
//   node tools/2026-10-03_trend-backtest.mjs <datensatz.json>
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync } from 'node:fs';
const FILE = process.argv[2];
const raw = JSON.parse(readFileSync(FILE, 'utf8'));
const rows = raw.rows ?? raw.sample ?? [];
const DAY = 86400000;
const PROF = el => el === 'classic' ? { hl: 14, ma: 90 } : { hl: 3, ma: 21 };
const MIN = 3, STRETCH = 3;
const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };

// v3.6 plus optionale Trendkorrektur.
// trend: null = aus; sonst { mult: {0:..,1:..,2:..,3:..}, hlDiv: zahl }
function signalOf(hist, now, p) {
  const r = fmv(hist, now, p, 0, null, true);
  return r == null ? null : r.below;
}

function fmv(hist, now, p, floor, trend, withSignal) {
  const priced = hist.filter(s => s.eur > 0);
  const mgr = priced.filter(s => s.deal === 'TokenOffer');
  const win = (list, maxAge) => list
    .map(s => ({ v: s.eur, age: (now - new Date(s.date).getTime()) / DAY }))
    .filter(e => e.age >= 0 && e.age <= maxAge);
  let base = win(mgr, p.ma);
  if (base.length < MIN) { const w = win(mgr, p.ma * STRETCH); if (w.length > base.length) base = w; }
  let used = base;
  if (!used.length) { used = win(priced, p.ma); if (used.length < MIN) { const w = win(priced, p.ma * STRETCH); if (w.length > used.length) used = w; } }
  if (!used.length) return null;

  const liq = win(priced, p.ma);
  const nLiq = Math.max(used.length, liq.length);
  const newest = Math.min(Math.min(...used.map(x => x.age)), liq.length ? Math.min(...liq.map(x => x.age)) : Infinity);

  const weighted = (entries, hl) => {
    const hlEff = hl / (1 + entries.length / 5);
    let e = entries.map(x => ({ ...x, w: Math.pow(0.5, x.age / hlEff) }));
    if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }
    const tw = e.reduce((s, x) => s + x.w, 0);
    return e.reduce((s, x) => s + x.v * x.w, 0) / tw;
  };

  let sv = weighted(used, p.hl);

  const last3all = [...used].sort((a, b) => a.age - b.age).slice(0, 3);
  const belowCount = last3all.length === 3 ? last3all.filter(x => x.v < sv).length : null;
  if (withSignal) return { below: belowCount };

  if (trend) {
    // Signal: wie viele der letzten DREI Manager-Verkaeufe lagen unter dem
    // unkorrigierten Wert? 3 = klarer Abwaertstrend, 0 = klarer Aufwaertstrend.
    const last3 = [...used].sort((a, b) => a.age - b.age).slice(0, 3);
    if (last3.length === 3) {
      const below = last3.filter(x => x.v < sv).length;
      if (trend.hlDiv && (below === 3 || below === 0)) sv = weighted(used, p.hl / trend.hlDiv);
      if (trend.mult) sv *= (trend.mult[below] ?? 1);
    }
  }

  const thin = nLiq < MIN || newest > p.hl;
  return (!(floor > 0) || floor >= sv) ? sv : (thin ? Math.min(sv, floor * 1.5) : sv);
}

const VAR = {
  'v3.6 (heute live)':              null,
  'A halbe Korrektur':              { mult: { 3: 0.945, 2: 0.978, 1: 1, 0: 1.04 } },
  'B volle Korrektur':              { mult: { 3: 0.887, 2: 0.956, 1: 0.987, 0: 1.08 } },
  'C Halbwertszeit halbiert':       { hlDiv: 2 },
  'D Halbwertszeit gedrittelt':     { hlDiv: 3 },
  'E HL halbiert + halbe Korrektur':{ hlDiv: 2, mult: { 3: 0.945, 2: 0.978, 1: 1, 0: 1.04 } },
};
const K = Object.keys(VAR);

const res = {};
for (const k of K) res[k] = { all: [], liq: [], thin: [], est: {} };
let targets = 0;
for (const r of rows) {
  const p = PROF(r.eligibility);
  const s = [...r.sales].sort((a, b) => new Date(b.date) - new Date(a.date));
  const fl = r.floor_price ?? 0;
  for (let t = 0; t < s.length - 3; t++) {
    if (s[t].deal !== 'TokenOffer') continue;
    const now = new Date(s[t].date).getTime(), hist = s.slice(t + 1);
    const inWin = hist.filter(x => x.deal === 'TokenOffer' && (now - new Date(x.date).getTime()) / DAY <= p.ma).length;
    const vals = {};
    let ok = true;
    for (const k of K) { const v = fmv(hist, now, p, fl, VAR[k]); if (!v || v <= 0) { ok = false; break; } vals[k] = v; }
    if (!ok) continue;
    targets++;
    const sig = signalOf(hist, now, p);
    for (const k of K) {
      const dev = (s[t].eur - vals[k]) / vals[k] * 100;
      res[k].all.push(dev);
      (inWin >= 5 ? res[k].liq : res[k].thin).push(dev);
      if (sig === 3) (res[k].down ??= []).push(dev);
      if (sig === 0) (res[k].up ??= []).push(dev);
      const key = r.player_slug + '|' + r.scarcity;
      (res[k].est[key] ??= []).push(vals[k]);
    }
  }
}

const jump = est => {
  const j = [];
  for (const seq of Object.values(est)) for (let i = 1; i < seq.length; i++)
    if (seq[i - 1] > 0 && seq[i] > 0) j.push(Math.abs(seq[i] - seq[i - 1]) / seq[i - 1] * 100);
  return med(j);
};
const line = (k, d) => `| ${k} | ±${med(d.map(Math.abs)).toFixed(1)} % | ${(med(d) > 0 ? '+' : '') + med(d).toFixed(1)} % | ${(d.filter(x => Math.abs(x) <= 20).length / d.length * 100).toFixed(0)} % |`;

console.log(`Datensatz: ${FILE.split(/[\/]/).pop()}, ${rows.length} Karten, ${targets} Walk-Forward-Ziele\n`);
for (const [label, sel] of [['GESAMT', 'all'], ['liquide (5+ Manager-Verkaeufe)', 'liq'], ['duenn', 'thin'],
                            ['ABWAERTS: letzte 3 Verkaeufe UNTER dem Wert', 'down'],
                            ['AUFWAERTS: letzte 3 Verkaeufe UEBER dem Wert', 'up']]) {
  const n = (res[K[0]][sel] ?? []).length;
  if (n < 100) { console.log(`## ${label}: nur ${n} Ziele\n`); continue; }
  console.log(`## ${label} (n=${n})\n`);
  console.log('| Variante | Median | Bias | Treffer ±20 % |');
  console.log('|---|---|---|---|');
  for (const k of K) console.log(line(k, res[k][sel]));
  console.log('');
}
console.log('## Sprunghoehe (kleiner = ruhiger)\n');
for (const k of K) console.log(`  ${k.padEnd(34)} ${jump(res[k].est).toFixed(1)} %`);
