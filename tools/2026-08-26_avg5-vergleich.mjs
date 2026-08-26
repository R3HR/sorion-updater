// ═══════════════════════════════════════════════════════════════════════════
// Wettbewerbs-Vergleich: "Last-5-Average" (Sorare-Inside-Methode) vs. FMV v3.3
// (26.08.2026, Frage Jonas) — auf EXAKT denselben Walk-Forward-Zielen wie der
// v3.3-Backtest (tools/2026-08-25_fmv-v33-backtest.mjs), komplett offline.
//
// avg5 = ungewichteter Durchschnitt der (bis zu) 5 juengsten Verkaeufe VOR dem
// Ziel, ohne Altersgrenze — so rechnet ein simples "Last 5 Avg Sales".
// Zum Einordnen laeuft avg10 (unsere Tabellen-Spalte "Avg Sales") mit.
//
// Aufruf:  node tools/2026-08-26_avg5-vergleich.mjs
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DATA = join(dirname(fileURLToPath(import.meta.url)), 'analysis-out', '2026-08-25_factor-data.json');
const fd = JSON.parse(readFileSync(DATA, 'utf8'));
const DAY = 86400000;
const NOW = new Date(fd.meta.generated_at).getTime();
const PROF = el => el === 'classic' ? { hl: 14, ma: 90 } : { hl: 3, ma: 21 };

function svInfo(sales, now, p) {
  let e = sales.filter(s => s && s.eur > 0)
    .map(s => { const age = Math.max(0, (now - new Date(s.date).getTime()) / DAY); return { v: s.eur, w: Math.pow(.5, age / p.hl), age }; })
    .filter(x => x.age <= p.ma);
  const nWin = e.length, newest = e.length ? Math.min(...e.map(x => x.age)) : Infinity;
  if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }
  if (!e.length) return null;
  const tw = e.reduce((s, x) => s + x.w, 0);
  return { sv: e.reduce((s, x) => s + x.v * x.w, 0) / tw, nWin, newest };
}
const v33 = (i, fl, hl) => (i.nWin < 3 || i.newest > hl) && fl > 0 && fl < i.sv ? Math.min(i.sv, fl * 1.5) : i.sv;
const avgN = (sales, n) => {
  const xs = sales.filter(s => s && s.eur > 0).slice(0, n).map(s => s.eur);
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
};

const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };
const stats = d => ({ n: d.length, med: med(d.map(Math.abs)), bias: med(d), hit: d.filter(x => Math.abs(x) <= 20).length / d.length * 100 });
const line = (label, o) => `${label.padEnd(14)} Median ±${o.med.toFixed(1).padStart(5)}%   Bias ${((o.bias > 0 ? '+' : '') + o.bias.toFixed(1)).padStart(6)}%   ±20%: ${o.hit.toFixed(0).padStart(3)}%`;

const rows = [];
for (const r of fd.sample) {
  const s = r.sales; if (s.length < 4) continue;
  const p = PROF(r.eligibility);
  for (let t = 0; t < s.length - 3; t++) {
    const now = new Date(s[t].date).getTime();
    const info = svInfo(s.slice(t + 1), now, p);
    if (!info || info.sv <= 0) continue;                 // identische Zielmenge wie v3.3-Backtest
    const a5 = avgN(s.slice(t + 1), 5), a10 = avgN(s.slice(t + 1), 10);
    if (a5 == null || a5 <= 0 || a10 == null || a10 <= 0) continue;
    const fl = r.floor_price ?? 0;
    const est = { v33: v33(info, fl, p.hl), avg5: a5, avg10: a10 };
    rows.push({
      grp: `${r.scarcity}/${r.eligibility}`,
      liq: info.nWin < 3 ? '1-2 Sales' : '3+ Sales',
      fresh: (NOW - now) / DAY <= 7,
      d: Object.fromEntries(Object.entries(est).map(([k, v]) => [k, (s[t].eur - v) / v * 100])),
    });
  }
}
console.log(`Identische Walk-Forward-Ziele: ${rows.length}\n`);
const block = (label, sel) => {
  const d = rows.filter(sel);
  if (d.length < 30) return;
  console.log(`── ${label}  (n=${d.length})`);
  for (const k of ['v33', 'avg5', 'avg10']) console.log('   ' + line(k === 'v33' ? 'FMV v3.3' : k, stats(d.map(x => x.d[k]))));
  console.log();
};
block('GESAMT', () => true);
for (const g of ['limited/in_season', 'limited/classic', 'rare/in_season']) block(g, r => r.grp === g);
for (const l of ['1-2 Sales', '3+ Sales']) block(`Liquiditaet: ${l}`, r => r.liq === l);
block('nur frische Ziele (≤7d)', r => r.fresh);
