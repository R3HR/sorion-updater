// ═══════════════════════════════════════════════════════════════════════════
// SORION — Reproduktion des v3.3-Backtests (bedingter SELL_CAP), 25.08.2026
//
// Läuft KOMPLETT OFFLINE auf den Rohdaten des Faktoren-Datenlaufs:
//   tools/analysis-out/2026-08-25_factor-data.json  (tools/2026-08-25_factor-data.mjs)
// Kein API-Call, kein DB-Zugriff. Walk-Forward ohne Leakage: jeder Verkauf wird
// ausschließlich aus den ÄLTEREN Verkäufen vorhergesagt; v3.2 und v3.3 laufen
// über exakt dieselben Ziele.
//
// Aufruf:  node tools/2026-08-25_fmv-v33-backtest.mjs
// Bekannte Grenze (wie tools/fmv-backtest.mjs): der Floor ist der HEUTIGE —
// die Rangfolge trägt, Absolutwerte für alte Ziele sind unscharf. Die frische
// Scheibe (Ziele ≤7 Tage) ist deshalb separat ausgewiesen.
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DATA = join(dirname(fileURLToPath(import.meta.url)), 'analysis-out', '2026-08-25_factor-data.json');
const fd = JSON.parse(readFileSync(DATA, 'utf8'));
const DAY = 86400000;
const NOW = new Date(fd.meta.generated_at).getTime();   // fixer Bezug = Datenlauf
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
// v3.2: Cap immer. v3.3: Cap nur bei <3 Sales im Fenster ODER jüngstem Sale älter als halfLife.
const v32 = (i, fl) => fl > 0 && fl < i.sv ? Math.min(i.sv, fl * 1.5) : i.sv;
const v33 = (i, fl, hl) => (i.nWin < 3 || i.newest > hl) && fl > 0 && fl < i.sv ? Math.min(i.sv, fl * 1.5) : i.sv;

const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };
const stats = d => ({ n: d.length, med: med(d.map(Math.abs)), bias: med(d), hit: d.filter(x => Math.abs(x) <= 20).length / d.length * 100 });
const fmt = (label, o, n) => `${label.padEnd(36)} n=${String(o.n).padStart(5)}  ${o.med.toFixed(1).padStart(6)}% → ${n.med.toFixed(1).padStart(5)}%   ${((o.bias > 0 ? '+' : '') + o.bias.toFixed(1)).padStart(6)}% → ${((n.bias > 0 ? '+' : '') + n.bias.toFixed(1)).padStart(6)}%   ${o.hit.toFixed(0).padStart(3)}% → ${n.hit.toFixed(0).padStart(3)}%`;

const rows = [];
for (const r of fd.sample) {
  const s = r.sales; if (s.length < 4) continue;
  const p = PROF(r.eligibility);
  for (let t = 0; t < s.length - 3; t++) {
    const now = new Date(s[t].date).getTime();
    const info = svInfo(s.slice(t + 1), now, p);
    if (!info || info.sv <= 0) continue;
    const fl = r.floor_price ?? 0;
    const pOld = v32(info, fl), pNew = v33(info, fl, p.hl);
    rows.push({
      grp: `${r.scarcity}/${r.eligibility}`,
      liq: info.nWin < 3 ? '1-2 Sales' : '3+ Sales',
      fresh: (NOW - now) / DAY <= 7,
      cheap: pOld < 2,
      changed: Math.abs(pOld - pNew) > 1e-9,
      dOld: (s[t].eur - pOld) / pOld * 100,
      dNew: (s[t].eur - pNew) / pNew * 100,
    });
  }
}
console.log(`Ziele: ${rows.length} — durch v3.3 verändert: ${rows.filter(r => r.changed).length} (${(100 * rows.filter(r => r.changed).length / rows.length).toFixed(0)}%)`);
console.log('\nSegment                                      Median-Abw (v3.2→v3.3)  Bias (v3.2→v3.3)   <20% (v3.2→v3.3)');
console.log('─'.repeat(112));
const cut = (label, sel) => { const d = rows.filter(sel); if (d.length < 30) return; console.log(fmt(label, stats(d.map(x => x.dOld)), stats(d.map(x => x.dNew)))); };
cut('GESAMT', () => true);
for (const g of ['limited/in_season', 'limited/classic', 'rare/in_season']) cut(g, r => r.grp === g);
console.log();
for (const g of ['limited/in_season', 'limited/classic', 'rare/in_season'])
  for (const l of ['1-2 Sales', '3+ Sales']) cut(`  ${g} · ${l}`, r => r.grp === g && r.liq === l);
console.log();
cut('nur frische Ziele (≤7d)', r => r.fresh);
cut('Billigsegment (<2 €)', r => r.cheap);
cut('nur durch v3.3 veränderte Ziele', r => r.changed);
