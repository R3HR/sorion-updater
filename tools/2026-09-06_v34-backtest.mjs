// ═══════════════════════════════════════════════════════════════════════════
// FMV v3.4-Kandidaten: Backtest gegen v3.3 und avg5  (06.09.2026)
//
// KANDIDAT (HANDOFF seit 26.08.): "bei sehr frischer, sehr liquider Basis die
// juengsten Verkaeufe noch staerker gewichten (kuerzere effektive Halbwertszeit
// bei hoher Sales-Dichte)".
//
// BEGRUENDUNG (neu, 06.09.): Die Stichprobenmessung
// docs/2026-09-06_SUPPLY_VS_ACCURACY.md zeigt bei den MEISTGEHANDELTEN Karten
// einen durchgehend POSITIVEN Bias (+7 bis +26 %): dort schaetzen wir zu
// niedrig. Genau das erwartet man, wenn die Gewichtung der Preisbewegung
// hinterherhinkt. Waehrend die Gesamtmessung negativen Bias zeigt (zu hoch).
//
// METHODE: exakt dieselbe Zielmenge wie der v3.3-Backtest und der avg5-Vergleich
// (tools/analysis-out/2026-08-25_factor-data.json, 800 geschichtete Spieler,
// Walk-Forward ueber jeden Verkauf). Komplett offline, keine API-Aufrufe.
//
//   node tools/2026-09-06_v34-backtest.mjs
//
// AUSGABE: docs/2026-09-06_V34_BACKTEST.md. **Kein Deploy ohne Jonas.**
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const DATA = join(dirname(fileURLToPath(import.meta.url)), 'analysis-out', '2026-08-25_factor-data.json');
const fd = JSON.parse(readFileSync(DATA, 'utf8'));
const DAY = 86400000;
const NOW = new Date(fd.meta.generated_at).getTime();
const PROF = el => el === 'classic' ? { hl: 14, ma: 90 } : { hl: 3, ma: 21 };

// ── Gewichteter Schnitt mit frei waehlbarer Halbwertszeit ───────────────────
// Zweistufig: erst zaehlen (nWin bestimmt die effektive HL), dann gewichten.
function svWith(sales, now, p, hlFn) {
  const raw = sales.filter(s => s && s.eur > 0)
    .map(s => ({ v: s.eur, age: Math.max(0, (now - new Date(s.date).getTime()) / DAY) }))
    .filter(x => x.age <= p.ma);
  const nWin = raw.length;
  if (!nWin) return null;
  const newest = Math.min(...raw.map(x => x.age));
  const spread = Math.max(...raw.map(x => x.age)) - newest;   // Zeitspanne der Basis
  const hlEff = hlFn({ nWin, newest, spread, hl: p.hl });
  let e = raw.map(x => ({ ...x, w: Math.pow(0.5, x.age / hlEff) }));
  if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }   // Ausreisser trimmen
  if (!e.length) return null;
  const tw = e.reduce((s, x) => s + x.w, 0);
  return { sv: e.reduce((s, x) => s + x.v * x.w, 0) / tw, nWin, newest, hlEff };
}
// Sicherheitsdeckel wie v3.3 (unveraendert in allen Kandidaten)
const cap = (i, fl, hl) => (i.nWin < 3 || i.newest > hl) && fl > 0 && fl < i.sv ? Math.min(i.sv, fl * 1.5) : i.sv;
const avgN = (sales, n) => {
  const xs = sales.filter(s => s && s.eur > 0).slice(0, n).map(s => s.eur);
  return xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null;
};

// ── Die Kandidaten ──────────────────────────────────────────────────────────
// Alle lassen v3.3 unangetastet, sobald die Basis duenn ist; sie greifen NUR
// bei liquider Basis. Damit bleibt das Verhalten fuer kalte Karten identisch.
const VARIANTS = {
  'v3.3 (heute live)': ({ hl }) => hl,
  'C: /(1+nWin/10)': ({ nWin, hl }) => hl / (1 + nWin / 10),
  'D: /(1+nWin/5)': ({ nWin, hl }) => hl / (1 + nWin / 5),
  'F: /(1+nWin/4)': ({ nWin, hl }) => hl / (1 + nWin / 4),
  'G: /(1+nWin/3)': ({ nWin, hl }) => hl / (1 + nWin / 3),
  'H: /(1+nWin/2)': ({ nWin, hl }) => hl / (1 + nWin / 2),
  'I: /(1+nWin/3), Untergrenze 0,75d': ({ nWin, hl }) => Math.max(0.75, hl / (1 + nWin / 3)),
  'J: /(1+nWin/5), Untergrenze 0,75d': ({ nWin, hl }) => Math.max(0.75, hl / (1 + nWin / 5)),
};
const KEYS = Object.keys(VARIANTS);

// ── Walk-Forward ────────────────────────────────────────────────────────────
const rows = [];
for (const r of fd.sample) {
  const s = r.sales; if (!s || s.length < 4) continue;
  const p = PROF(r.eligibility);
  for (let t = 0; t < s.length - 3; t++) {
    const now = new Date(s[t].date).getTime();
    const hist = s.slice(t + 1);
    const base = svWith(hist, now, p, VARIANTS[KEYS[0]]);   // Referenz bestimmt die Zielmenge
    if (!base || base.sv <= 0) continue;
    const a5 = avgN(hist, 5);
    if (a5 == null || a5 <= 0) continue;
    const fl = r.floor_price ?? 0;
    const est = { avg5: a5 };
    let ok = true;
    for (const k of KEYS) {
      const i = svWith(hist, now, p, VARIANTS[k]);
      if (!i || i.sv <= 0) { ok = false; break; }
      est[k] = cap(i, fl, p.hl);
    }
    if (!ok) continue;
    rows.push({
      grp: `${r.scarcity}/${r.eligibility}`,
      liq: base.nWin < 3 ? '1-2 Sales' : base.nWin < 6 ? '3-5 Sales' : '6+ Sales',
      key: r.player_slug + '|' + r.scarcity + '|' + r.eligibility,
      est,
      d: Object.fromEntries(Object.entries(est).map(([k, v]) => [k, (s[t].eur - v) / v * 100])),
    });
  }
}

// ── Auswertung ──────────────────────────────────────────────────────────────
const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };
const stats = d => ({ n: d.length, med: med(d.map(Math.abs)), bias: med(d), hit: d.filter(x => Math.abs(x) <= 20).length / d.length * 100 });
const out = [];
const say = s => { console.log(s); out.push(s); };

say(`# FMV v3.4-Kandidaten: Backtest (${new Date().toISOString().slice(0, 16)} UTC)\n`);
say(`Zielmenge: **${rows.length} Walk-Forward-Ziele**, identisch fuer alle Varianten`);
say(`(Datensatz 25.08., 800 geschichtete Spieler). Median = mittlere absolute Abweichung`);
say(`vom tatsaechlichen Verkauf, kleiner ist besser. Bias signiert: **positiv = zu niedrig geschaetzt**.\n`);

const block = (label, sel) => {
  const d = rows.filter(sel);
  if (d.length < 40) return;
  say(`\n## ${label} (n=${d.length})\n`);
  say('| Variante | Median | Bias | Treffer ±20 % |');
  say('|---|---|---|---|');
  const ref = stats(d.map(x => x.d[KEYS[0]]));
  for (const k of [...KEYS, 'avg5']) {
    const o = stats(d.map(x => x.d[k]));
    const better = k !== KEYS[0] && k !== 'avg5' ? (o.med < ref.med ? ' ✅' : o.med > ref.med ? ' ❌' : ' =') : '';
    say(`| ${k === 'avg5' ? '*avg5 (Wettbewerb)*' : k}${better} | ±${o.med.toFixed(1)} % | ${(o.bias > 0 ? '+' : '') + o.bias.toFixed(1)} % | ${o.hit.toFixed(0)} % |`);
  }
};
block('GESAMT', () => true);
for (const g of ['limited/in_season', 'limited/classic', 'rare/in_season', 'rare/classic']) block(g, r => r.grp === g);
for (const l of ['1-2 Sales', '3-5 Sales', '6+ Sales']) block(`Liquiditaet: ${l}`, r => r.liq === l);

// ── Stabilitaet: wie stark springt die Schaetzung von Ziel zu Ziel? ─────────
// Wichtig fuer Nutzer: ein Wert, der staendig springt, wirkt unzuverlaessig, auch
// wenn er im Median naeher liegt. Gemessen als Median der absoluten prozentualen
// Aenderung zwischen aufeinanderfolgenden Schaetzungen derselben Karte.
const byCard = {};
for (const r of rows) (byCard[r.key] ??= []).push(r.est);
const jumps = {};
for (const k of [...KEYS, 'avg5']) jumps[k] = [];
for (const seq of Object.values(byCard)) {
  for (let i = 1; i < seq.length; i++) {
    for (const k of [...KEYS, 'avg5']) {
      const a = seq[i - 1][k], b = seq[i][k];
      if (a > 0 && b > 0) jumps[k].push(Math.abs(b - a) / a * 100);
    }
  }
}
say('\n## Stabilitaet (Nebenkriterium)\n');
say('Median der Sprunghoehe zwischen aufeinanderfolgenden Schaetzungen derselben Karte.');
say('Kleiner = ruhiger. Ein niedriger Fehler bei hoher Sprunghoehe waere ein schlechter');
say('Tausch: der Wert traefe im Schnitt besser, wuerde aber staendig zappeln.\n');
say('| Variante | Sprunghoehe |');
say('|---|---|');
for (const k of [...KEYS, 'avg5']) {
  if (!jumps[k].length) continue;
  say('| ' + (k === 'avg5' ? '*avg5 (Wettbewerb)*' : k) + ' | ' + med(jumps[k]).toFixed(1) + ' % |');
}

writeFileSync('docs/2026-09-06_V34_BACKTEST.md', out.join('\n') + '\n');
console.log('\nBericht: docs/2026-09-06_V34_BACKTEST.md');
