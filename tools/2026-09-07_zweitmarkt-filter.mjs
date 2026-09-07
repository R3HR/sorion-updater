// ═══════════════════════════════════════════════════════════════════════════
// FMV nur aus Manager-Verkaeufen: Datensatz ziehen und messen  (07.09.2026)
//
// VORGABE Jonas (bindend): Sofortkauf (TokenPrimaryOffer) und Auktion
// (TokenAuction) sind KEINE Marktpreise. Es gibt Gutscheine mit bis zu 50 %
// Rabatt, die nur auf diesen Maerkten gelten, dazu Zugaben (Essence,
// Wheel-Tickets) beim Kauf. Nur TokenOffer (Manager an Manager) zaehlt.
//
// DIESES SKRIPT beantwortet die zwei offenen Fragen zur Umsetzung:
//   1. ABDECKUNG: Wie viele Karten haben ueberhaupt noch genug Manager-Verkaeufe?
//      (Das ist das Risiko: bei In-Season Limited sind ~79 % der Sales Sorare selbst.)
//   2. GENAUIGKEIT: Trifft der FMV besser, wenn nur Manager-Verkaeufe zaehlen?
//
// Zieht einen frischen Datensatz mit Verkaufsart (die Historie in fmv_accuracy hat
// sie nicht, die Migration lief erst am 07.09.) und legt ihn ab, damit spaetere
// Laeufe offline gehen koennen.
//
//   railway run --service "Updater Limited" node tools/2026-09-07_zweitmarkt-filter.mjs
//   node tools/2026-09-07_zweitmarkt-filter.mjs --offline   (nutzt den Datensatz erneut)
// ═══════════════════════════════════════════════════════════════════════════
import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const OLD = join(HERE, 'analysis-out', '2026-08-25_factor-data.json');
const OUT = join(HERE, 'analysis-out', '2026-09-07_deal-type-data.json');
const OFFLINE = process.argv.includes('--offline');
const DAY = 86400000;
const PROF = el => el === 'classic' ? { hl: 14, ma: 90 } : { hl: 3, ma: 21 };
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── 1) Datensatz besorgen ───────────────────────────────────────────────────
let data;
if (OFFLINE || existsSync(OUT)) {
  data = JSON.parse(readFileSync(OUT, 'utf8'));
  console.log(`Datensatz vom ${data.meta.generated_at.slice(0, 16)} geladen: ${data.sample.length} Zeilen\n`);
} else {
  const KEY = process.env.SORARE_APIKEY;
  if (!KEY) { console.log('FEHLT: SORARE_APIKEY (railway run ...)'); process.exit(1); }
  const base = JSON.parse(readFileSync(OLD, 'utf8'));
  // Ein Aufruf zur Probe, BEVOR 800 Aufrufe laufen. Ohne ihn scheiterten am 07.09.
  // alle 800 still an `first: 40` (die API erlaubt hoechstens 20).
  const probe = await fetch('https://api.sorare.com/graphql', {
    method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY: KEY },
    body: JSON.stringify({ query: '{ tokens { tokenPrices(rarity: limited, seasonEligibility: IN_SEASON, playerSlug: "gregor-kobel", first: 20) { date amounts { eurCents } deal { __typename } } } }' }),
  }).then(r => r.json()).catch(e => ({ errors: [{ message: e.message }] }));
  if (probe.errors || !(probe.data?.tokens?.tokenPrices ?? []).length) {
    console.log('Probe-Aufruf fehlgeschlagen, Abbruch:', probe.errors?.map(e => e.message).join(' | ') ?? 'leere Antwort');
    process.exit(1);
  }
  console.log(`Probe OK (${probe.data.tokens.tokenPrices.length} Verkaeufe).`);
  console.log(`Hole Verkaufsarten fuer ${base.sample.length} Zeilen (je 1 Aufruf, ~350 ms)...\n`);
  const sample = [];
  let done = 0, failed = 0, firstError = null;
  for (const r of base.sample) {
    const elig = r.eligibility === 'classic' ? 'CLASSIC' : 'IN_SEASON';
    try {
      const res = await fetch('https://api.sorare.com/graphql', {
        method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY: KEY },
        body: JSON.stringify({ query: `{ tokens { tokenPrices(rarity: ${r.scarcity}, seasonEligibility: ${elig}, playerSlug: "${r.player_slug}", first: 20) { date amounts { eurCents } deal { __typename } } } }` }),
      });
      const d = await res.json();
      if (d.errors) { failed++; firstError ??= d.errors[0]?.message; }
      const sales = (d?.data?.tokens?.tokenPrices ?? [])
        .map(p => ({ date: p.date, eur: p.amounts.eurCents / 100, deal: p.deal?.__typename ?? null }));
      if (sales.length) sample.push({ player_slug: r.player_slug, scarcity: r.scarcity, eligibility: r.eligibility, floor_price: r.floor_price, sales });
    } catch (e) { failed++; firstError ??= e.message; }
    if (++done % 100 === 0) process.stdout.write(`  ${done}/${base.sample.length}\n`);
    await sleep(350);
  }
  data = { meta: { generated_at: new Date().toISOString(), rows: sample.length, failed }, sample };
  writeFileSync(OUT, JSON.stringify(data));
  console.log(`\n${sample.length} Zeilen gespeichert (${failed} Fehlschlaege)\n`);
}

const NOW = new Date(data.meta.generated_at).getTime();
const out = [];
const say = s => { console.log(s); out.push(s); };
const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };

// ── 2) Abdeckung: was bleibt nach dem Filter uebrig? ────────────────────────
say(`# FMV nur aus Manager-Verkaeufen: Abdeckung und Genauigkeit (${new Date().toISOString().slice(0, 16)} UTC)\n`);
say(`Datensatz: ${data.sample.length} Karten-Zeilen, je bis zu 20 Verkaeufe mit Verkaufsart.\n`);
say('## 1. Abdeckung: wie viele Karten behalten eine Basis?\n');
say('Gezaehlt wird pro Karte, wie viele Verkaeufe im Bewertungsfenster liegen (In-Season 21 Tage,');
say('Classic 90 Tage) — einmal mit allen Arten, einmal nur mit Manager-Verkaeufen.\n');
say('| Segment | Karten | mit Basis (alle) | mit Basis (nur Manager) | Verlust | >=3 Sales (alle) | >=3 (nur Manager) |');
say('|---|---|---|---|---|---|---|');
const segs = [...new Set(data.sample.map(r => `${r.scarcity}/${r.eligibility}`))].sort();
const inWin = (r, onlyOffer) => {
  const p = PROF(r.eligibility);
  return r.sales.filter(s => s.eur > 0
    && (NOW - new Date(s.date).getTime()) / DAY <= p.ma
    && (!onlyOffer || s.deal === 'TokenOffer')).length;
};
for (const seg of [...segs, 'ALLE']) {
  const rows = seg === 'ALLE' ? data.sample : data.sample.filter(r => `${r.scarcity}/${r.eligibility}` === seg);
  if (!rows.length) continue;
  const a1 = rows.filter(r => inWin(r, false) >= 1).length, o1 = rows.filter(r => inWin(r, true) >= 1).length;
  const a3 = rows.filter(r => inWin(r, false) >= 3).length, o3 = rows.filter(r => inWin(r, true) >= 3).length;
  const loss = a1 ? ((a1 - o1) / a1 * 100).toFixed(0) : '0';
  say(`| ${seg} | ${rows.length} | ${a1} (${(a1 / rows.length * 100).toFixed(0)} %) | ${o1} (${(o1 / rows.length * 100).toFixed(0)} %) | **−${loss} %** | ${a3} | ${o3} |`);
}

// ── 3) Genauigkeit: Walk-Forward, alle Arten vs nur Manager ─────────────────
// Ziel ist IMMER ein Manager-Verkauf (nur der ist ein echter Marktpreis, an dem
// man messen sollte). Geschaetzt wird einmal aus allen frueheren Verkaeufen,
// einmal nur aus frueheren Manager-Verkaeufen.
function fmv(sales, now, p, onlyOffer, floor) {
  const raw = sales.filter(s => s.eur > 0 && (!onlyOffer || s.deal === 'TokenOffer'))
    .map(s => ({ v: s.eur, age: Math.max(0, (now - new Date(s.date).getTime()) / DAY) }))
    .filter(x => x.age <= p.ma);
  const n = raw.length;
  if (!n) return null;
  const newest = Math.min(...raw.map(x => x.age));
  const hlEff = p.hl / (1 + n / 5);                       // v3.4
  let e = raw.map(x => ({ ...x, w: Math.pow(0.5, x.age / hlEff) }));
  if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }
  const tw = e.reduce((s, x) => s + x.w, 0);
  const sv = e.reduce((s, x) => s + x.v * x.w, 0) / tw;
  if (!(floor > 0) || floor >= sv) return sv;
  return (n < 3 || newest > p.hl) ? Math.min(sv, floor * 1.5) : sv;
}

const rows = [];
for (const r of data.sample) {
  const p = PROF(r.eligibility);
  const s = r.sales;
  for (let t = 0; t < s.length - 3; t++) {
    if (s[t].deal !== 'TokenOffer') continue;             // nur echte Marktpreise als Ziel
    const now = new Date(s[t].date).getTime();
    const hist = s.slice(t + 1);
    const all  = fmv(hist, now, p, false, r.floor_price ?? 0);
    const only = fmv(hist, now, p, true,  r.floor_price ?? 0);
    if (!all || all <= 0) continue;
    rows.push({
      seg: `${r.scarcity}/${r.eligibility}`,
      dAll: (s[t].eur - all) / all * 100,
      dOnly: only > 0 ? (s[t].eur - only) / only * 100 : null,
    });
  }
}
say(`\n## 2. Genauigkeit gegen echte Manager-Verkaeufe (${rows.length} Ziele)\n`);
say('Gemessen wird gegen Manager-Verkaeufe, denn nur die sind Marktpreise. "alle Arten" ist');
say('der heutige Stand, "nur Manager" die Vorgabe. `ohne Schaetzung` = keine Manager-Verkaeufe');
say('in der Historie, die Karte bekaeme gar keinen FMV mehr.\n');
say('| Segment | Ziele | alle Arten | nur Manager | ohne Schaetzung |');
say('|---|---|---|---|---|');
for (const seg of [...segs, 'ALLE']) {
  const d = seg === 'ALLE' ? rows : rows.filter(r => r.seg === seg);
  if (d.length < 30) continue;
  const withOnly = d.filter(r => r.dOnly != null);
  const mAll = med(d.map(r => Math.abs(r.dAll)));
  const mOnlyPaired = med(withOnly.map(r => Math.abs(r.dAll)));      // fairer Vergleich: gleiche Ziele
  const mOnly = med(withOnly.map(r => Math.abs(r.dOnly)));
  const gone = ((d.length - withOnly.length) / d.length * 100).toFixed(0);
  const mark = mOnly < mOnlyPaired ? ' ✅' : mOnly > mOnlyPaired ? ' ❌' : '';
  say(`| ${seg} | ${d.length} | ±${mAll.toFixed(1)} % (auf gleicher Menge ±${mOnlyPaired.toFixed(1)} %) | ±${mOnly.toFixed(1)} %${mark} | ${gone} % |`);
}

writeFileSync('docs/2026-09-07_ZWEITMARKT_FILTER.md', out.join('\n') + '\n');
console.log('\nBericht: docs/2026-09-07_ZWEITMARKT_FILTER.md');
