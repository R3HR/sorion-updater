// Hilft Knappheit dem FMV?  (IDEA-006 Punkt 1b, 06.09.2026)
//
//   railway run --service "Updater Limited" node tools/2026-09-06_supply-vs-accuracy.mjs
//
// FRAGE: Schaetzt unser FMV bei knappen Karten systematisch anders als bei haeufigen?
// Wenn ja, ist der Kartenbestand ein Kandidat fuer die Formel (Entscheidung Jonas).
//
// VORGEHEN: Stichprobe der meistgehandelten Spieler je Segment aus fmv_accuracy
// (dort liegt je Verkauf unsere Schaetzung VOR dem Verkauf, kein Leakage). Fuer
// jeden Spieler EIN Sorare-Aufruf, der Supply fuer alle Rarities und Saisons
// liefert. Dann: Median-Abweichung je Bestands-Gruppe.
//
// LESART: `bias` ist signiert ((Verkauf - FMV) / FMV). Positiv = wir schaetzen zu
// NIEDRIG. Steigt der Bias systematisch, je knapper die Karte, dann unterschaetzen
// wir Knappheit. `median_abs` ist die Treffgenauigkeit (kleiner = besser).
import { createClient } from '@supabase/supabase-js';
import { writeFileSync } from 'node:fs';

const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { auth: { persistSession: false } });
const KEY = process.env.SORARE_APIKEY;
if (!process.env.SUPABASE_URL || !KEY) { console.log('FEHLT: SUPABASE_URL / SUPABASE_SERVICE_KEY / SORARE_APIKEY (railway run)'); process.exit(1); }

const SAMPLE_PER_SEGMENT = 60;   // Spieler je Segment; ueberschneiden sich, ein Aufruf deckt alle Rarities
const PAUSE_MS = 500;            // ~120 Aufrufe/min, bleibt unter dem 200er-Limit neben dem Updater
const DAYS = 14;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const median = a => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const SEGMENTS = [['in_season', 'limited'], ['in_season', 'rare'], ['classic', 'limited'], ['classic', 'rare']];

// ── 1) Meistgehandelte Spieler je Segment ───────────────────────────────────
console.log(`Stichprobe: ${SAMPLE_PER_SEGMENT} Spieler je Segment, ${DAYS} Tage Verkaufsdaten\n`);
const since = new Date(Date.now() - DAYS * 86400e3).toISOString();
const wanted = new Map();   // player_slug -> Set(segment)
const perSeg = {};
for (const [elig, sc] of SEGMENTS) {
  const rows = [];
  for (let off = 0; ; off += 1000) {
    const { data, error } = await sb.from('fmv_accuracy')
      .select('player_slug,delta_pct').eq('eligibility', elig).eq('scarcity', sc)
      .gte('created_at', since).gt('sale_price', 0).range(off, off + 999);
    if (error) { console.log('DB-Fehler:', error.message); process.exit(1); }
    rows.push(...data);
    if (data.length < 1000 || rows.length > 60000) break;
  }
  const by = {};
  for (const r of rows) (by[r.player_slug] ??= []).push(Number(r.delta_pct));
  const top = Object.entries(by).filter(([, v]) => v.length >= 5)
    .sort((a, b) => b[1].length - a[1].length).slice(0, SAMPLE_PER_SEGMENT);
  perSeg[`${elig}|${sc}`] = new Map(top);
  for (const [slug] of top) wanted.set(slug, true);
  console.log(`  ${elig} ${sc}: ${rows.length} Verkaeufe, ${Object.keys(by).length} Spieler, Stichprobe ${top.length}`);
}

// ── 2) Supply holen (ein Aufruf je Spieler, deckt alle Rarities/Saisons) ────
console.log(`\nHole Supply fuer ${wanted.size} Spieler...`);
const supply = new Map();   // slug -> { limited: {is, cl}, rare: {...}, super_rare: {...} }
let done = 0;
for (const slug of wanted.keys()) {
  try {
    const r = await fetch('https://api.sorare.com/graphql', {
      method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY: KEY },
      body: JSON.stringify({ query: `{ anyPlayer(slug: "${slug}") { cardSupply { season { startYear } limited rare superRare unique } } }` }),
    });
    const d = await r.json();
    const rows = d?.data?.anyPlayer?.cardSupply ?? [];
    if (rows.length) {
      const years = rows.map(x => x.season?.startYear).filter(Number.isFinite);
      const max = Math.max(...years);
      const pick = (field, inSeason) => inSeason
        ? (rows.find(x => x.season?.startYear === max)?.[field] ?? 0)
        : rows.filter(x => x.season?.startYear < max).reduce((s, x) => s + (x[field] ?? 0), 0);
      supply.set(slug, {
        limited:    { is: pick('limited', true),   cl: pick('limited', false) },
        rare:       { is: pick('rare', true),      cl: pick('rare', false) },
        super_rare: { is: pick('superRare', true), cl: pick('superRare', false) },
      });
    }
  } catch { /* einzelne Ausfaelle sind egal */ }
  if (++done % 25 === 0) process.stdout.write(`  ${done}/${wanted.size}\n`);
  await sleep(PAUSE_MS);
}
console.log(`  ${supply.size} Spieler mit Supply-Daten\n`);

// ── 3) Auswertung: Median-Abweichung je Bestands-Gruppe ─────────────────────
const out = [];
const say = s => { console.log(s); out.push(s); };
say(`# Knappheit vs. FMV-Genauigkeit (${new Date().toISOString().slice(0, 16)} UTC)\n`);
say(`Stichprobe: meistgehandelte Spieler je Segment, ${DAYS} Tage, mindestens 5 Verkaeufe je Spieler.`);
say(`bias = (Verkauf - FMV) / FMV in %. **Positiv = wir schaetzen zu niedrig.**`);
say(`median_abs = Treffgenauigkeit (kleiner ist besser).\n`);

for (const [elig, sc] of SEGMENTS) {
  const seg = perSeg[`${elig}|${sc}`];
  const pts = [];
  for (const [slug, deltas] of seg) {
    const sup = supply.get(slug)?.[sc];
    const n = sup ? (elig === 'in_season' ? sup.is : sup.cl) : null;
    if (!n || n <= 0) continue;
    pts.push({ slug, supply: n, bias: median(deltas), abs: median(deltas.map(Math.abs)), sales: deltas.length });
  }
  if (pts.length < 12) { say(`\n## ${elig} ${sc}: nur ${pts.length} Spieler mit Supply — zu duenn\n`); continue; }
  pts.sort((a, b) => a.supply - b.supply);
  const q = Math.ceil(pts.length / 4);
  say(`\n## ${elig} ${sc} (${pts.length} Spieler, ${pts.reduce((s, p) => s + p.sales, 0)} Verkaeufe)\n`);
  say('| Gruppe | Bestand | Spieler | median bias | median abs |');
  say('|---|---|---|---|---|');
  const labels = ['knappste 25 %', '2. Viertel', '3. Viertel', 'haeufigste 25 %'];
  for (let i = 0; i < 4; i++) {
    const g = pts.slice(i * q, (i + 1) * q);
    if (!g.length) continue;
    const lo = g[0].supply, hi = g[g.length - 1].supply;
    say(`| ${labels[i]} | ${lo} bis ${hi} | ${g.length} | ${median(g.map(p => p.bias)).toFixed(1)} % | ${median(g.map(p => p.abs)).toFixed(1)} % |`);
  }
  // Spearman-Rangkorrelation Bestand vs. Bias
  const rank = arr => { const idx = arr.map((v, i) => [v, i]).sort((a, b) => a[0] - b[0]); const r = new Array(arr.length); idx.forEach(([, i], k) => r[i] = k + 1); return r; };
  const rs = rank(pts.map(p => p.supply)), rb = rank(pts.map(p => p.bias));
  const n = pts.length, dsum = rs.reduce((s, v, i) => s + (v - rb[i]) ** 2, 0);
  const rho = 1 - (6 * dsum) / (n * (n * n - 1));
  say(`\nRangkorrelation Bestand vs. Bias: **${rho.toFixed(3)}** ` +
      (Math.abs(rho) < 0.2 ? '(kein Zusammenhang)' : Math.abs(rho) < 0.4 ? '(schwach)' : '(deutlich)'));
}

say('\n## Einordnung\n');
say('- Ein **negativer** Zusammenhang hiesse: je haeufiger die Karte, desto niedriger der Bias,');
say('  also schaetzen wir knappe Karten zu niedrig. Dann waere Bestand ein Formel-Kandidat.');
say('- Liegt die Rangkorrelation nahe null, traegt Knappheit nichts bei, was der Preis nicht');
say('  schon enthaelt (die Verkaeufe selbst spiegeln die Knappheit bereits).');
say('- **Formel bleibt unveraendert, bis Jonas entscheidet.**');

const file = 'docs/2026-09-06_SUPPLY_VS_ACCURACY.md';
writeFileSync(file, out.join('\n') + '\n');
console.log(`\nBericht: ${file}`);
