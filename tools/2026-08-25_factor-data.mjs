// ═══════════════════════════════════════════════════════════════════════════
// SORION — FMV-Faktoren-Analyse, Step 1: Datenbasis (NUR LESEND)
//
// Zieht fuer eine geschichtete Stichprobe (600 limited + 200 rare Zeilen)
//   a) Verkaufshistorien von Sorare (tokenPrices, first:20) je Karte,
//   b) Score-Daten je Spieler (Einzel-Scores mit Spieldatum, sofern die API
//      sie hergibt — sonst averageScore-Aggregate L5/L10/L40 + Appearances),
//   c) einen Vollabzug card_prices (limited+rare) fuer Liga-/Liquiditaets-
//      Grundgesamtheiten,
//   d) fmv_accuracy seit 20.08. (v3.2-Aera ab 23.08. 13:00 UTC).
//
// Schreibt NICHTS in die Datenbank. Ausgabe: tools/analysis-out/*.json|ndjson
//
// AUFRUF:   railway run node tools/2026-08-25_factor-data.mjs
// Optionen: --with-key   Sorare-API-Key nutzen (schneller, ABER belastet die
//                        geteilten 200 req/min — nur im freien Fenster
//                        05-15 / 21 UTC laufen lassen!)
//           --dry        nur Stichprobe ziehen + Budget schaetzen, keine API
// Default ist ANONYM (1 Call / 1,2 s wie tools/fmv-backtest.mjs): belastet
// das Key-Budget der Updater nicht und darf jederzeit laufen. Laufzeit ~35 min.
//
// INC-005/006-Regeln: kein Cron, Einmal-Lauf, Supabase nur paginierte SELECTs.
// ═══════════════════════════════════════════════════════════════════════════
import { createClient } from '@supabase/supabase-js';
import { mkdirSync, writeFileSync, createWriteStream } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const OUT_DIR = join(dirname(fileURLToPath(import.meta.url)), 'analysis-out');
mkdirSync(OUT_DIR, { recursive: true });

const WITH_KEY = process.argv.includes('--with-key');
const DRY      = process.argv.includes('--dry');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SERVICE_KEY) { console.error('SUPABASE_URL / SUPABASE_SERVICE_KEY fehlen (railway run nutzen)'); process.exit(1); }
const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

const SORARE = 'https://api.sorare.com/graphql';
const APIKEY = WITH_KEY ? (process.env.SORARE_APIKEY ?? null) : null;
const DELAY  = APIKEY ? 700 : 1200;
const BATCH  = APIKEY ? 5 : 1;      // Aliase pro GraphQL-Call (anonym: Complexity-Limit 500)
const sleep  = ms => new Promise(r => setTimeout(r, ms));

if (APIKEY) {
  const h = new Date().getUTCHours();
  const inUpdaterWindow = (h >= 22 || h <= 4 || (h >= 16 && h <= 20));
  if (inUpdaterWindow && !process.argv.includes('--force')) {
    console.error(`Mit --with-key nicht im Updater-Fenster laufen (jetzt ${h}:xx UTC; frei sind 05-15 und 21 UTC). Abbruch — ohne --with-key darf es jederzeit laufen.`);
    process.exit(1);
  }
}

let apiCalls = 0;
async function gql(query, label) {
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch(SORARE, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(APIKEY ? { APIKEY } : {}) },
        body: JSON.stringify({ query }),
      });
      apiCalls++;
      if (res.status === 429) { const w = 30000 * (attempt + 1); console.warn(`  429 (${label}) — warte ${w / 1000}s`); await sleep(w); continue; }
      if (!res.ok) { console.warn(`  HTTP ${res.status} (${label})`); return { data: null, errors: [`http ${res.status}`] }; }
      const json = await res.json();
      return { data: json.data ?? null, errors: json.errors?.map(e => e.message) ?? null };
    } catch (e) { console.warn(`  Fetch (${label}): ${e.message}`); await sleep(2000 * (attempt + 1)); }
  }
  return { data: null, errors: ['retries exhausted'] };
}

// Deterministische Stichprobe (reproduzierbar): mulberry32, Seed fix.
function rng(seed) { return () => { seed |= 0; seed = seed + 0x6D2B79F5 | 0; let t = Math.imul(seed ^ seed >>> 15, 1 | seed); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296; }; }
function shuffle(a, rnd) { for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); [a[i], a[j]] = [a[j], a[i]]; } return a; }

const COLS = 'player_slug, scarcity, eligibility, fmv, floor_price, avg_sales, sale_1, sale_2, sale_3, sale_4, sale_5, '
  + 'last_sale_at, sales_count, sales_72h, sales_7d, team_name, league_name, league_country, position, '
  + 'gameplay_tier, player_age, player_nation, updated_at';

async function dumpCardPrices() {
  console.log('Phase A — card_prices-Vollabzug (limited + rare), paginiert…');
  const out = createWriteStream(join(OUT_DIR, '2026-08-25_card-prices-dump.ndjson'));
  const rows = [];
  for (const sc of ['limited', 'rare']) {
    let off = 0;
    while (true) {
      const { data, error } = await supabase.from('card_prices').select(COLS)
        .eq('scarcity', sc).order('id', { ascending: true }).range(off, off + 999);
      if (error) { console.error(`  Abbruch bei ${sc}/${off}: ${error.message}`); process.exit(1); }
      for (const r of data) { rows.push(r); out.write(JSON.stringify(r) + '\n'); }
      if (data.length < 1000) break;
      off += 1000;
      if (off % 10000 === 0) console.log(`  ${sc}: ${off}…`);
      await sleep(150);   // Ruecksicht auf die frische Micro-Instanz (INC-006)
    }
  }
  await new Promise(r => out.end(r));
  console.log(`  ${rows.length} Zeilen gedumpt.`);
  return rows;
}

function stratify(rows) {
  console.log('Phase B — Stichprobe schichten…');
  const rnd = rng(42);
  const liq = r => (r.sales_7d ?? 0) >= 3 ? 'liquid' : (r.sales_7d ?? 0) >= 1 ? 'semi' : (r.last_sale_at ? 'cold' : 'dead');
  const PLAN = [
    ['limited', 'in_season', 'liquid', 150], ['limited', 'in_season', 'semi', 150], ['limited', 'in_season', 'cold', 100],
    ['limited', 'classic',   'liquid',  80], ['limited', 'classic',   'semi',  70], ['limited', 'classic',   'cold',  50],
    ['rare',    'in_season', 'liquid',  80], ['rare',    'in_season', 'semi',  80], ['rare',    'in_season', 'cold',  40],
  ];
  const buckets = new Map();
  for (const r of rows) {
    const k = `${r.scarcity}|${r.eligibility}|${liq(r)}`;
    if (!buckets.has(k)) buckets.set(k, []);
    buckets.get(k).push(r);
  }
  // Populationszaehlung (fuer den Bericht): auch 'dead' (nie ein Sale) mitzaehlen
  const population = {};
  for (const [k, v] of buckets) population[k] = v.length;

  const sample = [];
  for (const [sc, el, stratum, n] of PLAN) {
    const pool = (buckets.get(`${sc}|${el}|${stratum}`) ?? []).filter(r => /^[a-z0-9-]+$/.test(r.player_slug));
    const take = shuffle([...pool], rnd).slice(0, n);
    for (const r of take) sample.push({ ...r, stratum });
    console.log(`  ${sc}/${el}/${stratum}: ${take.length}/${n} (Pool ${pool.length})`);
  }
  return { sample, population };
}

// ── Score-Feld-Probe: welche Felder kennt die API (anonym)? ────────────────
const FIELD_CANDIDATES = [
  ['avgL5',  'avgL5: averageScore(type: LAST_FIVE_SO5_AVERAGE_SCORE)'],
  ['avgL10', 'avgL10: averageScore(type: LAST_TEN_PLAYED_SO5_AVERAGE_SCORE)'],
  ['avgL40', 'avgL40: averageScore(type: LAST_FORTY_SO5_AVERAGE_SCORE)'],
  ['appL5',  'appL5: lastFiveSo5Appearances'],
  ['appL10', 'appL10: lastTenPlayedSo5Appearances'],
  ['appL10b','appL10b: lastTenSo5Appearances'],
  ['appL40', 'appL40: lastFortySo5Appearances'],
  ['scoresDated', 'scoresDated: so5Scores(last: 40) { score game { date } }'],
  ['scoresPlain', 'scoresPlain: so5Scores(last: 40) { score }'],
];
// Zwei moegliche Wurzeln: anyPlayer(slug) oder football.player(slug)
const WRAP = {
  anyPlayer: { open: s => `anyPlayer(slug: "${s}") {`, close: '}', pick: d => d?.anyPlayer },
  football:  { open: s => `football { player(slug: "${s}") {`, close: '} }', pick: d => d?.football?.player },
};
let wrapKey = 'anyPlayer';

async function probeFields(slug) {
  console.log(`Phase C0 — Feld-Probe an "${slug}"…`);
  for (const wk of ['anyPlayer', 'football']) {
    const W = WRAP[wk];
    const ok = [];
    for (const [key, frag] of FIELD_CANDIDATES) {
      if (key === 'scoresPlain' && ok.some(([k]) => k === 'scoresDated')) continue;   // Fallback unnoetig
      if (key === 'appL10b' && ok.some(([k]) => k === 'appL10')) continue;
      const { data, errors } = await gql(`{ ${W.open(slug)} ${frag} ${W.close} }`, `probe:${wk}:${key}`);
      if (!errors && W.pick(data)) { ok.push([key, frag]); console.log(`  ✓ ${key}`); }
      else console.log(`  ✗ ${key} (${errors?.[0]?.slice(0, 90) ?? 'keine Daten'})`);
      await sleep(DELAY);
    }
    if (ok.length >= 2) { wrapKey = wk; return ok; }
    console.log(`  Wurzel ${wk} liefert zu wenig — probiere Alternative…`);
  }
  console.error('Keine Score-Felder verfuegbar — Scores bleiben leer, Aggregat-Fallback im Bericht vermerken.');
  return [];
}

async function fetchSales(sample) {
  console.log(`Phase C1 — Verkaufshistorien (${sample.length} Karten, Batch ${BATCH})…`);
  const bySlugKey = new Map();
  for (let i = 0; i < sample.length; i += BATCH) {
    const chunk = sample.slice(i, i + BATCH);
    const parts = chunk.map((r, j) => {
      const elig = r.eligibility === 'classic' ? 'CLASSIC' : 'IN_SEASON';
      return `a${j}: tokens { tokenPrices(rarity: ${r.scarcity} seasonEligibility: ${elig} playerSlug: "${r.player_slug}" first: 20) { date amounts { eurCents } } }`;
    });
    let { data, errors } = await gql(`{ ${parts.join(' ')} }`, `sales:${i}`);
    if (!data && chunk.length > 1) {   // Batch kaputt -> einzeln nachfassen
      data = {};
      for (let j = 0; j < chunk.length; j++) {
        const single = await gql(`{ a0: ${parts[j].slice(parts[j].indexOf(':') + 1)} }`, `sales1:${i + j}`);
        data[`a${j}`] = single.data?.a0 ?? null;
        await sleep(DELAY);
      }
    }
    chunk.forEach((r, j) => {
      const prices = data?.[`a${j}`]?.tokenPrices ?? [];
      bySlugKey.set(`${r.player_slug}|${r.scarcity}|${r.eligibility}`,
        prices.map(p => ({ date: p.date, eur: (p.amounts?.eurCents ?? 0) / 100 })).filter(s => s.eur > 0));
    });
    if ((i / BATCH) % 25 === 0) process.stdout.write(`  ${Math.min(i + BATCH, sample.length)}/${sample.length}  (API-Calls: ${apiCalls})\r`);
    await sleep(DELAY);
  }
  console.log(`\n  fertig (${apiCalls} Calls bisher).`);
  return bySlugKey;
}

async function fetchScores(sample, fields) {
  const players = [...new Set(sample.map(r => r.player_slug))];
  console.log(`Phase C2 — Scores/Meta (${players.length} Spieler, Batch ${BATCH})…`);
  if (!fields.length) return new Map();
  const frag = fields.map(([, f]) => f).join(' ');
  const W = WRAP[wrapKey];
  const one = (alias, slug) => `${alias}: ${W.open(slug)} slug ${frag} ${W.close}`;
  const unwrap = node => wrapKey === 'football' ? (node?.player ?? null) : (node ?? null);
  const byPlayer = new Map();
  for (let i = 0; i < players.length; i += BATCH) {
    const chunk = players.slice(i, i + BATCH);
    const parts = chunk.map((slug, j) => one(`p${j}`, slug));
    let { data } = await gql(`{ ${parts.join(' ')} }`, `scores:${i}`);
    if (!data && chunk.length > 1) {
      data = {};
      for (let j = 0; j < chunk.length; j++) {
        const single = await gql(`{ ${one(`p${j}`, chunk[j])} }`, `scores1:${i + j}`);
        data[`p${j}`] = single.data?.[`p${j}`] ?? null;
        await sleep(DELAY);
      }
    }
    chunk.forEach((slug, j) => {
      const rec = unwrap(data?.[`p${j}`]);
      if (rec) byPlayer.set(slug, rec);
    });
    if ((i / BATCH) % 25 === 0) process.stdout.write(`  ${Math.min(i + BATCH, players.length)}/${players.length}  (API-Calls: ${apiCalls})\r`);
    await sleep(DELAY);
  }
  console.log(`\n  ${byPlayer.size}/${players.length} Spieler mit Daten (${apiCalls} Calls gesamt).`);
  return byPlayer;
}

async function dumpAccuracy() {
  console.log('Phase D — fmv_accuracy seit 20.08. dumpen…');
  const out = createWriteStream(join(OUT_DIR, '2026-08-25_fmv-accuracy.ndjson'));
  let off = 0, n = 0;
  while (true) {
    const { data, error } = await supabase.from('fmv_accuracy').select('*')
      .gte('created_at', '2026-08-20T00:00:00Z')
      .order('id', { ascending: true }).range(off, off + 999);
    if (error) { console.error(`  Fehler: ${error.message}`); break; }
    for (const r of data) { out.write(JSON.stringify(r) + '\n'); n++; }
    if (data.length < 1000) break;
    off += 1000;
    await sleep(150);
  }
  await new Promise(r => out.end(r));
  console.log(`  ${n} Zeilen.`);
}

async function main() {
  const t0 = Date.now();
  console.log(`[${new Date().toISOString()}] Faktoren-Datenlauf startet ${APIKEY ? 'MIT Key' : 'anonym'}${DRY ? ' (DRY)' : ''}`);

  const rows = await dumpCardPrices();
  const { sample, population } = stratify(rows);

  if (DRY) {
    const players = new Set(sample.map(r => r.player_slug)).size;
    console.log(`DRY: ${sample.length} Karten, ${players} Spieler -> ~${Math.ceil(sample.length / BATCH) + Math.ceil(players / BATCH) + 12} Sorare-Calls, ~${Math.round((Math.ceil(sample.length / BATCH) + Math.ceil(players / BATCH)) * DELAY / 60000)} min`);
    return;
  }

  const fields  = await probeFields((sample.find(r => r.stratum === 'liquid') ?? sample[0]).player_slug);
  const sales   = await fetchSales(sample);
  const scores  = await fetchScores(sample, fields);

  const outSample = sample.map(r => ({
    ...r,
    sales: sales.get(`${r.player_slug}|${r.scarcity}|${r.eligibility}`) ?? [],
    player: scores.get(r.player_slug) ?? null,
  }));
  writeFileSync(join(OUT_DIR, '2026-08-25_factor-data.json'), JSON.stringify({
    meta: {
      generated_at: new Date().toISOString(),
      mode: APIKEY ? 'apikey' : 'anonym',
      api_calls: apiCalls,
      fields_available: fields.map(([k]) => k),
      population,
      note: 'sales: neueste zuerst, brutto EUR. player.scoresDated: neueste zuerst (falls verfuegbar).',
    },
    sample: outSample,
  }));

  await dumpAccuracy();
  console.log(`[${new Date().toISOString()}] Fertig in ${((Date.now() - t0) / 60000).toFixed(1)} min, ${apiCalls} Sorare-Calls.`);
  console.log(`Ausgaben in tools/analysis-out/ — bitte im Chat Bescheid geben.`);
}

main().catch(e => { console.error('Fehlgeschlagen:', e); process.exit(1); });
