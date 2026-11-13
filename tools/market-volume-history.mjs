// ═══════════════════════════════════════════════════════════════════════════
// Marktvolumen aus Sorares Verkaufshistorie  (13.11.2026, Freigabe Jonas)
//
// Laeuft als Railway-Cron alle 15 Minuten, arbeitet hoechstens BUDGET_MS und endet.
// Der Fortschritt liegt in der Datenbank (migrations/2026-11-13_market_volume_history.sql),
// deshalb kann jeder Lauf dort weitermachen, wo der letzte aufgehoert hat, und ein
// abgebrochener Lauf verliert nur Zeit: ein Paket wird erst mit seinem Ergebnis als
// erledigt vermerkt, beides in einer Transaktion.
//
// Reihenfolge der Auftraege (Prioritaet):
//   1. recent          04.09.2026 bis 12.11.2026  -> Woche, Monat, Monatsvergleich
//   2. day:JJJJ-MM-TT  jeder beendete Tag ab 13.11.2026 (laufende Aktualitaet)
//   3. year            13.11.2025 bis 03.09.2026  -> Rest des Jahres
//
// Quelle: anyPlayer(slug) { tokenPrices(rarity, seasonEligibility, since, first: 50) }
// Alle 6 Kombinationen (3 Seltenheiten x In-Season/Classic) im ersten Abruf je Spieler,
// weitergeblaettert wird nur, wo es mehr als 50 Verkaeufe gibt. seasonEligibility gilt
// zum Verkaufszeitpunkt (geprueft 13.11.).
//
//   node tools/market-volume-history.mjs            (Railway, mit Umgebungsvariablen)
//   railway run --service "Updater Limited" node tools/market-volume-history.mjs --budget=120
// ═══════════════════════════════════════════════════════════════════════════
import { createClient } from '@supabase/supabase-js';

const arg = (k, d) => { const m = process.argv.find(a => a.startsWith(`--${k}=`)); return m ? m.split('=')[1] : d; };
const BUDGET_MS   = Number(arg('budget', process.env.MV_BUDGET_S ?? 780)) * 1000;   // 13 min
const RATE_FREE   = Number(process.env.MV_RATE_FREE ?? 150);   // Abrufe/min ausserhalb der Updater-Fenster
const RATE_SHARED = Number(process.env.MV_RATE_SHARED ?? 90);  // waehrend der Updater laufen
const BATCH_SIZE  = 100;
const MIN_LEFT_MS = 4 * 60 * 1000;                              // neues Paket nur mit genug Restzeit

// Feste Grenzen des Rueckrechnungs-Plans (Berliner Tage)
const CUT_DAY     = '2026-11-13';   // ab hier Tagesauftraege
const RECENT_FROM = '2026-09-04';   // CUT_DAY - 70 Tage
const YEAR_FROM   = '2025-11-13';

const KEY = process.env.SORARE_APIKEY;
if (!KEY || !process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_KEY) {
  console.error('FEHLT: SORARE_APIKEY / SUPABASE_URL / SUPABASE_SERVICE_KEY'); process.exit(1);
}
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { auth: { persistSession: false } });
const startedAt = Date.now();
const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Datum (Europe/Berlin) ───────────────────────────────────────────────────
const berlinDay = t => new Date(t).toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
const dayAdd = (ds, n) => { const x = new Date(ds + 'T00:00:00Z'); x.setUTCDate(x.getUTCDate() + n); return x.toISOString().slice(0, 10); };
// UTC-Zeitpunkt von 00:00 Berliner Zeit an diesem Tag (MEZ +1 / MESZ +2)
const berlinMidnight = ds => {
  for (const off of [1, 2]) {
    const t = Date.parse(ds + 'T00:00:00Z') - off * 3600000;
    if (berlinDay(t) === ds && berlinDay(t - 1) !== ds) return new Date(t).toISOString();
  }
  throw new Error('Mitternacht nicht bestimmbar: ' + ds);
};

// ── Abruf mit Drosselung und Wiederholung ───────────────────────────────────
// Updater-Fenster (Cron */5 22-23,0-4,16-20 UTC): dann langsamer, damit die
// Preisaktualisierung ihr Kontingent behaelt (200/min teilen sich alle Dienste).
const inUpdaterWindow = () => { const h = new Date().getUTCHours(); return h >= 22 || h <= 4 || (h >= 16 && h <= 20); };
let lastCall = 0, calls = 0;
async function gql(query, variables) {
  for (let attempt = 1; attempt <= 4; attempt++) {
    const gap = 60000 / (inUpdaterWindow() ? RATE_SHARED : RATE_FREE);
    const wait = lastCall + gap - Date.now();
    if (wait > 0) await sleep(wait);
    lastCall = Date.now(); calls++;
    try {
      const res = await fetch('https://api.sorare.com/graphql', {
        method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY: KEY },
        body: JSON.stringify({ query, variables }),
      });
      if (res.status === 429) { console.warn(`  429, warte ${30 * attempt}s`); await sleep(30000 * attempt); continue; }
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const d = await res.json();
      if (d.errors) throw new Error(d.errors[0]?.message ?? 'GraphQL-Fehler');
      return d.data;
    } catch (e) {
      if (attempt === 4) throw e;
      await sleep(3000 * attempt);
    }
  }
}

const COMBOS = [];
for (const r of ['limited', 'rare', 'super_rare'])
  for (const e of ['IN_SEASON', 'CLASSIC'])
    COMBOS.push({ r, e, alias: `${r}_${e.toLowerCase()}`, elig: e === 'IN_SEASON' ? 'in_season' : 'classic' });
const NODE = 'pageInfo { hasNextPage endCursor } nodes { date amounts { eurCents } deal { __typename } }';

// Alle Verkaeufe eines Spielers im Bereich [from, to) (Berliner Tage) aufsummieren
async function addPlayer(slug, from, to, agg) {
  const since = berlinMidnight(from);
  const first = `query($slug: String!) { anyPlayer(slug: $slug) { ${COMBOS.map(c =>
    `${c.alias}: tokenPrices(rarity: ${c.r}, seasonEligibility: ${c.e}, first: 50, since: "${since}") { ${NODE} }`).join(' ')} } }`;
  const d = await gql(first, { slug });
  if (!d?.anyPlayer) return;                       // Spieler bei Sorare nicht (mehr) da
  for (const c of COMBOS) {
    let conn = d.anyPlayer[c.alias];
    for (;;) {
      for (const x of conn.nodes) {
        const day = berlinDay(x.date);
        if (day < from || day >= to) continue;     // juenger als der Bereich: nur durchgeblaettert
        const deal = x.deal?.__typename ?? 'unknown';
        const k = `${day}|${c.elig}|${c.r}|${deal}`;
        const g = agg.get(k) ?? { day, eligibility: c.elig, scarcity: c.r, deal_type: deal, sales: 0, volume: 0 };
        g.sales++; g.volume += x.amounts.eurCents / 100;
        agg.set(k, g);
      }
      if (!conn.pageInfo.hasNextPage) break;
      const q = `query($slug: String!, $after: String) { anyPlayer(slug: $slug) {
        tokenPrices(rarity: ${c.r}, seasonEligibility: ${c.e}, first: 50, after: $after, since: "${since}") { ${NODE} } } }`;
      conn = (await gql(q, { slug, after: conn.pageInfo.endCursor })).anyPlayer.tokenPrices;
    }
  }
}

// ── Auftraege anlegen ───────────────────────────────────────────────────────
async function ensureJobs() {
  const today = berlinDay(Date.now());
  const want = [
    { job: 'recent', from: RECENT_FROM, to: CUT_DAY, priority: 1 },
    { job: 'year',   from: YEAR_FROM,   to: RECENT_FROM, priority: 3 },
  ];
  for (let d = CUT_DAY; d < today; d = dayAdd(d, 1)) want.push({ job: `day:${d}`, from: d, to: dayAdd(d, 1), priority: 2 });
  const { data: have, error } = await sb.from('market_volume_jobs').select('job');
  if (error) throw new Error('Auftraege lesen: ' + error.message);
  const known = new Set((have ?? []).map(j => j.job));
  for (const w of want) {
    if (known.has(w.job)) continue;
    const { data: b, error: e } = await sb.rpc('mv_create_job',
      { p_job: w.job, p_from: w.from, p_to: w.to, p_priority: w.priority, p_batch_size: BATCH_SIZE });
    if (e) throw new Error(`Auftrag ${w.job}: ${e.message}`);
    console.log(`Auftrag angelegt: ${w.job} [${w.from}, ${w.to}) in ${b} Paketen`);
  }
}

// ── Hauptschleife ───────────────────────────────────────────────────────────
async function main() {
  await ensureJobs();
  let done = 0, sales = 0;
  while (Date.now() - startedAt < BUDGET_MS - MIN_LEFT_MS) {
    const { data, error } = await sb.rpc('mv_claim_batch', { p_batch_size: BATCH_SIZE });
    if (error) throw new Error('Paket reservieren: ' + error.message);
    const b = Array.isArray(data) ? data[0] : null;
    if (!b) { console.log('Nichts offen: alle Pakete erledigt oder reserviert.'); break; }

    const t0 = Date.now(), c0 = calls, agg = new Map();
    let ok = true;
    for (const slug of b.players) {
      if (Date.now() - startedAt > BUDGET_MS) { ok = false; break; }   // Zeit um: Paket verfaellt, naechster Lauf macht es neu
      try { await addPlayer(slug, b.range_from, b.range_to, agg); }
      catch (e) { console.warn(`  ${b.job}/${b.batch}: ${slug} scheitert (${e.message}), Paket wird spaeter wiederholt`); ok = false; break; }
    }
    if (!ok) break;

    const rows = [...agg.values()].map(g => ({ ...g, volume: Math.round(g.volume * 100) / 100 }));
    const { data: n, error: e2 } = await sb.rpc('mv_complete_batch',
      { p_job: b.job, p_batch: b.batch, p_calls: calls - c0, p_rows: rows });
    if (e2) throw new Error('Paket abschliessen: ' + e2.message);
    const s = rows.reduce((a, r) => a + r.sales, 0);
    done++; sales += s;
    console.log(`${b.job} Paket ${b.batch}: ${b.players.length} Spieler, ${s} Verkaeufe, ${calls - c0} Abrufe, ${((Date.now() - t0) / 1000).toFixed(0)} s` + (n === -1 ? ' (war schon erledigt)' : ''));
  }
  console.log(`[${new Date().toISOString()}] Ende: ${done} Pakete, ${sales} Verkaeufe, ${calls} Abrufe, ${((Date.now() - startedAt) / 1000).toFixed(0)} s`);
}

main().catch(e => { console.error('Abbruch:', e.message); process.exitCode = 1; });
