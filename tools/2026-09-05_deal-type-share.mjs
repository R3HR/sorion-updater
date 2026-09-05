// Marktweite Messung: Welcher Anteil der "Sales" ist Sorare selbst, und wie
// weit liegt der Zweitmarkt (Manager-zu-Manager) darunter?  (05.09.2026)
//
//   railway run --service "Updater Limited" node tools/2026-09-05_deal-type-share.mjs
//
// Quelle: Sorare tokenPrices(first:20) je Spieler/Rarity/Eligibility mit
// deal { __typename }. Stichprobe: die liquidesten Spieler je Segment aus
// card_prices (sales_7d absteigend) — sagt also etwas ueber den GEHANDELTEN
// Markt, nicht ueber Karteikarten ohne Umsatz. Das ist fuer FMV genau die
// relevante Menge.
//
// Kontingent: 1 Anfrage je Spieler, ~180 gesamt, 350 ms Pause -> ~1 Minute,
// bleibt unter 200/min. Nur AUSSERHALB der Updater-Fenster laufen lassen
// (UTC 16-20, 22-04), sonst konkurrieren wir mit den Preis-Updatern.
import { createClient } from '@supabase/supabase-js';
import { writeFileSync } from 'node:fs';

const SB = process.env.SUPABASE_URL, SK = process.env.SUPABASE_SERVICE_KEY, KEY = process.env.SORARE_APIKEY;
if (!SB || !SK || !KEY) { console.log('FEHLT: SUPABASE_URL / SUPABASE_SERVICE_KEY / SORARE_APIKEY (unter railway run starten)'); process.exit(1); }
const sb = createClient(SB, SK, { auth: { persistSession: false } });

const SEGMENTS = [
  { scarcity: 'limited', elig: 'in_season', n: 100 },
  { scarcity: 'rare',    elig: 'in_season', n: 40 },
  { scarcity: 'limited', elig: 'classic',   n: 40 },
];
const ELIG = { in_season: 'IN_SEASON', classic: 'CLASSIC' };
const sleep = ms => new Promise(r => setTimeout(r, ms));
const median = a => { if (!a.length) return null; const s = [...a].sort((x, y) => x - y); const m = Math.floor(s.length / 2); return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

async function sorare(query) {
  for (let i = 0; i < 3; i++) {
    const r = await fetch('https://api.sorare.com/graphql', { method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY: KEY }, body: JSON.stringify({ query }) });
    if (r.status === 429) { await sleep(3000 * (i + 1)); continue; }
    const d = await r.json();
    if (d.errors) throw new Error(d.errors[0].message);
    return d.data;
  }
  throw new Error('429 dauerhaft');
}

const all = [];               // {seg, player, type, eur, date}
const perPlayer = [];         // {seg, player, byType:{type:[eur]}}
for (const seg of SEGMENTS) {
  const { data, error } = await sb.from('card_prices').select('player_slug')
    .eq('scarcity', seg.scarcity).eq('eligibility', seg.elig).gt('sales_7d', 0)
    .order('sales_7d', { ascending: false }).limit(seg.n);
  if (error) { console.log('DB-Fehler', error.message); process.exit(1); }
  const label = `${seg.elig} ${seg.scarcity}`;
  console.log(`\n── ${label}: ${data.length} Spieler`);
  let done = 0;
  for (const row of data) {
    try {
      const d = await sorare(`{ tokens { tokenPrices(rarity: ${seg.scarcity}, seasonEligibility: ${ELIG[seg.elig]}, playerSlug: "${row.player_slug}", first: 20) { date amounts { eurCents } deal { __typename } } } }`);
      const ps = d?.tokens?.tokenPrices ?? [];
      const byType = {};
      for (const p of ps) {
        const type = p.deal?.__typename ?? 'unknown', eur = p.amounts.eurCents / 100;
        all.push({ seg: label, player: row.player_slug, type, eur, date: p.date });
        (byType[type] ??= []).push(eur);
      }
      perPlayer.push({ seg: label, player: row.player_slug, byType });
    } catch (e) { console.log(`   ${row.player_slug}: ${e.message.slice(0, 60)}`); }
    if (++done % 25 === 0) process.stdout.write(`   ${done}/${data.length}\n`);
    await sleep(350);
  }
}

// ── Auswertung ──────────────────────────────────────────────────────────────
const lines = [];
const out = s => { console.log(s); lines.push(s); };
out(`\n# Verkaufsarten in Sorares tokenPrices (Messung ${new Date().toISOString().slice(0, 16)} UTC)`);
out(`\nStichprobe: ${perPlayer.length} Spieler, ${all.length} "Sales" (je Spieler die letzten bis zu 20).\n`);

const TYPES = ['TokenPrimaryOffer', 'TokenAuction', 'TokenOffer'];
const NAMES = { TokenPrimaryOffer: 'Sofortkauf (Sorare)', TokenAuction: 'Auktion (Sorare)', TokenOffer: 'Manager → Manager' };
out('## Anteil der Verkaufsarten (nach Anzahl)\n');
out('| Segment | Sales | Sofortkauf Sorare | Auktion Sorare | Manager→Manager | **Sorare gesamt** |');
out('|---|---|---|---|---|---|');
const segs = [...new Set(all.map(a => a.seg))];
for (const s of [...segs, 'ALLE']) {
  const rows = s === 'ALLE' ? all : all.filter(a => a.seg === s);
  const n = rows.length; if (!n) continue;
  const c = t => rows.filter(a => a.type === t).length;
  const pct = t => (c(t) / n * 100).toFixed(1) + '%';
  const sorare = ((c('TokenPrimaryOffer') + c('TokenAuction')) / n * 100).toFixed(1) + '%';
  out(`| ${s} | ${n} | ${pct('TokenPrimaryOffer')} | ${pct('TokenAuction')} | ${pct('TokenOffer')} | **${sorare}** |`);
}

out('\n## Preisabstand innerhalb desselben Spielers (nur Spieler mit beiden Arten im Fenster)\n');
out('Median von (Manager-Preis / Sorare-Preis) − 1: negativ = Zweitmarkt liegt unter Sorare.\n');
out('| Segment | Spieler mit beidem | Manager vs Sofortkauf | Manager vs Auktion |');
out('|---|---|---|---|');
for (const s of [...segs, 'ALLE']) {
  const pp = s === 'ALLE' ? perPlayer : perPlayer.filter(p => p.seg === s);
  const vsPrimary = [], vsAuction = [];
  for (const p of pp) {
    const off = median(p.byType.TokenOffer ?? []);
    const pri = median(p.byType.TokenPrimaryOffer ?? []);
    const auc = median(p.byType.TokenAuction ?? []);
    if (off && pri) vsPrimary.push(off / pri - 1);
    if (off && auc) vsAuction.push(off / auc - 1);
  }
  const f = a => a.length ? `${(median(a) * 100).toFixed(1)}% (n=${a.length})` : 'zu wenig Daten';
  out(`| ${s} | ${Math.max(vsPrimary.length, vsAuction.length)} | ${f(vsPrimary)} | ${f(vsAuction)} |`);
}

out('\n## Einordnung\n');
out('- "Sales" in Sorares Preishistorie sind zu einem grossen Teil Sorares eigene Verkaeufe (Sofortkauf + Auktion), keine Zweitmarkt-Transaktionen.');
out('- Der Manager-zu-Manager-Preis ist der einzige echte Marktpreis. Liegt er systematisch unter Sorares Preisen, ist jeder Schaetzer, der alle Arten mischt (unser FMV, der Verkaufsschnitt, Sorares eigene Anzeige), nach oben verzerrt.');
out('- Stichprobe = liquideste Spieler je Segment; fuer illiquide Karten kann der Sorare-Anteil noch hoeher liegen (dort verkauft oft NUR Sorare).');
out('- Formel-Entscheidung (Jonas): Trennung oder Gewichtung der Verkaufsarten im FMV. Daten dafuer sammelt ab jetzt fmv_accuracy.deal_type; Auswertung per RPC accuracy_by_deal(p_days).');

const file = `docs/2026-09-05_VERKAUFSARTEN_MESSUNG.md`;
writeFileSync(file, lines.join('\n') + '\n');
console.log(`\nBericht: ${file}`);
