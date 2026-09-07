// Zeigt EINE konkrete Aufstellung samt Karten und heutigen Preisen.
// Beispiel:
//   railway run -s "Updater Limited" node tools/show-lineup.mjs \
//     --lb=football-28-aug-1-sep-2026-seasonal-japan-in_season_japan_rare_pvp --rank=8
import { createClient } from '@supabase/supabase-js';

const arg = k => (process.argv.find(a => a.startsWith(`--${k}=`)) || '').split('=')[1];
const LB = arg('lb'), RANK = parseInt(arg('rank'), 10), PAGE = 50;
if (!LB || !RANK) { console.error('--lb=<leaderboard-slug> --rank=<n> noetig'); process.exit(1); }

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const APIKEY = process.env.SORARE_APIKEY;
if (!APIKEY) { console.error('SORARE_APIKEY fehlt: mit "railway run -s \"Updater Limited\"" starten'); process.exit(1); }

const page = Math.max(0, Math.floor((RANK - 1) / PAGE));   // 0-indexiert!
const res = await fetch('https://api.sorare.com/graphql', {
  method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY },
  body: JSON.stringify({ query: `{ so5 { so5Leaderboard(slug:"${LB}") {
    displayName
    so5RankingsPaginated(page: ${page}, pageSize: ${PAGE}) {
      nodes { ranking score so5Lineup { so5Appearances {
        score anyPlayer { slug displayName anyPositions activeClub { name } }
        anyCard { rarityTyped inSeasonEligible } } } } } } } }` }),
});
const j = await res.json();
if (j.errors) { console.error(j.errors[0].message); process.exit(1); }
const lb = j.data.so5.so5Leaderboard;
const node = lb.so5RankingsPaginated.nodes.find(n => n.ranking === RANK);
if (!node) { console.error(`Rang ${RANK} nicht auf Seite ${page}`); process.exit(1); }

const apps = node.so5Lineup.so5Appearances;
const keys = apps.map(a => ({
  slug: a.anyPlayer.slug,
  scarcity: a.anyCard.rarityTyped.toLowerCase(),
  inseason: a.anyCard.inSeasonEligible,
}));
const { data: prices, error } = await supabase.from('card_prices')
  .select('player_slug, scarcity, eligibility, fmv, floor_price, sales_count, supply')
  .in('player_slug', keys.map(k => k.slug));
if (error) { console.error(error.message); process.exit(1); }

console.log(`\n${lb.displayName} — Rang ${node.ranking}, ${Number(node.score).toFixed(2)} Punkte\n`);
let sum = 0;
for (const a of apps) {
  const k = keys.find(x => x.slug === a.anyPlayer.slug);
  const want = k.inseason ? 'in_season' : 'classic';
  const p = (prices ?? []).find(r => r.player_slug === k.slug
    && r.scarcity === k.scarcity && r.eligibility === want);
  const eur = p?.fmv ?? p?.floor_price ?? null;
  if (eur != null) sum += Number(eur);
  console.log(`  ${(a.anyPlayer.displayName || k.slug).padEnd(26)} `
    + `${((a.anyPlayer.anyPositions || [])[0] || "").padEnd(12)} `
    + `${(a.anyPlayer.activeClub?.name || '').padEnd(24)} `
    + `${k.scarcity.padEnd(10)} ${k.inseason ? 'in-season' : 'classic  '} `
    + `${eur != null ? (Number(eur).toFixed(2) + ' EUR').padStart(10) : '   kein Preis'}`
    + `${p ? (p.fmv == null ? ' (nur Floor)' : '') : ''} `
    + `${a.score != null ? String(Math.round(a.score)).padStart(5) + ' pts' : ''}`);
}
console.log(`\n  Summe heute: ${sum.toFixed(2)} EUR fuer ${apps.length} Karten\n`);
