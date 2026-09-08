// Wer ist fuer den KOMMENDEN Spieltag der beste Kauf im Budget?
// (Frage Jonas 07.09.2026: "5 EUR, All-Star-tauglicher Limited-Verteidiger fuer GW12".)
//
// Anders als cheapest-reliable-lineup.mjs (Rueckschau) schaut das hier nach VORN:
// Startelf-Quote und Punkte-Prognose kommen von Sorare Inside ueber die Sorare-API und
// sind nur wenige Tage vor dem Spieltag verfuegbar.
//
// Rangfolge = Prognose x Startelf-Quote ("erwartete Punkte"), denn eine hohe Prognose
// nuetzt nichts, wenn der Spieler auf der Bank sitzt. Form (L5/L15) steht daneben als
// Gegenprobe, damit eine einzelne Prognose nicht allein entscheidet.
//
// Aufruf: railway run -s "Updater Limited" node tools/pick-player.mjs \
//           --pos=Defender --rarity=limited --budget=5 [--liga="..."] [--top=10] [--pruefe=240]
import { createClient } from '@supabase/supabase-js';

const arg = (k, d) => { const a = process.argv.find(x => x.startsWith(`--${k}=`)); return a ? a.split('=').slice(1).join('=') : d; };
const POS    = arg('pos', 'Defender');
const RARITY = arg('rarity', 'limited');
const BUDGET = parseFloat(arg('budget', '5'));
const LIGA   = arg('liga', '');
const TOP    = parseInt(arg('top', '10'), 10);
const PRUEFE = parseInt(arg('pruefe', '240'), 10);   // wie viele Kandidaten an die API gehen
// Zielspieltag: nur Spieler, die in DIESEM Fenster ein Spiel haben, sind ueberhaupt
// brauchbar. Sorares Marktplatz filtert genauso ("spielt in Game Week X").
const FIXTURE = arg('fixture', '');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const APIKEY = process.env.SORARE_APIKEY;
if (!APIKEY) { console.error('SORARE_APIKEY fehlt'); process.exit(1); }
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gql(query, label) {
  for (let i = 0; i < 3; i++) {
    const res = await fetch('https://api.sorare.com/graphql',
      { method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY }, body: JSON.stringify({ query }) });
    if (res.status === 429) { await sleep(20000 * (i + 1)); continue; }
    const j = await res.json().catch(() => null);
    if (j?.errors) { console.warn(`  GraphQL (${label}): ${j.errors[0]?.message?.slice(0, 120)}`); return j.data ?? null; }
    if (j?.data) return j.data;
    await sleep(1500 * (i + 1));
  }
  return null;
}

// Kandidaten: guenstigste zulaessige Karte je Spieler im Budget.
// All Star laeuft als "all_seasons", also zaehlt auch die Classic-Karte.
let q = supabase.from('card_prices')
  .select('player_slug, player_name, position, eligibility, fmv, floor_price, team_name, league_name, gameplay_tier')
  .eq('scarcity', RARITY).eq('position', POS).limit(5000);
if (LIGA) q = q.eq('league_name', LIGA);
const { data: cards, error } = await q;
if (error) { console.error(error.message); process.exit(1); }

const cand = new Map();
for (const c of cards ?? []) {
  const eur = c.fmv ?? c.floor_price;
  if (eur == null || Number(eur) > BUDGET) continue;
  const cur = cand.get(c.player_slug);
  if (!cur || Number(eur) < cur.eur)
    cand.set(c.player_slug, { slug: c.player_slug, name: c.player_name, team: c.team_name,
      liga: c.league_name, tier: c.gameplay_tier, eur: Number(eur), elig: c.eligibility });
}
// Teuerste zuerst: im selben Budget ist der hoehere Preis das Marktsignal fuer Qualitaet.
const list = [...cand.values()].sort((a, b) => b.eur - a.eur).slice(0, PRUEFE);
console.log(`${cand.size} ${RARITY}-${POS} bis ${BUDGET} EUR${LIGA ? ` in ${LIGA}` : ''}; `
  + `geprueft werden die ${list.length} teuersten davon\n`);

// Ein Aufruf je Spieler. Buendeln geht nicht: mehrere anyPlayer als Wurzel lehnt
// Sorare ab ("Duplicated root field"), und football.players(slugs:) ist abgeschafft
// (steht noch im lokalen reference/schema.graphql, existiert live nicht mehr).
const QUERY = slug => `{ anyPlayer(slug:"${slug}") {
  displayName gameplayTier
  l5:  averageScore(type: LAST_FIVE_SO5_AVERAGE_SCORE)
  l15: averageScore(type: LAST_FIFTEEN_SO5_AVERAGE_SCORE)
  a5:  lastFiveSo5Appearances
  grade: nextClassicFixtureProjectedGrade { grade score reliabilityBasisPoints }
  daily: nextDailyFixtureProjectedGrade   { grade score reliabilityBasisPoints }
  nextGame(so5FixtureEligible: true) { date homeTeam { name } awayTeam { name } }
  ... on Player { playingStatus
    odds: nextClassicFixturePlayingStatusOdds { starterOddsBasisPoints substituteOddsBasisPoints reliability } }
} }`;
// "Classic" heisst bei Sorare WOCHENEND-Spieltag, "Daily" der Spieltag unter der Woche.
// Fuer einen Midweek-Spieltag ist also die Daily-Prognose die richtige. Eine
// Startelf-Quote gibt es nur fuer Classic; sie dient hier als Anhalt, ob der Spieler
// ueberhaupt Stammkraft ist.
const MIDWEEK = arg('midweek', '') === '1';

let fenster = null;
if (FIXTURE) {
  const f = await gql(`{ so5 { so5Fixture(slug:"${FIXTURE}") { displayName startDate endDate } } }`, FIXTURE);
  const x = f?.so5?.so5Fixture;
  if (!x) { console.error(`Spieltag ${FIXTURE} nicht gefunden`); process.exit(1); }
  fenster = { name: x.displayName, von: new Date(x.startDate), bis: new Date(x.endDate) };
  console.log(`Zielspieltag: ${fenster.name} (${x.startDate} bis ${x.endDate})
`);
}

const rows = [];
for (const [i, c] of list.entries()) {
  const d = await gql(QUERY(c.slug), c.slug);
  const p = d?.anyPlayer;
  if (p) {
    const starter = typeof p.odds?.starterOddsBasisPoints === 'number' ? p.odds.starterOddsBasisPoints / 10000 : null;
    const proj = (MIDWEEK ? p.daily?.score : p.grade?.score) ?? null;
    const grade = (MIDWEEK ? p.daily?.grade : p.grade?.grade) ?? null;
    rows.push({ ...c, name: p.displayName || c.name, tier: p.gameplayTier ?? c.tier,
      l5: p.l5 ?? null, l15: p.l15 ?? null, a5: p.a5 ?? null,
      next: p.nextGame?.date ?? null,
      gegner: p.nextGame ? `${p.nextGame.homeTeam?.name ?? '?'} - ${p.nextGame.awayTeam?.name ?? '?'}` : null,
      status: p.playingStatus ?? null, grade, proj, starter,
      // Ohne Startelf-Quote (Midweek) zaehlt die Prognose allein; sie enthaelt die
      // Einsatzwahrscheinlichkeit bereits teilweise.
      erwartet: proj == null ? null : (starter != null ? proj * starter : proj) });
  }
  if (i % 10 === 0 || i === list.length - 1)
    process.stdout.write(`\r  ${i + 1}/${list.length} abgefragt`);
  await sleep(400);
}
console.log('\n');

// Wer im Zielfenster kein Spiel hat, faellt raus. Das ist kein Feinschliff, sondern
// die wichtigste Bedingung: eine Karte ohne Spiel bringt sicher null Punkte.
const imFenster = r => !fenster || (r.next && new Date(r.next) >= fenster.von && new Date(r.next) <= fenster.bis);
const vorAuswahl = rows.length;
if (fenster) {
  const raus = rows.filter(r => !imFenster(r)).length;
  console.log(`${rows.length - raus} von ${vorAuswahl} haben ein Spiel in ${fenster.name}, ${raus} nicht.
`);
}
rows.splice(0, rows.length, ...rows.filter(imFenster));

const ok = rows.filter(r => r.erwartet != null);
if (!ok.length) {
  console.log('Keine Prognosen verfuegbar. Sorare Inside veroeffentlicht sie erst wenige Tage');
  console.log('vor dem Spieltag; vorher ist diese Frage nicht beantwortbar.');
  const mitForm = rows.filter(r => r.l15 != null).sort((a, b) => b.l15 - a.l15).slice(0, TOP);
  if (mitForm.length) {
    console.log('\nErsatzweise nach Form (Schnitt der letzten 15 Einsaetze):');
    for (const r of mitForm)
      console.log(`  ${(r.name || r.slug).slice(0, 24).padEnd(25)} ${(r.team || '').slice(0, 20).padEnd(21)} `
        + `${r.eur.toFixed(2).padStart(6)} EUR  L15 ${String(Math.round(r.l15)).padStart(3)}  `
        + `L5 ${r.l5 != null ? String(Math.round(r.l5)).padStart(3) : '  -'}  Einsaetze ${r.a5 ?? '-'}/5  ${r.elig}`);
  }
  process.exit(0);
}

ok.sort((a, b) => b.erwartet - a.erwartet);
console.log(`Beste erwartete Punkte fuer den naechsten Spieltag (Prognose x Startelf-Quote):\n`);
console.log(`  ${'Spieler'.padEnd(25)}${'Verein'.padEnd(21)}${'Preis'.padStart(6)}   ${'erw.'.padStart(5)} ${'Prog'.padStart(5)} ${'Start'.padStart(6)} ${'L15'.padStart(4)} ${'L5'.padStart(4)}  Karte`);
for (const r of ok.slice(0, TOP))
  console.log(`  ${(r.name || r.slug).slice(0, 24).padEnd(25)}${(r.team || '').slice(0, 20).padEnd(21)}`
    + `${r.eur.toFixed(2).padStart(6)}   ${r.erwartet.toFixed(0).padStart(5)} ${String(r.proj.toFixed(0)).padStart(5)} `
    + `${r.starter != null ? (r.starter * 100).toFixed(0).padStart(5) + '%' : '     -'} ${r.l15 != null ? String(Math.round(r.l15)).padStart(4) : '   -'} `
    + `${r.l5 != null ? String(Math.round(r.l5)).padStart(4) : '   -'}  ${r.elig} ${r.grade ? '(' + r.grade + ')' : ''}`);
console.log(`\n${ok.length} von ${rows.length} geprueften Spielern hatten eine Prognose.`);
