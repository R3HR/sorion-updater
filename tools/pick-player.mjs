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
// Altersgrenze fuer die U23-Wettbewerbe. Sorare hat kein Feld "ist U23-tauglich",
// also filtern wir ueber das Alter aus der Spieler-Meta (player_age, kommt von
// Sorares eigenem age-Feld). 23 ist die Grenze, das Alter steht in der Ausgabe,
// damit Grenzfaelle sichtbar bleiben.
const MAXALTER = arg('maxalter', '') ? parseInt(arg('maxalter'), 10) : null;
// `eligibleSo5Competitions` (Hinweis Jonas 07.09.) listet NUR die Liga-Wettbewerbe
// eines Spielers: MLS, Turkish League, Contender, Rest of the World. Die
// Sonderwettbewerbe U23, All Star und Champion stehen dort NICHT drin, geprueft an
// vier Spielern. Fuer die bleibt die Altersgrenze der einzige Weg.
// Empirisch aus echten U23-Aufstellungen (GW10, 241 Spieler): eingesetzt wurden
// 18- bis 24-Jaehrige, Schwerpunkt 22 und 23. Die Grenze haengt also am Stichtag zu
// Saisonbeginn, nicht am heutigen Alter. --maxalter=23 trifft es fast immer,
// --maxalter=24 nimmt die Grenzfaelle mit.
const WETTBEWERB = arg('wettbewerb', '');

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
  .select('player_slug, player_name, position, eligibility, fmv, floor_price, team_name, league_name, gameplay_tier, player_age')
  .eq('scarcity', RARITY).eq('position', POS).limit(5000);
if (LIGA) q = q.eq('league_name', LIGA);
if (MAXALTER) q = q.lte('player_age', MAXALTER).not('player_age', 'is', null);
const { data: cards, error } = await q;
if (error) { console.error(error.message); process.exit(1); }

const cand = new Map();
for (const c of cards ?? []) {
  const eur = c.fmv ?? c.floor_price;
  if (eur == null || Number(eur) > BUDGET) continue;
  const cur = cand.get(c.player_slug);
  if (!cur || Number(eur) < cur.eur)
    cand.set(c.player_slug, { slug: c.player_slug, name: c.player_name, team: c.team_name,
      liga: c.league_name, tier: c.gameplay_tier, eur: Number(eur), elig: c.eligibility,
      alter: c.player_age ?? null });
}
// Teuerste zuerst: im selben Budget ist der hoehere Preis das Marktsignal fuer Qualitaet.
const list = [...cand.values()].sort((a, b) => b.eur - a.eur).slice(0, PRUEFE);
console.log(`${cand.size} ${RARITY}-${POS} bis ${BUDGET} EUR${LIGA ? ` in ${LIGA}` : ''}`
  + `${MAXALTER ? `, hoechstens ${MAXALTER} Jahre alt` : ''}; `
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
  wettbewerbe: eligibleSo5Competitions { slug displayName }
  spiele: anyGamesForFixture(so5FixtureSlug: "${FIXTURE || 'x'}") { date homeTeam { name } awayTeam { name } }
  kommend: anyFutureGameStats(first: 4) {
    ... on PlayerGameStats { game { date }
      footballPlayingStatusOdds { starterOddsBasisPoints substituteOddsBasisPoints nonPlayingOddsBasisPoints reliability } } }
  angebotIn: lowestPriceAnyCard(inSeason: true, rarity: ${RARITY}) {
    liveSingleSaleOffer { receiverSide { amounts { eurCents } } } }
  angebotCl: lowestPriceAnyCard(inSeason: false, rarity: ${RARITY}) {
    liveSingleSaleOffer { receiverSide { amounts { eurCents } } } }
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
    // Startelf-Quote JE SPIEL statt nur fuer den Wochenend-Spieltag: die Odds haengen
    // an den Spielstatistiken des jeweiligen Spiels (footballPlayingStatusOdds).
    // nextClassicFixturePlayingStatusOdds ist bei Midweek-Spieltagen immer null und
    // hat deshalb Verletzte durchgelassen (Fehlgriff 07.09.: Bensebaini, Foyth).
    const spiel = (p.spiele ?? [])[0] ?? null;
    const statsFuerSpiel = (p.kommend ?? []).find(g => spiel && g.game?.date === spiel.date)
      ?? (p.kommend ?? [])[0] ?? null;
    const o = statsFuerSpiel?.footballPlayingStatusOdds ?? p.odds ?? null;
    const starter = typeof o?.starterOddsBasisPoints === 'number' ? o.starterOddsBasisPoints / 10000 : null;
    const bank    = typeof o?.substituteOddsBasisPoints === 'number' ? o.substituteOddsBasisPoints / 10000 : null;
    // Der FMV ist ein Schaetzwert und sagt NICHT, ob die Karte gerade zu haben ist.
    // Wer kaufen will, braucht das echte Angebot. Fehlt es, gibt es die Karte nicht.
    const cent = x => x?.liveSingleSaleOffer?.receiverSide?.amounts?.eurCents ?? null;
    const angebote = [];
    if (cent(p.angebotIn) != null) angebote.push({ elig: 'in_season', eur: cent(p.angebotIn) / 100 });
    if (cent(p.angebotCl) != null) angebote.push({ elig: 'classic',   eur: cent(p.angebotCl) / 100 });
    angebote.sort((a, b) => a.eur - b.eur);
    const proj = (MIDWEEK ? p.daily?.score : p.grade?.score) ?? null;
    const grade = (MIDWEEK ? p.daily?.grade : p.grade?.grade) ?? null;
    rows.push({ ...c, name: p.displayName || c.name, tier: p.gameplayTier ?? c.tier,
      l5: p.l5 ?? null, l15: p.l15 ?? null, a5: p.a5 ?? null,
      next: spiel?.date ?? null, bank,
      comps: (p.wettbewerbe ?? []).map(w => w.displayName || w.slug),
      gegner: spiel ? `${spiel.homeTeam?.name ?? '?'} - ${spiel.awayTeam?.name ?? '?'}` : null,
      spieltIm: (p.spiele ?? []).length > 0,
      status: p.playingStatus ?? null, grade, proj, starter,
      kauf: angebote[0]?.eur ?? null, kaufElig: angebote[0]?.elig ?? null,
      angebote,
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
// Sorare beantwortet die Frage "spielt dieser Spieler in Game Week X?" selbst
// (anyGamesForFixture) — genau der Filter, den auch der Marktplatz anbietet.
// Der frueherer Datumsvergleich ueber nextGame liess Testspiele durch.
const imFenster = r => !fenster || r.spieltIm;
const vorAuswahl = rows.length;
if (fenster) {
  const raus = rows.filter(r => !imFenster(r)).length;
  console.log(`${rows.length - raus} von ${vorAuswahl} haben ein Spiel in ${fenster.name}, ${raus} nicht.
`);
}
rows.splice(0, rows.length, ...rows.filter(imFenster));

// Drei harte Ausschluesse, alle drei aus echten Fehlgriffen gelernt (07.09.):
//   - kein Angebot  = die Karte ist nicht kaufbar, der FMV ist dann eine Fata Morgana
//   - Budget        = das Angebot zaehlt, nicht unsere Schaetzung
//   - Startelf 0 %  = verletzt oder gesperrt, Sorare sagt es, man muss nur hinsehen
const raus = { kein_angebot: 0, zu_teuer: 0, faellt_aus: 0, falscher_wettbewerb: 0 };
const passtZumWettbewerb = r => !WETTBEWERB
  || (r.comps ?? []).some(c => c.toLowerCase().includes(WETTBEWERB.toLowerCase()));
const brauchbar = rows.filter(r => {
  if (!passtZumWettbewerb(r)) { raus.falscher_wettbewerb++; return false; }
  if (r.kauf == null)        { raus.kein_angebot++; return false; }
  if (r.kauf > BUDGET)       { raus.zu_teuer++;     return false; }
  if (r.starter != null && r.starter < 0.4) { raus.faellt_aus++; return false; }
  if (r.status && /injur|suspend|out/i.test(r.status)) { raus.faellt_aus++; return false; }
  return true;
});
if (WETTBEWERB && raus.falscher_wettbewerb === rows.length)
  console.log(`WARNUNG: kein einziger Kandidat ist fuer "${WETTBEWERB}" gelistet. `
    + `eligibleSo5Competitions kennt nur Liga-Wettbewerbe (MLS, Turkish League, Contender, `
    + `Rest of the World). Fuer U23, All Star und Champion stattdessen --maxalter benutzen.`);
console.log(`Aussortiert: ${WETTBEWERB ? `${raus.falscher_wettbewerb} nicht fuer "${WETTBEWERB}" zugelassen, ` : ''}`
  + `${raus.kein_angebot} ohne Angebot, ${raus.zu_teuer} ueber Budget `
  + `(Angebot teurer als unser FMV), ${raus.faellt_aus} verletzt/gesperrt/0 % Startelf.`);
console.log(`Bleiben ${brauchbar.length} kaufbare Kandidaten.
`);
rows.splice(0, rows.length, ...brauchbar);

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
