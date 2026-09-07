// Welche BILLIGSTE Aufstellung haette in einem Wettbewerb moeglichst JEDE Woche Cash geholt?
// (Frage Jonas 07.09.2026: nicht der historische Glueckstreffer, sondern Verlaesslichkeit.)
//
// VORGEHEN
//   1. Kandidaten: alle Spieler der Liga mit Karte in der gewuenschten Rarity und Preis.
//      Preis = guenstigste zulaessige Karte des Spielers (in-season ODER classic; in den
//      echten Gewinner-Aufstellungen tauchen beide auf).
//   2. Punkte je Spieltag: so5Scores(last:) je Spieler, ueber das Spieldatum dem
//      Spieltag zugeordnet. KEIN Ausfall wird geschoent: wer nicht spielte, bekommt 0.
//      Genau das ist das Risiko, um das es geht.
//   3. Backtest: Fuer jede Kandidaten-Elf wird jede abgeschlossene Woche dieser Saison
//      nachgerechnet und gegen die ECHTE Cash-Schwelle der Woche geprueft.
//      Trefferquote = Wochen mit Cash / gespielte Wochen.
//   4. Optimierung: aufsteigend nach Preis, Formation 1 TW + 1 ABW + 1 MF + 1 ST + 1 frei.
//      Gesucht ist das billigste Team ueber der geforderten Quote, nicht das beste Team.
//
// GRENZEN (ehrlich): Rueckschau auf wenige Spieltage ist eine kleine Stichprobe, und die
// Punkte enthalten keine Karten-Boni (XP, Kapitaen), sind also eher zu niedrig.
// Verletzungen und Transfers der Zukunft kennt niemand.
//
// Aufruf: railway run -s "Updater Limited" node tools/cheapest-reliable-lineup.mjs \
//           --liga="J1 League" --rarity=rare [--quote=0.7] [--top=5]
import { createClient } from '@supabase/supabase-js';

const arg = (k, d) => { const a = process.argv.find(x => x.startsWith(`--${k}=`)); return a ? a.split('=').slice(1).join('=') : d; };
const LIGA   = arg('liga', 'J1 League');
const RARITY = arg('rarity', 'rare');
const QUOTE  = parseFloat(arg('quote', '0.7'));
const TOP    = parseInt(arg('top', '5'), 10);
// Sicherheitsabstand: Ein Team, das die Schwelle nur knapp trifft, ist an die
// Vergangenheit angepasst, nicht verlaesslich. Mit --puffer=0.1 muss es 10 %
// ueber der Schwelle liegen, damit die Woche als Treffer zaehlt.
const PUFFER = parseFloat(arg('puffer', '0.1'));

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
    if (j?.errors) { console.warn(`  GraphQL (${label}): ${j.errors[0]?.message?.slice(0, 110)}`); return null; }
    if (j?.data) return j.data;
    await sleep(1500 * (i + 1));
  }
  return null;
}

// 1) Schwellen der abgeschlossenen Spieltage
const { data: thr } = await supabase.from('reward_thresholds')
  .select('fixture_slug, fixture_name, start_date, cash_score, lineups')
  .eq('competition', LIGA).eq('rarity', RARITY).not('cash_score', 'is', null)
  .order('start_date');
if (!thr?.length) { console.error(`Keine Cash-Schwellen fuer ${LIGA} / ${RARITY}`); process.exit(1); }
console.log(`Sicherheitsabstand: ${(PUFFER * 100).toFixed(0)} % ueber der Cash-Schwelle`);
console.log(`${LIGA} ${RARITY}: ${thr.length} Spieltage mit Cash-Schwelle `
  + `(${Math.min(...thr.map(t => +t.cash_score)).toFixed(0)} bis ${Math.max(...thr.map(t => +t.cash_score)).toFixed(0)} Punkte)`);

const weeks = thr.map((t, i) => ({
  slug: t.fixture_slug, name: t.fixture_name, need: Number(t.cash_score),
  from: new Date(t.start_date + 'T00:00:00Z'),
  to: thr[i + 1] ? new Date(thr[i + 1].start_date + 'T00:00:00Z')
                 : new Date(+new Date(t.start_date + 'T00:00:00Z') + 7 * 864e5),
}));

// 2) Kandidaten mit Preisen
const { data: cards } = await supabase.from('card_prices')
  .select('player_slug, player_name, position, eligibility, fmv, floor_price, team_name')
  .eq('league_name', LIGA).eq('scarcity', RARITY);
const cand = new Map();
for (const c of cards ?? []) {
  const eur = c.fmv ?? c.floor_price;
  if (eur == null) continue;
  const cur = cand.get(c.player_slug);
  if (!cur || Number(eur) < cur.eur)
    cand.set(c.player_slug, { slug: c.player_slug, name: c.player_name, pos: c.position,
      team: c.team_name, eur: Number(eur), elig: c.eligibility });
}
console.log(`${cand.size} Spieler mit ${RARITY}-Karte und Preis`);

// 3) Punkte je Spieltag ueber die Vereine
const teams = [...new Set([...cand.values()].map(c => c.team).filter(Boolean))];
const clubsData = await gql(`{ football { clubsReady { slug name } } }`, 'clubs');
const clubs = (clubsData?.football?.clubsReady ?? []).filter(c => teams.includes(c.name));
console.log(`${clubs.length} von ${teams.length} Vereinen ueber die API erreichbar`);

const scores = new Map();
for (const club of clubs) {
  const d = await gql(`{ football { club(slug:"${club.slug}") { activePlayers(first: 50) { nodes {
      slug so5Scores(last: 30) { score anyGame { date
        classic: so5Fixture(so5FixtureType: CLASSIC) { slug }
        daily:   so5Fixture(so5FixtureType: DAILY)   { slug } } } } } } } }`, club.slug);
  for (const n of d?.football?.club?.activePlayers?.nodes ?? []) {
    if (!cand.has(n.slug)) continue;
    scores.set(n.slug, (n.so5Scores ?? []).filter(s => s?.anyGame?.date)
      .map(s => ({ date: new Date(s.anyGame.date), score: Number(s.score),
        fixtures: [s.anyGame.classic?.slug, s.anyGame.daily?.slug].filter(Boolean) })));
  }
  await sleep(700);
}
console.log(`Punkte fuer ${scores.size} Spieler geladen`);

for (const [slug, c] of cand) {
  const list = scores.get(slug) ?? [];
  c.byWeek = weeks.map(w => {
    // Sorare sagt selbst, zu welchem Spieltag ein Spiel zaehlt. Das Datumsfenster
    // war zu grob: J1-Spieltage laufen teils unter der Woche und ueberlappen.
    let hit = list.filter(s => s.fixtures.includes(w.slug));
    if (!hit.length) hit = list.filter(s => s.date >= w.from && s.date < w.to);
    return hit.length ? Math.max(...hit.map(h => h.score)) : 0;
  });
  c.played = c.byWeek.filter(x => x > 0).length;
  c.total  = c.byWeek.reduce((s, x) => s + x, 0);
}

// Abdeckung je Spieltag: wie viele Kandidaten haben ueberhaupt gespielt?
// Ein Spieltag, an dem die Liga praktisch nicht antrat (Pokalwoche, Sonderfixture),
// darf die Verlaesslichkeit nicht kaputtrechnen. Er wird ausgewiesen und uebersprungen.
const alive = [...cand.values()];
for (const [i, w] of weeks.entries()) {
  w.coverage = alive.filter(c => c.byWeek[i] > 0).length / (alive.length || 1);
  w.count    = alive.filter(c => c.byWeek[i] > 0).length;
}
console.log('Abdeckung je Spieltag (Kandidaten mit Einsatz):');
for (const [i, w] of weeks.entries())
  console.log(`  ${w.name.padEnd(13)} noetig ${String(Math.round(w.need)).padStart(3)} pts  `
    + `${String(w.count).padStart(3)} Spieler (${(w.coverage * 100).toFixed(0)} %)`
    + (w.coverage < 0.2 ? '   <== zu duenn, fliesst nicht in die Quote' : ''));
const scored = weeks.map((w, i) => i).filter(i => weeks[i].coverage >= 0.2);
console.log(`Bewertet werden ${scored.length} von ${weeks.length} Spieltagen
`);

// 4) Billigstes Team ueber der geforderten Quote
const POS = { Goalkeeper: 'GK', Defender: 'DEF', Midfielder: 'MID', Forward: 'FWD' };
const pool = [...cand.values()].filter(c => POS[c.pos] && c.played >= 3).sort((a, b) => a.eur - b.eur);
console.log(`${pool.length} Kandidaten mit mindestens 3 Einsaetzen\n`);

const rate = team => {
  let hit = 0;
  for (const w of scored)
    if (team.reduce((s, p) => s + p.byWeek[w], 0) >= weeks[w].need * (1 + PUFFER)) hit++;
  return hit / scored.length;
};

// Kandidaten je Position auf die Pareto-Front eindampfen: wer teurer ist als ein
// anderer UND weniger Punkte bringt, kann in keinem billigsten Team stehen.
// (Die alte Auswahl "12 billigste + 8 staerkste" liess genau das Mittelfeld weg,
// in dem das beste Preis-Leistungs-Verhaeltnis sitzt.)
const avg = c => scored.reduce((s, w) => s + c.byWeek[w], 0) / (scored.length || 1);
for (const c of pool) c.avg = avg(c);
// Dominanz je WOCHE, nicht im Schnitt: ein Spieler faellt nur raus, wenn ein anderer
// hoechstens so teuer ist und in JEDER bewerteten Woche mindestens so viel bringt.
// Ueber den Schnitt zu filtern warf Spieler weg, die genau in den knappen Wochen tragen.
const slice = k => {
  const l = pool.filter(c => POS[c.pos] === k).sort((a, b) => a.eur - b.eur);
  const front = [];
  for (const c of l) {
    const dominated = front.some(f => f.eur <= c.eur && scored.every(w => f.byWeek[w] >= c.byWeek[w]));
    if (!dominated) front.push(c);
  }
  return front;
};
const G = slice('GK'), D = slice('DEF'), M = slice('MID'), F = slice('FWD');
const extras = [...new Set([...D, ...M, ...F])].sort((a, b) => a.eur - b.eur);
console.log(`Suchraum (Pareto): ${G.length} TW x ${D.length} ABW x ${M.length} MF x ${F.length} ST x ${extras.length} frei`);

// Suche mit Abbruchschranke: alle Listen sind nach Preis sortiert, also kann jeder
// Zweig verworfen werden, sobald er das bisher billigste Team nicht mehr unterbieten kann.
const minE = extras.length ? extras[0].eur : 0;
const found = [];
let cap = Infinity, bestQ = -1, bestTeam = null;
for (const g of G) {
  if (g.eur + D[0].eur + M[0].eur + F[0].eur + minE >= cap) break;
  for (const d of D) {
    if (g.eur + d.eur + M[0].eur + F[0].eur + minE >= cap) break;
    for (const m of M) {
      if (g.eur + d.eur + m.eur + F[0].eur + minE >= cap) break;
      for (const f of F) {
        const baseCost = g.eur + d.eur + m.eur + f.eur;
        if (baseCost + minE >= cap) break;
        const base = [g, d, m, f];
        for (const e of extras) {
          const cost = baseCost + e.eur;
          if (cost >= cap) break;
          if (base.some(p => p.slug === e.slug)) continue;
          const team = [...base, e], q = rate(team);
          if (q > bestQ) { bestQ = q; bestTeam = { cost, q, team }; }
          if (q >= QUOTE) { found.push({ cost, q, team }); if (cost < cap) cap = cost; }
        }
      }
    }
  }
}
// Gleiche Elf, andere Reihenfolge, ist dieselbe Elf.
const seen = new Set();
const uniq = [];
for (const r of found.sort((a, b) => a.cost - b.cost || b.q - a.q)) {
  const key = r.team.map(p => p.slug).sort().join('|');
  if (seen.has(key)) continue;
  seen.add(key); uniq.push(r);
}
found.length = 0; found.push(...uniq);

const show = r => {
  console.log(`== ${r.cost.toFixed(2)} EUR, Cash in ${Math.round(r.q * scored.length)} von ${scored.length} Wochen (${(r.q * 100).toFixed(0)} %)`);
  for (const p of r.team)
    console.log(`   ${(p.name || p.slug).slice(0, 22).padEnd(23)} ${POS[p.pos].padEnd(4)} `
      + `${(p.team || '').slice(0, 20).padEnd(21)} ${p.eur.toFixed(2).padStart(7)} EUR ${p.elig.padEnd(10)} `
      + p.byWeek.map(x => String(Math.round(x)).padStart(4)).join(''));
  const pad = ' '.repeat(23 + 1 + 4 + 1 + 21 + 1 + 7 + 5 + 10 + 1);
  console.log(`   Summe:${pad.slice(6)}` + weeks.map((w, i) => String(Math.round(r.team.reduce((s, p) => s + p.byWeek[i], 0))).padStart(4)).join(''));
  console.log(`   noetig:${pad.slice(7)}` + weeks.map(w => String(Math.round(w.need)).padStart(4)).join('') + '\n');
};

if (!found.length) {
  console.log(`Kein Team im Suchraum erreicht ${(QUOTE * 100).toFixed(0)} %. Bestes gefundenes:`);
  if (bestTeam) show(bestTeam);
} else {
  console.log(`${found.length} Kombinationen ueber ${(QUOTE * 100).toFixed(0)} % Trefferquote\n`);
  for (const r of found.slice(0, TOP)) show(r);
}
