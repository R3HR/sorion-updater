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
// Liga, wie sie in card_prices heisst, falls der Wettbewerb anders heisst
// (LALIGA EA SPORTS = "Primera División"). Wird per ILIKE verglichen, damit Akzente
// in der Windows-Konsole mit _ umschrieben werden koennen: --kartenliga="Primera Divisi_n".
const KARTENLIGA = arg('kartenliga', LIGA);
// Nur Karten dieser Eligibility kaufen. In-Season-Wettbewerbe verlangen mindestens
// 4 In-Season-Karten, mit einer eigenen Classic-Karte muessen die uebrigen 4 In-Season sein.
const ELIG = arg('elig', null);
// Fester Spieler, den man schon besitzt: kostet nichts, belegt seinen Platz.
const MIT = arg('mit', null);
// Land der Liga (league_country). Noetig, weil Ligennamen nicht eindeutig sind:
// "Primera División" heisst die Liga auch in Chile, Venezuela, Costa Rica, Bolivien.
const LAND = arg('land', null);
// Ziel: 'cash' (Standard) oder 'essence'. Essence-Raenge sind viel breiter, das billigste
// verlaessliche Team liegt dort deutlich tiefer (Frage Jonas 14.09.: Essence zum Craften).
const ZIEL = arg('ziel', 'cash');
const SCORE_COL = ZIEL === 'essence' ? 'essence_score' : 'cash_score';
// Nur Spieltage, die freitags beginnen. Die Dienstag-bis-Freitag-Fenster sind fuer Ligen
// wie LaLiga fast leer (kaum Spiele unter der Woche) und ein eigener, kleiner Wettbewerb.
const WOCHENENDE = process.argv.includes('--wochenende');
// Nur die juengsten N Spieltage bewerten. Die Felder wachsen ueber die Saison
// (J1 Rare: 300 Aufstellungen im August, 985 im Oktober), alte Spieltage
// beschoenigen die Gegenwart.
const LETZTE = parseInt(arg('letzte', '0'), 10);
// Wie viele CLASSIC-Karten die Aufstellung enthalten darf. In-Season-Wettbewerbe verlangen
// mindestens 4 In-Season-Karten, bei 5 Slots ist also genau 1 Classic erlaubt
// (docs/competitions/rules.json). Classic ist deutlich billiger und hat den Saison-Abschlag
// schon hinter sich, deshalb gehoert dieser Slot ausgereizt.
const MAXCLASSIC = parseInt(arg('classic', '0'), 10);

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
  .select(`fixture_slug, fixture_name, start_date, ${SCORE_COL}, lineups`)
  .eq('competition', LIGA).eq('rarity', RARITY).not(SCORE_COL, 'is', null)
  .order('start_date');
if (!thr?.length) { console.error(`Keine ${ZIEL}-Schwellen fuer ${LIGA} / ${RARITY}`); process.exit(1); }
console.log(`Ziel: ${ZIEL.toUpperCase()}, Sicherheitsabstand ${(PUFFER * 100).toFixed(0)} % ueber der Schwelle`);
console.log(`${LIGA} ${RARITY}: ${thr.length} Spieltage mit ${ZIEL}-Schwelle `
  + `(${Math.min(...thr.map(t => +t[SCORE_COL])).toFixed(0)} bis ${Math.max(...thr.map(t => +t[SCORE_COL])).toFixed(0)} Punkte)`);

let weeks = thr.map((t, i) => ({
  slug: t.fixture_slug, name: t.fixture_name, need: Number(t[SCORE_COL]),
  from: new Date(t.start_date + 'T00:00:00Z'),
  to: thr[i + 1] ? new Date(thr[i + 1].start_date + 'T00:00:00Z')
                 : new Date(+new Date(t.start_date + 'T00:00:00Z') + 7 * 864e5),
}));
if (LETZTE > 0 && weeks.length > LETZTE) {
  const vorher = weeks.length;
  weeks = weeks.slice(-LETZTE);
  console.log(`Nur die juengsten ${weeks.length} von ${vorher} Spieltagen (${weeks[0].name} bis ${weeks[weeks.length - 1].name})`);
}
if (WOCHENENDE) {
  const vorher = weeks.length;
  weeks = weeks.filter(w => w.from.getUTCDay() === 5);
  console.log(`Nur Wochenend-Spieltage (Start Freitag): ${weeks.length} von ${vorher}`);
}

// 2) Kandidaten mit Preisen
let cq = supabase.from('card_prices')
  .select('player_slug, player_name, position, eligibility, fmv, floor_price, team_name, league_country')
  .ilike('league_name', KARTENLIGA).eq('scarcity', RARITY);
if (ELIG) cq = cq.eq('eligibility', ELIG);
const { data: rawCards, error: cardErr } = await cq;
if (cardErr) { console.error(`card_prices: ${cardErr.message}`); process.exit(1); }
if (!rawCards?.length) {
  console.error(`Keine Karten fuer league_name ILIKE "${KARTENLIGA}", scarcity ${RARITY}`
    + `${ELIG ? `, eligibility ${ELIG}` : ''}. Schreibweise pruefen.`);
  process.exit(1);
}
// Laenderfilter mit Rueckfall: Manche Zeilen haben league_country = null (BUG-042),
// obwohl der Verein eindeutig spanisch ist. Ein strikter Filter wuerde sie verlieren.
// Deshalb: Land passt, ODER kein Land und der Verein kommt mit diesem Land vor.
let cards = rawCards ?? [];
if (LAND) {
  const landTeams = new Set(cards.filter(c => c.league_country === LAND).map(c => c.team_name));
  const vorher = cards.length;
  cards = cards.filter(c => c.league_country === LAND || (c.league_country == null && landTeams.has(c.team_name)));
  console.log(`Laenderfilter ${LAND}: ${cards.length} von ${vorher} Kartenzeilen, ${landTeams.size} Vereine`);
}
// Je Spieler BEIDE Kartenarten fuehren, sobald ein Classic-Slot erlaubt ist. Die Punkte
// haengen am Spieler, nicht an der Karte, nur der Preis unterscheidet sich.
const cand = new Map();                      // Key: slug + '|' + eligibility
for (const c of cards ?? []) {
  const eur = c.fmv ?? c.floor_price;
  if (eur == null) continue;
  if (MAXCLASSIC === 0 && !ELIG && c.eligibility !== 'in_season') continue;
  const key = c.player_slug + '|' + c.eligibility;
  const cur = cand.get(key);
  if (!cur || Number(eur) < cur.eur)
    cand.set(key, { key, slug: c.player_slug, name: c.player_name, pos: c.position,
      team: c.team_name, eur: Number(eur), elig: c.eligibility });
}
if (MIT && ![...cand.values()].some(c => c.slug === MIT)) {
  // Der eigene Spieler kann eine andere Eligibility haben (Classic in In-Season-Wettbewerb).
  const { data: own } = await supabase.from('card_prices')
    .select('player_slug, player_name, position, team_name').eq('player_slug', MIT).eq('scarcity', RARITY).limit(1);
  if (own?.[0]) cand.set(MIT + '|besitzt', { key: MIT + '|besitzt', slug: MIT, name: own[0].player_name,
    pos: own[0].position, team: own[0].team_name, eur: 0, elig: 'besitzt' });
}
if (MIT) {
  // Eigene Karte: alle Kaufvarianten dieses Spielers raus, eine Gratis-Variante rein.
  const vorhanden = [...cand.values()].find(c => c.slug === MIT);
  for (const k of [...cand.keys()]) if (cand.get(k).slug === MIT) cand.delete(k);
  if (!vorhanden) { console.error(`Fester Spieler ${MIT} nicht gefunden`); process.exit(1); }
  cand.set(MIT + '|besitzt', { ...vorhanden, key: MIT + '|besitzt', eur: 0, elig: 'besitzt' });
  const f = cand.get(MIT + '|besitzt');
  console.log(`Fest gesetzt (kostet nichts): ${f.name}, ${f.pos}, ${f.team}`);
}
console.log(`${new Set([...cand.values()].map(c => c.slug)).size} Spieler, ${cand.size} Kartenvarianten`
  + `${ELIG ? ` (nur ${ELIG})` : ''}${MAXCLASSIC ? `, bis zu ${MAXCLASSIC} Classic erlaubt` : ''}`);

// 3) Punkte je Spieltag ueber die Vereine
const playerSlugs = new Set([...cand.values()].map(c => c.slug));
const teams = [...new Set([...cand.values()].map(c => c.team).filter(Boolean))];
const clubsData = await gql(`{ football { clubsReady { slug name } } }`, 'clubs');
const clubs = (clubsData?.football?.clubsReady ?? []).filter(c => teams.includes(c.name));
console.log(`${clubs.length} von ${teams.length} Vereinen ueber die API erreichbar`);
const notFound = teams.filter(t => !clubs.some(c => c.name === t));
if (notFound.length) console.log(`  NICHT gefunden (Spieler dieser Vereine haben keine Punkte): ${notFound.join(' | ')}`);

const scores = new Map();
for (const club of clubs) {
  const d = await gql(`{ football { club(slug:"${club.slug}") { activePlayers(first: 50) { nodes {
      slug so5Scores(last: 30) { score anyGame { date
        classic: so5Fixture(so5FixtureType: CLASSIC) { slug }
        daily:   so5Fixture(so5FixtureType: DAILY)   { slug } } } } } } } }`, club.slug);
  for (const n of d?.football?.club?.activePlayers?.nodes ?? []) {
    if (!playerSlugs.has(n.slug)) continue;
    scores.set(n.slug, (n.so5Scores ?? []).filter(s => s?.anyGame?.date)
      .map(s => ({ date: new Date(s.anyGame.date), score: Number(s.score),
        fixtures: [s.anyGame.classic?.slug, s.anyGame.daily?.slug].filter(Boolean) })));
  }
  await sleep(700);
}
console.log(`Punkte fuer ${scores.size} Spieler geladen`);

for (const [, c] of cand) {
  const list = scores.get(c.slug) ?? [];
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
// Nur Spieler, fuer die Punkte geladen wurden. Sonst druecken Vereine, die ueber die
// API nicht erreichbar sind, die Quote und ein normaler Spieltag gilt als "zu duenn".
// Abdeckung je Spieltag nur ueber In-Season-Varianten zaehlen, sonst wird jeder Spieler
// doppelt gezaehlt und die Prozentzahl ist halbiert.
const alive = [...cand.values()].filter(c => scores.has(c.slug) && c.elig !== 'classic');
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
const pool = [...cand.values()].filter(c => POS[c.pos] && c.played >= 3 && c.slug !== MIT).sort((a, b) => a.eur - b.eur);
console.log(`${pool.length} Kandidaten mit mindestens 3 Einsaetzen\n`);

// Ein Spieler darf nur einmal in der Elf stehen, egal mit welcher Karte, und die
// Classic-Grenze des Wettbewerbs muss eingehalten sein.
const gueltig = team => {
  if (new Set(team.map(p => p.slug)).size !== team.length) return false;
  return team.filter(p => p.elig === 'classic').length <= MAXCLASSIC;
};

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
// Dominanz NUR innerhalb derselben Kartenart pruefen. Sonst verdraengt die Classic-Karte
// eines Spielers immer seine In-Season-Karte (gleiche Punkte, billiger) und die Suche
// landete bei lauter Classic-Karten, also ausserhalb der Wettbewerbsregel.
const frontOf = l => {
  const front = [];
  for (const c of [...l].sort((a, b) => a.eur - b.eur)) {
    const dominated = front.some(f => f.eur <= c.eur && scored.every(w => f.byWeek[w] >= c.byWeek[w]));
    if (!dominated) front.push(c);
  }
  return front;
};
const slice = k => {
  const l = pool.filter(c => POS[c.pos] === k);
  const inS = frontOf(l.filter(c => c.elig !== 'classic'));
  const cls = MAXCLASSIC > 0 ? frontOf(l.filter(c => c.elig === 'classic')) : [];
  return [...inS, ...cls].sort((a, b) => a.eur - b.eur);
};
const G = slice('GK'), D = slice('DEF'), M = slice('MID'), F = slice('FWD');
const extras = [...new Set([...D, ...M, ...F])].sort((a, b) => a.eur - b.eur);
console.log(`Suchraum (Pareto): ${G.length} TW x ${D.length} ABW x ${M.length} MF x ${F.length} ST x ${extras.length} frei`);

// Suche mit Abbruchschranke: alle Listen sind nach Preis sortiert, also kann jeder
// Zweig verworfen werden, sobald er das bisher billigste Team nicht mehr unterbieten kann.
const minE = extras.length ? extras[0].eur : 0;

// ZWEITE SCHRANKE, neben den Kosten: die erreichbaren PUNKTE.
// Fuer jede Positionsliste wird je Woche das Maximum vorberechnet. Steht eine Teilauswahl
// fest, laesst sich damit die bestmoegliche Vervollstaendigung abschaetzen. Schafft selbst
// die die geforderte Quote nicht, ist der ganze Zweig tot.
// Ohne das explodiert die Suche: mit Classic-Slot sind es ueber 15 Milliarden Kombinationen,
// und die Kosten-Schranke greift erst, sobald das erste gueltige Team gefunden ist.
const maxJeWoche = list => scored.map(w => list.reduce((m, c) => Math.max(m, c.byWeek[w]), 0));
const maxD = maxJeWoche(D), maxM = maxJeWoche(M), maxF = maxJeWoche(F), maxE = maxJeWoche(extras);
const nullArr = scored.map(() => 0);
// Was schafft die OBERGRENZE, also je Position der punktbeste Spieler dieser Woche?
// Das ist kein kaufbares Team (die Besten stehen nicht in einer Elf zusammen und waeren
// unbezahlbar), aber es beantwortet die Vorfrage: Ist die geforderte Quote ueberhaupt
// erreichbar? Liegt die Obergrenze unter der Schwelle, sucht man sonst stundenlang Nichts.
{
  const maxG = maxJeWoche(G);
  let erreichbar = 0;
  console.log('Obergrenze je Spieltag (punktbester Spieler je Position, nicht kaufbar):');
  for (let i = 0; i < scored.length; i++) {
    const w = scored[i];
    const sum = maxG[i] + maxD[i] + maxM[i] + maxF[i] + maxE[i];
    const ok = sum >= weeks[w].need * (1 + PUFFER);
    if (ok) erreichbar++;
    console.log(`  ${weeks[w].name.padEnd(13)} hoechstens ${String(Math.round(sum)).padStart(4)} pts, `
      + `noetig ${String(Math.round(weeks[w].need * (1 + PUFFER))).padStart(4)}  ${ok ? 'machbar' : 'UNMOEGLICH'}`);
  }
  const maxQuote = erreichbar / scored.length;
  console.log(`  Hoechstens erreichbare Quote: ${(maxQuote * 100).toFixed(0)} % `
    + `(${erreichbar} von ${scored.length} Spieltagen), gefordert ${(QUOTE * 100).toFixed(0)} %`);
  if (maxQuote < QUOTE) {
    console.log(`\nABBRUCH: Die geforderte Quote ist in diesem Wettbewerb nicht erreichbar, `
      + `auch nicht mit unbegrenztem Budget. Mit --quote=${maxQuote.toFixed(2)} oder weniger erneut starten.`);
    process.exit(0);
  }
}
// Erreicht die Quote noch, wenn die offenen Positionen ihr jeweiliges Maximum liefern?
const moeglich = (feste, ...offene) => {
  let hit = 0;
  for (let i = 0; i < scored.length; i++) {
    const w = scored[i];
    let sum = feste.reduce((acc, pl) => acc + pl.byWeek[w], 0);
    for (const arr of offene) sum += arr[i];
    if (sum >= weeks[w].need * (1 + PUFFER)) hit++;
  }
  return hit / scored.length >= QUOTE;
};
const found = [];
let cap = Infinity, bestQ = -1, bestTeam = null, geprueft = 0;
const t0 = Date.now();
const tick = () => {
  if (++geprueft % 2000000 === 0)
    console.log(`  ... ${(geprueft / 1e6).toFixed(0)} Mio geprueft, `
      + `${((Date.now() - t0) / 1000).toFixed(0)} s, bestes bisher `
      + `${cap === Infinity ? 'keins' : cap.toFixed(2) + ' EUR'}`);
};
const fixed = MIT ? cand.get(MIT) : null;
if (fixed) {
  // Der feste Spieler belegt seinen Positionsplatz. Gesucht werden die drei uebrigen
  // Pflichtpositionen plus ein freier Platz; der freie Platz darf auch seine Position sein.
  const byKey = { GK: G, DEF: D, MID: M, FWD: F };
  const L = ['GK', 'DEF', 'MID', 'FWD'].filter(k => k !== POS[fixed.pos]).map(k => byKey[k]);
  if (L.some(x => !x.length) || !extras.length) { console.error('Leere Positionsliste, keine Suche moeglich'); process.exit(1); }
  for (const a of L[0]) {
    if (a.eur + L[1][0].eur + L[2][0].eur + minE >= cap) break;
    for (const b of L[1]) {
      if (a.eur + b.eur + L[2][0].eur + minE >= cap) break;
      for (const c of L[2]) {
        const bc = a.eur + b.eur + c.eur;
        if (bc + minE >= cap) break;
        const base = [fixed, a, b, c];
        for (const e of extras) {
          const cost = bc + e.eur;
          if (cost >= cap) break;
          if (base.some(p => p.slug === e.slug)) continue;
          const team = [...base, e];
          if (!gueltig(team)) continue;
          tick();
          const q = rate(team);
          if (q > bestQ) { bestQ = q; bestTeam = { cost, q, team }; }
          if (q >= QUOTE) { found.push({ cost, q, team }); if (cost < cap) cap = cost; }
        }
      }
    }
  }
} else {
  for (const g of G) {
    if (g.eur + D[0].eur + M[0].eur + F[0].eur + minE >= cap) break;
    if (!moeglich([g], maxD, maxM, maxF, maxE)) continue;
    for (const d of D) {
      if (g.eur + d.eur + M[0].eur + F[0].eur + minE >= cap) break;
      if (!moeglich([g, d], maxM, maxF, maxE)) continue;
      for (const m of M) {
        if (g.eur + d.eur + m.eur + F[0].eur + minE >= cap) break;
        if (!moeglich([g, d, m], maxF, maxE)) continue;
        for (const f of F) {
          if (!moeglich([g, d, m, f], maxE)) continue;
          const baseCost = g.eur + d.eur + m.eur + f.eur;
          if (baseCost + minE >= cap) break;
          const base = [g, d, m, f];
          for (const e of extras) {
            const cost = baseCost + e.eur;
            if (cost >= cap) break;
            if (base.some(p => p.slug === e.slug)) continue;
            const team = [...base, e];
            if (!gueltig(team)) continue;
            tick();
            const q = rate(team);
            if (q > bestQ) { bestQ = q; bestTeam = { cost, q, team }; }
            if (q >= QUOTE) { found.push({ cost, q, team }); if (cost < cap) cap = cost; }
          }
        }
      }
    }
  }
}
// Gleiche Elf, andere Reihenfolge, ist dieselbe Elf.
const seen = new Set();
const uniq = [];
for (const r of found.sort((a, b) => a.cost - b.cost || b.q - a.q)) {
  const key = r.team.map(p => p.key).sort().join('#');
  if (seen.has(key)) continue;
  seen.add(key); uniq.push(r);
}
found.length = 0; found.push(...uniq);

const show = r => {
  console.log(`== ${r.cost.toFixed(2)} EUR, ${ZIEL} in ${Math.round(r.q * scored.length)} von ${scored.length} Wochen (${(r.q * 100).toFixed(0)} %)`);
  for (const p of r.team)
    console.log(`   ${(p.name || p.slug).slice(0, 22).padEnd(23)} ${POS[p.pos].padEnd(4)} `
      + `${(p.team || '').slice(0, 20).padEnd(21)} ${p.eur.toFixed(2).padStart(7)} EUR ${p.elig.padEnd(10)} `
      + p.byWeek.map(x => String(Math.round(x)).padStart(4)).join(''));
  const pad = ' '.repeat(23 + 1 + 4 + 1 + 21 + 1 + 7 + 5 + 10 + 1);
  console.log(`   Summe:${pad.slice(6)}` + weeks.map((w, i) => String(Math.round(r.team.reduce((s, p) => s + p.byWeek[i], 0))).padStart(4)).join(''));
  console.log(`   noetig:${pad.slice(7)}` + weeks.map(w => String(Math.round(w.need)).padStart(4)).join('') + '\n');
};

if (!found.length) {
  console.log(`Kein Team im Suchraum erreicht ${(QUOTE * 100).toFixed(0)} %.`);
  if (bestTeam) { console.log('Bestes gefundenes:'); show(bestTeam); }
  else console.log('Es wurde keine einzige gueltige Elf bewertet: die Punkte-Schranke hat alle '
    + 'Zweige verworfen. Quote senken oder Puffer entfernen.');
} else {
  console.log(`${found.length} Kombinationen ueber ${(QUOTE * 100).toFixed(0)} % Trefferquote\n`);
  for (const r of found.slice(0, TOP)) show(r);
}
