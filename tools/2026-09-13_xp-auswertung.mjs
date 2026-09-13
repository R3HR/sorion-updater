// Auswertung XP-Effekt auf frischen Verkaeufen (<= 24 h).
import { readFileSync } from 'node:fs';
const all = JSON.parse(readFileSync(process.env.IN, 'utf8'));
const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : NaN; };
const pct = y => ((Math.exp(y) - 1) * 100);
const fmt = v => (v > 0 ? '+' : '') + v.toFixed(1) + ' %';

// Stoerfaktoren raus: Nummer 1 und Spezialeditionen (beide nachweislich teurer)
const s = all.filter(x => x.serial !== 1 && !x.edition);
console.log(`${all.length} frische Verkaeufe, nach Ausschluss von Nr. 1 und Spezialedition: ${s.length}\n`);

// 1) Paarweise innerhalb derselben Karte (Spieler+Rarity+Eligibility), gleiche Verkaufsart
const key = x => `${x.player}|${x.scarcity}|${x.elig}`;
const groups = {};
for (const x of s) (groups[key(x)] ??= []).push(x);

const perLevel = {};          // Steigung je Level-Differenz
const bySeg = {};             // Aufschlag je Level, getrennt nach Segment
const xpPairs = [];           // log-Preis gegen XP-Differenz
for (const [k, g] of Object.entries(groups)) {
  const seg = k.split('|').slice(1).join('/');
  for (let i = 0; i < g.length; i++) for (let j = i + 1; j < g.length; j++) {
    let a = g[i], b = g[j];
    if (a.deal !== b.deal) continue;
    if (a.grade === b.grade && a.xp === b.xp) continue;
    if (a.grade < b.grade || (a.grade === b.grade && a.xp < b.xp)) { const t = a; a = b; b = t; }  // a = mehr XP
    const y = Math.log(a.eur / b.eur);
    const dg = a.grade - b.grade;
    if (dg > 0) {
      (perLevel[Math.min(dg, 4)] ??= []).push(y);
      (bySeg[seg] ??= []).push(y / dg);
    }
    xpPairs.push({ y, dxp: a.xp - b.xp, seg });
  }
}

console.log('## Paare derselben Karte, gleiche Verkaufsart (positiv = mehr Level ist teurer)\n');
console.log('| Level-Differenz | Paare | Preisunterschied |');
console.log('|---|---|---|');
for (const dg of Object.keys(perLevel).sort()) {
  const v = perLevel[dg];
  console.log(`| ${dg === '4' ? '4+' : '+' + dg} | ${v.length} | ${v.length >= 15 ? fmt(pct(med(v))) : 'zu wenige'} |`);
}
console.log('\n| Segment | Paare | Aufschlag je Level |');
console.log('|---|---|---|');
for (const [seg, v] of Object.entries(bySeg).sort()) {
  console.log(`| ${seg} | ${v.length} | ${v.length >= 15 ? fmt(pct(med(v))) : 'zu wenige'} |`);
}

// 2) Relativ zum Median-Verkaufspreis derselben Karte in diesem Zeitfenster
//    (braucht keine Paare mit gleicher Verkaufsart, nutzt mehr Daten)
console.log('\n## Preis relativ zum Median derselben Karte, nach Level des verkauften Exemplars\n');
const rel = {};
for (const [k, g] of Object.entries(groups)) {
  const mgr = g.filter(x => x.deal === 'TokenOffer');
  if (mgr.length < 3) continue;
  const m = med(mgr.map(x => x.eur));
  const seg = k.split('|').slice(1).join('/');
  for (const x of mgr) {
    const lv = x.grade >= 4 ? '4+' : String(x.grade);
    (rel[seg + '|' + lv] ??= []).push(Math.log(x.eur / m));
  }
}
console.log('| Segment | Level | Verkaeufe | gegenueber Median der Karte |');
console.log('|---|---|---|---|');
for (const k of Object.keys(rel).sort()) {
  const [seg, lv] = k.split('|'); const v = rel[k];
  console.log(`| ${seg} | ${lv} | ${v.length} | ${v.length >= 15 ? fmt(pct(med(v))) : 'zu wenige'} |`);
}
