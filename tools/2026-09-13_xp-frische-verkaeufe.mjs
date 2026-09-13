// XP-Effekt, sauber: nur Verkaeufe der letzten 24 Stunden. TokenPrice.card zeigt die
// Karte im HEUTIGEN Zustand; bei alten Verkaeufen hat der Kaeufer seitdem XP gesammelt.
// Bei frischen Verkaeufen ist der heutige Stand fast genau das, was der Kaeufer bekam.
import { readFileSync, writeFileSync } from 'node:fs';
const KEY = process.env.SORARE_APIKEY;
const list = JSON.parse(readFileSync(process.env.LIST, 'utf8'));
const sleep = ms => new Promise(r => setTimeout(r, ms));
const MAX_AGE_H = 24;

// Probe zuerst (Lehre 07.09.)
const probeQ = `{ tokens { tokenPrices(rarity: limited, seasonEligibility: IN_SEASON, playerSlug: "gregor-kobel", first: 3) {
  date card { grade xp specialEdition serialNumber } } } }`;
const probe = await fetch('https://api.sorare.com/graphql', { method: 'POST',
  headers: { 'Content-Type': 'application/json', APIKEY: KEY }, body: JSON.stringify({ query: probeQ }) }).then(r => r.json());
if (probe.errors) { console.log('Probe fehlgeschlagen:', probe.errors[0].message); process.exit(1); }

const out = []; let done = 0, failed = 0, firstErr = null;
for (const p of list) {
  const elig = p.eligibility === 'classic' ? 'CLASSIC' : 'IN_SEASON';
  const q = `{ tokens { tokenPrices(rarity: ${p.scarcity}, seasonEligibility: ${elig}, playerSlug: "${p.player_slug}", first: 20) {
    date amounts { eurCents } deal { __typename }
    card { serialNumber grade xp specialEdition } } } }`;
  try {
    const d = await fetch('https://api.sorare.com/graphql', { method: 'POST',
      headers: { 'Content-Type': 'application/json', APIKEY: KEY }, body: JSON.stringify({ query: q }) }).then(r => r.json());
    if (d.errors) { failed++; firstErr ??= d.errors[0].message; }
    const now = Date.now();
    for (const s of d?.data?.tokens?.tokenPrices ?? []) {
      const ageH = (now - new Date(s.date).getTime()) / 3600000;
      if (ageH > MAX_AGE_H || !s.card || !(s.amounts.eurCents > 0)) continue;
      out.push({ player: p.player_slug, scarcity: p.scarcity, elig: p.eligibility, ageH,
        eur: s.amounts.eurCents / 100, deal: s.deal?.__typename,
        grade: s.card.grade, xp: s.card.xp, edition: !!s.card.specialEdition, serial: s.card.serialNumber });
    }
  } catch (e) { failed++; firstErr ??= e.message; }
  if (++done % 200 === 0) console.log(`  ${done}/${list.length}`);
  await sleep(320);
}
writeFileSync(process.env.OUT, JSON.stringify(out));
console.log(`${out.length} frische Verkaeufe (<= ${MAX_AGE_H} h) gespeichert, ${failed} Fehlschlaege${firstErr ? ', erster: ' + firstErr : ''}`);
const byGrade = {};
for (const s of out) byGrade[s.grade] = (byGrade[s.grade] ?? 0) + 1;
console.log('Verteilung nach Level:', JSON.stringify(byGrade));
