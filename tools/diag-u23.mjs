// Welche Altersgrenze gilt im U23-Wettbewerb wirklich?
// Sorare hat kein Feld dafuer (eligibleSo5Competitions kennt nur Liga-Wettbewerbe),
// also lesen wir es aus echten Aufstellungen ab: wer dort gespielt hat, war zulaessig.
import { createClient } from '@supabase/supabase-js';
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const h = { 'Content-Type': 'application/json', APIKEY: process.env.SORARE_APIKEY };
const gql = async q => (await fetch('https://api.sorare.com/graphql',
  { method: 'POST', headers: h, body: JSON.stringify({ query: q }) })).json();

const { data: lbs } = await supabase.from('reward_thresholds')
  .select('leaderboard_slug, fixture_name, rarity')
  .eq('competition', 'Under 23').order('start_date', { ascending: false }).limit(3);
console.log('Geprueft:', lbs?.map(l => `${l.fixture_name}/${l.rarity}`).join(', '));

const slugs = new Set();
for (const lb of lbs ?? []) {
  const j = await gql(`{ so5 { so5Leaderboard(slug:"${lb.leaderboard_slug}") {
    so5RankingsPaginated(page: 0, pageSize: 40) { nodes { so5Lineup { so5Appearances {
      anyPlayer { slug } } } } } } } }`);
  for (const n of j?.data?.so5?.so5Leaderboard?.so5RankingsPaginated?.nodes ?? [])
    for (const a of n.so5Lineup?.so5Appearances ?? []) slugs.add(a.anyPlayer.slug);
  await new Promise(r => setTimeout(r, 700));
}
console.log(`${slugs.size} verschiedene Spieler aus echten U23-Aufstellungen`);

const alle = [...slugs];
const alter = new Map();
for (let i = 0; i < alle.length; i += 200) {
  const { data } = await supabase.from('card_prices')
    .select('player_slug, player_name, player_age')
    .in('player_slug', alle.slice(i, i + 200)).not('player_age', 'is', null);
  for (const r of data ?? []) alter.set(r.player_slug, { name: r.player_name, age: r.player_age });
}
const werte = [...alter.values()].map(x => x.age).sort((a, b) => a - b);
console.log(`Alter bekannt fuer ${werte.length} davon`);
const zaehl = new Map();
for (const a of werte) zaehl.set(a, (zaehl.get(a) ?? 0) + 1);
console.log('\nAltersverteilung der tatsaechlich eingesetzten U23-Spieler:');
for (const [a, n] of [...zaehl].sort((x, y) => x[0] - y[0]))
  console.log(`  ${a} Jahre: ${'#'.repeat(Math.min(n, 60))} ${n}`);
const max = Math.max(...werte);
console.log(`\nHoechstes Alter: ${max}`);
for (const [slug, x] of alter) if (x.age === max) { console.log(`  z. B. ${x.name} (${slug})`); break; }
