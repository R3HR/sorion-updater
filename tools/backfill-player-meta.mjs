// Einmal-Backfill: gameplay_tier, player_age, player_nation fuer alle Spieler.
//
// Anlass (25.08.2026, Jonas): Die neuen Spalten sind angelegt, aber leer —
// der Updater fuellt sie erst ueber den Tageszyklus. Dieses Skript laedt sie
// SOFORT, auf dem effizienten Weg des Kader-Abgleichs: ~250 Club-Abfragen
// (activePlayers liefert age/country/gameplayTier je Spieler) statt ~25.000
// Einzelabfragen. Geschrieben wird gruppiert (ein Update je Wert + 200er-
// Slug-Block), nicht zeilenweise.
//
// Danach uebernehmen Updater (jede Beruehrung) und taeglicher Kader-Abgleich
// die Pflege — ein eigener Cron ist NICHT noetig (Dauerlast, INC-006-Lektion).
// Spieler ohne aktiven Club (Classic-Altbestand) fuellt der Updater nach.
//
// Aufruf:  railway run node tools/backfill-player-meta.mjs [--dry]
import { createClient } from '@supabase/supabase-js';

const DRY = process.argv.includes('--dry');
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SERVICE_KEY) { console.error('SUPABASE_URL / SUPABASE_SERVICE_KEY fehlen'); process.exit(1); }
const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

const SORARE_API = 'https://api.sorare.com/graphql';
const APIKEY = process.env.SORARE_APIKEY ?? null;
const headers = { 'Content-Type': 'application/json', ...(APIKEY ? { APIKEY } : {}) };
const DELAY_MS = APIKEY ? 400 : 1200;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gql(query, label) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(SORARE_API, { method: 'POST', headers, body: JSON.stringify({ query }) });
      if (res.status === 429) { const w = 30000 * (attempt + 1); console.warn(`  429 (${label}) — warte ${w / 1000}s`); await sleep(w); continue; }
      if (!res.ok) { console.warn(`  HTTP ${res.status} (${label})`); return null; }
      const json = await res.json();
      if (json.errors) { console.warn(`  GraphQL (${label}): ${json.errors[0]?.message}`); return null; }
      return json.data;
    } catch (e) { console.warn(`  Fetch (${label}): ${e.message}`); await sleep(2000 * (attempt + 1)); }
  }
  return null;
}

async function fetchRosterMeta(clubSlug) {
  const players = [];
  let cursor = null;
  for (let page = 0; page < 6; page++) {
    const after = cursor ? `, after: "${cursor}"` : '';
    const data = await gql(`{
      football { club(slug: "${clubSlug}") {
        activePlayers(first: 50${after}) {
          pageInfo { hasNextPage endCursor }
          nodes { slug age country { code } gameplayTier }
        }
      } }
    }`, clubSlug);
    const club = data?.football?.club;
    if (!club) return players;
    for (const n of club.activePlayers.nodes) {
      players.push({ slug: n.slug,
                     age: Number.isFinite(n.age) ? n.age : null,
                     nation: n.country?.code ?? null,
                     tier: n.gameplayTier ?? null });
    }
    if (!club.activePlayers.pageInfo.hasNextPage) break;
    cursor = club.activePlayers.pageInfo.endCursor;
    await sleep(DELAY_MS);
  }
  return players;
}

// Ein Feld schreiben: Slugs nach Wert gruppieren, je Gruppe in 200er-Bloecken
async function writeField(column, valueOf, all) {
  const groups = new Map();
  for (const p of all.values()) {
    const v = valueOf(p);
    if (v == null) continue;
    if (!groups.has(v)) groups.set(v, []);
    groups.get(v).push(p.slug);
  }
  let written = 0, calls = 0;
  for (const [value, slugs] of groups) {
    for (let i = 0; i < slugs.length; i += 200) {
      const chunk = slugs.slice(i, i + 200);
      if (!DRY) {
        // Aenderungs-Waechter: nur Zeilen schreiben, deren Wert fehlt oder
        // abweicht — ein erneuter Lauf auf gepflegtem Bestand kostet damit
        // praktisch nichts (unveraenderte Zeilen erzeugen sonst trotzdem
        // neue Zeilenversionen + WAL, Jonas' Einwand 25.08.).
        const { error, count } = await supabase.from('card_prices')
          .update({ [column]: value }, { count: 'exact' })
          .in('player_slug', chunk)
          .or(`${column}.is.null,${column}.neq.${value}`);
        if (error) { console.warn(`  ${column}=${value} (${i}): ${error.message}`); continue; }
        written += count ?? 0;
      }
      calls++;
      await sleep(120);
    }
  }
  console.log(`${column}: ${groups.size} Werte, ${calls} Updates, ${written} Zeilen${DRY ? ' (DRY)' : ''}`);
}

async function main() {
  console.log(`[${new Date().toISOString()}] Meta-Backfill startet${DRY ? ' (DRY-RUN)' : ''}${APIKEY ? ' (mit API-Key)' : ''}`);

  const clubsData = await gql('{ football { clubsReady { slug name } } }', 'clubsReady');
  const clubs = clubsData?.football?.clubsReady ?? [];
  if (!clubs.length) { console.error('Keine Clubs — Abbruch'); process.exit(1); }
  console.log(`${clubs.length} Clubs`);

  const all = new Map();   // slug -> {age, nation, tier} (letzter Club gewinnt)
  let done = 0;
  for (const club of clubs) {
    const roster = await fetchRosterMeta(club.slug);
    for (const p of roster) all.set(p.slug, p);
    if (++done % 25 === 0) console.log(`  ${done}/${clubs.length} Clubs — ${all.size} Spieler`);
    await sleep(DELAY_MS);
  }
  const stat = f => [...all.values()].filter(p => p[f] != null).length;
  console.log(`Fertig gesammelt: ${all.size} Spieler (tier: ${stat('tier')}, age: ${stat('age')}, nation: ${stat('nation')})`);

  await writeField('gameplay_tier', p => p.tier, all);
  await writeField('player_age',    p => p.age, all);
  await writeField('player_nation', p => p.nation, all);

  console.log(`[${new Date().toISOString()}] Backfill fertig.`);
  console.log('Hinweis: Nation-Facette erscheint nach dem naechsten refresh_market_aggregates (09:20 UTC oder manuell). Sterne/Alters-Slider/Tier-Dropdown ziehen sofort.');
}

main().catch(e => { console.error(e); process.exit(1); });
