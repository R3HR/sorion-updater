// SORION — Kader-Abgleich: zieht die Spieler ALLER Sorare-Clubs und legt fehlende an.
//
// Hintergrund (30.07.): Der neue Club-Filter machte Lücken sichtbar — für Borussia
// Dortmund zeigte Sorion 18 statt 39 Spieler. Bisherige Quellen (Seed, Harvester,
// Portfolio-Ansichten) finden nur Spieler, die gehandelt/gelistet werden oder in einer
// Sammlung liegen. Spieler ohne Marktaktivität fehlten dadurch dauerhaft — und ihre
// Slugs sind nicht ableitbar (z. B. "felix-kalu-nmecha", "daniel-svensson-2002-02-12").
//
// Lösung: Rückwärts über die Vereine. football.clubsReady liefert alle Clubs (247),
// club.activePlayers den kompletten Kader je Club. Fehlende Kombinationen
// (player_slug × scarcity × eligibility) werden mit updated_at = epoch angelegt,
// damit der Preis-Updater sie sofort priorisiert.
//
// Aufruf: node sync-club-rosters.mjs [--dry]
// Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, optional SORARE_APIKEY, DELAY_MS

import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL  = process.env.SUPABASE_URL;
const SERVICE_KEY   = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API    = 'https://api.sorare.com/graphql';
const SORARE_APIKEY = process.env.SORARE_APIKEY ?? null;
const DELAY_MS      = parseInt(process.env.DELAY_MS ?? '1500', 10);
const DRY_RUN       = process.argv.includes('--dry');

if (!SUPABASE_URL || !SERVICE_KEY) {
  console.error('SUPABASE_URL / SUPABASE_SERVICE_KEY fehlen');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_KEY);
const headers  = { 'Content-Type': 'application/json', ...(SORARE_APIKEY ? { APIKEY: SORARE_APIKEY } : {}) };
const sleep    = ms => new Promise(r => setTimeout(r, ms));

const SCARCITIES   = ['limited', 'rare', 'super_rare'];
const ELIGIBILITIES = ['in_season', 'classic'];

async function gql(query, label) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(SORARE_API, { method: 'POST', headers, body: JSON.stringify({ query }) });
      if (res.status === 429) {
        const wait = 30000 * (attempt + 1);
        console.warn(`  429 rate limit (${label}) — warte ${wait / 1000}s`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) { console.warn(`  HTTP ${res.status} (${label})`); return null; }
      const json = await res.json();
      if (json.errors) { console.warn(`  GraphQL (${label}): ${json.errors[0]?.message}`); return null; }
      return json.data;
    } catch (e) { console.warn(`  Fetch (${label}): ${e.message}`); }
    await sleep(2000 * (attempt + 1));
  }
  return null;
}

// Kader eines Clubs (paginiert; Trainer werden übersprungen)
async function fetchRoster(clubSlug) {
  const players = [];
  let cursor = null;
  for (let page = 0; page < 6; page++) {   // 6 × 50 = 300 reicht für jeden Kader
    const after = cursor ? `, after: "${cursor}"` : '';
    const data = await gql(`{
      football {
        club(slug: "${clubSlug}") {
          name
          country { code }
          domesticLeague { name country { code } }
          activePlayers(first: 50${after}) {
            pageInfo { hasNextPage endCursor }
            nodes { slug displayName anyPositions pictureUrl }
          }
        }
      }
    }`, clubSlug);
    const club = data?.football?.club;
    if (!club) return null;
    for (const n of club.activePlayers.nodes) {
      const pos = n.anyPositions?.[0] ?? null;
      if (pos === 'Coach') continue;                       // Trainer sind keine Karten
      players.push({
        slug: n.slug,
        name: n.displayName,
        position: pos,
        picture: n.pictureUrl ?? null,
        team: club.name,
        league: club.domesticLeague?.name ?? null,
        // Liga-Land, Fallback Club-Land: Clubs ohne Liga-Zuordnung (Sorare liefert
        // z. B. fuer Leicester domesticLeague=null) bleiben so unterm Land filterbar
        country: club.domesticLeague?.country?.code ?? club.country?.code ?? null,
      });
    }
    if (!club.activePlayers.pageInfo.hasNextPage) break;
    cursor = club.activePlayers.pageInfo.endCursor;
    await sleep(DELAY_MS);
  }
  return players;
}

// Vorhandene Kombinationen für eine Slug-Liste holen (in Blöcken, PostgREST-URL-Limit)
async function existingKeys(slugs) {
  const keys = new Set();
  for (let i = 0; i < slugs.length; i += 200) {
    const batch = slugs.slice(i, i + 200);
    const { data, error } = await supabase
      .from('card_prices')
      .select('player_slug, scarcity, eligibility')
      .in('player_slug', batch);
    if (error) { console.warn(`  DB-Abfrage: ${error.message}`); continue; }
    for (const r of data) keys.add(`${r.player_slug}|${r.scarcity}|${r.eligibility ?? 'in_season'}`);
  }
  return keys;
}

async function main() {
  console.log(`[${new Date().toISOString()}] Kader-Abgleich startet${DRY_RUN ? ' (DRY-RUN, keine Schreibvorgänge)' : ''}`);

  const clubsData = await gql(`{ football { clubsReady { slug name } } }`, 'clubsReady');
  const clubs = clubsData?.football?.clubsReady ?? [];
  if (!clubs.length) { console.error('Keine Clubs erhalten — Abbruch'); return; }
  console.log(`${clubs.length} Clubs gefunden`);

  const all = new Map();     // slug -> Spielerdaten (letzter Club gewinnt)
  let done = 0;
  for (const club of clubs) {
    const roster = await fetchRoster(club.slug);
    if (roster) for (const p of roster) all.set(p.slug, p);
    done++;
    if (done % 25 === 0) console.log(`  ${done}/${clubs.length} Clubs — ${all.size} Spieler gesammelt`);
    await sleep(DELAY_MS);
  }
  console.log(`Kader komplett: ${all.size} aktive Spieler`);

  const slugs = [...all.keys()];
  const have = await existingKeys(slugs);

  const rows = [];
  for (const [slug, p] of all) {
    for (const sc of SCARCITIES) {
      for (const el of ELIGIBILITIES) {
        if (have.has(`${slug}|${sc}|${el}`)) continue;
        rows.push({
          player_slug:  slug,
          player_name:  p.name,
          scarcity:     sc,
          eligibility:  el,
          team_name:    p.team,
          league_name:  p.league,
          league_country: p.country,
          position:     p.position,
          ...(p.picture ? { picture_url: p.picture } : {}),
          updated_at:   new Date(0).toISOString(),   // epoch → Updater holt sie als Nächstes
        });
      }
    }
  }

  const newPlayers = new Set(rows.map(r => r.player_slug)).size;
  console.log(`Fehlend: ${rows.length} Zeilen (${newPlayers} Spieler ganz oder teilweise neu)`);

  if (DRY_RUN) {
    console.log('DRY-RUN — Beispiele:');
    for (const r of rows.slice(0, 10)) console.log(`  ${r.player_slug} | ${r.scarcity} | ${r.eligibility} | ${r.team_name}`);
    return;
  }
  if (!rows.length) { console.log('Nichts einzufügen — Datenbank ist vollständig.'); return; }

  let inserted = 0;
  for (let i = 0; i < rows.length; i += 100) {
    const chunk = rows.slice(i, i + 100);
    const { error } = await supabase
      .from('card_prices')
      .upsert(chunk, { onConflict: 'player_slug,scarcity,eligibility', ignoreDuplicates: true });
    if (error) console.warn(`  Insert fehlgeschlagen (${i}): ${error.message}`);
    else inserted += chunk.length;
    await sleep(100);
  }
  console.log(`[${new Date().toISOString()}] Fertig. ${inserted} Zeilen angelegt.`);
}

main().catch(e => { console.error('Fataler Fehler:', e.message); process.exitCode = 1; });
