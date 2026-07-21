import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API   = 'https://api.sorare.com/graphql';

const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

const SCARCITIES  = ['limited', 'rare', 'super_rare'];
const PAGE_SIZE   = 200;
const DELAY_MS    = 300;
const INSERT_BATCH = 50;

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Fetch one page of in-season cards ─────────────────────────────────────────
async function fetchCardPage(scarcity, cursor) {
  const query = `{
    football {
      allCards(
        inSeasonEligible: true
        rarities: [${scarcity}]
        first: ${PAGE_SIZE}
        ${cursor ? `after: "${cursor}"` : ''}
      ) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            pictureUrl
            rarityTyped
            anyPlayer {
              slug
              displayName
              activeClub {
                shortName
                domesticLeague { displayName }
              }
            }
          }
        }
      }
    }
  }`;

  try {
    const res = await fetch(SORARE_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
    if (!res.ok) return null;
    const data = await res.json();
    if (data.errors) { console.error('GraphQL errors:', data.errors[0]?.message); return null; }
    return data?.data?.football?.allCards ?? null;
  } catch (e) { console.error('Fetch error:', e.message); return null; }
}

// ── Collect all unique players for one scarcity ────────────────────────────────
async function collectPlayers(scarcity) {
  const seen    = new Map(); // slug → player object
  let cursor    = null;
  let page      = 0;
  let totalCards = 0;

  console.log(`\n[${scarcity}] Fetching all in-season cards...`);

  while (true) {
    const result = await fetchCardPage(scarcity, cursor);
    if (!result) { console.error(`  Page ${page + 1}: fetch failed, stopping.`); break; }

    for (const edge of result.edges) {
      const card   = edge.node;
      const player = card.anyPlayer;
      if (!player?.slug) continue;
      totalCards++;

      if (!seen.has(player.slug)) {
        seen.set(player.slug, {
          player_slug:  player.slug,
          player_name:  player.displayName,
          scarcity,
          picture_url:  card.pictureUrl || null,
          team_name:    player.activeClub?.shortName || null,
          league_name:  player.activeClub?.domesticLeague?.displayName || null,
        });
      }
    }

    page++;
    console.log(`  Page ${page}: ${result.edges.length} cards, ${seen.size} unique players so far`);

    if (!result.pageInfo.hasNextPage) break;
    cursor = result.pageInfo.endCursor;
    await sleep(DELAY_MS);
  }

  console.log(`[${scarcity}] Done: ${totalCards} cards → ${seen.size} unique players`);
  return [...seen.values()];
}

// ── Insert missing players into Supabase ──────────────────────────────────────
// Nach der Eligibility-Migration bekommt jeder neue Spieler ZWEI Zeilen
// (in_season + classic); davor wie bisher eine.
async function insertMissing(players, scarcity, migrated) {
  if (!players.length) return { added: 0, skipped: 0 };

  // Fetch existing rows in batches of 200 (Key: slug bzw. slug|eligibility)
  const existing = new Set();
  const allSlugs = players.map(p => p.player_slug);
  for (let i = 0; i < allSlugs.length; i += 200) {
    const batch = allSlugs.slice(i, i + 200);
    const { data } = await supabase
      .from('card_prices')
      .select(migrated ? 'player_slug, eligibility' : 'player_slug')
      .eq('scarcity', scarcity)
      .in('player_slug', batch);
    (data || []).forEach(r => existing.add(migrated ? `${r.player_slug}|${r.eligibility}` : r.player_slug));
  }

  const wanted = migrated
    ? players.flatMap(p => [{ ...p, eligibility: 'in_season' }, { ...p, eligibility: 'classic' }])
    : players;
  const missing = wanted.filter(p => !existing.has(migrated ? `${p.player_slug}|${p.eligibility}` : p.player_slug));
  console.log(`[${scarcity}] ${existing.size} rows already in DB, ${missing.length} to insert`);

  if (!missing.length) return { added: 0, skipped: wanted.length };

  // Insert in batches
  let added = 0;
  for (let i = 0; i < missing.length; i += INSERT_BATCH) {
    const batch = missing.slice(i, i + INSERT_BATCH).map(p => ({
      ...p,
      updated_at: new Date(0).toISOString(), // epoch → prioritized by update script
    }));
    const { error } = await supabase
      .from('card_prices')
      .insert(batch);
    if (error) {
      console.error(`  Insert batch ${Math.floor(i / INSERT_BATCH) + 1} failed:`, error.message);
    } else {
      added += batch.length;
      process.stdout.write(`\r  Inserted ${added}/${missing.length}...`);
    }
    await sleep(100);
  }
  console.log(`\n[${scarcity}] Inserted ${added} new players`);
  return { added, skipped: existingSlugs.size };
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[${new Date().toISOString()}] Starting full player seed...`);
  const probe = await supabase.from('card_prices').select('eligibility').limit(1);
  const migrated = !probe.error;
  console.log(migrated ? 'Eligibility-Modus: in_season + classic Zeilen' : 'Alt-Modus: nur eine Zeile pro Spieler (Migration fehlt)');
  const summary = [];

  for (const scarcity of SCARCITIES) {
    const players = await collectPlayers(scarcity);
    const result  = await insertMissing(players, scarcity, migrated);
    summary.push({ scarcity, total: players.length, ...result });
    await sleep(500);
  }

  console.log('\n── Summary ──────────────────────────────');
  for (const s of summary) {
    console.log(`  ${s.scarcity.padEnd(12)}: ${s.total} players found, ${s.added} inserted, ${s.skipped} already existed`);
  }
  console.log(`[${new Date().toISOString()}] Seed complete.`);
}

main().catch(console.error);
