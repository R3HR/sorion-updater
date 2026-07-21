// SORION Market-Harvester — sammelt Spieler-Slugs aus dem globalen Listings-Feed.
// Ergänzt seed-all-players (nur in-season-fähige Spieler) um ALLE Spieler, deren
// Karten aktuell zum Verkauf stehen — auch aus Crafts rotierte / Classic-only.
// Spieler ohne Listings & Sales brauchen wir nicht: kein Markt = kein Preis.
//
// Anonyme API: max. Query-Depth 7 → wir holen nur Karten-Slugs und parsen den
// Spieler-Slug daraus (<player>-<jahr>-<rarity>-<serial>).
//
// Env: HARVEST_HOURS (optional) — nur Listings der letzten N Stunden (inkrementell).
//      Ohne: kompletter 8-Tage-Feed (Erstlauf).
import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API   = 'https://api.sorare.com/graphql';
const HOURS        = process.env.HARVEST_HOURS ? parseInt(process.env.HARVEST_HOURS, 10) : null;

const supabase  = createClient(SUPABASE_URL, SERVICE_KEY);
const PAGE_SIZE = 50;
const MAX_PAGES = 600;
const DELAY_MS  = parseInt(process.env.DELAY_MS ?? '1500', 10); // ~40 Calls/Min, rate-limit-schonend

const CARD_RE = /^(.+)-(\d{4})-(limited|rare|super[-_]rare|unique)-(\d+)$/;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gql(query) {
  for (let attempt = 0; attempt < 3; attempt++) {
    const res = await fetch(SORARE_API, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query }),
    });
    if (res.status === 429) { await sleep(30000 * (attempt + 1)); continue; }
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    if (data.errors) throw new Error(data.errors[0]?.message);
    return data.data;
  }
  throw new Error('rate limited');
}

// ── 1) Listings-Feed durchgehen, (slug, scarcity) sammeln ─────────────────────
async function sweepListings() {
  const updatedAfter = HOURS ? `updatedAfter: "${new Date(Date.now() - HOURS * 3600000).toISOString()}"` : '';
  const found = new Set(); // "slug|scarcity"
  let cursor = null, pages = 0;

  while (pages < MAX_PAGES) {
    const q = `{
      tokens {
        liveSingleSaleOffers(first: ${PAGE_SIZE}, sport: FOOTBALL ${updatedAfter} ${cursor ? `after: "${cursor}"` : ''}) {
          pageInfo { hasNextPage endCursor }
          edges { node { senderSide { anyCards { slug } } } }
        }
      }
    }`;
    let conn;
    try { conn = (await gql(q)).tokens.liveSingleSaleOffers; }
    catch (e) { console.warn(`Page ${pages + 1} failed: ${e.message}`); break; }

    for (const { node } of conn.edges) {
      for (const c of node.senderSide.anyCards) {
        const m = c.slug.match(CARD_RE);
        if (!m) continue;
        const scarcity = m[3].replace('-', '_'); // super-rare → super_rare
        if (scarcity === 'unique') continue;     // tracken wir nicht
        found.add(`${m[1]}|${scarcity}`);
      }
    }
    pages++;
    if (pages % 25 === 0) console.log(`  Page ${pages}: ${found.size} unique (player, scarcity)`);
    if (!conn.pageInfo.hasNextPage) break;
    cursor = conn.pageInfo.endCursor;
    await sleep(DELAY_MS);
  }
  console.log(`Sweep done: ${pages} pages, ${found.size} unique (player, scarcity)`);
  return found;
}

// ── 2) Namen für neue Spieler nachladen (Batch, Depth 4) ──────────────────────
async function fetchNames(slugs) {
  const names = new Map();
  for (let i = 0; i < slugs.length; i += 100) {
    const batch = slugs.slice(i, i + 100);
    try {
      const data = await gql(`{
        players(slugs: ${JSON.stringify(batch)}) {
          slug
          displayName
          activeClub { shortName }
        }
      }`);
      for (const p of data.players ?? []) {
        names.set(p.slug, { name: p.displayName, team: p.activeClub?.shortName ?? null });
      }
    } catch (e) { console.warn(`Name-Batch failed: ${e.message}`); }
    await sleep(DELAY_MS);
  }
  return names;
}

// ── 3) Fehlende Zeilen einfügen ───────────────────────────────────────────────
async function main() {
  console.log(`[${new Date().toISOString()}] Market-Harvest (${HOURS ? `letzte ${HOURS}h` : 'voller 8-Tage-Feed'})...`);

  const probe = await supabase.from('card_prices').select('eligibility').limit(1);
  const migrated = !probe.error;

  const found = await sweepListings();
  if (!found.size) { console.log('Nichts gefunden.'); return; }

  // Existierende Zeilen laden (nur Slugs aus dem Sweep)
  const bySc = {};
  for (const key of found) {
    const [slug, sc] = key.split('|');
    (bySc[sc] ??= new Set()).add(slug);
  }

  const missing = []; // { player_slug, scarcity }
  for (const [scarcity, slugSet] of Object.entries(bySc)) {
    const slugs = [...slugSet];
    const existing = new Set();
    for (let i = 0; i < slugs.length; i += 200) {
      const { data } = await supabase
        .from('card_prices')
        .select('player_slug')
        .eq('scarcity', scarcity)
        .in('player_slug', slugs.slice(i, i + 200));
      (data || []).forEach(r => existing.add(r.player_slug));
    }
    for (const s of slugs) if (!existing.has(s)) missing.push({ player_slug: s, scarcity });
    console.log(`[${scarcity}] ${slugs.length} im Feed, ${existing.size} bekannt, ${slugs.length - existing.size} neu`);
  }

  if (!missing.length) { console.log('Keine neuen Spieler.'); return; }

  const names = await fetchNames([...new Set(missing.map(m => m.player_slug))]);

  const rows = missing.flatMap(m => {
    const info = names.get(m.player_slug);
    const base = {
      player_slug: m.player_slug,
      player_name: info?.name ?? m.player_slug.replace(/-/g, ' ').replace(/\b\w/g, c => c.toUpperCase()),
      team_name:   info?.team ?? null,
      scarcity:    m.scarcity,
      updated_at:  new Date(0).toISOString(), // epoch → Updater priorisiert
    };
    return migrated
      ? [{ ...base, eligibility: 'in_season' }, { ...base, eligibility: 'classic' }]
      : [base];
  });

  let added = 0;
  for (let i = 0; i < rows.length; i += 50) {
    const { error } = await supabase.from('card_prices').insert(rows.slice(i, i + 50));
    if (error) console.warn(`Insert failed: ${error.message}`);
    else added += Math.min(50, rows.length - i);
    await sleep(100);
  }
  console.log(`[${new Date().toISOString()}] Done. ${added} Zeilen eingefügt (${missing.length} neue Spieler).`);
}

main().catch(console.error);
