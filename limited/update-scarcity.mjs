// SORION Preis-Updater — ein Script für alle Scarcities.
// Aufruf: node update-scarcity.mjs <limited|rare|super_rare>
import { createClient } from '@supabase/supabase-js';
import { calculateFMV } from './lib/fmv.mjs';

const SCARCITY = process.argv[2];
if (!['limited', 'rare', 'super_rare'].includes(SCARCITY)) {
  console.error('Usage: node update-scarcity.mjs <limited|rare|super_rare>');
  process.exit(1);
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API   = 'https://api.sorare.com/graphql';

const supabase   = createClient(SUPABASE_URL, SERVICE_KEY);
const BATCH_SIZE = 200;
const DELAY_MS   = 200;

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchData(playerSlug) {
  const query = `{
    player: anyPlayer(slug: "${playerSlug}") {
      lowestPriceAnyCard(inSeason: true, rarity: ${SCARCITY}) {
        liveSingleSaleOffer { receiverSide { amounts { eurCents } } }
      }
    }
    tokens {
      tokenPrices(rarity: ${SCARCITY} seasonEligibility: IN_SEASON playerSlug: "${playerSlug}" first: 20) {
        date
        amounts { eurCents }
      }
    }
  }`;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(SORARE_API, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query }),
      });
      if (res.status === 429) {
        const wait = 30000 * (attempt + 1);
        console.warn(`  429 rate limit (${playerSlug}), waiting ${wait / 1000}s...`);
        await sleep(wait);
        continue;
      }
      if (!res.ok) { console.warn(`  HTTP ${res.status} for ${playerSlug}`); return null; }
      const data = await res.json();
      if (data.errors) { console.warn(`  GraphQL error for ${playerSlug}: ${data.errors[0]?.message}`); return null; }
      const sales = (data?.data?.tokens?.tokenPrices ?? []).map(p => ({ date: p.date, eur: p.amounts.eurCents / 100 }));
      const floorCents = data?.data?.player?.lowestPriceAnyCard?.liveSingleSaleOffer?.receiverSide?.amounts?.eurCents;
      return { sales, fetchedFloor: floorCents ? floorCents / 100 : null };
    } catch (e) { console.warn(`  Fetch failed for ${playerSlug}: ${e.message}`); return null; }
  }
  return null;
}

// FMV heute vs. price_history vor 1 bzw. 7 Tagen
async function calcChanges(playerSlug, fmv, now) {
  if (!fmv) return { change_24h: null, change_7d: null };
  const since = new Date(now - 8 * 86400000).toISOString().split('T')[0];
  const { data: hist } = await supabase
    .from('price_history')
    .select('price, recorded_at')
    .eq('player_slug', playerSlug)
    .eq('scarcity', SCARCITY)
    .gte('recorded_at', since)
    .order('recorded_at', { ascending: true });
  if (!hist?.length) return { change_24h: null, change_7d: null };

  const priceAt = (daysAgo) => {
    const target = new Date(now - daysAgo * 86400000).toISOString().split('T')[0];
    const older = hist.filter(h => h.recorded_at <= target);
    return older.length ? older[older.length - 1].price : null;
  };
  const p1 = priceAt(1), p7 = priceAt(7);
  return {
    change_24h: p1 > 0 ? parseFloat((((fmv - p1) / p1) * 100).toFixed(2)) : null,
    change_7d:  p7 > 0 ? parseFloat((((fmv - p7) / p7) * 100).toFixed(2)) : null,
  };
}

async function main() {
  console.log(`[${new Date().toISOString()}] Starting ${SCARCITY} update...`);

  // Migration-Probe: existieren die change-Spalten schon?
  const probe = await supabase.from('card_prices').select('change_24h').limit(1);
  const hasChangeCols = !probe.error;
  if (!hasChangeCols) console.warn('change_24h/change_7d columns missing — run migrations/2026-07-06_add_change_columns.sql');

  const { data: players, error } = await supabase
    .from('card_prices')
    .select('id, player_slug')
    .eq('scarcity', SCARCITY)
    .order('updated_at', { ascending: true })
    .limit(BATCH_SIZE);
  if (error) { console.error(error.message); process.exit(1); }
  console.log(`Processing ${players.length} ${SCARCITY} players...`);

  let updated = 0, failed = 0;
  const today = new Date().toISOString().split('T')[0];

  for (const player of players) {
    const result = await fetchData(player.player_slug);
    if (!result || !result.sales.length) {
      await supabase.from('card_prices').update({ updated_at: new Date().toISOString() }).eq('id', player.id);
      failed++; await sleep(DELAY_MS); continue;
    }
    const { sales, fetchedFloor } = result;
    const sorted = [...sales].sort((a, b) => a.eur - b.eur);
    const floorPrice = fetchedFloor ?? sorted[0]?.eur ?? null;
    const fmvRaw = calculateFMV(sales, floorPrice);
    const fmv = fmvRaw ? parseFloat(fmvRaw.toFixed(2)) : null;

    const now = Date.now();
    const h24ago = new Date(now - 24 * 3600000);
    const h72ago = new Date(now - 72 * 3600000);
    const d7ago  = new Date(now - 7 * 86400000);

    // Erst History für heute schreiben (Changes vergleichen gegen ältere Tage)
    if (fmv) {
      await supabase.from('price_history').upsert(
        { player_slug: player.player_slug, scarcity: SCARCITY, price: fmv, recorded_at: today },
        { onConflict: 'player_slug,scarcity,recorded_at' }
      );
    }

    const update = {
      floor_price:  floorPrice,
      fmv,
      sale_1: sales[0]?.eur ?? null, sale_2: sales[1]?.eur ?? null,
      sale_3: sales[2]?.eur ?? null, sale_4: sales[3]?.eur ?? null,
      sale_5: sales[4]?.eur ?? null,
      avg_sales:   sales.length ? parseFloat((sales.slice(0, 10).reduce((s, p) => s + p.eur, 0) / Math.min(sales.length, 10)).toFixed(2)) : null,
      sales_count: sales.filter(s => new Date(s.date) >= h24ago).length,
      sales_72h:   sales.filter(s => new Date(s.date) >= h72ago).length,
      sales_7d:    sales.filter(s => new Date(s.date) >= d7ago).length,
      updated_at:  new Date().toISOString(),
    };
    if (hasChangeCols) Object.assign(update, await calcChanges(player.player_slug, fmv, now));

    const { error: e } = await supabase.from('card_prices').update(update).eq('id', player.id);
    if (e) { console.warn(`  DB update failed for ${player.player_slug}: ${e.message}`); failed++; }
    else updated++;

    await sleep(DELAY_MS);
  }
  console.log(`[${new Date().toISOString()}] Done. Updated: ${updated}, Failed: ${failed}`);
}

main().catch(console.error);
