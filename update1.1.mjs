import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API   = 'https://api.sorare.com/graphql';

const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

const BATCH_SIZE = 200;
const DELAY_MS   = 500;

async function fetchPrices(playerSlug, scarcity) {
  const query = `{
    tokens {
      tokenPrices(
        rarity: ${scarcity}
        seasonEligibility: IN_SEASON
        playerSlug: "${playerSlug}"
        first: 20
      ) {
        date
        amounts { eurCents }
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
    if (data.errors) return null;
    const prices = data?.data?.tokens?.tokenPrices ?? [];
    return prices.map(p => ({ date: p.date, eur: p.amounts.eurCents / 100 }));
  } catch { return null; }
}

function calculateFMV(sales, floorPrice) {
  const s = sales.slice(0, 5);
  const weights = [
    [floorPrice,        0.30],
    [s[0]?.eur ?? null, 0.25],
    [s[1]?.eur ?? null, 0.20],
    [s[2]?.eur ?? null, 0.13],
    [s[3]?.eur ?? null, 0.08],
    [s[4]?.eur ?? null, 0.04],
  ].filter(([v]) => v !== null && v > 0);

  if (!weights.length) return null;
  const totalWeight = weights.reduce((s, [, w]) => s + w, 0);
  return weights.reduce((s, [v, w]) => s + v * w, 0) / totalWeight;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  console.log(`[${new Date().toISOString()}] Starting price update...`);

  const { data: players, error } = await supabase
    .from('card_prices')
    .select('id, player_slug, scarcity, floor_price')
    .order('updated_at', { ascending: true })
    .limit(BATCH_SIZE);

  if (error) { console.error('Failed to fetch players:', error.message); process.exit(1); }

  console.log(`Processing ${players.length} players...`);

  let updated = 0, failed = 0;
  const today = new Date().toISOString().split('T')[0];

  for (const player of players) {
    const sales = await fetchPrices(player.player_slug, player.scarcity);

    if (!sales || sales.length === 0) {
      await supabase.from('card_prices')
        .update({ updated_at: new Date().toISOString() })
        .eq('id', player.id);
      failed++;
      await sleep(DELAY_MS);
      continue;
    }

    const sortedSales = [...sales].sort((a, b) => a.eur - b.eur);
    const floorPrice  = player.floor_price ?? sortedSales[0]?.eur ?? null;
    const fmv         = calculateFMV(sales, floorPrice);

    // Update card_prices
    const now = new Date();
    const h24ago = new Date(now - 24 * 60 * 60 * 1000);
    const h72ago = new Date(now - 72 * 60 * 60 * 1000);
    const d7ago  = new Date(now - 7  * 24 * 60 * 60 * 1000);

    const sales24h = sales.filter(s => new Date(s.date) >= h24ago).length;
    const sales72h = sales.filter(s => new Date(s.date) >= h72ago).length;
    const sales7d  = sales.filter(s => new Date(s.date) >= d7ago).length;

    const update = {
      floor_price:  floorPrice,
      fmv:          fmv ? parseFloat(fmv.toFixed(2)) : null,
      sale_1:       sales[0]?.eur ?? null,
      sale_2:       sales[1]?.eur ?? null,
      sale_3:       sales[2]?.eur ?? null,
      sale_4:       sales[3]?.eur ?? null,
      sale_5:       sales[4]?.eur ?? null,
      avg_sales:    sales.length ? parseFloat((sales.slice(0,10).reduce((s,p) => s+p.eur,0)/Math.min(sales.length,10)).toFixed(2)) : null,
      sales_count:  sales24h,   // sales in last 24h = real liquidity
      sales_72h:    sales72h,
      sales_7d:     sales7d,
      updated_at:   new Date().toISOString(),
    };

    const { error: updateError } = await supabase
      .from('card_prices').update(update).eq('id', player.id);

    if (updateError) { failed++; }
    else {
      updated++;

      // Record daily price in price_history (upsert — one entry per day)
      if (fmv) {
        await supabase.from('price_history').upsert({
          player_slug:  player.player_slug,
          scarcity:     player.scarcity,
          price:        parseFloat(fmv.toFixed(2)),
          recorded_at:  today,
        }, { onConflict: 'player_slug,scarcity,recorded_at' });
      }
    }

    await sleep(DELAY_MS);
  }

  console.log(`[${new Date().toISOString()}] Done. Updated: ${updated}, Failed: ${failed}`);
}

main().catch(console.error);
