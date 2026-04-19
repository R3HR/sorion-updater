import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API   = 'https://api.sorare.com/graphql';
const SCARCITY     = 'rare';

const supabase = createClient(SUPABASE_URL, SERVICE_KEY);
const BATCH_SIZE = 200;
const DELAY_MS   = 200;

async function fetchPrices(playerSlug) {
  const query = `{
    tokens {
      tokenPrices(rarity: ${SCARCITY} seasonEligibility: IN_SEASON playerSlug: "${playerSlug}" first: 20) {
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
    return (data?.data?.tokens?.tokenPrices ?? []).map(p => ({ date: p.date, eur: p.amounts.eurCents / 100 }));
  } catch { return null; }
}

function calculateFMV(sales, floorPrice) {
  let values = sales.slice(0, 20).map(s => s.eur).filter(v => v > 0);
  if (values.length >= 4) {
    values.sort((a, b) => a - b);
    values.shift();
    values.pop();
  }
  if (!values.length && !floorPrice) return null;
  const saleWeights = [0.12,0.10,0.09,0.08,0.07,0.06,0.06,0.05,0.05,0.04,0.02,0.02,0.02,0.02,0.02,0.01,0.01,0.01,0.01,0.01];
  const entries = [];
  if (floorPrice > 0) entries.push([floorPrice, 0.15]);
  values.forEach((v, i) => { if (i < saleWeights.length) entries.push([v, saleWeights[i]]); });
  if (!entries.length) return null;
  const totalW = entries.reduce((s, [, w]) => s + w, 0);
  return entries.reduce((s, [v, w]) => s + v * w, 0) / totalW;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  console.log(`[${new Date().toISOString()}] Starting ${SCARCITY} update...`);
  const { data: players, error } = await supabase
    .from('card_prices')
    .select('id, player_slug, floor_price')
    .eq('scarcity', SCARCITY)
    .order('updated_at', { ascending: true })
    .limit(BATCH_SIZE);
  if (error) { console.error(error.message); process.exit(1); }
  console.log(`Processing ${players.length} ${SCARCITY} players...`);
  let updated = 0, failed = 0;
  const today = new Date().toISOString().split('T')[0];
  for (const player of players) {
    const sales = await fetchPrices(player.player_slug);
    if (!sales || !sales.length) {
      await supabase.from('card_prices').update({ updated_at: new Date().toISOString() }).eq('id', player.id);
      failed++; await sleep(DELAY_MS); continue;
    }
    const sorted = [...sales].sort((a, b) => a.eur - b.eur);
    const floorPrice = player.floor_price ?? sorted[0]?.eur ?? null;
    const fmv = calculateFMV(sales, floorPrice);
    const now = new Date();
    const h24ago = new Date(now - 24*60*60*1000);
    const h72ago = new Date(now - 72*60*60*1000);
    const d7ago  = new Date(now -  7*24*60*60*1000);
    const update = {
      floor_price:  floorPrice,
      fmv:          fmv ? parseFloat(fmv.toFixed(2)) : null,
      sale_1:  sales[0]?.eur ?? null, sale_2:  sales[1]?.eur ?? null,
      sale_3:  sales[2]?.eur ?? null, sale_4:  sales[3]?.eur ?? null,
      sale_5:  sales[4]?.eur ?? null,
      avg_sales:    sales.length ? parseFloat((sales.slice(0,10).reduce((s,p)=>s+p.eur,0)/Math.min(sales.length,10)).toFixed(2)) : null,
      sales_count:  sales.filter(s => new Date(s.date) >= h24ago).length,
      sales_72h:    sales.filter(s => new Date(s.date) >= h72ago).length,
      sales_7d:     sales.filter(s => new Date(s.date) >= d7ago).length,
      updated_at:   new Date().toISOString(),
    };
    const { error: e } = await supabase.from('card_prices').update(update).eq('id', player.id);
    if (e) { failed++; } else {
      updated++;
      if (fmv) await supabase.from('price_history').upsert({ player_slug: player.player_slug, scarcity: SCARCITY, price: parseFloat(fmv.toFixed(2)), recorded_at: today }, { onConflict: 'player_slug,scarcity,recorded_at' });
    }
    await sleep(DELAY_MS);
  }
  console.log(`[${new Date().toISOString()}] Done. Updated: ${updated}, Failed: ${failed}`);
}

main().catch(console.error);
