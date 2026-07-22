// SORION Preis-Updater — ein Script für alle Scarcities, In-Season UND Classic.
// Aufruf: node update-scarcity.mjs <limited|rare|super_rare>
// Jede DB-Zeile ist (player_slug, scarcity, eligibility); die Queue (ältestes
// updated_at zuerst) mischt beide Eligibilities — 1 API-Call pro Zeile.
import { createClient } from '@supabase/supabase-js';
import { calculateFMV, CLASSIC_PROFILE } from './lib/fmv.mjs';

const SCARCITY = process.argv[2];
if (!['limited', 'rare', 'super_rare'].includes(SCARCITY)) {
  console.error('Usage: node update-scarcity.mjs <limited|rare|super_rare>');
  process.exit(1);
}

const SUPABASE_URL  = process.env.SUPABASE_URL;
const SERVICE_KEY   = process.env.SUPABASE_SERVICE_KEY;
const SORARE_API    = 'https://api.sorare.com/graphql';
// Offizieller Sorare-API-Key (optional): hebt Rate-Limit auf bis zu 600 req/min
const SORARE_APIKEY = process.env.SORARE_APIKEY ?? null;
const sorareHeaders = { 'Content-Type': 'application/json', ...(SORARE_APIKEY ? { 'APIKEY': SORARE_APIKEY } : {}) };

const supabase   = createClient(SUPABASE_URL, SERVICE_KEY);
// Rate-Limit-Schonung: ~21 Sorare-Calls/Min statt Burst. Über Railway-Env-Vars
// justierbar (mit API-Key später: DELAY_MS runter, BATCH_SIZE rauf).
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '90', 10);
const DELAY_MS   = parseInt(process.env.DELAY_MS ?? '2800', 10);

const sleep = ms => new Promise(r => setTimeout(r, ms));

// eligibility: 'in_season' | 'classic'
// - Sales:  tokenPrices(seasonEligibility: IN_SEASON | CLASSIC) — Classic aggregiert alle Jahrgänge
// - Floor:  lowestPriceAnyCard(inSeason: true) bzw. inSeason: false für Classic
// - Achtung: liveSingleSaleOffer.amounts.eurCents kann null sein (reines ETH-Listing) → Fallback
async function fetchData(playerSlug, eligibility) {
  const seasonElig = eligibility === 'classic' ? 'CLASSIC' : 'IN_SEASON';
  const inSeason   = eligibility === 'classic' ? 'false' : 'true';
  const query = `{
    player: anyPlayer(slug: "${playerSlug}") {
      lowestPriceAnyCard(inSeason: ${inSeason}, rarity: ${SCARCITY}) {
        liveSingleSaleOffer { receiverSide { amounts { eurCents } } }
      }
    }
    tokens {
      tokenPrices(rarity: ${SCARCITY} seasonEligibility: ${seasonElig} playerSlug: "${playerSlug}" first: 20) {
        date
        amounts { eurCents }
      }
    }
  }`;
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(SORARE_API, {
        method: 'POST',
        headers: sorareHeaders,
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

// FMV heute vs. price_history vor 1 bzw. 7 Tagen (pro Eligibility getrennt)
async function calcChanges(playerSlug, eligibility, fmv, now, hasEligibility) {
  if (!fmv) return { change_24h: null, change_7d: null };
  const since = new Date(now - 8 * 86400000).toISOString().split('T')[0];
  let q = supabase
    .from('price_history')
    .select('price, recorded_at')
    .eq('player_slug', playerSlug)
    .eq('scarcity', SCARCITY)
    .gte('recorded_at', since)
    .order('recorded_at', { ascending: true });
  if (hasEligibility) q = q.eq('eligibility', eligibility);
  const { data: hist } = await q;
  if (!hist?.length) return { change_24h: null, change_7d: null };

  const priceAt = (daysAgo) => {
    const target = new Date(now - daysAgo * 86400000).toISOString().split('T')[0];
    const older = hist.filter(h => h.recorded_at <= target);
    return older.length ? older[older.length - 1].price : null;
  };
  const p1 = priceAt(1);
  let p7 = priceAt(7);
  // Übergangsphase: flächendeckende Tages-History erst seit 21.07. — bis dahin
  // ältesten verfügbaren Punkt (mind. 1 Tag alt) als 7d-Basis nehmen. Konvergiert
  // von selbst zur echten 7-Tage-Basis, sobald die History gefüllt ist (~29.07.).
  if (p7 == null && hist.length) {
    const yesterday = new Date(now - 1 * 86400000).toISOString().split('T')[0];
    if (hist[0].recorded_at <= yesterday) p7 = hist[0].price;
  }
  return {
    change_24h: p1 > 0 ? parseFloat((((fmv - p1) / p1) * 100).toFixed(2)) : null,
    change_7d:  p7 > 0 ? parseFloat((((fmv - p7) / p7) * 100).toFixed(2)) : null,
  };
}

async function main() {
  console.log(`[${new Date().toISOString()}] Starting ${SCARCITY} update...`);

  // Migrations-Probe: welche Spalten existieren schon?
  const probe = await supabase.from('card_prices').select('change_24h, eligibility').limit(1);
  const migrated = !probe.error;
  if (!migrated) console.warn('Migration fehlt (migrations/2026-07-21_eligibility_and_changes.sql) — laufe im Alt-Modus (nur in_season, keine Prozente)');

  // Accuracy-Tracking verfügbar? (migrations/2026-07-22_accuracy.sql)
  const accProbe = await supabase.from('fmv_accuracy').select('id').limit(1);
  const hasAccuracy = !accProbe.error;
  if (!hasAccuracy) console.warn('fmv_accuracy-Tabelle fehlt — Accuracy-Tracking übersprungen');

  const cols = migrated ? 'id, player_slug, eligibility, fmv, updated_at' : 'id, player_slug';
  const { data: players, error } = await supabase
    .from('card_prices')
    .select(cols)
    .eq('scarcity', SCARCITY)
    .order('updated_at', { ascending: true })
    .limit(BATCH_SIZE);
  if (error) { console.error(error.message); process.exit(1); }
  console.log(`Processing ${players.length} ${SCARCITY} rows...`);

  let updated = 0, failed = 0;
  const today = new Date().toISOString().split('T')[0];

  for (const player of players) {
    const eligibility = player.eligibility ?? 'in_season';
    const result = await fetchData(player.player_slug, eligibility);
    if (!result || !result.sales.length) {
      await supabase.from('card_prices').update({ updated_at: new Date().toISOString() }).eq('id', player.id);
      failed++; await sleep(DELAY_MS); continue;
    }
    const { sales, fetchedFloor } = result;

    // ── Accuracy-Tracking: neue Sales seit dem letzten Lauf gegen den DAMALS
    // geschätzten FMV loggen (kein Leakage — der aktuelle Lauf sieht den Sale,
    // aber verglichen wird mit der Schätzung von vorher). Signiertes Delta.
    if (hasAccuracy && player.fmv > 0 && player.updated_at) {
      const prevAt = new Date(player.updated_at).getTime();
      const newSales = sales.filter(s => s.eur > 0 && new Date(s.date).getTime() > prevAt);
      if (newSales.length) {
        const accRows = newSales.slice(0, 5).map(s => ({
          player_slug: player.player_slug,
          scarcity:    SCARCITY,
          eligibility,
          fmv_est:     player.fmv,
          sale_price:  s.eur,
          delta_pct:   parseFloat((((s.eur - player.fmv) / player.fmv) * 100).toFixed(2)),
          est_at:      player.updated_at,
          sale_at:     s.date,
          hours_gap:   parseFloat(((new Date(s.date).getTime() - prevAt) / 3600000).toFixed(1)),
        }));
        const { error: accErr } = await supabase.from('fmv_accuracy').insert(accRows);
        if (accErr) console.warn(`  Accuracy-Insert failed: ${accErr.message}`);
      }
    }

    const sorted = [...sales].sort((a, b) => a.eur - b.eur);
    const floorPrice = fetchedFloor ?? sorted[0]?.eur ?? null;
    const fmvRaw = calculateFMV(sales, floorPrice, Date.now(), eligibility === 'classic' ? CLASSIC_PROFILE : undefined);
    const fmv = fmvRaw ? parseFloat(fmvRaw.toFixed(2)) : null;

    const now = Date.now();
    const h24ago = new Date(now - 24 * 3600000);
    const h72ago = new Date(now - 72 * 3600000);
    const d7ago  = new Date(now - 7 * 86400000);

    // History zuerst schreiben (Changes vergleichen gegen ältere Tage)
    if (fmv) {
      const histRow = { player_slug: player.player_slug, scarcity: SCARCITY, price: fmv, recorded_at: today };
      if (migrated) histRow.eligibility = eligibility;
      await supabase.from('price_history').upsert(histRow, {
        onConflict: migrated ? 'player_slug,scarcity,recorded_at,eligibility' : 'player_slug,scarcity,recorded_at',
      });
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
    if (migrated) Object.assign(update, await calcChanges(player.player_slug, eligibility, fmv, now, migrated));

    const { error: e } = await supabase.from('card_prices').update(update).eq('id', player.id);
    if (e) { console.warn(`  DB update failed for ${player.player_slug}: ${e.message}`); failed++; }
    else updated++;

    await sleep(DELAY_MS);
  }
  console.log(`[${new Date().toISOString()}] Done. Updated: ${updated}, Failed: ${failed}`);
}

main().catch(console.error);
