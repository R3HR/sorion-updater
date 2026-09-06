// Reward-Schwellen je Leaderboard und Spieltag (04.09.2026, Wunsch Jonas):
// "Wie viele Punkte braucht man fuer Geld / Essence?" — nur diese Saison.
//
// Quelle: so5Fixtures(PAST) -> je Fixture die Leaderboards -> je Leaderboard
// rewardsConfig.ranking mit fromRank/toRank UND den Rankings an den Grenzen
// (toSo5Ranking.score). Anonym gilt Komplexitaet 500 je Query, deshalb ein
// Call je Leaderboard (~40 relevante je Spieltag). Arena/PvP/Cap/Beginner-
// Raeume werden uebersprungen — sie sind keine Wettbewerbs-Leaderboards.
//
// Idempotent (Upsert auf fixture_slug+leaderboard_slug). Bereits vollstaendig
// synchronisierte Fixtures werden uebersprungen (--force erzwingt).
//
// Aufruf:  railway run node tools/sync-reward-thresholds.mjs [--dry] [--force] [--since=2026-08-01]
import { createClient } from '@supabase/supabase-js';

const DRY   = process.argv.includes('--dry');
const FORCE = process.argv.includes('--force');
const SINCE = (process.argv.find(a => a.startsWith('--since=')) || '--since=2026-07-31').slice(8);   // Saison 26/27: GW1 startete 31.07.

const SUPABASE_URL = process.env.SUPABASE_URL, SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!DRY && (!SUPABASE_URL || !SERVICE_KEY)) { console.error('SUPABASE_URL / SUPABASE_SERVICE_KEY fehlen'); process.exit(1); }
const supabase = (!DRY) ? createClient(SUPABASE_URL, SERVICE_KEY) : null;

const SORARE_API = 'https://api.sorare.com/graphql';
const APIKEY = process.env.SORARE_APIKEY ?? null;
const headers = { 'Content-Type': 'application/json', ...(APIKEY ? { APIKEY } : {}) };
const DELAY_MS = APIKEY ? 300 : 1100;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gql(query, label) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch(SORARE_API, { method: 'POST', headers, body: JSON.stringify({ query }) });
      if (res.status === 429) { await sleep(30000 * (attempt + 1)); continue; }
      if (!res.ok) { console.warn(`  HTTP ${res.status} (${label})`); return null; }
      const json = await res.json();
      if (json.errors) { console.warn(`  GraphQL (${label}): ${json.errors[0]?.message}`); return null; }
      return json.data;
    } catch (e) { console.warn(`  Fetch (${label}): ${e.message}`); await sleep(2000 * (attempt + 1)); }
  }
  return null;
}

// Wettbewerbs-Leaderboards erkennen: seasonal-<comp>-(in_season|all_seasons)_<comp>_<rarity>,
// KEINE Arena-/PvP-/Cap-/Beginner-/Elite-/Uncapped-Raeume.
const isCompetition = slug => /-(in_season|all_seasons)_[a-z_]+_(limited|rare|super_rare|unique)$/.test(slug)
  && !/arena|pvp|_cap_|beginner|elite|uncapped/.test(slug);
const SLUG_NAMES = { champions: 'Champion', contenders: 'Contender', rest_of_the_world: 'Rest of the World',
  under_twenty_one: 'Under 23', all_star: 'All Star', england: 'English League Players',
  england_second: 'EFL Championship', germany: 'Bundesliga', spain: 'LALIGA EA SPORTS', france: 'Ligue 1',
  italy: 'Italian League', us: 'MLS', jupiler: 'Jupiler Pro League', netherlands: 'Eredivisie', japan: 'J1 League',
  korea: 'K League 1', portugal: 'Liga Portugal', turkey: 'Turkish League', scotland: 'SPFL' };
const compName = lb => {
  const n = lb.displayName.replace(/\s*[–-]\s*(Limited|Rare|Super Rare|Unique)\s*$/i, '').trim();
  if (n && !/^in-season$|^classic$|^all seasons$/i.test(n)) {
    const classic = /-all_seasons_/.test(lb.slug) && !/under_twenty_one|all_star/.test(lb.slug);
    return classic && !/classic/i.test(n) ? n + ' (Classic)' : n;
  }
  const m = lb.slug.match(/-seasonal-([a-z_]+?)-(in_season|all_seasons)_/);
  const key = m?.[1] ?? '';
  const base = SLUG_NAMES[key] ?? key.replace(/_/g, ' ');
  // Champion/Contender/RoW/Ligen gibt es als In-Season UND als Classic-Leaderboard
  // (all_seasons) mit eigenen Preisen — getrennt fuehren. U23/All Star existieren
  // nur als all_seasons, bleiben ohne Zusatz.
  const classic = /-all_seasons_/.test(lb.slug) && !/under_twenty_one|all_star/.test(lb.slug);
  return classic ? base + ' (Classic)' : base;
};

async function listSeasonFixtures() {
  // so5Fixtures kennt kein `type`; Zustand ueber aasmState, Abschluss ueber endDate.
  const out = []; let after = null;
  for (let page = 0; page < 8; page++) {
    const q = `{ so5 { so5Fixtures(sport: FOOTBALL, first: 25${after ? `, after: "${after}"` : ''}) {
      pageInfo { hasNextPage endCursor }
      nodes { slug displayName aasmState gameWeek seasonGameWeek startDate endDate } } } }`;
    const d = await gql(q, 'fixtures');
    const c = d?.so5?.so5Fixtures; if (!c) break;
    const now = new Date().toISOString();
    for (const f of c.nodes) {
      if (f.startDate.slice(0, 10) >= SINCE && f.aasmState === 'closed') out.push(f);
    }
    // Liste ist NEUESTE zuerst: sobald ein Spieltag vor SINCE auftaucht, sind wir durch
    if (!c.pageInfo.hasNextPage || c.nodes.some(f => f.startDate.slice(0, 10) < SINCE)) break;
    after = c.pageInfo.endCursor;
    await sleep(DELAY_MS);
  }
  const seen = new Set();
  return out.filter(f => !seen.has(f.slug) && seen.add(f.slug)).sort((a, b) => a.startDate.localeCompare(b.startDate));
}

async function leaderboardsOf(fixtureSlug) {
  const d = await gql(`{ so5 { so5Fixture(slug: "${fixtureSlug}") { so5Leaderboards { slug displayName rarityType } } } }`, fixtureSlug);
  return (d?.so5?.so5Fixture?.so5Leaderboards ?? []).filter(l => isCompetition(l.slug));
}

async function thresholdsOf(lb) {
  const d = await gql(`{ so5 { so5Leaderboard(slug: "${lb.slug}") { so5LineupsCount rewardedLineupsCount
    rewardsConfig { ranking { fromRank toRank usdAmount ethAmount sharedPool poolSize
      cards { rarity quantity } cardShardRewardConfigs { quantity rarity }
      fromSo5Ranking { ranking score } toSo5Ranking { ranking score } } } } } }`, lb.slug);
  const L = d?.so5?.so5Leaderboard; if (!L) return null;
  const tiers = (L.rewardsConfig?.ranking ?? []).map(t => ({
    from: t.fromRank, to: t.toRank,
    scoreFrom: t.fromSo5Ranking?.score ?? null, scoreTo: t.toSo5Ranking?.score ?? null,
    usd: t.usdAmount ?? null, eth: t.ethAmount ?? null,
    essence: (t.cardShardRewardConfigs ?? []).reduce((s, c) => s + (c.quantity || 0), 0) || null,
    cards: (t.cards ?? []).reduce((s, c) => s + (c.quantity || 0), 0) || null,
  })).filter(t => t.from != null).sort((a, b) => a.from - b.from);
  // Letzte Stufe je Belohnungsart => Rang + Score-Schwelle
  const last = pred => { const xs = tiers.filter(pred).filter(t => t.scoreTo != null); return xs.length ? xs[xs.length - 1] : null; };
  const cash = last(t => (t.usd || 0) > 0 || (t.eth || 0) > 0);
  const ess  = last(t => (t.essence || 0) > 0);
  const card = last(t => (t.cards || 0) > 0);
  const top  = tiers.find(t => t.scoreFrom != null);
  return {
    lineups: L.so5LineupsCount, rewarded_lineups: L.rewardedLineupsCount,
    top_score: top?.scoreFrom ?? null,
    cash_rank: cash?.to ?? null,    cash_score: cash?.scoreTo ?? null,
    essence_rank: ess?.to ?? null,  essence_score: ess?.scoreTo ?? null,
    card_rank: card?.to ?? null,    card_score: card?.scoreTo ?? null,
    prize_pool_usd: tiers.reduce((s, t) => s + (t.usd ? t.usd * (t.sharedPool ? 1 : (t.to - t.from + 1)) : 0), 0) || null,
    tiers,
  };
}

async function main() {
  console.log(`[${new Date().toISOString()}] Reward-Schwellen-Sync${DRY ? ' (DRY)' : ''}, Saison ab ${SINCE}`);
  const fixtures = await listSeasonFixtures();
  console.log(`${fixtures.length} abgeschlossene Spieltage: ${fixtures.map(f => `${f.displayName}(${f.startDate.slice(5, 10)})`).join(', ')}`);

  let done = new Set();
  if (!DRY && !FORCE) {
    const { data } = await supabase.from('reward_thresholds').select('fixture_slug');
    const counts = new Map(); for (const r of data ?? []) counts.set(r.fixture_slug, (counts.get(r.fixture_slug) || 0) + 1);
    for (const [k, n] of counts) if (n >= 20) done.add(k);   // "vollstaendig genug"
  }

  let rows = 0, calls = 0;
  for (const f of fixtures) {
    if (done.has(f.slug)) { console.log(`  ${f.displayName}: bereits synchronisiert — uebersprungen`); continue; }
    const lbs = await leaderboardsOf(f.slug); calls++;
    console.log(`  ${f.displayName}: ${lbs.length} Wettbewerbs-Leaderboards`);
    const batch = [];
    for (const lb of lbs) {
      await sleep(DELAY_MS);
      const t = await thresholdsOf(lb); calls++;
      if (!t) continue;
      batch.push({
        fixture_slug: f.slug, leaderboard_slug: lb.slug,
        game_week: f.gameWeek, season_game_week: f.seasonGameWeek, fixture_name: f.displayName,
        start_date: f.startDate.slice(0, 10),
        competition: compName(lb), rarity: lb.rarityType,
        ...t, synced_at: new Date().toISOString(),
      });
    }
    if (DRY) {
      for (const r of batch.slice(0, 6)) console.log(`    ${r.competition} – ${r.rarity}: Cash ab Rang ${r.cash_rank} (${r.cash_score}), Essence ab ${r.essence_rank} (${r.essence_score}), Top ${r.top_score}, ${r.lineups} Lineups`);
      if (batch.length > 6) console.log(`    ... +${batch.length - 6} weitere`);
    } else if (batch.length) {
      const { error } = await supabase.from('reward_thresholds').upsert(batch, { onConflict: 'fixture_slug,leaderboard_slug' });
      if (error) console.warn(`  Upsert ${f.displayName}: ${error.message}`); else rows += batch.length;
    }
  }
  console.log(`[${new Date().toISOString()}] Fertig: ${rows} Zeilen geschrieben, ${calls} API-Calls.`);
}
main().catch(e => { console.error(e); process.exit(1); });
