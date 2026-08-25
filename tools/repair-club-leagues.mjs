// Einmalige Reparatur: Liga-Felder aller Clubs auf den LIVE-Stand von Sorare setzen.
//
// Anlass (25.08.2026, Nutzer-Report "Englands 1. und 2. Liga vermischt"):
// Der Updater schrieb Liga-Felder nur, wenn sie nicht-null waren. Wechselte ein
// Spieler zu einem Club OHNE domesticLeague (Sorare liefert z. B. fuer Leicester
// und Wigan null), blieb die Liga des ALTEN Vereins stehen — Leicester-Spieler
// standen dadurch teils in der Premier League, teils in der Championship, teils
// in der Ligue 1. Der Updater ist inzwischen gefixt (Club-Felder als Einheit);
// dieses Skript bereinigt den ALTBESTAND sofort statt in 1-4 Tagen Selbstheilung.
//
// Vorgehen:
//   1. Alle (team_name, league_name)-Kombinationen aus card_prices ziehen.
//   2. Clubs finden, deren Zeilen mehr als eine Liga tragen (inkl. null-Mix).
//   3. Je Club die Wahrheit live von Sorare holen (Club-Slug via clubsReady,
//      Fallback: activeClub des zuletzt aktualisierten Spielers des Clubs).
//   4. ALLE Zeilen des Clubs einheitlich setzen — auch auf null, wenn Sorare
//      keine Liga kennt (Liga-Land faellt dann auf das Club-Land zurueck).
//
// Aufruf:  railway run node tools/repair-club-leagues.mjs [--dry]
//          (braucht SUPABASE_URL, SUPABASE_SERVICE_KEY, optional SORARE_APIKEY)
import { createClient } from '@supabase/supabase-js';

const DRY = process.argv.includes('--dry');
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_KEY  = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SERVICE_KEY) { console.error('SUPABASE_URL / SUPABASE_SERVICE_KEY fehlen'); process.exit(1); }
const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

const SORARE_API = 'https://api.sorare.com/graphql';
const APIKEY = process.env.SORARE_APIKEY ?? null;
const headers = { 'Content-Type': 'application/json', ...(APIKEY ? { APIKEY } : {}) };
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

async function main() {
  console.log(`[${new Date().toISOString()}] Liga-Reparatur startet${DRY ? ' (DRY-RUN)' : ''}`);

  // 1) Bestand einlesen (nur die drei kleinen Spalten, seitenweise)
  const byClub = new Map();  // team_name -> Map(league_name|'' -> Anzahl)
  for (let off = 0; ; off += 1000) {
    const { data, error } = await supabase.from('card_prices')
      .select('team_name, league_name')
      .not('team_name', 'is', null)
      .order('id', { ascending: true })
      .range(off, off + 999);
    if (error) { console.error(`Lesen bei offset ${off}: ${error.message}`); process.exit(1); }
    for (const r of data) {
      if (!byClub.has(r.team_name)) byClub.set(r.team_name, new Map());
      const m = byClub.get(r.team_name);
      const k = r.league_name ?? '';
      m.set(k, (m.get(k) || 0) + 1);
    }
    if (data.length < 1000) break;
  }
  const mixed = [...byClub.entries()].filter(([, m]) => m.size > 1);
  console.log(`${byClub.size} Clubs im Bestand, davon mit GEMISCHTEN Ligen: ${mixed.length}`);
  for (const [club, m] of mixed) {
    console.log(`  ${club}: ${[...m].map(([l, n]) => `${l || 'NULL'}×${n}`).join(' | ')}`);
  }
  if (!mixed.length) { console.log('Nichts zu reparieren.'); return; }

  // 2) Club-Slugs von Sorare (Name -> Slug), fuer die Live-Wahrheit je Club
  const clubsData = await gql('{ football { clubsReady { slug name } } }', 'clubsReady');
  const slugByName = new Map((clubsData?.football?.clubsReady ?? []).map(c => [c.name, c.slug]));

  let repaired = 0, skippedClubs = 0;
  for (const [club] of mixed) {
    // 3a) Wahrheit ueber den Club selbst
    let truth = null;
    const slug = slugByName.get(club);
    if (slug) {
      const d = await gql(`{ football { club(slug: "${slug}") { name country { code } domesticLeague { name country { code } } } } }`, club);
      const c = d?.football?.club;
      if (c) truth = { league: c.domesticLeague?.name ?? null,
                       country: c.domesticLeague?.country?.code ?? c.country?.code ?? null };
      await sleep(1200);
    }
    // 3b) Fallback: activeClub des frischesten Spielers dieses Clubs
    if (!truth) {
      const { data: rows } = await supabase.from('card_prices')
        .select('player_slug').eq('team_name', club)
        .order('updated_at', { ascending: false }).limit(5);
      for (const r of rows ?? []) {
        const d = await gql(`{ anyPlayer(slug: "${r.player_slug}") { activeClub { name country { code } domesticLeague { name country { code } } } } }`, r.player_slug);
        const c = d?.anyPlayer?.activeClub;
        await sleep(1200);
        if (c?.name === club) {   // Spieler muss noch dort sein, sonst naechster
          truth = { league: c.domesticLeague?.name ?? null,
                    country: c.domesticLeague?.country?.code ?? c.country?.code ?? null };
          break;
        }
      }
    }
    if (!truth) { console.warn(`  ${club}: keine Live-Wahrheit ermittelbar — uebersprungen`); skippedClubs++; continue; }

    console.log(`  ${club} -> Liga: ${truth.league ?? 'NULL'} (${truth.country ?? 'NULL'})`);
    if (DRY) continue;
    const { error, count } = await supabase.from('card_prices')
      .update({ league_name: truth.league, league_country: truth.country }, { count: 'exact' })
      .eq('team_name', club);
    if (error) console.warn(`  ${club}: Update fehlgeschlagen — ${error.message}`);
    else repaired += count ?? 0;
    await sleep(300);
  }
  console.log(`[${new Date().toISOString()}] Fertig. ${repaired} Zeilen vereinheitlicht, ${skippedClubs} Clubs uebersprungen.`);
  console.log('Hinweis: Facetten/Ranking zeigen den neuen Stand nach dem naechsten refresh_market_aggregates (09:20 UTC).');
}

main().catch(e => { console.error(e); process.exit(1); });
