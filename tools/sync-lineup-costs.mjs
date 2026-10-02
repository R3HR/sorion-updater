// Was kostet eine Aufstellung, die Geld bzw. Essence abgeraeumt hat?
// (06.09.2026, Wunsch Jonas) — NUR diese Saison, ab GW1 (31.07.2026).
//
// WARUM NUR DIESE SAISON (Ansage Jonas 06.09.): Mit jeder Saison aendern sich
// Spielmechaniken; aeltere Aufstellungen sind als Kostenmassstab wertlos.
//
// VORGEHEN:
//   1. Leaderboards + Preisgrenzen kommen aus reward_thresholds (bereits
//      synchronisiert). cash_rank/essence_rank sagen, wer was gewonnen hat.
//   2. Je Leaderboard drei gezielte Seiten holen (so5RankingsPaginated liefert
//      SEITENNUMMERN, also kein Durchblaettern): Spitzenfeld, die Seite an der
//      Cash-Grenze und die Seite an der Essence-Grenze. Das deckt genau die
//      Baender ab, um die es geht, statt blind zu mitteln.
//   3. Karten (player_slug + rarityTyped + inSeasonEligible) gegen unsere
//      card_prices bepreisen -> Kosten der Aufstellung zu HEUTIGEN Marktwerten.
//      Das ist die richtige Frage: "Was muesste ich heute zahlen, um so ein
//      Team zu stellen?" — nicht, was es damals gekostet haette.
//   4. Nur vollstaendig bepreiste Aufstellungen werden gewertet (5 von 5),
//      sonst waere die Summe systematisch zu niedrig.
//
// BRAUCHT den SORARE_APIKEY (Tiefe 8 noetig, anonym nur 7) — also einen der
// Updater-Dienste:  railway run -s "Updater Limited" node tools/sync-lineup-costs.mjs
// Optionen: --dry  --force  --since=2026-07-31  --pagesize=50
import { createClient } from '@supabase/supabase-js';

const DRY   = process.argv.includes('--dry');
const FORCE = process.argv.includes('--force');
const SINCE = (process.argv.find(a => a.startsWith('--since=')) || '--since=2026-07-31').slice(8);
const PAGE  = parseInt((process.argv.find(a => a.startsWith('--pagesize=')) || '--pagesize=50').slice(11), 10);

const SUPABASE_URL = process.env.SUPABASE_URL, SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SERVICE_KEY) { console.error('SUPABASE_URL / SUPABASE_SERVICE_KEY fehlen'); process.exit(1); }
const supabase = createClient(SUPABASE_URL, SERVICE_KEY);

const APIKEY = process.env.SORARE_APIKEY;
if (!APIKEY) { console.error('SORARE_APIKEY fehlt — mit "railway run -s \\"Updater Limited\\"" starten'); process.exit(1); }
const headers = { 'Content-Type': 'application/json', APIKEY };
// 200 Anfragen/min sind fuer ALLE Dienste zusammen das Limit (HANDOFF) —
// 700 ms lassen den Updatern Luft.
const DELAY_MS = 700;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gql(query, label) {
  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      const res = await fetch('https://api.sorare.com/graphql',
        { method: 'POST', headers, body: JSON.stringify({ query }) });
      if (res.status === 429) { await sleep(30000 * (attempt + 1)); continue; }
      if (!res.ok) { console.warn(`  HTTP ${res.status} (${label})`); return null; }
      const json = await res.json();
      if (json.errors) { console.warn(`  GraphQL (${label}): ${json.errors[0]?.message?.slice(0, 120)}`); return null; }
      return json.data;
    } catch (e) { console.warn(`  Fetch (${label}): ${e.message}`); await sleep(2000 * (attempt + 1)); }
  }
  return null;
}

// ACHTUNG: so5RankingsPaginated zaehlt Seiten ab 0 (verifiziert 06.09.:
// page 5 bei pageSize 25 liefert die Raenge 126-150). Mit 1-basierter Rechnung
// fehlten in der ersten Fassung ueberall die Raenge 1-50 — also genau die
// Cash-Gewinner.
const pageOf = rank => Math.max(0, Math.floor((rank - 1) / PAGE));

async function rankingsPage(slug, page) {
  const d = await gql(`{ so5 { so5Leaderboard(slug:"${slug}") {
    so5RankingsPaginated(page: ${page}, pageSize: ${PAGE}) {
      nodes { ranking score so5Lineup { so5Appearances {
        score anyTeam { name } anyPlayer { slug displayName } anyCard { rarityTyped inSeasonEligible } } } } } } } }`,
    `${slug} S.${page}`);
  return d?.so5?.so5Leaderboard?.so5RankingsPaginated?.nodes ?? [];
}

// Preise fuer alle vorkommenden Spieler holen (Bloecke wegen URL-Laenge)
async function priceMap(slugs) {
  const map = new Map();
  for (let i = 0; i < slugs.length; i += 150) {
    const batch = slugs.slice(i, i + 150);
    const { data, error } = await supabase.from('card_prices')
      .select('player_slug, scarcity, eligibility, fmv')
      .in('player_slug', batch).not('fmv', 'is', null);
    if (error) { console.warn(`  Preise (${i}): ${error.message}`); continue; }
    for (const r of data) map.set(`${r.player_slug}|${r.scarcity}|${r.eligibility}`, Number(r.fmv));
  }
  return map;
}

async function main() {
  console.log(`[${new Date().toISOString()}] Aufstellungs-Kosten${DRY ? ' (DRY)' : ''}, Saison ab ${SINCE}`);

  const { data: lbs, error } = await supabase.from('reward_thresholds')
    .select('fixture_slug, leaderboard_slug, fixture_name, competition, rarity, cash_rank, essence_rank, lineups, fixture_state, tiers')
    .gte('start_date', SINCE).order('start_date', { ascending: true });
  if (error) { console.error('reward_thresholds:', error.message); process.exit(1); }
  console.log(`${lbs.length} Leaderboard-Wochen dieser Saison`);

  // Bereits erfasste Leaderboards ueberspringen. WICHTIG seitenweise lesen:
  // PostgREST liefert hoechstens 1000 Zeilen, die Tabelle hat zehntausende —
  // ohne Paginierung waere die Liste unvollstaendig und der taegliche Cron
  // wuerde jedes Mal die ganze Saison neu abfragen.
  let done = new Set();
  const earnDone = new Set();
  if (!FORCE && !DRY) {
    for (let off = 0; ; off += 1000) {
      const { data, error } = await supabase.from('lineup_costs')
        .select('fixture_slug, leaderboard_slug').range(off, off + 999);
      if (error) { console.warn(`  Bestand lesen: ${error.message}`); break; }
      for (const r of data) done.add(r.fixture_slug + '|' + r.leaderboard_slug);
      if (data.length < 1000) break;
    }
    console.log(`Bereits erfasst: ${done.size} Leaderboard-Wochen`);
    // Spieler-Verdienste (Top-Verdiener-Tabelle, 02.10.) gibt es erst seit spaeter. Eine
    // Woche gilt nur als fertig, wenn auch ihre Cash-Verdienste gespeichert sind.
    const { data: ew, error: ee } = await supabase.from('player_earnings_weeks')
      .select('fixture_slug, leaderboard_slug').eq('reward_kind', 'cash');
    if (ee) console.warn(`  Verdienst-Bestand lesen: ${ee.message}`);
    for (const r of ew ?? []) earnDone.add(r.fixture_slug + '|' + r.leaderboard_slug);
    console.log(`Spieler-Verdienste erfasst: ${earnDone.size} Leaderboard-Wochen`);
  }

  // 1) Aufstellungen einsammeln
  const raw = [];          // {lb, ranking, score, cards:[{slug,scarcity,elig}]}
  const slugSet = new Set();
  let calls = 0;
  for (const lb of lbs) {
    // Vorlaeufige Spieltage (noch nicht "closed", Ausschuettung laeuft) neu rechnen:
    // ihre Raenge koennen sich mit einer Score-Korrektur noch verschieben.
    const wk = lb.fixture_slug + '|' + lb.leaderboard_slug;
    if (lb.fixture_state === 'closed' && done.has(wk) && (!lb.cash_rank || earnDone.has(wk))) continue;
    const maxRank = lb.essence_rank || lb.cash_rank;
    if (!maxRank) continue;                       // Leaderboard ohne Geld/Essence
    // CASH: VOLLERHEBUNG, jede Seite bis zum letzten bezahlten Rang.
    //   Grund (Einwand Jonas 09.09.): Die alte Stichprobe nahm nur das Spitzenfeld und
    //   die Seite an der Grenze. Bei einem Leaderboard mit 250 bezahlten Raengen fehlte
    //   die ganze Mitte, und genau dort kann das billigste Gewinner-Team stehen. Der
    //   Median war dadurch der einer Mischung aus Spitze und Rand, nicht der aller Gewinner.
    //   Kostet WENIGER als vorher: der groesste Cash-Rang der Saison ist 250 (5 Seiten),
    //   waehrend der alte Sprung an die Essence-Grenze allein bis Seite 59 ging.
    //   Ganze Saison: 316 Seiten gegenueber 546 mit der alten Logik.
    // ESSENCE: weiter Stichprobe. Vollerhebung waere 7.209 Seiten statt 316, dafuer ist
    //   die Zahl den Aufwand nicht wert.
    const cashPages = lb.cash_rank
      ? Array.from({ length: pageOf(lb.cash_rank) + 1 }, (_, i) => i)
      : [];
    const pages = [...new Set([...cashPages, 0, pageOf(maxRank)])].sort((a, b) => a - b);
    let got = 0;
    for (const p of pages) {
      await sleep(DELAY_MS);
      const nodes = await rankingsPage(lb.leaderboard_slug, p); calls++;
      for (const n of nodes) {
        const apps = n.so5Lineup?.so5Appearances ?? [];
        if (!apps.length) continue;
        const cards = apps.map(a => ({
          slug: a.anyPlayer?.slug,
          name: a.anyPlayer?.displayName ?? null,
          // Verein IN DIESEM SPIEL, nicht der heutige (BUG-046: Spieler wechseln nach ihren Punkten)
          club: a.anyTeam?.name ?? null,
          pts: Number(a.score) || 0,
          scarcity: a.anyCard?.rarityTyped,
          elig: a.anyCard?.inSeasonEligible ? 'in_season' : 'classic',
        })).filter(c => c.slug && c.scarcity);
        for (const c of cards) slugSet.add(c.slug);
        raw.push({ lb, ranking: n.ranking, score: n.score, cards });
        got++;
      }
    }
    const wie = lb.cash_rank ? `Cash 1-${lb.cash_rank} vollstaendig` : 'nur Stichprobe';
    console.log(`  ${lb.fixture_name} · ${lb.competition} ${lb.rarity}: ${got} Aufstellungen, `
      + `${pages.length} Seiten (${wie})`);
  }
  console.log(`Gesammelt: ${raw.length} Aufstellungen, ${slugSet.size} verschiedene Spieler, ${calls} API-Calls`);
  if (!raw.length) { console.log('Nichts zu tun.'); return; }

  // 2) Bepreisen
  const prices = await priceMap([...slugSet]);
  console.log(`Preise geladen: ${prices.size} Karten-Kombinationen`);

  const rows = [];
  for (const r of raw) {
    let cost = 0, priced = 0;
    for (const c of r.cards) {
      const v = prices.get(`${c.slug}|${c.scarcity}|${c.elig}`);
      if (v != null) { cost += v; priced++; }
    }
    const kind = r.lb.cash_rank && r.ranking <= r.lb.cash_rank ? 'cash'
               : r.lb.essence_rank && r.ranking <= r.lb.essence_rank ? 'essence' : 'none';
    rows.push({
      fixture_slug: r.lb.fixture_slug, leaderboard_slug: r.lb.leaderboard_slug,
      ranking: r.ranking, score: r.score,
      cost_eur: Math.round(cost * 100) / 100,
      cards_total: r.cards.length, cards_priced: priced,
      reward_kind: kind, priced_at: new Date().toISOString(),
    });
  }
  const full = rows.filter(r => r.cards_priced === r.cards_total && r.cards_total >= 5);
  const med = a => { const x = [...a].sort((p, q) => p - q); return x.length ? x[Math.floor(x.length / 2)] : null; };
  for (const k of ['cash', 'essence']) {
    const v = full.filter(r => r.reward_kind === k).map(r => r.cost_eur);
    console.log(`  ${k.padEnd(8)}: n=${String(v.length).padStart(4)}  Median ${med(v)?.toFixed(2) ?? '—'} EUR`);
  }
  console.log(`Vollstaendig bepreist: ${full.length}/${rows.length} (${Math.round(100 * full.length / rows.length)} %)`);

  // 3) Spieler-Verdienste (Top-Verdiener je Liga, Entscheidung Jonas 02.10.)
  //    Gewinn der Aufstellung aus der Preisstufe (from <= Rang <= to), verteilt nach
  //    PUNKTEANTEIL: wer 80 von 400 Punkten holte, bekommt 20 % zugerechnet. Holte die
  //    ganze Aufstellung 0 Punkte, wird gleich verteilt. Nur CASH, weil nur dort alle
  //    Gewinner erhoben werden; Essence ist eine Stichprobe und waere verzerrt.
  const usdFor = (lb, rank) => {
    for (const t of lb.tiers ?? []) if (rank >= t.from && rank <= t.to) return Number(t.usd) || 0;
    return 0;
  };
  const earn = new Map();            // "fixture|leaderboard" -> Map(player -> Summen)
  for (const r of raw) {
    if (!(r.lb.cash_rank && r.ranking <= r.lb.cash_rank)) continue;
    const usd = usdFor(r.lb, r.ranking);
    if (!usd) continue;
    const total = r.cards.reduce((a, c) => a + c.pts, 0);
    const key = r.lb.fixture_slug + '|' + r.lb.leaderboard_slug;
    if (!earn.has(key)) earn.set(key, { lb: r.lb, players: new Map() });
    const pm = earn.get(key).players;
    for (const c of r.cards) {
      const share = total > 0 ? c.pts / total : 1 / r.cards.length;
      const e = pm.get(c.slug) ?? { name: c.name, club: c.club, lineups: 0, points: 0, earned: 0 };
      e.lineups++; e.points += c.pts; e.earned += usd * share;
      pm.set(c.slug, e);
    }
  }
  const earnRows = [];
  for (const { lb, players } of earn.values())
    for (const [slug, e] of players) earnRows.push({
      fixture_slug: lb.fixture_slug, leaderboard_slug: lb.leaderboard_slug,
      competition: lb.competition, rarity: lb.rarity, reward_kind: 'cash',
      player_slug: slug, player_name: e.name, club: e.club, lineups: e.lineups,
      points: Math.round(e.points * 100) / 100, earned: Math.round(e.earned * 100) / 100,
      synced_at: new Date().toISOString(),
    });
  const usdSum = earnRows.reduce((a, r) => a + r.earned, 0);
  console.log(`Spieler-Verdienste: ${earnRows.length} Zeilen aus ${earn.size} Leaderboard-Wochen, ${Math.round(usdSum)} USD verteilt`);

  if (DRY) { console.log('DRY-RUN, nichts geschrieben.'); return; }

  // Je Leaderboard-Woche ERSETZEN statt nur upserten: faellt ein Spieler nach einer
  // Score-Korrektur aus den Gewinnern, darf seine alte Zeile nicht stehen bleiben.
  let earnWritten = 0;
  for (const { lb } of earn.values()) {
    const { error: de } = await supabase.from('player_earnings').delete()
      .eq('fixture_slug', lb.fixture_slug).eq('leaderboard_slug', lb.leaderboard_slug).eq('reward_kind', 'cash');
    if (de) console.warn(`  Verdienste loeschen (${lb.leaderboard_slug}): ${de.message}`);
  }
  for (let i = 0; i < earnRows.length; i += 500) {
    const { error: ie } = await supabase.from('player_earnings').insert(earnRows.slice(i, i + 500));
    if (ie) console.warn(`  Verdienste schreiben (${i}): ${ie.message}`); else earnWritten += Math.min(500, earnRows.length - i);
  }
  console.log(`Spieler-Verdienste geschrieben: ${earnWritten}`);
  let written = 0;
  for (let i = 0; i < rows.length; i += 500) {
    const { error: e } = await supabase.from('lineup_costs')
      .upsert(rows.slice(i, i + 500), { onConflict: 'fixture_slug,leaderboard_slug,ranking' });
    if (e) console.warn(`  Upsert (${i}): ${e.message}`); else written += Math.min(500, rows.length - i);
  }
  console.log(`[${new Date().toISOString()}] Fertig: ${written} Zeilen, ${calls} API-Calls.`);
}

main().catch(e => { console.error(e); process.exit(1); });
