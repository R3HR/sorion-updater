// Top-Verdiener nach ESSENCE je Liga (02.10.2026, Wunsch Jonas)
//
// WARUM EIN EIGENES SKRIPT: Fuer Cash erheben wir in sync-lineup-costs.mjs ohnehin jede
// bezahlte Aufstellung. Essence-Raenge gehen aber bis in die Zehntausende (All Star Limited
// bis Rang 30.200), fuer die ganze Saison rund 11.000 API-Seiten und 570.000 Aufstellungen.
// Die passen weder in den Speicher eines Laufs noch als Rohzeilen in die Datenbank.
// Deshalb hier: je Leaderboard-Woche ALLE Essence-Raenge holen, sofort zu Summen je Spieler
// verdichten, die Woche ersetzen, Speicher freigeben. Rohaufstellungen werden nicht gespeichert.
//
// VERTEILUNG wie bei Cash (Entscheidung Jonas 02.10.): Essence der Aufstellung laut
// Preisstufe, aufgeteilt nach PUNKTEANTEIL. Holte die Aufstellung 0 Punkte, gleich verteilt.
//
// ESSENCE-SORTE: Jede Stufe zahlt genau eine Sorte (geprueft 02.10.), normalerweise die
// Rarity des Wettbewerbs. AUSNAHME: Unique-Wettbewerbe zahlen Super-Rare-Essence, weil es
// keine Unique-Essence gibt. Die Seite beschriftet das entsprechend.
//
// ERHALTUNGSPROBE je Leaderboard-Woche: verteilte Essence = Summe der Preisstufen bis zum
// letzten tatsaechlich besetzten Rang. Abweichungen werden ausgegeben.
//
// Aufruf:  railway run -s "Updater Limited" node tools/sync-essence-earnings.mjs
// Optionen: --dry  --force  --since=2026-07-31  --max-weeks=N (zum Testen)
import { createClient } from '@supabase/supabase-js';

const DRY   = process.argv.includes('--dry');
const FORCE = process.argv.includes('--force');
const arg = (k, d) => (process.argv.find(a => a.startsWith(`--${k}=`)) || `--${k}=${d}`).split('=').slice(1).join('=');
const SINCE = arg('since', '2026-07-31');
const MAX_WEEKS = parseInt(arg('max-weeks', '0'), 10);
const PAGE = 50;
const DELAY_MS = 700;      // 200 Anfragen/min gelten fuer ALLE Dienste zusammen (HANDOFF)

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);
const APIKEY = process.env.SORARE_APIKEY;
if (!APIKEY) { console.error('SORARE_APIKEY fehlt: mit railway run -s "Updater Limited" starten'); process.exit(1); }
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gql(query, label) {
  for (let attempt = 0; attempt < 4; attempt++) {
    try {
      const res = await fetch('https://api.sorare.com/graphql',
        { method: 'POST', headers: { 'Content-Type': 'application/json', APIKEY }, body: JSON.stringify({ query }) });
      if (res.status === 429) { await sleep(30000 * (attempt + 1)); continue; }
      if (!res.ok) { console.warn(`  HTTP ${res.status} (${label})`); await sleep(3000); continue; }
      const json = await res.json();
      if (json.errors) { console.warn(`  GraphQL (${label}): ${json.errors[0]?.message?.slice(0, 120)}`); return null; }
      return json.data;
    } catch (e) { console.warn(`  Fetch (${label}): ${e.message}`); await sleep(2000 * (attempt + 1)); }
  }
  return null;
}

// so5RankingsPaginated zaehlt Seiten ab 0 (HANDOFF, Falle 06.09.)
const pageOf = rank => Math.max(0, Math.floor((rank - 1) / PAGE));

async function rankingsPage(slug, page) {
  const d = await gql(`{ so5 { so5Leaderboard(slug:"${slug}") {
    so5RankingsPaginated(page: ${page}, pageSize: ${PAGE}) {
      nodes { ranking so5Lineup { so5Appearances { score anyTeam { name } anyPlayer { slug displayName } } } } } } } }`,
    `${slug} S.${page}`);
  return d?.so5?.so5Leaderboard?.so5RankingsPaginated?.nodes ?? null;
}

async function readAll(table, select, filter = q => q) {
  const out = [];
  for (let off = 0; ; off += 1000) {
    const { data, error } = await filter(supabase.from(table).select(select)).range(off, off + 999);
    if (error) throw new Error(`${table}: ${error.message}`);
    out.push(...data);
    if (data.length < 1000) break;
  }
  return out;
}

async function main() {
  const t0 = Date.now();
  console.log(`[${new Date().toISOString()}] Essence-Verdienste${DRY ? ' (DRY)' : ''}${FORCE ? ' (FORCE)' : ''}, Saison ab ${SINCE}`);

  const lbs = (await readAll('reward_thresholds',
    'fixture_slug, leaderboard_slug, fixture_name, start_date, competition, rarity, essence_rank, lineups, fixture_state, tiers',
    q => q.gte('start_date', SINCE).not('essence_rank', 'is', null).order('start_date', { ascending: true })));
  const done = new Set();
  if (!FORCE) for (const r of await readAll('player_earnings_weeks', 'fixture_slug, leaderboard_slug', q => q.eq('reward_kind', 'essence')))
    done.add(r.fixture_slug + '|' + r.leaderboard_slug);

  let todo = lbs.filter(lb => FORCE || lb.fixture_state !== 'closed' || !done.has(lb.fixture_slug + '|' + lb.leaderboard_slug));
  if (MAX_WEEKS) todo = todo.slice(0, MAX_WEEKS);
  const pagesTotal = todo.reduce((s, lb) => s + pageOf(Math.min(lb.essence_rank, lb.lineups || lb.essence_rank)) + 1, 0);
  console.log(`${lbs.length} Leaderboard-Wochen mit Essence, erledigt ${done.size}, offen ${todo.length} (${pagesTotal} Seiten, `
    + `~${Math.round(pagesTotal * (DELAY_MS + 350) / 60000)} min)`);

  let calls = 0, rowsWritten = 0, abweichungen = 0, n = 0;
  for (const lb of todo) {
    n++;
    const last = Math.min(lb.essence_rank, lb.lineups || lb.essence_rank);
    const essFor = rank => {
      for (const t of lb.tiers ?? []) if (rank >= t.from && rank <= t.to) return Number(t.essence) || 0;
      return 0;
    };
    let soll = 0;
    for (const t of lb.tiers ?? []) {
      const e = Number(t.essence) || 0;
      if (e > 0) soll += e * Math.max(0, Math.min(t.to, last) - t.from + 1);
    }

    const players = new Map();
    let ist = 0, lineups = 0, failed = false;
    for (let p = 0; p <= pageOf(last); p++) {
      await sleep(DELAY_MS);
      const nodes = await rankingsPage(lb.leaderboard_slug, p); calls++;
      if (nodes == null) { failed = true; break; }
      for (const node of nodes) {
        if (node.ranking > last) continue;
        const ess = essFor(node.ranking);
        if (!ess) continue;
        const apps = (node.so5Lineup?.so5Appearances ?? []).filter(a => a.anyPlayer?.slug);
        if (!apps.length) continue;
        lineups++;
        const total = apps.reduce((s, a) => s + (Number(a.score) || 0), 0);
        for (const a of apps) {
          const pts = Number(a.score) || 0;
          const share = total > 0 ? pts / total : 1 / apps.length;
          // Verein IN DIESEM SPIEL, nicht der heutige (BUG-049)
          const e = players.get(a.anyPlayer.slug) ?? { name: a.anyPlayer.displayName, club: a.anyTeam?.name ?? null, lineups: 0, points: 0, earned: 0 };
          e.lineups++; e.points += pts; e.earned += ess * share;
          players.set(a.anyPlayer.slug, e);
        }
        ist += ess;
      }
    }
    // Eine unvollstaendig geholte Woche NICHT schreiben: lieber beim naechsten Lauf neu als falsch.
    if (failed) { console.warn(`  ${lb.fixture_name} · ${lb.competition} ${lb.rarity}: Abruf abgebrochen, nicht geschrieben`); continue; }

    const diff = Math.round(ist - soll);
    if (Math.abs(diff) > 0) abweichungen++;
    const rows = [...players].map(([slug, e]) => ({
      fixture_slug: lb.fixture_slug, leaderboard_slug: lb.leaderboard_slug,
      competition: lb.competition, rarity: lb.rarity, reward_kind: 'essence',
      player_slug: slug, player_name: e.name, club: e.club, lineups: e.lineups,
      points: Math.round(e.points * 100) / 100, earned: Math.round(e.earned * 100) / 100,
      synced_at: new Date().toISOString(),
    }));
    if (!DRY) {
      const { error: de } = await supabase.from('player_earnings').delete()
        .eq('fixture_slug', lb.fixture_slug).eq('leaderboard_slug', lb.leaderboard_slug).eq('reward_kind', 'essence');
      if (de) { console.warn(`  Loeschen ${lb.leaderboard_slug}: ${de.message}`); continue; }
      for (let i = 0; i < rows.length; i += 500) {
        const { error: ie } = await supabase.from('player_earnings').insert(rows.slice(i, i + 500));
        if (ie) console.warn(`  Schreiben ${lb.leaderboard_slug} (${i}): ${ie.message}`); else rowsWritten += Math.min(500, rows.length - i);
      }
    }
    const min = Math.round((Date.now() - t0) / 60000);
    console.log(`  [${n}/${todo.length}, ${min} min] ${lb.fixture_name} · ${lb.competition} ${lb.rarity}: `
      + `${lineups} Aufstellungen, ${rows.length} Spieler, ${Math.round(ist)} Essence`
      + (diff ? `  ABWEICHUNG ${diff > 0 ? '+' : ''}${diff} (Soll ${Math.round(soll)})` : '  Erhaltung ok'));
    players.clear();
  }
  console.log(`[${new Date().toISOString()}] Fertig: ${n} Wochen, ${calls} API-Calls, ${rowsWritten} Zeilen, `
    + `${abweichungen} Wochen mit Abweichung, ${Math.round((Date.now() - t0) / 60000)} min.`);
}

main().catch(e => { console.error(e); process.exit(1); });
