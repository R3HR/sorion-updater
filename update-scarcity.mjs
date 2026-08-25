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
// Eine Karte kostet DELAY_MS + ~430 ms (Sorare-Call ~95 ms, zwei Supabase-Writes
// je ~170 ms) — der sleep ist also NICHT die ganze Taktzeit. Gemessen 07.08.:
// bei DELAY_MS=1500 sind das ~1,93 s/Karte, macht 31 Anfragen/min je Service,
// drei Services = 93/min von 200 erlaubten. BATCH_SIZE ist seit der Zeitbremse
// nur noch eine Obergrenze (siehe MAX_RUN_MS) — den Durchsatz regelt DELAY_MS.
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE ?? '90', 10);
const DELAY_MS   = parseInt(process.env.DELAY_MS ?? '2800', 10);
// Anteil der Batch, der an In-Season-Zeilen geht. Dort bewegen sich die Preise
// täglich (FMV-Halbwertszeit 3 Tage), Classic ist träge (14 Tage) — beide gleich
// oft zu holen verschenkt Kontingent. 0.75 bei BATCH_SIZE=190 heißt: In-Season
// täglich durch, Classic alle ~3 Tage.
const IN_SEASON_SHARE = Math.min(1, Math.max(0, parseFloat(process.env.IN_SEASON_SHARE ?? '0.75')));
// Harte Zeitbremse. Der Cron-Slot ist 5 min; laeuft ein Job darueber hinaus,
// UEBERSPRINGT Railway den naechsten Tick — der Durchsatz sinkt also, statt zu
// steigen. Pro Karte fallen ~1,9 s an (sleep + Sorare-Call + 2 Supabase-Writes),
// aber das haengt an der Netzlatenz und laesst sich nicht sauber vorausberechnen.
// Deshalb ist BATCH_SIZE nur eine Obergrenze: Was in MAX_RUN_MS nicht durchlaeuft,
// bleibt mit unveraendertem updated_at in der Queue und kommt beim naechsten Tick
// zuerst dran (die Batch ist nach Alter sortiert).
const MAX_RUN_MS = parseInt(process.env.MAX_RUN_MS ?? '255000', 10);

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
      anyPositions
      country { code }
      age
      gameplayTier
      activeClub { name country { code } domesticLeague { name country { code } } }
      lowestPriceAnyCard(inSeason: ${inSeason}, rarity: ${SCARCITY}) {
        pictureUrl
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
      // Kartenbild der RICHTIGEN Rarity/Eligibility (Backfill hatte Bilder quer kopiert, z. B. Yamal rare mit SR-Bild)
      const cardPic = data?.data?.player?.lowestPriceAnyCard?.pictureUrl ?? null;
      // Verein/Liga: fuellt die Luecke fuer Zeilen, die nie ueber update-prices liefen
      // (27 % der sichtbaren Zeilen hatten keinen Verein -> Club-Filter waere loechrig)
      const position   = data?.data?.player?.anyPositions?.[0] ?? null; // Goalkeeper|Defender|Midfielder|Forward
      const club       = data?.data?.player?.activeClub ?? null;
      const teamName   = club?.name ?? null;
      const leagueName = club?.domesticLeague?.name ?? null;
      // Land der LIGA (nicht die Nationalität des Spielers — das ist player.country!).
      // Fallback: Land des CLUBS, damit ein Verein ohne Liga-Zuordnung (Sorare
      // liefert z. B. fuer Leicester domesticLeague=null) unter seinem Land
      // filterbar bleibt, statt komplett aus den Filtern zu fallen.
      const leagueCountry = club?.domesticLeague?.country?.code ?? club?.country?.code ?? null;
      // NATIONALITAET des Spielers (player.country) — nicht das Liga-Land!
      const nation = data?.data?.player?.country?.code ?? null;
      // Alter direkt von Sorare (Jonas' Wunsch statt Geburtsdatum) — wird bei
      // jeder Beruehrung neu geschrieben, Geburtstage ziehen also von selbst nach
      const age = Number.isFinite(data?.data?.player?.age) ? data.data.player.age : null;
      // Sterne-Klassifizierung des Spielers: GOAT/STAR/IMPACT/ROSTER/DNP
      const gameplayTier = data?.data?.player?.gameplayTier ?? null;
      return { sales, fetchedFloor: floorCents ? floorCents / 100 : null, cardPic, hasClub: !!club, teamName, leagueName, position, leagueCountry, nation, age, gameplayTier };
    } catch (e) { console.warn(`  Fetch failed for ${playerSlug}: ${e.message}`); return null; }
  }
  return null;
}

// Historie EINMAL laden — daraus entscheiden wir sowohl, ob ein neuer Punkt noetig
// ist, als auch die 24h/7d-Prozente. Fenster bewusst 45 Tage statt 8: seit dem
// Aenderungs-Schreiben (siehe unten) kann der letzte Punkt aelter sein, und die
// Prozente rechnen ohnehin mit Carry-Forward (letzter Wert <= Stichtag).
async function loadHistory(playerSlug, eligibility, now, hasEligibility) {
  const since = new Date(now - 45 * 86400000).toISOString().split('T')[0];
  let q = supabase
    .from('price_history')
    .select('price, recorded_at')
    .eq('player_slug', playerSlug)
    .eq('scarcity', SCARCITY)
    .gte('recorded_at', since)
    .order('recorded_at', { ascending: true })
    .limit(60);
  if (hasEligibility) q = q.eq('eligibility', eligibility);
  const { data } = await q;
  return data ?? [];
}

// FMV heute vs. Historie vor 1 bzw. 7 Tagen (pro Eligibility getrennt)
function calcChanges(hist, fmv, now) {
  if (!fmv || !hist?.length) return { change_24h: null, change_7d: null };

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

  // Spalte last_sale_at vorhanden? (migrations/2026-08-02_last_sale_date.sql)
  // Vergleichs-Spalten vorhanden? (migrations/2026-08-22_accuracy_benchmarks.sql)
  const bmProbe = await supabase.from('fmv_accuracy').select('floor_est').limit(1);
  const hasBenchmarks = !bmProbe.error;
  if (!hasBenchmarks) console.warn('fmv_accuracy.floor_est fehlt — Gegenprobe (Floor/Avg) wird uebersprungen');

  const lsProbe = await supabase.from('card_prices').select('last_sale_at').limit(1);
  const hasLastSale = !lsProbe.error;
  if (!hasLastSale) console.warn('Spalte last_sale_at fehlt — Verkaufsdatum wird übersprungen');

  // Spalte player_nation vorhanden? (migrations/2026-08-25_player_nation.sql)
  const natProbe = await supabase.from('card_prices').select('player_nation').limit(1);
  const hasNation = !natProbe.error;
  if (!hasNation) console.warn('Spalte player_nation fehlt — Nationalitaet wird uebersprungen');

  // Spalten player_age/gameplay_tier vorhanden? (2026-08-25_age_and_gameplay_tier.sql)
  const ageProbe = await supabase.from('card_prices').select('player_age').limit(1);
  const hasAge = !ageProbe.error;
  if (!hasAge) console.warn('Spalte player_age fehlt — Alter wird uebersprungen');
  const gtProbe = await supabase.from('card_prices').select('gameplay_tier').limit(1);
  const hasGameplayTier = !gtProbe.error;
  if (!hasGameplayTier) console.warn('Spalte gameplay_tier fehlt — wird uebersprungen');

  // floor_price/avg_sales: die Vergleichs-Schaetzer fuer accuracy_benchmark —
  // MUESSEN aus derselben (alten) Zeile stammen wie fmv, sonst waere es Leakage.
  const cols = migrated ? 'id, player_slug, eligibility, fmv, floor_price, avg_sales, updated_at' : 'id, player_slug';
  // Queue-Query mit Retry: WHERE scarcity ORDER BY updated_at LIMIT n lief unter
  // naechtlicher Parallel-Last in den statement_timeout (57014). Root-Cause-Fix ist der
  // Index card_prices(scarcity, updated_at) — migrations/2026-07-28_card_prices_queue_index.sql.
  // Bis der greift (und fuer echte transiente Timeouts): 3 Versuche mit Backoff, danach
  // SAUBERER Abbruch (return -> exit 0) statt process.exit(1), damit Railway keinen
  // "Deploy Crashed" meldet — der naechste Cron-Tick uebernimmt die Zeilen ohnehin.
  const fetchQueue = async (limit, eligibility) => {
    if (limit <= 0) return { rows: [], err: null };
    for (let attempt = 1; attempt <= 3; attempt++) {
      let q = supabase.from('card_prices').select(cols).eq('scarcity', SCARCITY);
      if (eligibility) q = q.eq('eligibility', eligibility);
      const res = await q.order('updated_at', { ascending: true }).limit(limit);
      if (!res.error) return { rows: res.data, err: null };
      console.warn(`Batch-Query (${eligibility ?? 'alle'}) Versuch ${attempt}/3 fehlgeschlagen: ${res.error.message}`);
      if (attempt === 3) return { rows: null, err: res.error };
      await sleep(2000 * attempt);
    }
  };

  // Nach Eligibility gewichtet ziehen statt stur die ältesten Zeilen: sonst
  // bekommen In-Season und Classic gleich viele Slots, obwohl nur In-Season
  // täglich frisch sein muss (siehe IN_SEASON_SHARE oben).
  let players = null, qErr = null;
  if (migrated) {
    const nIn = Math.round(BATCH_SIZE * IN_SEASON_SHARE);
    const [a, b] = [await fetchQueue(nIn, 'in_season'), await fetchQueue(BATCH_SIZE - nIn, 'classic')];
    qErr = a.err ?? b.err;
    if (!qErr) {
      // Schöpft eine Sorte ihr Kontingent nicht aus (zu wenige Zeilen), geht der
      // Rest an die andere — sonst bliebe der Slot ungenutzt.
      let fill = [];
      const rest = BATCH_SIZE - a.rows.length - b.rows.length;
      if (rest > 0) {
        const seen = new Set([...a.rows, ...b.rows].map(r => r.id));
        const f = await fetchQueue(BATCH_SIZE, a.rows.length < nIn ? 'classic' : 'in_season');
        if (f.rows) fill = f.rows.filter(r => !seen.has(r.id)).slice(0, rest);
      }
      // Wieder nach Alter mischen, damit die ältesten Zeilen zuerst drankommen —
      // falls der Lauf vorzeitig endet, sind dann die dringendsten erledigt.
      players = [...a.rows, ...b.rows, ...fill]
        .sort((x, y) => String(x.updated_at ?? '').localeCompare(String(y.updated_at ?? '')));
    }
  } else {
    ({ rows: players, err: qErr } = await fetchQueue(BATCH_SIZE, null));
  }
  if (qErr) {
    console.error(`Batch-Query nach 3 Versuchen fehlgeschlagen — sauberer Abbruch (kein Crash): ${qErr.message}`);
    return;
  }
  const nInS = players.filter(p => (p.eligibility ?? 'in_season') === 'in_season').length;
  console.log(`Processing ${players.length} ${SCARCITY} rows (in-season ${nInS} / classic ${players.length - nInS})...`);

  let updated = 0, failed = 0, skipped = 0;
  const today = new Date().toISOString().split('T')[0];
  const startedAt = Date.now();

  for (const player of players) {
    if (Date.now() - startedAt > MAX_RUN_MS) { skipped = players.length - updated - failed; break; }
    const eligibility = player.eligibility ?? 'in_season';
    const result = await fetchData(player.player_slug, eligibility);
    if (!result) {
      // Echter Fehlschlag (API-Fehler): nichts überschreiben, nur Queue-Position
      await supabase.from('card_prices').update({ updated_at: new Date().toISOString() }).eq('id', player.id);
      failed++; await sleep(DELAY_MS); continue;
    }
    // WICHTIG: leere Sales sind KEIN Fehlschlag, sondern ein Marktzustand —
    // die Zeile wird voll verarbeitet (fmv→null via v3.1, alte Werte werden
    // ÜBERSCHRIEBEN statt konserviert). Vorher blieben Troll-FMVs ewig stehen
    // (BUG-011: Denholm 1999,99 € bei 0 Sales).
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
          // Gegenprobe (22.08.): Floor und einfacher Durchschnitt aus DEMSELBEN
          // Stand — nur so sind alle drei Schaetzer fair vergleichbar.
          ...(hasBenchmarks ? { floor_est: player.floor_price ?? null,
                                avg_sales_est: player.avg_sales ?? null } : {}),
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

    // Historie einmal laden (ersetzt die frühere separate Abfrage in calcChanges)
    const hist = await loadHistory(player.player_slug, eligibility, now, migrated);

    // NUR schreiben, wenn sich der Preis geaendert hat. Vorher entstand pro Karte
    // und Tag eine Zeile — bei ~104k bewerteten Karten rund 100.000 Zeilen taeglich,
    // die meisten davon Wiederholungen desselben Werts. Auswertungen tragen den
    // letzten bekannten Wert ohnehin vor (Carry-Forward), verlieren also nichts.
    if (fmv) {
      const last = hist.length ? hist[hist.length - 1] : null;
      if (!last || Number(last.price) !== Number(fmv)) {
        const histRow = { player_slug: player.player_slug, scarcity: SCARCITY, price: fmv, recorded_at: today };
        if (migrated) histRow.eligibility = eligibility;
        const { error: hErr } = await supabase.from('price_history').upsert(histRow, {
          onConflict: migrated ? 'player_slug,scarcity,recorded_at,eligibility' : 'player_slug,scarcity,recorded_at',
        });
        if (hErr) console.warn(`  History-Insert ${player.player_slug}: ${hErr.message}`);
        else hist.push({ price: fmv, recorded_at: today });   // fuer die Prozente unten
      }
    }

    const update = {
      floor_price:  floorPrice,
      fmv,
      sale_1: sales[0]?.eur ?? null, sale_2: sales[1]?.eur ?? null,
      sale_3: sales[2]?.eur ?? null, sale_4: sales[3]?.eur ?? null,
      sale_5: sales[4]?.eur ?? null,
      // Datum des juengsten Verkaufs: "6,72 EUR" sagt wenig, wenn unklar ist, ob
      // der Verkauf gestern oder vor drei Wochen war (Liquiditaets-Kontext).
      // Nur setzen, wenn die Spalte da ist — sonst scheitert JEDES Update.
      ...(hasLastSale ? { last_sale_at: sales[0]?.date ?? null } : {}),
      avg_sales:   sales.length ? parseFloat((sales.slice(0, 10).reduce((s, p) => s + p.eur, 0) / Math.min(sales.length, 10)).toFixed(2)) : null,
      sales_count: sales.filter(s => new Date(s.date) >= h24ago).length,
      sales_72h:   sales.filter(s => new Date(s.date) >= h72ago).length,
      sales_7d:    sales.filter(s => new Date(s.date) >= d7ago).length,
      updated_at:  new Date().toISOString(),
      ...(result.cardPic ? { picture_url: result.cardPic } : {}), // nur setzen, wenn vorhanden — nie löschen
      // Club-Felder als EINHEIT schreiben, sobald ein Club bekannt ist — auch
      // wenn die Liga darin null ist. Vorher blieb beim Vereinswechsel zu einem
      // Club ohne domesticLeague (Sorare: z. B. Leicester) die Liga des ALTEN
      // Vereins stehen → Ipswich-Abgaenge standen als Leicester-Spieler in der
      // "Premier League", Swansea-Abgaenge in der "Championship" (Nutzer-Report
      // 25.08.: "Englands 1. und 2. Liga vermischt"). Ohne Club: nichts anfassen.
      ...(result.hasClub ? {
        team_name:      result.teamName,
        league_name:    result.leagueName,
        league_country: result.leagueCountry,
      } : {}),
      ...(result.position ? { position: result.position } : {}),
      // Nationalitaet aendert sich nie — schreiben wenn bekannt, nie auf null
      ...(hasNation && result.nation ? { player_nation: result.nation } : {}),
      // Alter bei jeder Beruehrung frisch (Geburtstage), nie auf null
      ...(hasAge && result.age != null ? { player_age: result.age } : {}),
      // Gameplay-Tier (GOAT/STAR/IMPACT/ROSTER/DNP) aendert sich mit der Form
      ...(hasGameplayTier && result.gameplayTier ? { gameplay_tier: result.gameplayTier } : {}),
    };
    if (migrated) Object.assign(update, calcChanges(hist, fmv, now));

    const { error: e } = await supabase.from('card_prices').update(update).eq('id', player.id);
    if (e) { console.warn(`  DB update failed for ${player.player_slug}: ${e.message}`); failed++; }
    else updated++;

    await sleep(DELAY_MS);
  }
  console.log(`[${new Date().toISOString()}] Done. Updated: ${updated}, Failed: ${failed}, Skipped: ${skipped}` +
    ` (${((Date.now() - startedAt) / 1000).toFixed(0)}s Laufzeit, ${((Date.now() - startedAt) / Math.max(1, updated + failed)).toFixed(0)} ms/Karte)`);
}

main().catch(console.error);
