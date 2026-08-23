// SORION — FMV-Backtest-Harness
// ═══════════════════════════════════════════════════════════════════════════
// FRAGE: Wie muessen SELL_CAP und FLOOR_BLEND stehen, damit der FMV moeglichst
// nah am naechsten tatsaechlichen Verkauf liegt?
//
// METHODE (Walk-Forward, ohne Leakage):
//   Fuer jede Karte holen wir die letzten 20 Verkaeufe (Sorare, mit Datum).
//   Dann wird JEDER Verkauf ab dem 4. der Reihe nach zum "Ziel": vorhergesagt
//   wird er ausschliesslich aus den AELTEREN Verkaeufen — die Formel sieht ihr
//   eigenes Ziel nie. Gemessen wird die Abweichung Vorhersage vs. Realitaet.
//
// WARUM DAS DIE SELEKTION AUFHEBT: Alle Varianten laufen ueber EXAKT dieselben
// Karten und dieselben Zielverkaeufe. Der Einwand "ungedeckelte Karten sind
// ohnehin leichter zu schaetzen" trifft damit jede Variante gleich — die
// Rangfolge bleibt aussagekraeftig.
//
// EHRLICHE GRENZE: Wir kennen den Floor nur von HEUTE, nicht von damals. Er
// geht als Naeherung in alle Varianten gleich ein; die absoluten Zahlen sind
// dadurch unschaerfer als die RANGFOLGE. Deshalb entscheidet der Backtest die
// Richtung, und die Live-Messung (accuracy_benchmark) bestaetigt sie danach.
//
// AUFRUF:  node tools/fmv-backtest.mjs [anzahlKarten]
//   Ohne Argument 250 Karten. Braucht SUPABASE_URL + SUPABASE_SERVICE_KEY nicht —
//   liest oeffentlich; Sorare-Key optional via SORARE_APIKEY.
//   API-Last: 1 Sorare-Call je Karte, 1,2 s Pause. 250 Karten ~ 5 Minuten.
//   BITTE ausserhalb der Updater-Fenster laufen lassen (frei: 05-15, 21 UTC).
// ═══════════════════════════════════════════════════════════════════════════

const SUPA = 'https://jxhdlcpdupmkpsoytzes.supabase.co/rest/v1';
const ANON = 'sb_publishable_cplVdUDlMw1S5IcjlwxPTA_Mx8G7016';
const SORARE = 'https://api.sorare.com/graphql';
const APIKEY = process.env.SORARE_APIKEY ?? null;
const N = parseInt(process.argv[2] ?? '250', 10);
const DELAY = 1200;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const H = { apikey: ANON, Authorization: 'Bearer ' + ANON };

// ── Die Formel, mit den zu testenden Stellschrauben als Parameter ──────────
// (Bewusst hier nachgebaut statt lib/fmv.mjs importiert: dort sind CAP und
//  BLEND Konstanten. Die uebrige Logik ist identisch — Zeit-Decay, Trimmen,
//  Floor nur nach unten, kein Sale => null.)
function fmv(sales, floor, now, o) {
  const halfLife = o.halfLifeDays, maxAge = o.maxAgeDays;
  let e = (sales || [])
    .filter(s => s && s.eur > 0)
    .map(s => ({ v: s.eur, w: Math.pow(0.5, Math.max(0, (now - new Date(s.date).getTime()) / 86400000) / halfLife),
                 age: Math.max(0, (now - new Date(s.date).getTime()) / 86400000) }))
    .filter(x => x.age <= maxAge);
  if (e.length >= 5) { e.sort((a, b) => a.v - b.v); e = e.slice(1, -1); }
  if (!e.length) return null;
  const tw = e.reduce((s, x) => s + x.w, 0);
  const sv = e.reduce((s, x) => s + x.v * x.w, 0) / tw;
  if (!(typeof floor === 'number' && floor > 0)) return sv;
  if (floor >= sv) return sv;
  const blended = o.floorBlend * floor + (1 - o.floorBlend) * sv;
  return o.sellCap === Infinity ? blended : Math.min(blended, floor * o.sellCap);
}

// ── Varianten ──────────────────────────────────────────────────────────────
const VAR = [];
for (const cap of [1.05, 1.15, 1.25, 1.50, Infinity])
  for (const blend of [0.35, 0.20, 0.00])
    VAR.push({ name: `cap ${cap === Infinity ? '∞' : cap.toFixed(2)} · blend ${blend.toFixed(2)}`,
               sellCap: cap, floorBlend: blend });

async function main() {
  // 1) Stichprobe: Karten mit genug Marktaktivitaet, quer ueber die Sorten
  console.log(`Stichprobe zusammenstellen (${N} Karten)…`);
  const cards = [];
  for (const [sc, el] of [['limited','in_season'], ['limited','classic'], ['rare','in_season'], ['super_rare','in_season']]) {
    const take = Math.ceil(N / 4);
    const r = await fetch(`${SUPA}/card_prices?select=player_slug,scarcity,eligibility,floor_price`
      // Bewusst NICHT nach sales_7d absteigend: das haette nur die liquidesten
      // (= am leichtesten schaetzbaren) Karten genommen und die absoluten Zahlen
      // geschoent. sales_7d>=1 sichert nur genug Historie fuer den Walk-Forward.
      + `&scarcity=eq.${sc}&eligibility=eq.${el}&floor_price=not.is.null&sales_7d=gte.1`
      + `&order=id.asc&limit=${take}`, { headers: H });
    if (r.ok) cards.push(...await r.json());
  }
  console.log(`  ${cards.length} Karten\n`);

  // 2) Verkaufshistorien holen
  const data = [];
  for (let i = 0; i < cards.length; i++) {
    const c = cards[i];
    const elig = c.eligibility === 'classic' ? 'CLASSIC' : 'IN_SEASON';
    const q = `{tokens{tokenPrices(rarity:${c.scarcity} seasonEligibility:${elig} playerSlug:"${c.player_slug}" first:20){date amounts{eurCents}}}}`;
    try {
      const r = await fetch(SORARE, { method: 'POST',
        headers: { 'Content-Type': 'application/json', ...(APIKEY ? { APIKEY } : {}) },
        body: JSON.stringify({ query: q }) });
      const j = await r.json();
      const sales = (j?.data?.tokens?.tokenPrices ?? [])
        .map(p => ({ date: p.date, eur: (p.amounts?.eurCents ?? 0) / 100 }))
        .filter(s => s.eur > 0);
      if (sales.length >= 6) data.push({ ...c, sales });
    } catch {}
    if ((i + 1) % 25 === 0) process.stdout.write(`  ${i + 1}/${cards.length}\r`);
    await sleep(DELAY);
  }
  console.log(`  ${data.length} Karten mit >= 6 Verkaeufen\n`);

  // 3) Walk-Forward je Variante
  const profile = el => el === 'classic' ? { halfLifeDays: 14, maxAgeDays: 90 } : { halfLifeDays: 3, maxAgeDays: 21 };
  const results = VAR.map(v => ({ v, abs: [], sig: [], capped: 0, total: 0 }));

  for (const card of data) {
    const s = card.sales;                       // neueste zuerst
    for (let t = 0; t < s.length - 3; t++) {    // Ziel = s[t], Historie = s[t+1..]
      const target = s[t];
      const hist = s.slice(t + 1);
      const now = new Date(target.date).getTime();
      for (const R of results) {
        const p = fmv(hist, card.floor_price, now, { ...profile(card.eligibility), ...R.v });
        if (!p || p <= 0) continue;
        R.total++;
        if (R.v.sellCap !== Infinity && Math.abs(p - card.floor_price * R.v.sellCap) < 0.005) R.capped++;
        const d = (target.eur - p) / p * 100;
        R.abs.push(Math.abs(d)); R.sig.push(d);
      }
    }
  }

  // 4) Ausgabe
  const q = (a, p) => { const x = [...a].sort((m, n) => m - n); return x[Math.floor(p * (x.length - 1))]; };
  console.log('Variante                    n      Median-Abw   Bias      <20%   am Deckel');
  console.log('─'.repeat(78));
  const scored = results.filter(R => R.abs.length > 100)
    .map(R => ({ R, med: q(R.abs, .5), bias: q(R.sig, .5), hit: R.abs.filter(x => x <= 20).length / R.abs.length * 100 }))
    .sort((a, b) => a.med - b.med);
  for (const { R, med, bias, hit } of scored) {
    console.log(`${R.v.name.padEnd(24)} ${String(R.abs.length).padStart(6)}   ${med.toFixed(1).padStart(6)}%  ${((bias > 0 ? '+' : '') + bias.toFixed(1)).padStart(7)}%  ${hit.toFixed(0).padStart(4)}%   ${(R.capped / R.total * 100).toFixed(0).padStart(4)}%`);
  }
  const heute = scored.find(x => x.R.v.sellCap === 1.05 && x.R.v.floorBlend === 0.35);
  const best  = scored[0];
  console.log('─'.repeat(78));
  console.log(`HEUTE   ${heute ? heute.R.v.name + '  Median ' + heute.med.toFixed(1) + '%  Bias ' + (heute.bias > 0 ? '+' : '') + heute.bias.toFixed(1) + '%' : '—'}`);
  console.log(`BESTE   ${best.R.v.name}  Median ${best.med.toFixed(1)}%  Bias ${(best.bias > 0 ? '+' : '') + best.bias.toFixed(1)}%`);
  if (heute) console.log(`\nVerbesserung: ${(heute.med - best.med).toFixed(1)} Prozentpunkte Median-Abweichung`
    + `, Verzerrung ${Math.abs(heute.bias).toFixed(1)}% -> ${Math.abs(best.bias).toFixed(1)}%`);
  console.log('\nHinweis: Der Floor ist der HEUTIGE (historische Floors speichern wir nicht).');
  console.log('Die Rangfolge ist davon unberuehrt — alle Varianten nutzen denselben Wert.');
}

main().catch(e => { console.error('Fehlgeschlagen:', e.message); process.exit(1); });
