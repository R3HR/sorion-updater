// Kurz-Analyse: Genauigkeit nach Tag und Liquiditaet (v3.2-Wirkungskontrolle).
// Aufruf: railway run node tools/accuracy-briefing.mjs   (nur lesend)
import { createClient } from '@supabase/supabase-js';
const s = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY);

const med = a => { if (!a.length) return null; const x = [...a].sort((p, q) => p - q); return x[Math.floor(x.length / 2)]; };

const probe = await s.from('fmv_accuracy').select('*').limit(1);
if (probe.error) { console.log('Fehler:', probe.error.message); process.exit(1); }
console.log('Spalten:', Object.keys(probe.data[0] || {}).join(', '));

let all = [], off = 0;
const since = new Date(Date.now() - 12 * 86400000).toISOString();
while (true) {
  const { data, error } = await s.from('fmv_accuracy').select('*')
    .gte('created_at', since).order('created_at', { ascending: true }).range(off, off + 999);
  if (error) { console.log('ERR', error.message); process.exit(1); }
  all.push(...data);
  if (data.length < 1000) break;
  off += 1000;
}
console.log('Zeilen (12 Tage):', all.length);

const errOf = r => r.delta_pct != null ? Math.abs(r.delta_pct) : null;
const fErr = r => (r.floor_est && r.sale_price) ? Math.abs(r.floor_est - r.sale_price) / r.sale_price * 100 : null;

const byDay = new Map();
for (const r of all) {
  // nach VORHERSAGE-Zeitpunkt gruppieren: est_at entscheidet, welche Formel-Version gemessen wird
  const d = String(r.est_at ?? r.created_at).slice(0, 10);
  if (!byDay.has(d)) byDay.set(d, { e: [], f: [] });
  const e = errOf(r); if (e != null && isFinite(e)) byDay.get(d).e.push(e);
  const f = fErr(r); if (f != null && isFinite(f)) byDay.get(d).f.push(f);
}
console.log('\nTag           n    Median-FMV%   Median-Floor%');
for (const [d, v] of [...byDay].sort()) {
  console.log(`${d}  ${String(v.e.length).padStart(5)}   ${med(v.e)?.toFixed(1).padStart(9)}%   ${(med(v.f)?.toFixed(1) ?? '  k.A.').padStart(9)}%`);
}

// Vorher/Nachher um den v3.2-Deploy (23.08. ~13:00 UTC)
const CUT = '2026-08-23T13:00:00Z';
const pre  = all.filter(r => (r.est_at ?? r.created_at) <  CUT).map(errOf).filter(x => x != null && isFinite(x));
const post = all.filter(r => (r.est_at ?? r.created_at) >= CUT).map(errOf).filter(x => x != null && isFinite(x));
console.log(`
VOR  v3.2 (est_at < 23.08. 13:00): n=${pre.length},  Median ${med(pre)?.toFixed(1)}%`);
console.log(`NACH v3.2 (est_at >= 23.08. 13:00): n=${post.length}, Median ${med(post)?.toFixed(1)}%`);
// nach Frische der Vorhersage (hours_gap)
for (const [lo, hi] of [[0, 24], [24, 72], [72, 999]]) {
  const g = all.filter(r => (r.est_at ?? r.created_at) >= CUT && r.hours_gap >= lo && r.hours_gap < hi).map(errOf).filter(x => x != null && isFinite(x));
  console.log(`  nach v3.2, Vorhersage ${lo}-${hi}h vor Verkauf: n=${g.length}, Median ${med(g)?.toFixed(1)}%`);
}
