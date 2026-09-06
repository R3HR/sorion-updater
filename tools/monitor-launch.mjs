// Launch-Monitor: Zugriffe heute vs gestern (Herkunft, Seiten, Ereignisse) + Backend-Antwortzeiten.
//   railway run --service "Updater Limited" node tools/monitor-launch.mjs
// Liest analytics_events direkt (Service-Key); die Admin-RPCs sind absichtlich an Jonas' Login gebunden.
import { createClient } from '@supabase/supabase-js';
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { auth: { persistSession: false } });
const ANON = 'sb_publishable_cplVdUDlMw1S5IcjlwxPTA_Mx8G7016';
const stamp = new Date().toISOString().slice(11, 16) + ' UTC';
const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });
const yday = new Date(Date.now() - 86400e3).toLocaleDateString('sv-SE', { timeZone: 'Europe/Berlin' });

const rows = [];
for (let off = 0; ; off += 1000) {
  const { data, error } = await sb.from('analytics_events').select('day,event,path,referrer_host,country,device,visitor_hash,created_at')
    .eq('site', 'sorion').gte('day', yday).order('created_at', { ascending: false }).range(off, off + 999);
  if (error) { console.log('DB-Fehler:', error.message); break; }
  rows.push(...data); if (data.length < 1000) break;
}
const T = rows.filter(r => r.day === today), Y = rows.filter(r => r.day === yday);
const pv = a => a.filter(r => r.event === 'pageview').length;
const vis = a => new Set(a.map(r => r.visitor_hash)).size;
const top = (a, key, n = 8) => { const m = {}; for (const r of a) { const k = r[key] ?? 'direct'; m[k] = (m[k] || 0) + 1; } return Object.entries(m).sort((x, y) => y[1] - x[1]).slice(0, n); };
const lastHour = T.filter(r => Date.now() - new Date(r.created_at) < 3600e3);

console.log(`\n══ Launch-Monitor ${stamp} ══`);
console.log(`Heute:   ${pv(T)} Pageviews, ${vis(T)} Besucher   |   letzte Stunde: ${pv(lastHour)} PV, ${vis(lastHour)} Besucher`);
console.log(`Gestern: ${pv(Y)} Pageviews, ${vis(Y)} Besucher (ganzer Tag)`);
console.log('\nHerkunft heute:      ' + top(T.filter(r => r.event === 'pageview'), 'referrer_host').map(([k, v]) => `${k} ${v}`).join('  ·  '));
console.log('Seiten heute:        ' + top(T.filter(r => r.event === 'pageview'), 'path').map(([k, v]) => `${k} ${v}`).join('  ·  '));
console.log('Ereignisse heute:    ' + top(T.filter(r => r.event !== 'pageview'), 'event', 10).map(([k, v]) => `${k} ${v}`).join('  ·  '));
console.log('Laender heute:       ' + top(T.filter(r => r.country), 'country', 6).map(([k, v]) => `${k} ${v}`).join('  ·  '));

console.log('\nBackend:');
const H = { 'Content-Type': 'application/json', apikey: ANON, Authorization: 'Bearer ' + ANON };
const timed = async (label, f) => { const t = Date.now(); try { const v = await f(); console.log(`   ${label}: ${Date.now() - t} ms ${v ?? ''}`); } catch (e) { console.log(`   ${label}: FEHLER ${e.message.slice(0, 60)}`); } };
await timed('sync-portfolio (gecacht)', async () => { const r = await fetch('https://jxhdlcpdupmkpsoytzes.supabase.co/functions/v1/sync-portfolio', { method: 'POST', headers: H, body: JSON.stringify({ slug: 'jr3hr' }) }); const d = await r.json(); return `${r.status} ${d.skipped ?? d.error ?? 'synced'}`; });
// Seit 05.09. 13:05 UTC nutzt accuracy.html die Cache-Function, nicht die RPC direkt.
// Gemessen wird deshalb, was Besucher wirklich treffen (X-Cache zeigt hit/stale/miss).
await timed('accuracy-benchmark (Function)', async () => { const r = await fetch('https://jxhdlcpdupmkpsoytzes.supabase.co/functions/v1/accuracy-benchmark?days=3', { headers: H }); return r.status === 200 ? `ok cache=${r.headers.get('x-cache')}` : `HTTP ${r.status}`; });
await timed('card_prices (Markt)', async () => { const r = await fetch('https://jxhdlcpdupmkpsoytzes.supabase.co/rest/v1/card_prices?select=player_slug&limit=1', { headers: H }); return `HTTP ${r.status}`; });
const since = new Date(Date.now() - 3600e3).toISOString();
const { count: synced } = await sb.from('manager_sync').select('*', { count: 'exact', head: true }).gte('synced_at', since);
const { data: errs } = await sb.from('manager_sync').select('sorare_slug,last_error').not('last_error', 'is', null).gte('synced_at', since).limit(8);
console.log(`\nPortfolios synchronisiert (letzte Stunde): ${synced ?? '?'}   Sync-Fehler: ${errs?.length ?? 0}` + (errs?.length ? '\n   ' + errs.map(e => `${e.sorare_slug}: ${e.last_error}`).join('\n   ') : ''));
