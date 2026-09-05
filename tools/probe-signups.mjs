// Abgleich: signup_done-Ereignisse vs. tatsaechlich angelegte Profile (heute).
import { createClient } from '@supabase/supabase-js';
const sb = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_KEY, { auth: { persistSession: false } });
const today = new Date().toISOString().slice(0, 10);
const { data: ev } = await sb.from('analytics_events').select('event,created_at,path').eq('site', 'sorion').eq('day', today).in('event', ['signup_done', 'login_done']).order('created_at');
console.log('Ereignisse heute:'); for (const e of ev ?? []) console.log(`   ${e.created_at.slice(11, 19)}  ${e.event}  ${e.path}`);
const { data: prof, error } = await sb.from('profiles').select('user_id,created_at,sorare_slug,display_name').gte('created_at', today).order('created_at');
if (error) console.log('profiles-Fehler:', error.message);
console.log(`\nProfile angelegt heute: ${prof?.length ?? 0}`); for (const p of prof ?? []) console.log(`   ${p.created_at.slice(11, 19)}  slug=${p.sorare_slug ?? '-'}  name=${p.display_name ?? '-'}`);
const { count: total } = await sb.from('profiles').select('*', { count: 'exact', head: true });
const { count: linked } = await sb.from('profiles').select('*', { count: 'exact', head: true }).not('sorare_slug', 'is', null);
console.log(`\nProfile gesamt: ${total}, davon mit Sorare-Slug: ${linked}`);
// auth.users ueber die Admin-API (Service-Key), heute angelegt + bestaetigt?
const r = await fetch(`${process.env.SUPABASE_URL}/auth/v1/admin/users?per_page=200`, { headers: { apikey: process.env.SUPABASE_SERVICE_KEY, Authorization: 'Bearer ' + process.env.SUPABASE_SERVICE_KEY } });
const u = await r.json(); const users = u.users ?? [];
const todayUsers = users.filter(x => x.created_at?.startsWith(today));
console.log(`\nauth.users gesamt: ${users.length}, heute angelegt: ${todayUsers.length}`);
for (const x of todayUsers) console.log(`   ${x.created_at.slice(11, 19)}  bestaetigt=${x.email_confirmed_at ? 'ja' : 'NEIN'}  letzter Login=${x.last_sign_in_at?.slice(11, 19) ?? '-'}`);
