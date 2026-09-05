// Wann wurden die Konten angelegt? Gab es heute Loeschungen?
const H = { apikey: process.env.SUPABASE_SERVICE_KEY, Authorization: 'Bearer ' + process.env.SUPABASE_SERVICE_KEY };
const r = await fetch(`${process.env.SUPABASE_URL}/auth/v1/admin/users?per_page=500`, { headers: H });
const d = await r.json(); const users = (d.users ?? []).sort((a, b) => a.created_at.localeCompare(b.created_at));
const today = new Date().toISOString().slice(0, 10);
console.log(`Konten gesamt laut Admin-API: ${users.length}  (total-Feld: ${d.total ?? '-'}, aud/Seiten: ${d.aud ?? '-'})`);
console.log(`Vor heute angelegt: ${users.filter(u => !u.created_at.startsWith(today)).length}   heute: ${users.filter(u => u.created_at.startsWith(today)).length}`);
console.log(`Mit deleted_at (soft-deleted): ${users.filter(u => u.deleted_at).length}   Banned: ${users.filter(u => u.banned_until).length}`);
console.log('\nLetzte 8 Konten nach Anlegedatum (Mail maskiert):');
for (const u of users.slice(-8)) {
  const mail = (u.email ?? '').replace(/^(.{2}).*(@.*)$/, '$1***$2');
  console.log(`   ${u.created_at.slice(0, 16)}  ${mail.padEnd(22)}  site=${u.raw_user_meta_data?.site ?? u.user_metadata?.site ?? '-'}  bestaetigt=${u.email_confirmed_at ? 'ja' : 'nein'}`);
}
// Anlegedatum-Verteilung der letzten 10 Tage
const byDay = {}; for (const u of users) { const k = u.created_at.slice(0, 10); byDay[k] = (byDay[k] || 0) + 1; }
console.log('\nNeue Konten je Tag (letzte 10 Tage mit Eintraegen):');
for (const [k, v] of Object.entries(byDay).sort().slice(-10)) console.log(`   ${k}  ${v}`);
