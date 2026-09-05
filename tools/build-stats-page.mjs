// Kopiert UI/stats.html (mit absoluten Verweisen) nach services/discord/static/,
// damit der Railway-Service sie unter /stats ausliefert.  (05.09.2026)
//
// WARUM SO: Jonas oeffnet die Statistik per Lesezeichen. Als file:// speichert Chrome
// keine Passwoerter und der Sitzungsspeicher ist unzuverlaessig. Supabase-Functions
// koennen kein HTML ausliefern (Gateway erzwingt text/plain, gemessen 05.09.). Der
// ohnehin laufende Railway-Service ist eine echte https-Adresse, kostet nichts extra,
// und die Seite bleibt ausserhalb des oeffentlichen Repos. Datenschutz bleibt
// serverseitig (is_analytics_admin im SQL).
//
//   node tools/build-stats-page.mjs
//   railway up services/discord --service sorion-discord --path-as-root
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs';

let html = readFileSync('UI/stats.html', 'utf8');
html = html.replace(/href="fonts\/fonts\.css"/g, 'href="https://sorion.pro/fonts/fonts.css"');
html = html.replace(/href="([a-z0-9-]+\.html)"/g, 'href="https://sorion.pro/$1"');
mkdirSync('services/discord/static', { recursive: true });
writeFileSync('services/discord/static/stats.html', html);
console.log(`services/discord/static/stats.html geschrieben (${(html.length / 1024).toFixed(1)} KB)`);
