// Patch Notes in den Sorion-Discord posten (Webhook).
//
// REGELN (Ansage Jonas, 01.09.2026 — bindend fuer ALLE Sessions):
//   1. KEINE Interna: keine Formel-Zutaten, keine Sicherheits-/Incident-
//      Details, keine Infrastruktur/Kosten, keine Dateipfade. Nur das, was
//      Nutzer auf der Seite sehen.
//   2. Vor JEDER Veroeffentlichung muss Jonas den Entwurf gesehen und
//      freigegeben haben. Dieses Skript ist der Veroeffentlichungs-Schritt —
//      es wird erst NACH dem "Go" ausgefuehrt, nie davor.
//
// Ablauf: Entwurf als Markdown-Datei unter docs/patchnotes/ ablegen
// (Dateiname JJJJ-MM-TT_kurztitel.md, erste Zeile "# Titel"), Jonas zeigen,
// nach Freigabe posten:
//
//   node --env-file=.env tools/publish-patchnotes.mjs docs/patchnotes/<datei>.md
//
// Webhook-URL liegt NUR in der lokalen .env (git-ignoriert) — niemals ins Repo.
import { readFileSync } from 'node:fs';

const HOOK = process.env.DISCORD_PATCH_WEBHOOK;
if (!HOOK) { console.error('DISCORD_PATCH_WEBHOOK fehlt — mit  node --env-file=.env  starten'); process.exit(1); }
const file = process.argv[2];
if (!file) { console.error('Aufruf: node --env-file=.env tools/publish-patchnotes.mjs <notes.md>'); process.exit(1); }

const raw = readFileSync(file, 'utf8').trim();
if (!raw) { console.error('Datei ist leer.'); process.exit(1); }
const lines = raw.split('\n');
const title = lines[0].replace(/^#+\s*/, '').trim() || 'SORION Update';
const body = lines.slice(1).join('\n').trim();
if (body.length > 4000) { console.error(`Zu lang fuer ein Discord-Embed (${body.length}/4000 Zeichen) — kuerzen.`); process.exit(1); }

const payload = {
  username: 'SORION Updates',
  embeds: [{
    title,
    description: body,
    color: 0xb060ff,                       // Sorion-Lila
    url: 'https://sorion.pro',
    footer: { text: 'sorion.pro' },
    timestamp: new Date().toISOString(),
  }],
};

const res = await fetch(HOOK, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(payload),
});
if (res.ok || res.status === 204) console.log(`Veroeffentlicht: "${title}" (${body.length} Zeichen)`);
else { console.error('Discord antwortete', res.status, await res.text()); process.exit(1); }
