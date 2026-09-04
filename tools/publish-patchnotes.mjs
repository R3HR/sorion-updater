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
// Nachtraegliche Korrektur derselben Nachricht (Tippfehler, falscher Verweis):
//   node --env-file=.env tools/publish-patchnotes.mjs <datei>.md --edit
//
// KANAL-HINWEIS: Der Webhook postet in den Update-Kanal. Fuer Rueckmeldungen
// hat der Server einen eigenen Kanal — Texte deshalb auf **#feedback**
// verweisen lassen, nicht auf "hier antworten" (Ansage Jonas 04.09.).
//
// Webhook-URL liegt NUR in der lokalen .env (git-ignoriert) — niemals ins Repo.
import { readFileSync, writeFileSync, existsSync } from 'node:fs';

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

// ?wait=true laesst Discord die erstellte Nachricht zurueckgeben — nur mit
// ihrer ID laesst sich ein Post spaeter noch korrigieren (Tippfehler, falscher
// Verweis). Die ID landet neben der Entwurfsdatei als .msgid.
// --edit: bestehende Nachricht ueberschreiben statt neu posten
const EDIT = process.argv.includes('--edit');
if (EDIT) {
  const idFile = file + '.msgid';
  if (!existsSync(idFile)) { console.error('Keine .msgid neben der Datei — die Nachricht wurde nie ueber dieses Skript gepostet.'); process.exit(1); }
  const id = readFileSync(idFile, 'utf8').trim();
  const r = await fetch(`${HOOK}/messages/${id}`, {
    method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
  });
  if (r.ok) { console.log(`Korrigiert: "${title}" (Nachricht ${id})`); process.exit(0); }
  console.error('Discord antwortete', r.status, await r.text()); process.exit(1);
}

const res = await fetch(HOOK + '?wait=true', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(payload),
});
if (!res.ok && res.status !== 204) {
  console.error('Discord antwortete', res.status, await res.text());
  process.exit(1);
}
let msgId = null;
try { msgId = (await res.json())?.id ?? null; } catch {}
if (msgId) {
  writeFileSync(file + '.msgid', msgId);
  console.log(`Veroeffentlicht: "${title}" (${body.length} Zeichen), Nachricht ${msgId}`);
  console.log(`Korrektur spaeter:  node --env-file=.env tools/publish-patchnotes.mjs ${file} --edit`);
} else {
  console.log(`Veroeffentlicht: "${title}" (${body.length} Zeichen)`);
}
