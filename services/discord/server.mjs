// ═══════════════════════════════════════════════════════════════════════════
// SORION Discord-Service  (Railway, Web-Service)
//
// Nimmt Ko-fi-Webhooks an (Spenden, Mitgliedschaften) und postet sie als
// gestaltete Nachricht in den Discord-Kanal "💰・donations".
//
// AUFBAU (bewusst in kleine Teile getrennt, damit der Service mit der
// Community mitwachsen kann, ohne dass diese Datei anschwillt):
//   lib/kofi.mjs      Payload lesen, Token pruefen, in ein Event normalisieren
//   lib/tiers.mjs     Ko-fi-Tiers (Supporter / Pro / VIP) und ihre Darstellung
//   lib/format.mjs    Discord-Embed im Sorion-Look bauen
//   lib/discord.mjs   Embed an den Webhook schicken (mit Retry)
//   lib/store.mjs     Event-Log in Supabase (optional; Basis fuer Milestones/Statistik)
//   handlers/kofi.mjs Pipeline: was mit einem Event alles passieren soll
//
// ERWEITERN: neue Aktion in handlers/kofi.mjs eintragen (z. B. Rollenvergabe,
// Milestone-Check) — der Rest bleibt unangetastet.
//
// ENV (Railway-Variablen, nie im Repo):
//   KOFI_VERIFICATION_TOKEN    aus ko-fi.com/manage/webhooks
//   DISCORD_DONATIONS_WEBHOOK  Webhook-URL des Kanals "💰・donations"
//   SUPABASE_URL / SUPABASE_SERVICE_KEY   optional, fuer das Event-Log
//   PORT                       setzt Railway selbst
// ═══════════════════════════════════════════════════════════════════════════
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { parseKofiRequest } from './lib/kofi.mjs';
import { handleKofiEvent } from './handlers/kofi.mjs';

const PORT = Number(process.env.PORT ?? 8787);
const MAX_BODY = 64 * 1024;        // Ko-fi-Payloads sind wenige hundert Byte

for (const k of ['KOFI_VERIFICATION_TOKEN', 'DISCORD_DONATIONS_WEBHOOK']) {
  if (!process.env[k]) console.warn(`[warn] ${k} fehlt — Webhook-Verarbeitung wird scheitern`);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0; const chunks = [];
    req.on('data', c => {
      size += c.length;
      if (size > MAX_BODY) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

const send = (res, status, body) => {
  res.writeHead(status, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
};

const server = createServer(async (req, res) => {
  const url = new URL(req.url, 'http://localhost');

  // Interne Seite: Traffic-Statistik (Admin-Dashboard, UI/stats.html via
  // tools/build-stats-page.mjs). Hier ausgeliefert, weil Supabase-Functions HTML
  // erzwungen als text/plain senden und file:// keine Passwort-Manager erlaubt
  // (05.09.). Der Datenzugriff bleibt serverseitig geschuetzt (is_analytics_admin);
  // die Seite selbst enthaelt nur den oeffentlichen anon-Key.
  if (req.method === 'GET' && (url.pathname === '/stats' || url.pathname === '/stats.html')) {
    try {
      const html = await readFile(new URL('./static/stats.html', import.meta.url), 'utf8');
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store',
                           'X-Robots-Tag': 'noindex, nofollow', 'Referrer-Policy': 'no-referrer' });
      return res.end(html);
    } catch { return send(res, 404, { ok: false }); }
  }

  // Healthcheck fuer Railway / Uptime-Monitor
  if (req.method === 'GET' && (url.pathname === '/' || url.pathname === '/health')) {
    return send(res, 200, { ok: true, service: 'sorion-discord', uptime_s: Math.round(process.uptime()) });
  }

  // Ko-fi ruft genau diesen Pfad auf (in ko-fi.com/manage/webhooks eintragen)
  if (req.method === 'POST' && url.pathname === '/kofi') {
    try {
      const raw = await readBody(req);
      const event = parseKofiRequest(raw, req.headers['content-type'] ?? '');
      if (!event.ok) {
        console.warn('[kofi] abgelehnt:', event.reason);
        // 200 statt 4xx: Ko-fi wiederholt sonst endlos. Der Grund steht im Log.
        return send(res, 200, { ok: false, reason: event.reason });
      }
      // Antwort sofort, Verarbeitung im Hintergrund — Ko-fi wartet nicht lange.
      send(res, 200, { ok: true });
      handleKofiEvent(event.value).catch(e => console.error('[kofi] Verarbeitung fehlgeschlagen:', e));
      return;
    } catch (e) {
      console.error('[kofi] Anfrage nicht lesbar:', e.message);
      return send(res, 200, { ok: false, reason: 'unreadable' });
    }
  }

  send(res, 404, { ok: false });
});

server.listen(PORT, () => console.log(`sorion-discord lauscht auf :${PORT}`));
