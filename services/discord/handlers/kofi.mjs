// Was passiert mit einem Ko-fi-Event? Eine Pipeline aus kleinen Schritten.
//
// ERWEITERN: neue Funktion schreiben, unten in STEPS einhaengen. Jeder Schritt
// bekommt das Event und einen gemeinsamen Kontext (ctx), in den er Ergebnisse
// legen kann (z. B. die Discord-Nachrichten-ID). Wirft ein Schritt, laufen die
// uebrigen trotzdem — ein kaputter Milestone-Check darf die Dankesnachricht
// nicht verhindern.
//
// Geplante Schritte (siehe HANDOFF): assignRole (braucht Bot-Token),
// checkMilestone (z. B. jeder 10. Supporter), monthlyDigest (Cron, nicht hier).
import { buildEmbed } from '../lib/format.mjs';
import { postToDiscord } from '../lib/discord.mjs';
import { alreadySeen, remember, logEvent, stats, storeEnabled } from '../lib/store.mjs';

async function dedup(ev, ctx) {
  if (await alreadySeen(ev)) { ctx.skip = 'duplicate'; return; }
  remember(ev.id);   // ab jetzt gilt die ID als verarbeitet, auch wenn das Posten noch laeuft
}

async function enrich(ev, ctx) {
  if (ctx.skip) return;
  Object.assign(ctx, await stats());
}

async function announce(ev, ctx) {
  if (ctx.skip) return;
  const payload = buildEmbed(ev, ctx);
  ctx.discordMessageId = await postToDiscord(payload);
  console.log(`[kofi] gepostet: ${ev.kind} ${ev.amount} ${ev.currency} von ${ev.name ?? 'anonym'}`
    + (ev.tier ? ` (${ev.tier.name})` : ''));
}

async function persist(ev, ctx) {
  if (ctx.skip) return;
  await logEvent(ev, ctx.discordMessageId ?? null);
}

const STEPS = [dedup, enrich, announce, persist];

export async function handleKofiEvent(ev) {
  const ctx = {};
  for (const step of STEPS) {
    try { await step(ev, ctx); }
    catch (e) { console.error(`[kofi] Schritt ${step.name} fehlgeschlagen:`, e.message); }
  }
  if (ctx.skip) console.log(`[kofi] uebersprungen (${ctx.skip}): ${ev.id}`);
  return ctx;
}

if (!storeEnabled) console.warn('[store] kein Supabase konfiguriert — Dedup nur im Speicher, keine Statistik');
