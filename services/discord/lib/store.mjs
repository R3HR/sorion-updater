// Event-Log in Supabase (optional).
//
// WOZU: Milestones ("100. Supporter"), Monatsstatistik, spaeter Rollen-Sync —
// all das braucht Historie. Ohne SUPABASE_URL/SERVICE_KEY laeuft der Service
// trotzdem, dann nur ohne Zaehler im Footer.
//
// Zusaetzlich dient die Tabelle als DEDUP: Ko-fi wiederholt Webhooks, wenn
// die Antwort ausbleibt. Gleiche message_id → schon verarbeitet → nicht
// noch einmal posten. Im Speicher haelt ein Set die letzten IDs, falls die
// DB gerade nicht antwortet.
//
// SCHREIBLAST (Lehre INC-005/006): ein Insert je Spende — vernachlaessigbar.
import { createClient } from '@supabase/supabase-js';

const URL_ = process.env.SUPABASE_URL, KEY = process.env.SUPABASE_SERVICE_KEY;
const sb = (URL_ && KEY) ? createClient(URL_, KEY, { auth: { persistSession: false } }) : null;
export const storeEnabled = Boolean(sb);

const seen = new Set();                    // Dedup im Speicher (Prozess-Lebensdauer)
/** ID SOFORT merken — vor dem Posten, nicht erst beim Speichern. Sonst rutscht
 *  ein Duplikat durch, das eintrifft, waehrend das Original noch verarbeitet
 *  wird (Testfund 04.09.). */
export const remember = id => { if (!id) return; seen.add(id); if (seen.size > 500) seen.delete(seen.values().next().value); };

/** true, wenn dieses Event schon verarbeitet wurde. */
export async function alreadySeen(ev) {
  if (!ev.id) return false;
  if (seen.has(ev.id)) return true;
  if (!sb) return false;
  const { data } = await sb.from('kofi_events').select('message_id').eq('message_id', ev.id).maybeSingle();
  return Boolean(data);
}

/** Event ablegen. Kein Fehler nach aussen — das Posten darf daran nicht scheitern. */
export async function logEvent(ev, discordMessageId = null) {
  remember(ev.id);
  if (!sb) return;
  const row = {
    message_id: ev.id || null, tx_id: ev.txId || null, occurred_at: ev.at.toISOString(),
    type: ev.type, kind: ev.kind, amount: ev.amount, currency: ev.currency,
    from_name: ev.isPublic ? ev.name : null, is_public: ev.isPublic,
    message: ev.message, tier_key: ev.tier?.key ?? null, tier_name: ev.tierName,
    discord_message_id: discordMessageId,
    discord_userid: ev.discordUserId,
  };
  const { error } = await sb.from('kofi_events').upsert(row, { onConflict: 'message_id', ignoreDuplicates: true });
  if (error) console.warn('[store] Event nicht gespeichert:', error.message);
}

/** Zahlen fuer den Footer: Unterstuetzer gesamt, Summe im laufenden Monat (EUR). */
export async function stats() {
  if (!sb) return {};
  try {
    const { data, error } = await sb.rpc('kofi_stats');
    if (error || !data) return {};
    const r = Array.isArray(data) ? data[0] : data;
    return { supportersTotal: Number(r?.supporters_total ?? 0) || 0, monthTotal: Number(r?.month_total_eur ?? 0) || 0 };
  } catch { return {}; }
}
