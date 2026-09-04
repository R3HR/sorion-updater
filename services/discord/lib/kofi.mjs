// Ko-fi-Webhook: lesen, pruefen, normalisieren.
//
// Ko-fi sendet POST als application/x-www-form-urlencoded mit EINEM Feld
// "data", dessen Wert ein JSON-String ist. Wir akzeptieren zusaetzlich reines
// JSON, falls Ko-fi das Format einmal aendert oder wir intern testen.
//
// Felder laut Ko-fi (Stand 2026): verification_token, message_id, timestamp,
// type ("Tip" | "Subscription" | "Commission" | "Shop Order"), is_public,
// from_name, message, amount ("3.00"), url, email, currency,
// is_subscription_payment, is_first_subscription_payment,
// kofi_transaction_id, shop_items, tier_name, shipping.
//
// DATENSCHUTZ: "email" wird gelesen, aber NIE weitergegeben — nicht an
// Discord, nicht ins Log. Sie bleibt in dieser Datei.
import { timingSafeEqual } from 'node:crypto';
import { resolveTier } from './tiers.mjs';

const TOKEN = process.env.KOFI_VERIFICATION_TOKEN ?? '';

function safeEqual(a, b) {
  const A = Buffer.from(String(a)), B = Buffer.from(String(b));
  return A.length === B.length && timingSafeEqual(A, B);
}

function extractJson(raw, contentType) {
  if (contentType.includes('application/json')) return JSON.parse(raw);
  // Formular: data=<urlencoded JSON>
  const params = new URLSearchParams(raw);
  const data = params.get('data');
  if (data) return JSON.parse(data);
  // Fallback: vielleicht doch nacktes JSON ohne passenden Header
  return JSON.parse(raw);
}

/** @returns {{ok:true, value:KofiEvent} | {ok:false, reason:string}} */
export function parseKofiRequest(raw, contentType) {
  let p;
  try { p = extractJson(raw, contentType); }
  catch { return { ok: false, reason: 'kein JSON' }; }
  if (!p || typeof p !== 'object') return { ok: false, reason: 'leerer Payload' };

  if (!TOKEN) return { ok: false, reason: 'KOFI_VERIFICATION_TOKEN nicht gesetzt' };
  if (!safeEqual(p.verification_token ?? '', TOKEN)) return { ok: false, reason: 'Token stimmt nicht' };

  const amount = Number.parseFloat(p.amount);
  if (!Number.isFinite(amount)) return { ok: false, reason: 'Betrag unlesbar' };

  const type = String(p.type ?? '');
  const isSub = Boolean(p.is_subscription_payment) || type === 'Subscription';
  const tier = isSub ? resolveTier(p.tier_name, amount) : null;

  return {
    ok: true,
    value: {
      id:          String(p.message_id ?? p.kofi_transaction_id ?? ''),
      txId:        String(p.kofi_transaction_id ?? ''),
      at:          p.timestamp ? new Date(p.timestamp) : new Date(),
      type,                                        // Tip | Subscription | Commission | Shop Order
      kind:        isSub ? (p.is_first_subscription_payment ? 'sub_new' : 'sub_renew') : 'donation',
      amount,
      currency:    String(p.currency ?? 'EUR').toUpperCase(),
      // Ko-fi-Vorgabe: bei is_public=false Name UND Nachricht verbergen.
      name:        p.is_public === false ? null : (String(p.from_name ?? '').trim() || null),
      isPublic:    p.is_public !== false,
      message:     p.is_public === false ? null : ((p.message ?? '').toString().trim() || null),
      tierName:    p.tier_name ?? null,
      tier,                                        // aufgeloester Tier oder null
      url:         p.url ?? null,
    },
  };
}
