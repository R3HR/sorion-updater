// Test-Payload im Ko-fi-Format an den Service schicken.
//
//   node --env-file=.env services/discord/tools/send-test.mjs [donation|sub_new|sub_renew|vip] [URL]
//
// Ohne URL geht es an http://localhost:8787/kofi (lokaler Server). Das Token
// kommt aus KOFI_VERIFICATION_TOKEN, damit der Server die Anfrage akzeptiert.
// Achtung: Der Server postet dann WIRKLICH in Discord — fuer stille Tests den
// Server mit DISCORD_DONATIONS_WEBHOOK auf einen Test-Kanal zeigen lassen.
const kind = process.argv[2] ?? 'donation';
const url  = process.argv[3] ?? 'http://localhost:8787/kofi';
const token = process.env.KOFI_VERIFICATION_TOKEN ?? 'test-token';

const base = {
  verification_token: token,
  message_id: `test-${kind}-${Date.now()}`,
  timestamp: new Date().toISOString(),
  is_public: true,
  from_name: 'Jo Example',
  email: 'jo.example@example.com',        // wird vom Service NIE weitergegeben
  currency: 'EUR',
  url: 'https://ko-fi.com/sorionpro',
  kofi_transaction_id: `00000000-1111-2222-3333-${Date.now()}`,
  shop_items: null, shipping: null,
};
const variants = {
  donation:  { type: 'Tip', amount: '3.00', message: 'Great tool, the accuracy page convinced me.',
               is_subscription_payment: false, is_first_subscription_payment: false, tier_name: null },
  sub_new:   { type: 'Subscription', amount: '5.00', message: 'Pro it is.',
               is_subscription_payment: true, is_first_subscription_payment: true, tier_name: 'Pro-Supporter' },
  sub_renew: { type: 'Subscription', amount: '0.50', message: null,
               is_subscription_payment: true, is_first_subscription_payment: false, tier_name: 'Supporter' },
  vip:       { type: 'Subscription', amount: '25.00', message: 'Keep going.',
               is_subscription_payment: true, is_first_subscription_payment: true, tier_name: 'Sorion VIP' },
};
if (!variants[kind]) { console.error('Unbekannte Variante. Erlaubt:', Object.keys(variants).join(', ')); process.exit(1); }

const body = new URLSearchParams({ data: JSON.stringify({ ...base, ...variants[kind] }) }).toString();
const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body });
console.log(res.status, await res.text());
