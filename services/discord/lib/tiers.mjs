// Ko-fi-Mitgliedschaften von Sorion (Stand 04.09.2026, Entscheidung Jonas).
//
// Die Zuordnung laeuft ueber den Tier-Namen, den Ko-fi mitsendet. Als
// Rueckfallebene dient der Betrag — falls ein Tier bei Ko-fi umbenannt wird,
// bleibt die Erkennung stabil.
//
// "discordRole" ist heute noch leer: Die Rollenvergabe braucht einen echten
// Bot (Webhooks koennen keine Rollen setzen). Die Felder sind vorbereitet,
// damit spaeter nur der Handler dazukommt, nicht die Konfiguration.
export const TIERS = [
  { key: 'supporter', name: 'Supporter',     monthly: 0.50, icon: '💎', accent: 0xb060ff, discordRole: null,
    blurb: 'keeps the lights on' },
  { key: 'pro',       name: 'Pro-Supporter', monthly: 5.00, icon: '💎', accent: 0xc478ff, discordRole: null,
    blurb: 'funds a week of servers' },
  { key: 'vip',       name: 'Sorion VIP',    monthly: 25.00, icon: '💎', accent: 0xff2d78, discordRole: null,
    blurb: 'covers more than half a month of running SORION' },
];

const norm = s => String(s ?? '').toLowerCase().replace(/[^a-z]/g, '');

/** Tier ueber Namen, sonst ueber Betrag (naechstliegender Monatsbeitrag). */
export function resolveTier(tierName, amount) {
  if (tierName) {
    const n = norm(tierName);
    // Erst exakter Name ueber ALLE Tiers, dann erst Teilstring — und dabei den
    // laengsten Treffer bevorzugen. Sonst faengt "supporter" auch
    // "prosupporter" ab (Testfund 04.09.).
    const exact = TIERS.find(t => norm(t.name) === n);
    if (exact) return exact;
    const partial = TIERS.filter(t => n.includes(norm(t.name)) || n.includes(t.key))
                         .sort((a, b) => norm(b.name).length - norm(a.name).length)[0];
    if (partial) return partial;
  }
  if (Number.isFinite(amount)) {
    let best = null, dist = Infinity;
    for (const t of TIERS) {
      const d = Math.abs(t.monthly - amount);
      if (d < dist) { dist = d; best = t; }
    }
    // nur akzeptieren, wenn der Betrag halbwegs passt (Ko-fi rundet gelegentlich)
    if (best && dist <= Math.max(0.6, best.monthly * 0.2)) return best;
  }
  return { key: 'member', name: tierName || 'Member', monthly: amount, icon: '💎', accent: 0xb060ff,
           discordRole: null, blurb: null };
}
