// Discord-Embed im Sorion-Look.
//
// DESIGN-VORGABE (Jonas): minimalistisch, dunkel, weiss mit dezenten
// Lila/Magenta-Akzenten — hochwertig, kein "Donation Bot". Deshalb:
//   * keine Ausrufezeichen-Kaskaden, keine GIFs, kein "OMG THANK YOU"
//   * eine ruhige Ueberschrift, ein Satz, der Betrag als eigene Zeile
//   * die Nachricht des Spenders als Zitat, wenn vorhanden
//   * Farbleiste = Sorion-Lila, VIP bekommt Magenta
//   * Verlaengerungen deutlich leiser als Neueintritte
//
// Discord rendert die linke Farbleiste aus "color"; Titel/Description sind
// Markdown. Mehr Gestaltung gibt ein Webhook nicht her — das ist gut so.
const SITE = 'https://sorion.pro';
const PURPLE = 0xb060ff;

const money = (amount, cur) => {
  const sym = { EUR: '€', USD: '$', GBP: '£' }[cur] ?? `${cur} `;
  return `${sym}${amount.toFixed(2)}`;
};
// Oeffentlicher Spender mit verknuepftem Discord: als <@id> — Discord rendert
// den klickbaren Namen. allowed_mentions unten verhindert den Ping.
const who = ev => ev.isPublic && ev.discordUserId ? `<@${ev.discordUserId}>`
                : ev.name ? `**${escapeMd(ev.name)}**` : 'Someone';
const escapeMd = s => String(s).replace(/([*_`~|>\\])/g, '\\$1');
const quote = msg => msg ? `\n\n> ${escapeMd(msg).split('\n').join('\n> ')}` : '';

export function buildEmbed(ev, ctx = {}) {
  const amt = money(ev.amount, ev.currency);
  let title, body, color = PURPLE;

  if (ev.kind === 'donation') {
    title = '☕  New supporter';
    body  = `${who(ev)} just bought SORION a coffee.\n\`${amt}\`${quote(ev.message)}`;
  } else if (ev.kind === 'sub_new') {
    const t = ev.tier;
    color = t.accent;
    title = `${t.icon}  ${t.name}`;
    body  = `${who(ev)} joined as **${t.name}**.\n\`${amt} / month\``
          + (t.blurb ? `\n${t.blurb}.` : '')
          + quote(ev.message);
  } else {  // sub_renew: leise
    const t = ev.tier;
    color = t.accent;
    title = `${t.icon}  ${t.name} · renewed`;
    body  = `${who(ev)} is staying on as **${t.name}**.\n\`${amt} / month\``;
  }

  // Kontext-Zeile fuer spaetere Ausbaustufen (Milestones, Zaehler) — heute
  // nur, wenn der Store Zahlen liefert.
  const footerBits = ['sorion.pro'];
  if (ctx.supportersTotal) footerBits.push(`${ctx.supportersTotal} supporters`);
  if (ctx.monthTotal)      footerBits.push(`${money(ctx.monthTotal, 'EUR')} this month`);

  // Kein username/avatar_url: Name und Bild kommen aus der Webhook-Einstellung
  // in Discord (Jonas hat den Bot dort "Penny" genannt). So bleibt die Pflege
  // an einem Ort und der Code muss bei einer Umbenennung nicht angefasst werden.
  return {
    allowed_mentions: { parse: [] },   // Name verlinken, niemanden benachrichtigen
    embeds: [{
      title,
      description: body,
      color,
      url: SITE,
      footer: { text: footerBits.join('  ·  ') },
      timestamp: ev.at.toISOString(),
    }],
  };
}
