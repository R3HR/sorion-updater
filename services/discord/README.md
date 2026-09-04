# SORION Discord-Service

Nimmt Ko-fi-Webhooks an und postet Spenden und Mitgliedschaften als gestaltete
Nachricht in den Discord-Kanal **💰・donations**. Läuft als dauerhafter
Web-Service auf Railway (`railway-discord.toml`).

## Was er heute tut

| Ereignis | Nachricht |
|---|---|
| Einmalspende | ☕ New supporter · Name, Betrag, Nachricht als Zitat |
| Neue Mitgliedschaft | 💎 Tier-Name · „joined as …", Monatsbeitrag, Tier-Satz, Zitat |
| Verlängerung | 💎 Tier · renewed · bewusst leise, ohne Zitat |
| Anonyme Spende (is_public=false) | „Someone" statt Name |

Farbleiste Sorion-Lila, Sorion VIP in Magenta. Absender „SORION" mit
og-image als Avatar. Keine Ausrufezeichen-Kaskaden, kein „Donation Bot"-Look.

## Sicherheit und Datenschutz

- Jede Anfrage muss das **Ko-fi Verification Token** tragen (zeitkonstanter Vergleich).
- Die **E-Mail des Spenders wird nie weitergegeben**, weder an Discord noch ins Log.
- Anzeigename nur, wenn der Spender ihn bei Ko-fi öffentlich gesetzt hat.
- Antwort an Ko-fi immer 200 (auch bei Ablehnung), sonst wiederholt Ko-fi endlos; der Grund steht im Railway-Log.
- **Dedup** über `message_id`: im Speicher sofort, dauerhaft in `kofi_events` (wenn Supabase konfiguriert).

## Einrichtung (einmalig)

1. **Discord**: Kanal „💰・donations" → Integrationen → Webhook anlegen → URL kopieren.
2. **Ko-fi**: <https://ko-fi.com/manage/webhooks> → Verification Token kopieren.
3. **Supabase** (optional, für Statistik/Dedup): `migrations/2026-09-04_kofi_events.sql` ausführen.
4. **Railway**: New Service → dieses Repo → Config-Datei `railway-discord.toml`.
   Variablen: `KOFI_VERIFICATION_TOKEN`, `DISCORD_DONATIONS_WEBHOOK`,
   optional `SUPABASE_URL` + `SUPABASE_SERVICE_KEY`. Public Domain erzeugen.
5. **Ko-fi**: Webhook-URL eintragen: `https://<railway-domain>/kofi`, dann „Send single test".

## Lokal testen

```bash
# Server (Token/Webhook aus .env)
node --env-file=.env services/discord/server.mjs

# Beispiel-Ereignis schicken (postet WIRKLICH an den konfigurierten Webhook)
node --env-file=.env services/discord/tools/send-test.mjs donation
node --env-file=.env services/discord/tools/send-test.mjs vip
```

Für stille Tests den Webhook in `.env` auf einen Test-Kanal zeigen lassen.

## Erweitern

Die Verarbeitung ist eine Pipeline in `handlers/kofi.mjs`:
`dedup → enrich → announce → persist`. Neue Fähigkeit = neue Funktion, in
`STEPS` einhängen. Ein fehlschlagender Schritt stoppt die anderen nicht.

Vorbereitet, aber noch nicht gebaut:

- **Rollenvergabe** nach Tier: braucht einen Discord-**Bot** (Webhooks können keine Rollen setzen) und die Zuordnung Ko-fi-Spender → Discord-Nutzer (Ko-fi liefert die nicht; Weg: Ko-fi-Discord-Integration oder ein `/link`-Befehl). Feld `discordRole` in `lib/tiers.mjs` ist dafür da.
- **Milestones** („50. Supporter", „100 € diesen Monat"): Zahlen liegen in `kofi_events`, `kofi_stats()` liefert sie; ein Schritt `checkMilestone` vor `announce` kann Titel/Text ergänzen.
- **Monatsrückblick**: eigener Cron-Service (`railway-*.toml`), liest `kofi_events`, postet eine Zusammenfassung.
- **Tier-spezifische Texte**: `blurb` je Tier in `lib/tiers.mjs`.
