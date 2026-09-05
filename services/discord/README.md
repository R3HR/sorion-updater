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

## Nebenroute: /stats (Admin-Statistik)

Der Service liefert zusätzlich `UI/stats.html` unter `/stats` aus (seit 05.09.). Grund: Als
lokale Datei speichert Chrome keine Passwörter, und Supabase-Functions erzwingen für HTML
`text/plain`. Der Datenzugriff der Seite ist serverseitig geschützt (`is_analytics_admin`).
Nach Änderungen an `UI/stats.html`: `node tools/build-stats-page.mjs`, dann deployen.

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
4. **Railway** (so eingerichtet am 04.09., per CLI aus dem Repo-Root):
   `railway add --service sorion-discord`, Variablen setzen, dann aus **diesem Ordner**
   `railway up . --service sorion-discord --path-as-root` (ohne `--path-as-root` lädt die CLI
   das ganze Repo hoch und startet den Preis-Updater, Fehlerbild „Cannot find module update.mjs").
   Domain: `railway domain --service sorion-discord`. Deploy bei Änderungen: derselbe `up`-Befehl.
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

- **Rollenvergabe: NICHT hier.** Die übernimmt bereits Ko-fis eigener Discord-Bot auf dem Server (Stand Jonas 04.09.). Unser Service kündigt nur an. Ko-fi liefert trotzdem `discord_userid` mit (wenn der Spender Discord verknüpft hat); wir nutzen sie, um den Spender in der Nachricht zu verlinken (ohne Ping) und speichern sie für Statistik je Nutzer. Das Feld `discordRole` in `lib/tiers.mjs` bleibt als Reserve, falls Ko-fis Bot einmal wegfällt.
- **Milestones** („50. Supporter", „100 € diesen Monat"): Zahlen liegen in `kofi_events`, `kofi_stats()` liefert sie; ein Schritt `checkMilestone` vor `announce` kann Titel/Text ergänzen.
- **Monatsrückblick**: eigener Cron-Service (`railway-*.toml`), liest `kofi_events`, postet eine Zusammenfassung.
- **Tier-spezifische Texte**: `blurb` je Tier in `lib/tiers.mjs`.
