# SORION — Handoff & Status

> Zentrale Übergabedatei für alle Bots/Agents. **Vor jeder Arbeit lesen, nach jeder Arbeit aktualisieren.**
> Bugs → [BUGS.md](BUGS.md) · Crashes/Sicherheit → [INCIDENTS.md](INCIDENTS.md)

## Projekt-Überblick

Sorion ist ein Sorare-Marktpreis-Tracker (FMV = Fair Market Value) für Fußball-Karten.

| Komponente | Ort | Deployment |
|---|---|---|
| Update-Script (FMV-Berechnung) | `update-scarcity.mjs` + `lib/fmv.mjs` | Railway, 3 Services, Cron `*/5 22-23,0-4,16-20 UTC`, gedrosselt (BATCH_SIZE/DELAY_MS via Env, Sorare-API-Key aktiv mit 200 req/min) |
| Seed-Script (in-season Spieler) | `seed-all-players.mjs` | Railway, manueller Trigger (Redeploy) |
| Market-Harvester (Listings + Auktionen → neue/rotierte Spieler) | `harvest-market-players.mjs` | Railway, täglich 05:30 mit `HARVEST_HOURS=26`; triggert danach `update-pool`/`update-prices` (Crafthelper-Daten). Erstlauf 21.07.: +14.920 Zeilen inkl. 26/27-Releases |
| UI (Markt-Tabelle) | `UI/index.html` (kanonisch) | GitHub Pages aus separatem PUBLIC Repo `sorion-ui` (`C:\craft-log\sorion-ui`) — nach UI-Änderungen dorthin kopieren + pushen! Hintergrund: Haupt-Repo ist seit 2026-07-08 privat (SEC-003), Pages ging dabei offline |
| Edge Functions | `C:\craft-log\supabase\functions\*` | Supabase (Deploy via `supabase functions deploy <name>`) |
| DB | Supabase Projekt `jxhdlcpdupmkpsoytzes` | Tabellen: `card_prices`, `price_history`, `pool_cache` |
| CraftLog UI (Portfolio) | `C:\craft-log\Craft_Log UI\index.html` (kanonisch) | **craftlog.pro** = GitHub Pages aus Repo `R3HR/Craft_log` (lokaler Clone: `C:\craft-log\craft_log-repo`). Nach Änderungen: Datei dorthin kopieren + pushen. Enthält auch `auth/callback.html` (OAuth-Redirect `https://craftlog.pro/auth/callback`) |

## Aktueller Stand (2026-07-21)

- **InSeason/Classic komplett LIVE** (Migration 21.07. ausgeführt): jede Zeile in `card_prices`/`price_history` hat `eligibility`, Update-Script bedient beide aus einer Queue, UI hat Toggle. Saisonwechsel läuft **automatisch** — wenn Sorare den In-Season-Flip macht, trackt die in_season-Zeile die neuen Karten, die alten rutschen in die classic-Betrachtung
- DB-Stand 21.07. abends: **48.298 Zeilen** (~16.700 Spieler×2 aus Seed/Backfill + 14.920 vom Harvester-Erstlauf inkl. 26/27-Release-Spieler). Bepreisung der neuen Zeilen läuft über die Nacht-Fenster
- Live-Test 21.07.: In-Season liefert noch 2025er-Karten (Flip noch nicht vollzogen); `tokenPrices(CLASSIC)` aggregiert Alt-Jahrgänge sauber getrennt; Floor-Listings können `eurCents: null` haben (ETH-only) → Fallback greift
- FMV-Formel v3 in `lib/fmv.mjs`: zeitbasierter Decay (Halbwertszeit 3 Tage), Live-Listing-Floor als Anker, Sellability-Cap (FMV ≤ Floor × 1,05). Ziel: **Wer zum FMV listet, verkauft auch.**
- Portfolio läuft auf beiden Seiten (21.07. abends): Sorion öffentlich per Manager-Slug (`portfolio.html?manager=<slug>`, kein OAuth — Cross-Domain-Problem damit gelöst), CraftLog eingeloggt. Bewertung pro Karte nach `inSeasonEligible` → in_season- bzw. classic-FMV (Cache-Key `slug_rarity_eligibility`)
- `update-prices` Edge Function ist jetzt Metadaten-only (Tier/Team/Liga/Supply, kein FMV mehr) mit dynamischem Saisonjahr
- Migrations-Historie: die Datei vom 06.07. wurde nie ausgeführt; `migrations/2026-07-21_eligibility_and_changes.sql` (enthält alles) lief am 21.07. erfolgreich

## ⚠️ Offene Aktionen für Jonas

**Am 21.07. erledigt:** SQL-Migration ✅ · Keys auf neue sb_-Keys umgestellt + Legacy disabled (SEC-001 ✅) · alle 5 Edge Functions deployed ✅ · Railway-Configs auf Root-tomls umgestellt (Config File Path gesetzt) ✅ · Harvester-Service angelegt ✅ · Sorare-API-Key aktiv (200 req/min) ✅

**Noch offen:**
1. `legal.html`-Platzhalter füllen (Name/Adresse/E-Mail/Hoster/Region) — in `sorion-ui` UND `Craft_log` Repo, danach pushen
2. Ordner `limited/`, `rare/`, `sr/` im Repo löschen (Config File Paths zeigen jetzt verifiziert auf die Root-tomls — Ordner-Set ist tot; vorher 1 Nacht Betrieb abwarten)
3. Alten Offline-Service „Sorion-Updater" (Terminal-Icon) in Railway löschen, falls noch vorhanden
4. Sorare-OAuth-App: auf welchem Account liegt sie? (Für spätere Secret-Rotation; aktuell läuft das alte, nie geleakte Secret)
5. Optional: `DELAY_MS` bei den 3 Updater-Services von 1500 auf 500 (Tempo ~58→~90 Zeilen/Min)
6. Klein: Harvester um Foto-Nachpflege erweitern (Bestandszeilen mit `picture_url is null` batchweise via `players(slugs)` aktualisieren — neue Spieler bekommen Fotos seit 22.07. automatisch, Alt-Zeilen zeigen bis dahin den Platzhalter)

## TODO (priorisiert, Saisonstart August 2026)

| # | Task | Status | Notizen |
|---|---|---|---|
| 1 | InSeason/Classic-Trennung in DB + Scripts + UI | **✅ LIVE 2026-07-21** | Migration ausgeführt, Classic-Bepreisung läuft |
| 2 | Season-Rollover-Plan für August | **✅ durch #1 gelöst** | Übergang automatisch (Zwei-Zeilen-Design). Nach Sorares In-Season-Flip beobachten: FMV neuer Karten fällt anfangs auf Floor zurück (by design). `price_history` NICHT löschen |
| 3 | Keys rotiert + Env-Vars | **✅ erledigt 2026-07-21** | SEC-001 geschlossen: neue sb_-Keys überall, Legacy disabled, alter Key verifiziert tot (401) |
| 4 | FMV v3 (sellable FMV) | **erledigt 2026-07-06** | `lib/fmv.mjs`, getestet gegen Live-Daten (Maza) |
| 5 | UI-Prozente korrekt (zeitbasiert) | **✅ LIVE 2026-07-21** | Sichtbar ab 2 Tagen price_history-Daten (~23.07.) |
| 6 | sorarehoops-Jahr dynamisch | **✅ deployed 2026-07-21** | Restrisiko: sorarehoops könnte 26/27-Files anders benennen → nach Erscheinen prüfen |
| 7 | Crons ganztägig ausweiten | **✅ erledigt 2026-07-21** | 3 Fenster (22–23, 0–4, 16–20 UTC) alle 5 Min, gedrosselt statt Burst (~58 Zeilen/Min gesamt) |
| 8 | Sorare API-Key + 429-Backoff | **✅ erledigt 2026-07-21** | Key aktiv (200 req/min), Backoff in Scripts. Teilrest: `fail_count`-Spalte für tote Spieler (BUG-005) weiter offen |
| 9 | `add-missing-players` Slim-Insert | **✅ deployed 2026-07-21** | Eine Formel, eine Stelle |
| 10 | Repo aufgeräumt | **erledigt 2026-07-06** | node_modules aus Git, .gitignore, Versionsleichen in `_archive/` |
| 11 | Rechtliches: Impressum + Datenschutzerklärung | **Vorlage fertig 2026-07-06** | `UI/legal.html` (+ Kopie in CraftLog UI). Jonas muss die gelben `[PLATZHALTER]` füllen: Name, Adresse, E-Mail, Hosting-Anbieter, Supabase-Region. Vorlage ≠ Rechtsberatung |
| 12 | Google Fonts selbst hosten | **erledigt 2026-07-06** | `fonts/`-Ordner in beiden UIs (latin+latin-ext), Google-CDN-Links entfernt. CraftLog: auch Chart.js lokal statt cdnjs. ⚠️ Beim Deployen der UIs müssen `fonts/`, `legal.html` (und bei CraftLog `chart.umd.min.js`) mit hochgeladen werden |
| 13 | Footer-Disclaimer + Legal-Link | **erledigt 2026-07-06** | Beide UIs: „Not affiliated" + Schätzungs-Disclaimer + Link auf legal.html |

## Design: InSeason/Classic (TODO #1) — ✅ implementiert 2026-07-21, wartet nur auf Migration

**Erkenntnis:** `SeasonEligibility` im Sorare-Schema hat genau 2 Werte (`IN_SEASON`, `CLASSIC`). `tokenPrices(seasonEligibility: CLASSIC)` liefert Sales über **alle** Classic-Jahrgänge aggregiert — genau wie Sorare sie in „Letzte Verkäufe" anzeigt. Kein Per-Jahrgang-Tracking nötig.

**Plan:**
1. Spalte `eligibility text default 'in_season'` in `card_prices` (in Migration 2026-07-06 enthalten)
2. Unique-Constraint auf `(player_slug, scarcity, eligibility)` erweitern (prüfen wie der aktuelle Constraint heißt!)
3. `update-scarcity.mjs`: zweiten Durchlauf pro Spieler mit `seasonEligibility: CLASSic` + `lowestPriceCardAnySeason` (bzw. `lowestPriceAnyCard(inSeason: false)`) → Classic-Zeile
4. `price_history` ebenfalls um `eligibility` erweitern (Teil des Unique-Keys `player_slug,scarcity,recorded_at`)
5. UI: Toggle InSeason/Classic
6. Seed: Classic-Zeilen für alle Spieler anlegen

**Offen:** Floor für Classic — `lowestPriceCardAnySeason` prüfen (liefert es publicMinPrices?). Kapazität: verdoppelt die API-Calls pro Run.

## Architektur-Wissen (Memories)

- **Sorare GraphQL** (`api.sorare.com/graphql`): public, ohne Auth nutzbar, aber rate-limited und **max. Query-Depth 7** (mit APIKEY 13 — noch ein Grund für TODO #8). Schema-Dump: `C:\craft-log\schema.graphql`
- **Spieler-Vollabdeckung** (3 Quellen): (1) Seed via `allCards(inSeasonEligible: true)` = alle Craft-fähigen; (2) Market-Harvester via globalen `tokens.liveSingleSaleOffers`-Feed (8-Tage-Fenster) = alle mit aktiven Listings, auch rotierte/Classic-only — Spieler-Slug wird aus dem Karten-Slug geparst (`<player>-<jahr>-<rarity>-<serial>`, Achtung: `super-rare` UND `super_rare` kommen vor); (3) CraftLog-Sammlungs-Import. Spieler ohne Listings & Sales sind bewusst nicht drin: kein Markt = kein Preis. Einmal erfasste Zeilen bleiben für immer (kein Delete) → Abdeckung wächst monoton
- **Preise**: `amounts.eurCents / 100` = EUR. `tokenPrices(...first: 20)` = letzte 20 Sales, neueste zuerst
- **Floor**: `anyPlayer(slug).lowestPriceAnyCard(inSeason: true, rarity: X).publicMinPrices.eurCents` = günstigstes aktives Listing
- **Update-Queue**: Scripts nehmen die 200 Spieler mit ältestem `updated_at`. Seed setzt `updated_at = epoch(1970)` → neue Spieler kommen sofort dran
- **price_history**: 1 Zeile pro Spieler+Scarcity+Tag (Upsert auf `recorded_at`-Datum)
- **Railway**: Config-as-Code via `railway-*.toml` pro Service (Config File Path in Service-Settings). `cronSchedule` + `restartPolicyType=never`
- **FMV-Historie**: v1 = Index-Gewichte 0.12/0.10/..., v2 (2026-07-06 vormittags) = steilere Gewichte 0.22/0.18/..., v3 (2026-07-06) = Zeit-Decay + Cap in `lib/fmv.mjs`; seit 21.07. mit CLASSIC_PROFILE (Halbwertszeit 14d, Fenster 90d) für den trägen Classic-Markt
- **CraftLog** liest Sammlung via OAuth (`sorare-oauth` Edge Function), max 60 Seiten Pagination

## Regeln

- FMV-Logik wird **nur** in `lib/fmv.mjs` geändert — nirgendwo duplizieren
- Keine Secrets in Code oder Git — nur `process.env` / `Deno.env.get`
- Nach jeder Session: diese Datei + ggf. BUGS.md/INCIDENTS.md aktualisieren
- Push auf `main` = Railway-Deploy. Nichts Halbfertiges pushen

## Produkt-Roadmap: Sorion = Trading-Tool (Entscheidung 22.07.)

**Positionierung:** CraftLog = Craften · Sorion = Trading. Kein Merge.

- **Stufe 1 ✅ (22.07.):** Portfolio zeigt pro Karte Kaufpreis/-datum/-art (öffentlich via `tokenOwner` — amounts/from/transferType), P&L seit Kauf, Stats Investiert/P&L. SHARDS=gecraftet, REWARD=Reward. Achtung: user_cards-Query kostet ~12 Complexity/Karte → anonym max ~35/Seite (Limit 500); mit `SORARE_APIKEY` als Supabase-Secret 50/Seite (Limit 30.000) — Secret setzen: `npx supabase secrets set SORARE_APIKEY=<key>`
- **Stufe 2 (offen):** Watchlist + Zielpreise clientseitig (localStorage), „Ziel erreicht"-Badges beim Seitenbesuch. Kein Backend nötig
- **Stufe 3 (offen, braucht eigene OAuth-App auf Jonas' Account):** Accounts, serverseitige Watchlist/Ziele, Notifications (Vorschlag: Telegram-Bot + Railway-Cron der Ziele gegen fmv prüft). OAuth-App mit BEIDEN Redirect-URIs (craftlog.pro + sorion.pro) anfordern → löst auch die verwaiste Alt-App (SEC-001-Rest)

## Sorion-Accounts (22.07.) — Code fertig, 2 manuelle Schritte offen

**Entscheidung:** Sorion bekommt eigene Accounts (E-Mail+Passwort via Supabase Auth, unabhängig vom Sorare-OAuth). CraftLog bleibt simpel mit Sorare-Login.

**Gebaut:** `profile.html` (Signup mit Datenschutz-Checkbox, Login, Passwort-Reset per Mail, Passwort/E-Mail ändern, Profildaten inkl. Sorare-Slug, DSGVO-Datenexport als JSON, zweifach bestätigte Konto-Löschung via `delete-account`-Function mit CASCADE). Portfolio nutzt den Profil-Slug automatisch. Datenschutzerklärung um Konto-Abschnitt (5a) ergänzt. Tabellen: `profiles` + `watchlist` (mit RLS, nur eigene Zeilen) — Watchlist ist die Basis für Stufe 2/3.

**⚠️ Manuelle Schritte (Jonas):**
1. `migrations/2026-07-22_profiles.sql` im SQL Editor ausführen — vorher funktioniert die Profilseite nicht (profiles-Tabelle fehlt)
2. Supabase → Authentication → URL Configuration: **Site URL** `https://sorion.pro`, **Redirect URLs** `https://sorion.pro/profile.html` hinzufügen — sonst führen Bestätigungs-/Reset-Mails ins Leere. Dabei prüfen: „Confirm email" aktiv (empfohlen, Double-Opt-In)

## Mail-Versand (22.07.) — Resend läuft · Domain-Umzug ZURÜCKGESTELLT (22.07., bewusste Entscheidung: unwichtig solange der Dienst läuft)

- SMTP-Kette Supabase→Resend funktioniert (Absender aktuell `noreply@craftlog.pro`, Domain verified). Debug-Lektion: Test-Signups mit `@example.com` schlagen fehl — Resend lehnt reservierte Domains ab, GoTrue macht daraus „Error sending confirmation email" (500). Für Tests immer echte Mail-Domains nehmen!
- **Plan:** `sorion.pro` in Resend verifizieren (DNS beim Registrar) → Supabase-Sender auf `noreply@sorion.pro`/`SORION` umstellen. CraftLog braucht keine eigenen Auth-Mails mehr (nur noch Sorare-Login; E-Mail-Login-Formular dort ist Legacy → bei Gelegenheit ausbauen)
- Sorion-gebrandete Templates liegen in `mail-templates/` (confirm-signup + reset-password) → in Supabase unter Authentication → Emails → Templates einfügen

## Accuracy-Tracking + Movers-Fix (22.07.)

- **`fmv_accuracy`-Tabelle** (Migration `2026-07-22_accuracy.sql`): Updater loggt jeden neuen Sale gegen den FMV des VORHERIGEN Laufs (kein Leakage) — signiertes `delta_pct`, `hours_gap` zum Filtern, getrennt nach Scarcity/Eligibility. Volle Abdeckung, null Extra-API-Calls. Auswertungs-Query steht als Kommentar in der Migrationsdatei (Median-|Delta| = Genauigkeit, Avg-Delta = Bias; Bias leicht positiv ist erwartbar — sellable-FMV schätzt bewusst konservativ)
- **7d-Movers**: zeigen seit 21.07. ehrliche change_7d — waren leer, weil flächendeckende History erst seit 21.07. existiert. Übergangslösung: ältester verfügbarer History-Punkt (≥1 Tag) als Basis, konvergiert bis ~29.07. zur echten 7-Tage-Basis

## Sprachen (Entscheidung 22.07.)

Markt + Portfolio = Englisch (22.07. vereinheitlicht). Profilseite vorerst Deutsch. **Entscheidung Jonas:** kein Einzel-Übersetzen mehr — später kommt ein richtiger Sprachumschalter (wie CraftLog: DE/EN/ES/FR) als eigenes Projekt, weitere Sprachen dann dort. Bis dahin neue UI-Texte auf Englisch anlegen (außer Rechtstexte/legal.html: Deutsch).
