# SORION — Handoff & Status

> Zentrale Übergabedatei für alle Bots/Agents. **Vor jeder Arbeit lesen, nach jeder Arbeit aktualisieren.**
> Bugs → [BUGS.md](BUGS.md) · Crashes/Sicherheit → [INCIDENTS.md](INCIDENTS.md)

## Projekt-Überblick

Sorion ist ein Sorare-Marktpreis-Tracker (FMV = Fair Market Value) für Fußball-Karten.

| Komponente | Ort | Deployment |
|---|---|---|
| Update-Script (FMV-Berechnung) | `update-scarcity.mjs` + `lib/fmv.mjs` | Railway, 3 Services (limited/rare/sr), Cron 22–5 Uhr alle 5 Min |
| Seed-Script (in-season Spieler) | `seed-all-players.mjs` | Railway, manueller Trigger (Redeploy) |
| Market-Harvester (alle gelisteten Spieler, inkl. rotierte) | `harvest-market-players.mjs` | Railway, `railway-harvest.toml`, täglich 05:30. Service muss noch angelegt werden; Erstlauf OHNE `HARVEST_HOURS` (voller 8-Tage-Feed), danach `HARVEST_HOURS=26` als Env-Var setzen (inkrementell) |
| UI (Markt-Tabelle) | `UI/index.html` (kanonisch) | GitHub Pages aus separatem PUBLIC Repo `sorion-ui` (`C:\craft-log\sorion-ui`) — nach UI-Änderungen dorthin kopieren + pushen! Hintergrund: Haupt-Repo ist seit 2026-07-08 privat (SEC-003), Pages ging dabei offline |
| Edge Functions | `C:\craft-log\supabase\functions\*` | Supabase (Deploy via `supabase functions deploy <name>`) |
| DB | Supabase Projekt `jxhdlcpdupmkpsoytzes` | Tabellen: `card_prices`, `price_history`, `pool_cache` |
| CraftLog UI (Portfolio) | `C:\craft-log\Craft_Log UI\index.html` (kanonisch) | **craftlog.pro** = GitHub Pages aus Repo `R3HR/Craft_log` (lokaler Clone: `C:\craft-log\craft_log-repo`). Nach Änderungen: Datei dorthin kopieren + pushen. Enthält auch `auth/callback.html` (OAuth-Redirect `https://craftlog.pro/auth/callback`) |

## Aktueller Stand (2026-07-21)

- **InSeason/Classic komplett implementiert** (Code): jede Zeile in `card_prices`/`price_history` hat `eligibility` ('in_season' | 'classic'), Update-Script bedient beide aus einer Queue, Seed legt beide Zeilen an, UI hat Toggle. Saisonwechsel läuft damit **automatisch** — wenn Sorare den In-Season-Flip macht, trackt die in_season-Zeile die neuen Karten, die alten rutschen in die classic-Zeile. Sorare 26/27-Karten kommen etappenweise → Übergang ist weich. **Blockiert nur noch durch die SQL-Migration (s. u.)**
- Seed ist gelaufen: DB voll (5.972 limited / 5.775 rare / 4.942 sr Spieler, Stand 21.07.)
- Live-Test 21.07.: In-Season liefert noch 2025er-Karten (Flip noch nicht vollzogen); `tokenPrices(CLASSIC)` aggregiert Alt-Jahrgänge sauber getrennt; Floor-Listings können `eurCents: null` haben (ETH-only) → Fallback greift
- FMV-Formel v3 in `lib/fmv.mjs`: zeitbasierter Decay (Halbwertszeit 3 Tage), Live-Listing-Floor als Anker, Sellability-Cap (FMV ≤ Floor × 1,05). Ziel: **Wer zum FMV listet, verkauft auch.**
- `update-prices` Edge Function ist jetzt Metadaten-only (Tier/Team/Liga/Supply, kein FMV mehr) mit dynamischem Saisonjahr
- ⚠️ SQL-Migration vom 06.07. wurde NIE ausgeführt (per DB-Probe verifiziert 21.07.) → ersetzt durch `migrations/2026-07-21_eligibility_and_changes.sql` (enthält alles)

## ⚠️ Offene Aktionen für Jonas

**Am 21.07. erledigt:** SQL-Migration ✅ · Keys auf neue sb_-Keys umgestellt + Legacy disabled (SEC-001 ✅) · alle 5 Edge Functions deployed ✅ · Railway-Configs auf Root-tomls umgestellt (Config File Path gesetzt) ✅ · Harvester-Service angelegt ✅ · Sorare-API-Key aktiv (200 req/min) ✅

**Noch offen:**
1. `legal.html`-Platzhalter füllen (Name/Adresse/E-Mail/Hoster/Region) — in `sorion-ui` UND `Craft_log` Repo, danach pushen
2. Ordner `limited/`, `rare/`, `sr/` im Repo löschen (Config File Paths zeigen jetzt verifiziert auf die Root-tomls — Ordner-Set ist tot; vorher 1 Nacht Betrieb abwarten)
3. Alten Offline-Service „Sorion-Updater" (Terminal-Icon) in Railway löschen, falls noch vorhanden
4. Sorare-OAuth-App: auf welchem Account liegt sie? (Für spätere Secret-Rotation; aktuell läuft das alte, nie geleakte Secret)
5. Optional: `DELAY_MS` bei den 3 Updater-Services von 1500 auf 500 (Tempo ~58→~90 Zeilen/Min)

## TODO (priorisiert, Saisonstart August 2026)

| # | Task | Status | Notizen |
|---|---|---|---|
| 1 | InSeason/Classic-Trennung in DB + Scripts + UI | **Code fertig 2026-07-21**, Migration offen | Update-Script, Seed, UI-Toggle, update-prices — alles eligibility-aware mit Alt-Modus-Fallback |
| 2 | Season-Rollover-Plan für August | **durch #1 gelöst** | Übergang automatisch (Zwei-Zeilen-Design). Nach Sorares In-Season-Flip beobachten: FMV neuer Karten fällt anfangs auf Floor zurück (by design). `price_history` NICHT löschen |
| 3 | Keys rotiert + Env-Vars | **Code fertig**, Rotation durch Jonas offen | SEC-001 |
| 4 | FMV v3 (sellable FMV) | **erledigt 2026-07-06** | `lib/fmv.mjs`, getestet gegen Live-Daten (Maza) |
| 5 | UI-Prozente korrekt (zeitbasiert) | **Code fertig**, Migration offen | BUG-004 |
| 6 | sorarehoops-Jahr dynamisch | **Code fertig 2026-07-21**, Deploy offen | Beide Functions probieren aktuelles Jahr, Fallback Vorjahr. Stand 21.07.: nur 2025-Files existieren. Restrisiko: sorarehoops könnte 26/27 anders benennen → nach Saisonstart prüfen |
| 7 | Crons ganztägig ausweiten | **offen** | Aktuell nur 22–5 Uhr → tagsüber bis 17h alte Daten. Vorschlag: tagsüber alle 15–30 Min zusätzlich. Rate-Limits beachten (s. #8) |
| 8 | Sorare API-Key beantragen + 429-Backoff | **offen** | Nach Seed wächst DB auf tausende Spieler. Ohne API-Key drohen Rate-Limits. Aktuell: silent fail, `updated_at` wird trotzdem gesetzt → tote Spieler unsichtbar |
| 9 | `add-missing-players` auf Slim-Insert umgestellt | **Code fertig**, Deploy offen | Berechnet kein FMV mehr — nur Insert mit `updated_at=epoch`, Update-Script rechnet. Eine Formel, eine Stelle |
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
- **FMV-Historie**: v1 = Index-Gewichte 0.12/0.10/..., v2 (2026-07-06 vormittags) = steilere Gewichte 0.22/0.18/..., v3 (2026-07-06) = Zeit-Decay + Cap in `lib/fmv.mjs`
- **CraftLog** liest Sammlung via OAuth (`sorare-oauth` Edge Function), max 60 Seiten Pagination

## Regeln

- FMV-Logik wird **nur** in `lib/fmv.mjs` geändert — nirgendwo duplizieren
- Keine Secrets in Code oder Git — nur `process.env` / `Deno.env.get`
- Nach jeder Session: diese Datei + ggf. BUGS.md/INCIDENTS.md aktualisieren
- Push auf `main` = Railway-Deploy. Nichts Halbfertiges pushen
