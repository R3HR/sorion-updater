# SORION — Handoff & Status

> Zentrale Übergabedatei für alle Bots/Agents. **Vor jeder Arbeit lesen, nach jeder Arbeit aktualisieren.**
> Bugs → [BUGS.md](BUGS.md) · Crashes/Sicherheit → [INCIDENTS.md](INCIDENTS.md)

## Projekt-Überblick

Sorion ist ein Sorare-Marktpreis-Tracker (FMV = Fair Market Value) für Fußball-Karten.

| Komponente | Ort | Deployment |
|---|---|---|
| Update-Script (FMV-Berechnung) | `update-scarcity.mjs` + `lib/fmv.mjs` | Railway, 3 Services (limited/rare/sr), Cron 22–5 Uhr alle 5 Min |
| Seed-Script (alle Spieler) | `seed-all-players.mjs` | Railway, manueller Trigger (Redeploy) |
| UI (Markt-Tabelle) | `UI/index.html` | statisches Hosting |
| Edge Functions | `C:\craft-log\supabase\functions\*` | Supabase (Deploy via `supabase functions deploy <name>`) |
| DB | Supabase Projekt `jxhdlcpdupmkpsoytzes` | Tabellen: `card_prices`, `price_history`, `pool_cache` |
| CraftLog UI (Portfolio) | `C:\craft-log\Craft_Log UI\index.html` | separates Frontend, ruft `add-missing-players` auf |

## Aktueller Stand (2026-07-06)

- FMV-Formel v3 in `lib/fmv.mjs`: zeitbasierter Decay (Halbwertszeit 3 Tage), Live-Listing-Floor als Anker, Sellability-Cap (FMV ≤ Floor × 1,05). Ziel: **Wer zum FMV listet, verkauft auch.**
- 3 Update-Scripts zu einem konsolidiert (`update-scarcity.mjs`, Scarcity via CLI-Arg)
- Floor Price kommt live aus `lowestPriceAnyCard(inSeason: true).publicMinPrices` (aktive Listings, nicht letzte Sales)
- Seed-Script vorhanden, befüllt DB mit allen in-season-fähigen Spielern
- UI-Prozente (24h/7d) auf `price_history`-Basis umgestellt (DB-Spalten `change_24h`/`change_7d`)

## ⚠️ Offene Aktionen für Jonas (manuell, blockierend)

0. **Railway-Service-Configs identifizieren:** Es gibt ZWEI Config-Sets im Repo — Root (`railway-limited.toml` etc.) und Ordner (`limited/railway.toml` etc., mit eigenem Root Directory). Die Ordner-Configs hatten den neueren Cron (16–20 Uhr) → vermutlich sind DIE live, d. h. Pushes auf die Root-Scripts kamen nie an! Beide Sets sind jetzt identisch korrekt (Script: `update-scarcity.mjs` + `lib/fmv.mjs`). In Railway pro Service unter Settings → „Root Directory" + „Config File Path" nachsehen, das ungenutzte Set danach löschen. ⚠️ Bei Ordner-Setup: `lib/fmv.mjs` existiert als Kopie in jedem Ordner — bis zur Auflösung müssen Änderungen an der Formel in alle Kopien synct werden (`cp lib/fmv.mjs <dir>/lib/`).
1. **SQL-Migration ausführen** (Supabase → SQL Editor): `migrations/2026-07-06_add_change_columns.sql` — MUSS vor dem nächsten nächtlichen Update-Run laufen (Script erkennt fehlende Spalten und lässt sie dann weg, aber Prozente bleiben leer bis Migration läuft)
2. **Keys rotieren** — siehe [INCIDENTS.md](INCIDENTS.md) SEC-001 (Service-Role-Key + Sorare Client Secret waren im Klartext in Git)
3. **Edge Functions neu deployen** nach Secret-Setup: `supabase functions deploy get-pool update-pool update-prices sorare-oauth add-missing-players`
4. **Railway prüfen**: existiert noch ein alter Service der `railway.toml` / `update.mjs` nutzt? → löschen (Dateien wurden entfernt)
5. **Seed-Service in Railway anlegen** (Config: `railway-seed.toml`, Env-Vars wie bei den anderen Services)

## TODO (priorisiert, Saisonstart August 2026)

| # | Task | Status | Notizen |
|---|---|---|---|
| 1 | InSeason/Classic-Trennung in DB + Scripts | **offen** | Design steht (s. unten), Migration entworfen. Sorare aggregiert Classic über alle Jahrgänge → nur 1 Classic-Zeile pro Spieler+Scarcity nötig |
| 2 | Season-Rollover-Plan für August | **offen** | Beim Rollover: In-Season-Zeilen haben anfangs 0 Sales der neuen Saison. FMV-Formel fällt dann auf Floor zurück (by design ok). `price_history` NICHT löschen |
| 3 | Keys rotiert + Env-Vars | **Code fertig**, Rotation durch Jonas offen | SEC-001 |
| 4 | FMV v3 (sellable FMV) | **erledigt 2026-07-06** | `lib/fmv.mjs`, getestet gegen Live-Daten (Maza) |
| 5 | UI-Prozente korrekt (zeitbasiert) | **Code fertig**, Migration offen | BUG-004 |
| 6 | sorarehoops-2025-URL ersetzen | **offen** | `update-pool`/`update-prices` ziehen `footballRewardPool2025*.json` von Dritt-Site. Bricht im August. Tier-Daten direkt von Sorare beziehen oder Jahr dynamisch machen |
| 7 | Crons ganztägig ausweiten | **offen** | Aktuell nur 22–5 Uhr → tagsüber bis 17h alte Daten. Vorschlag: tagsüber alle 15–30 Min zusätzlich. Rate-Limits beachten (s. #8) |
| 8 | Sorare API-Key beantragen + 429-Backoff | **offen** | Nach Seed wächst DB auf tausende Spieler. Ohne API-Key drohen Rate-Limits. Aktuell: silent fail, `updated_at` wird trotzdem gesetzt → tote Spieler unsichtbar |
| 9 | `add-missing-players` auf Slim-Insert umgestellt | **Code fertig**, Deploy offen | Berechnet kein FMV mehr — nur Insert mit `updated_at=epoch`, Update-Script rechnet. Eine Formel, eine Stelle |
| 10 | Repo aufgeräumt | **erledigt 2026-07-06** | node_modules aus Git, .gitignore, Versionsleichen in `_archive/` |
| 11 | Rechtliches: Impressum + Datenschutzerklärung | **Vorlage fertig 2026-07-06** | `UI/legal.html` (+ Kopie in CraftLog UI). Jonas muss die gelben `[PLATZHALTER]` füllen: Name, Adresse, E-Mail, Hosting-Anbieter, Supabase-Region. Vorlage ≠ Rechtsberatung |
| 12 | Google Fonts selbst hosten | **erledigt 2026-07-06** | `fonts/`-Ordner in beiden UIs (latin+latin-ext), Google-CDN-Links entfernt. CraftLog: auch Chart.js lokal statt cdnjs. ⚠️ Beim Deployen der UIs müssen `fonts/`, `legal.html` (und bei CraftLog `chart.umd.min.js`) mit hochgeladen werden |
| 13 | Footer-Disclaimer + Legal-Link | **erledigt 2026-07-06** | Beide UIs: „Not affiliated" + Schätzungs-Disclaimer + Link auf legal.html |

## Design: InSeason/Classic (TODO #1)

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

- **Sorare GraphQL** (`api.sorare.com/graphql`): public, ohne Auth nutzbar, aber rate-limited. Schema-Dump: `C:\craft-log\schema.graphql`
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
