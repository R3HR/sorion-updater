# SORION — Handoff & Status

> Zentrale Übergabedatei für alle Bots/Agents. **Vor jeder Arbeit lesen, nach jeder Arbeit aktualisieren — auch bei kleinen Sessions!**
> Bugs → [BUGS.md](BUGS.md) · Crashes/Sicherheit → [INCIDENTS.md](INCIDENTS.md)

## Projekt-Überblick

Sorion = **Trading-Tool** für Sorare (FMV-Marktdaten, Portfolio mit P&L, Manager-Scout). CraftLog = separates, simples Craft-Tool (nur Sorare-Login). Entscheidung 22.07.: kein Merge.

| Komponente | Ort | Deployment |
|---|---|---|
| Preis-Updater (FMV) | `update-scarcity.mjs` + `lib/fmv.mjs` (Kopien in `limited/rare/sr/` — SYNC-PFLICHT!) | Railway, 3 Services, Cron `*/5 22-23,0-4,16-20 UTC`, Env: `SORARE_APIKEY`, `DELAY_MS=1500`, `BATCH_SIZE=120` |
| Market-Harvester | `harvest-market-players.mjs` | Railway, täglich 05:30, `HARVEST_HOURS=26`; Quellen: liveAuctions + liveSingleSaleOffers; triggert danach update-pool/update-prices |
| Seed (in-season Spieler) | `seed-all-players.mjs` | Railway, manueller Trigger |
| Sorion-UI (Markt/Portfolio/Profil/Legal) | `UI/*` kanonisch; **live aus PUBLIC Repo `sorion-ui`** (`C:\craft-log\sorion-ui`) | GitHub Pages → **sorion.pro**. Nach UI-Änderung: nach sorion-ui kopieren + BEIDE Repos pushen. CDN-Cache ~10 Min (`?v=x` zum Sofort-Testen) |
| CraftLog-UI | `C:\craft-log\Craft_Log UI\` kanonisch; live aus Repo `R3HR/Craft_log` (Clone: `craft_log-repo`) | GitHub Pages → **craftlog.pro**, inkl. `auth/callback.html` (OAuth-Redirect) |
| Edge Functions (7) | `C:\craft-log\supabase\functions\` | `npx supabase functions deploy <name>` (CLI eingeloggt). sorare-oauth, add-missing-players, get-pool, update-pool, update-prices (Metadaten-only), get-analytics, sorare-proxy, delete-account |
| DB | Supabase `jxhdlcpdupmkpsoytzes` | `card_prices` (Key: slug×scarcity×eligibility), `price_history` (+eligibility), `pool_cache`, `fmv_accuracy` (+View `fmv_accuracy_stats`), `profiles`, `watchlist`, `sorare_users` |

## Aktueller Stand (2026-07-26)

**Daten-Pipeline:**
- InSeason/Classic live; Saisonflip 25/26→Classic läuft ligaweise und wird automatisch mitvollzogen (Karten-Bewertung folgt `inSeasonEligible` der Karte)
- FMV v3.1: Zeit-Decay (In-Season HL 3d/Fenster 21d; `CLASSIC_PROFILE` HL 14d/90d), Floor nur noch als Anker NACH UNTEN, ohne Sales → FMV null („ein Ask ist kein Preis"). 0 Sales = Marktzustand, überschreibt Altwerte (BUG-011-Fix 26.07.)
- Updater pflegt bei jedem Durchlauf das rarity-korrekte Kartenbild mit (Backfill hatte Bilder quer kopiert)
- Accuracy-Tracking läuft: jeder neue Sale wird gegen den vorher geschätzten FMV geloggt (`fmv_accuracy`), UI-Anzeige auf der Marktseite ab 10 Samples pro Zelle
- Vollabdeckung: Spieler existieren immer in allen 3 Scarcities × 2 Eligibilities; Quellen: Seed + Harvester (Auktionen+Listings) + CraftLog-Import + **jede Portfolio-/Scout-Ansicht speist unbekannte Spieler ein**

**Sorion-UI (alles live auf sorion.pro):**
- Markt: schneller Loader (ID-Bereichs-Abfragen, erste Seite ~1 s, 5-Min-Cache in Cache API), In-Season/Classic-Toggle, ehrliche 24h/7d-Prozente, 7d-Marktbewegungs-Chips an den Avg-FMV-Boxen (Median), Accuracy-Zeile, Movers, Platzhalterbild für Karten ohne Foto
- Portfolio (öffentlich per Manager-Slug, kein Login): P&L pro Karte (Kaufpreis via tokenOwner), Karten-Detail-Modal (Position/Markt/7d/30d-Trend+Sparkline), Filter (Rarity, Eligibility, Herkunft, Winners/Losers) + Sortier-Button-Leiste, 10-Min-Cache, FMV-Batches mit Retry
- Manager Search (Scout): Nav-Punkt auf Markt- UND Portfolio-Seite — Kompakt-Bilanz (Stats + Top/Flop 3 Kauf→FMV) vor dem Voll-Portfolio
- Accounts: Signup (Double-Opt-In via Resend, Absender noch @craftlog.pro), Login, Reset, E-Mail-Wechsel, Profil (Sorare-Slug → Portfolio), DSGVO-Export + Konto-Löschung (`delete-account`). Header: `◉ <slug>` → Profil
- Einheitliches SORION_-Textlogo (blinkender Cursor), Startseite 38px
- Sprachen: Markt+Portfolio Englisch, Profil Deutsch — i18n später als eigenes Projekt, neue Texte auf Englisch (außer legal.html)

**Sicherheit:** SEC-001 geschlossen (neue sb_-Keys, Legacy disabled, alter Key verifiziert tot). Sorare-OAuth läuft mit dem alten, nie geleakten Secret; die App liegt auf einem unbekannten Account (TODO: neue App beantragen).

## ⚠️ Offene Aktionen für Jonas

1. **Impressum-Platzhalter füllen** (legal.html in `sorion-ui` UND `Craft_log`) — **Launch-Blocker!**
2. SQL-Bereinigung BUG-011 ausführen (Session 26.07.: konservierte Alt-FMVs nullen) — falls noch nicht geschehen
3. `SORARE_APIKEY` auch als Supabase-Secret setzen (`npx supabase secrets set SORARE_APIKEY=...`) — schnellere Portfolio-Ladezeiten, wichtig vor Promotion-Traffic
4. Neue Sorare-OAuth-App beantragen (beide Redirect-URIs: craftlog.pro + sorion.pro) — Grundlage für Stufe 3 (Notifications) und Secret-Hygiene
5. Repo-Aufräumen: Ordner `limited/rare/sr` löschen sobald Root-Configs verifiziert die einzigen sind; alter Offline-Service in Railway

## Roadmap → Launch (Plan: Saisonstart + ~3 Tage stabil)

| Schritt | Status |
|---|---|
| Betrieb stabilisieren, Accuracy-Zahlen täglich prüfen (Bias!) | 🔄 läuft |
| Season-Flip überstehen (ligaweise im Gang) | 🔄 läuft |
| Impressum | 🔴 Jonas |
| Watchlist mit Zielpreisen (Stufe 2; Tabelle existiert) | ⬜ nächstes Feature |
| OG-Tags/Favicon + Erstbesucher-Erklärtext | ⬜ Launch-Kosmetik |
| Mobile-Durchgang | ⬜ vor Launch |
| Notifications (Stufe 3, braucht OAuth-App) | ⬜ nach Launch |
| 30d-Marktbewegung ergänzen | ⬜ ab ~20.08. (History reicht dann) |

## Architektur-Wissen (Kern)

- **Sorare GraphQL**: anonym Depth 7 / Complexity 500; mit APIKEY-Header Depth 13 / Complexity 30.000. Jonas' Key: 200 req/min (geteilt von allen Nutzern des Keys!). `@example.com`-Testmails lehnt Resend ab (GoTrue → „Error sending confirmation email")
- **Kaufdaten sind öffentlich**: `card.tokenOwner { amounts, from, transferType }` (SHARDS=gecraftet, REWARD, SINGLE_SALE_OFFER…) — Grundlage des P&L-Portfolios
- **Karten-Slug-Format**: `<player>-<jahr>-<rarity>-<serial>` (rarity auch `super_rare` MIT Unterstrich!)
- **Bewertung pro Karte**: `inSeasonEligible ? in_season : classic`-Zeile; Cache-Keys `slug_rarity_eligibility`
- **Update-Queue**: ältestes `updated_at` zuerst; epoch = Sofort-Priorität. 0-Sales-Zeilen werden voll verarbeitet (nicht konserviert!)
- **GitHub Pages CDN cached ~10 Min** — nach Deploys `?v=x` zum Testen; Nutzer heilen sich von selbst
- **UI-Patches**: Dateien haben teils CRLF (git autocrlf) — Patch-Scripts per Write-Tool als .mjs-Datei schreiben (NIE komplexe Templates durch bash/sed quälen), vorher LF-normalisieren, Anker prüfen, `new Function`-Syntaxcheck; UI-Interaktionen danach im Browser WIRKLICH klicken (Lektion BUG-009)
- **FMV-Historie**: v1 Index-Gewichte → v2 steiler → v3 Zeit-Decay+Cap → v3.1 (Ask≠Preis, Floor nur abwärts). Formel NUR in `lib/fmv.mjs` + Ordner-Kopien syncen

## Regeln

- FMV-Logik nur in `lib/fmv.mjs` (+ `cp` in die 3 Ordner-Kopien)
- UI-Änderung = kanonische Datei + `sorion-ui`/`Craft_log` + `Sorion_pro/UI` synchron pushen
- Keine Secrets in Code/Git; Keys mit `sb_`-Präfix sind aktuell, alles mit `eyJ` ist Legacy/tot
- Neue UI-Texte Englisch (legal.html Deutsch)
- **Nach JEDER Session: HANDOFF-Stand + BUGS/INCIDENTS aktualisieren** (Verstoß am 22.–26.07. → Jonas musste es anmahnen)
