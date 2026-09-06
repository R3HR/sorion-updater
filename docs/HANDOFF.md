# SORION — Handoff & Status

> Zentrale Übergabedatei für alle Bots/Agents. **Vor jeder Arbeit lesen, nach jeder Arbeit aktualisieren — auch bei kleinen Sessions!**
> **Squad-Bot - Wegweiser zu allen Dateien: `C:\craft-log\squad-bot\README.md`** (Function, Migrationen, Tabellen, Cron-Jobs, Actions, Secrets, Kanal-Aufteilung, offene Punkte). Erster Anlaufpunkt bei Bot-Arbeit.
> **Squad-Leaderboard-Spezifikation (24.08., von Jonas): [SQUAD_LEADERBOARD.md](SQUAD_LEADERBOARD.md)** - vollstaendiges Regel- und Rechenwerk (Zyklen, Punkte, Strafen, Cap-Entscheidungsbaum, Ausgabeformate). **Maßgeblich bei Widerspruechen zu aelteren Notizen hier.**
> **Lineup-Optimizer (Konzept 04.09., IDEA-006): [LINEUP_OPTIMIZER.md](LINEUP_OPTIMIZER.md)** — bestmögliche Aufstellungen je Gameweek aus Portfolio-Daten, Zielgröße erwartete Belohnung in EUR; nicht gebaut, Bauplan in 4 Stufen, Stufe 0 = Regelwerk je Wettbewerb (`so5-competitions`).
> Bugs → [BUGS.md](BUGS.md) · Crashes/Sicherheit → [INCIDENTS.md](INCIDENTS.md) · Vorgemerkte Konzepte → [IDEAS.md](IDEAS.md)
> **Monetarisierungsstrategie (20.08., externe Product-Lead-Analyse): [MONETARISIERUNG.md](MONETARISIERUNG.md)** — Freemium-Empfehlung (Pro 3,99 €/Monat um Rendite-Suite/Alerts/Historie), Validierung vor Bau (Fake-Door + Founding Supporter), 5 priorisierte nächste Schritte. Preise/Schwellen sind Hypothesen.
> **Produktvision & Roadmap (20.08., Product-Lead-Analyse): [ROADMAP.md](ROADMAP.md)** — Vision, North Star (wöchentlich wiederkehrende Kern-Nutzer), 4 strategische Ziele, priorisierte Roadmap (P0: Launch + Tracking-Fixes vor JEDEM neuen Feature), Backlog inkl. DO-NOT-BUILD-Liste. Lebendes Dokument — bei größeren Sessions gegen den Ist-Stand prüfen und fortschreiben. Dabei gefunden: **Tracking-Whitelist-Lücke** — `trade_history`-Events werden von der `track`-Function still als `pageview` verbucht → Ökosystem-BUGS BUG-013 (`C:/craft-log/docs/BUGS.md`), 🔴 offen.
> **Technischer Bauplan** (Architektur, Datenfluss, FMV-Formel, Rechtemodell): [bauplan.html](bauplan.html) — Neubau via `node docs/bauplan.build.mjs`
> Funktionsübersicht (fertige Seite, im Browser öffnen): [funktionsuebersicht.html](funktionsuebersicht.html) — Neubau via `node docs/funktionsuebersicht.build.mjs` (bettet die Original-Schriften ein)
> Ökosystem-Ebene (geteilte Infra, CraftLog, Sicherheits-Review mit OFFENEN Befunden): `C:/craft-log/docs/HANDOFF.md` — bei Backend-Arbeit BEIDE lesen!

## Projekt-Überblick

Sorion = **Trading-Tool** für Sorare (FMV-Marktdaten, Portfolio mit P&L, Manager-Scout). CraftLog = separates, simples Craft-Tool (nur Sorare-Login). Entscheidung 22.07.: kein Merge.

| Komponente | Ort | Deployment |
|---|---|---|
| Preis-Updater (FMV) | `update-scarcity.mjs` + `lib/fmv.mjs` (EINE Quelle im Repo-Root — Dubletten `limited/rare/sr/` am 28.07. entfernt) | Railway, 3 Services (Update Limited/Rare/SR), alle bauen aus dem Repo-Root via `railway-<s>.toml`, keine Root Directory. Cron `*/5 22-23,0-4,16-20 UTC`, Env: `SORARE_APIKEY`, `DELAY_MS=1000`, `BATCH_SIZE=200`, `IN_SEASON_SHARE=0.80` (Stand 18.08., live gemessen: 200 Karten/Lauf, 1.200 ms/Karte, 4:03 Laufzeit). `MAX_RUN_MS`=255000 als Zeitbremse — greift derzeit nicht |
| Market-Harvester | `harvest-market-players.mjs` | Railway, täglich 05:30, `HARVEST_HOURS=26`; Quellen: liveAuctions + liveSingleSaleOffers; triggert danach update-pool/update-prices |
| Seed (in-season Spieler) | `seed-all-players.mjs` | Railway, manueller Trigger |
| **Kader-Abgleich** | `sync-club-rosters.mjs` | Railway (`railway-rosters.toml`), Cron **täglich 07:00 UTC** (18.08. von 04:00 verlegt — lag im Updater-Fenster, zusammen ~183 der 200 req/min; freie Stunden 5–15 und 21, 05:30 belegt der Harvester). Transferperiode; nach Deadline Day 01.09. auf wöchentlich `0 7 * * 1` stellen, im Januar-Fenster zurück auf täglich — Entscheidung Jonas 30.07.). Zieht via `football.clubsReady` + `club.activePlayers` ALLE aktiven Spieler und legt fehlende Zeilen mit `updated_at=epoch` an (inkl. Name/Team/Liga/Land/Position). **Clubzahl schwankt** (Testlauf 247, erster echter Lauf 228) — ein Grund fuer den taeglichen Rhythmus. Fuegt nur hinzu, ueberschreibt/loescht nie |
| Sorion-UI (Markt/Portfolio/Profil/Legal) | `UI/*` kanonisch; **live aus PUBLIC Repo `sorion-ui`** (`C:\craft-log\sorion-ui`) | GitHub Pages → **sorion.pro**. Nach UI-Änderung: nach sorion-ui kopieren + BEIDE Repos pushen. CDN-Cache ~10 Min (`?v=x` zum Sofort-Testen) |
| CraftLog-UI | `C:\craft-log\Craft_Log UI\` kanonisch; live aus Repo `R3HR/Craft_log` (Clone: `craft_log-repo`) | GitHub Pages → **craftlog.pro**, inkl. `auth/callback.html` (OAuth-Redirect) |
| Edge Functions (11) | `C:\craft-log\supabase\functions\` | `npx supabase functions deploy <name>` (CLI eingeloggt). sorare-oauth (Actions: user_cards, **user_trades**, login, exchange, refresh, userinfo, cards, link_sorare, card_pull, recent_crafts, **squad_board** neu 20.08.), add-missing-players, get-pool, update-pool, update-prices (Metadaten-only), get-analytics, sorare-proxy, delete-account, **track** (Analytics-Beacon), **sync-portfolio** (Portfolio-Spiegelung mit TTL-Sperre), **squad-poll** (Squad-Manager-Poller via pg_cron alle 10 min + 5-Min-Fenster um die Claim-Frist, neu 21.08.) |
| DB | Supabase `jxhdlcpdupmkpsoytzes` | `card_prices` (Key: slug×scarcity×eligibility; +`position`, +`league_country` seit 30.07.), `price_history` (+eligibility), `pool_cache`, `fmv_accuracy` (+View `fmv_accuracy_stats`), `market_daily` (Vollmarkt-Tagessnapshot), `manager_sync`/`manager_cards`/`manager_trades` (gespiegelte Portfolios), `analytics_events`, `profiles` (+`sorare_verified`), `watchlist`, `sorare_users` |

## Aktueller Stand (2026-08-22)

**Wochenbilanz 18.–22.08. (Kurzfassung, Details in den Bloecken unten und in BUGS/INCIDENTS):**
- **Marktseite serverseitig** (~100 KB statt 15 MB/Besuch), Aggregate als **Materialized Views** (2 Refreshs/Tag, Advisory-Lock), Hero-Zahlen aus Tages-Snapshots, Client mit Backoff-Retries + Wachhund. Kaltstart-Timeouts und "913 Spieler"-Zaehler-Verwirrung damit Geschichte.
- **Datenbank 515 → 225 MB** (price_history-Swap, tote Indizes weg, Wochen-Rollup + Accuracy-Putzdienst via pg_cron). Free-Tarif wieder mit Luft.
- **INC-005 (22.08.):** Mein Cache-Waermer vom 21.08. legte Supabase ~1,5 h lahm (IO-Drossel + ueberlappende Cron-Laeufe). Stillgelegt, durch MVs ersetzt, Lehre dokumentiert: keine periodische Last auf gedrosselte Instanzen, Cron-Jobs brauchen Ueberlappungsschutz, am Folgetag `cron.job_run_details` pruefen.
- **Accuracy** wiederhergestellt (INC-004), eigene Seite `accuracy.html` hinter Fusszeilen-Link (Formel bleibt geheim), Befund: FMV systematisch ~47 % zu niedrig bei Limited in-season → Backtest-Harness ist der naechste Schritt.
- **Impressum vollstaendig auf beiden Domains** — letzter Launch-Blocker geschlossen. Discord-Fahne, Handy-Tauglichkeit, Rarity-Akzente, Budget-Rahmen (~14 EUR/Monat laufend) dokumentiert.
- **Updater:** In-Season taeglich durch (gewichtete Queue, BATCH 200/DELAY 1000), Roster-Cron 07:00. Kein zweiter API-Key noetig.
- **Regel fuer ALLE Sessions seit SEC-004 (22.08.):** Neue Postgres-Funktionen starten PRIVAT (Default-Privileges geaendert). Eine RPC, die das Frontend anonym aufrufen soll, braucht zwingend `grant execute on function ... to anon, authenticated` — und interne Funktionen brauchen `revoke ... from public, anon, authenticated` (BEIDES, immer). Pruef-Abfrage steht in der SEC-004-Migration.
- **FMV-Backtest gelaufen (22.08.) — Ergebnis liegt vor, Entscheidung offen:** `tools/fmv-backtest.mjs` (Walk-Forward ohne Leakage: jeder Verkauf wird nur aus den AELTEREN vorhergesagt; alle Varianten auf identischen Daten, damit Selektionseffekte jede gleich treffen). 153 Karten, 2.433 Vorhersagen:
  | Variante | Median-Abw. | Bias | <20% | am Deckel |
  |---|---|---|---|---|
  | **heute** (cap 1,05 · blend 0,35) | 26,4 % | +21,2 % | 41 % | **76 %** |
  | cap 1,25 · blend 0 | 20,0 % | +9,4 % | 51 % | 53 % |
  | cap 1,50 · blend 0 | 17,6 % | +4,1 % | 54 % | 38 % |
  | **bestes** (kein Deckel · blend 0) | **16,0 %** | **−1,0 %** | 57 % | 0 % |
  Die Rangfolge ist streng monoton: je lockerer der Deckel, desto genauer. **Der Befund trifft eine PRAEMISSE, nicht nur einen Parameter:** SELL_CAP unterstellt „solange ein Angebot unter deinem Preis steht, verkauft deins nicht" — die Daten widerlegen das, Verkaeufe finden regelmaessig deutlich ueber dem Floor statt (andere Serials, andere Jahrgaenge, Kaeufer will genau diese Karte). **Entscheidung Jonas noetig**, weil sich damit die BEDEUTUNG des FMV aendert (Verkaufspreis-Schaetzung vs. „sofort verkaeuflich") und alle angezeigten Preise ~20 % steigen wuerden. Empfehlung: cap 1,50 · blend 0 als Mittelweg (fast die volle Genauigkeit, behaelt einen Riegel gegen Absurditaeten).
- **Offen (ohne Eile):** ~~FMV-Backtest-Harness~~ · Support-Link (Ko-fi, wartet auf Account) · Roster-Cron nach 01.09. auf woechentlich · 23.08. 16:30 Cron-Health-Check (Erinnerung gesetzt).


**Daten-Pipeline:**
- InSeason/Classic live; Saisonflip 25/26→Classic läuft ligaweise und wird automatisch mitvollzogen (Karten-Bewertung folgt `inSeasonEligible` der Karte)
- **FMV v3.2 (22.08.):** Zeit-Decay (In-Season HL 3d/Fenster 21d; `CLASSIC_PROFILE` HL 14d/90d), Floor nur noch als Anker NACH UNTEN, ohne Sales → FMV null („ein Ask ist kein Preis"). 0 Sales = Marktzustand, überschreibt Altwerte (BUG-011-Fix 26.07.). **Neu in v3.2: `SELL_CAP` 1,05 → 1,50 und `FLOOR_BLEND` 0,35 → 0,00.** Backtest-belegt: Median-Abweichung 26,4 → 17,6 %, Verzerrung +21,2 → +4,1 %. Die alte Sellability-Annahme ist durch Daten widerlegt (Verkäufe finden regelmäßig weit über dem billigsten Listing statt). **Folge: alle Werte stiegen einmalig ~20 %** — kein Marktereignis. `market_move` vergleicht seither nur Snapshots derselben Formel-Generation (Migration `2026-08-22_fmv_v32_change_guard.sql`), der 7d-Chip bleibt bis 29.08. leer. Nach jeder künftigen Formeländerung: `node tools/fmv-backtest.mjs 200` laufen lassen UND diese Sperre auf das neue Datum ziehen.
- Updater pflegt bei jedem Durchlauf das rarity-korrekte Kartenbild mit (Backfill hatte Bilder quer kopiert)
- Accuracy-Tracking läuft: jeder neue Sale wird gegen den vorher geschätzten FMV geloggt (`fmv_accuracy`), UI-Anzeige auf der Marktseite ab 10 Samples pro Zelle
- Vollabdeckung: Spieler existieren immer in allen 3 Scarcities × 2 Eligibilities; Quellen: Seed + Harvester (Auktionen+Listings) + CraftLog-Import + **jede Portfolio-/Scout-Ansicht speist unbekannte Spieler ein**
- **Spieler-Vollstaendigkeit gefixt (31.07.):** Der neue Club-Filter deckte auf, dass Kader unvollstaendig waren (Dortmund 18 von 38). Erster Lauf des Kader-Abgleichs: **228 Clubs, 7.195 aktive Spieler, 7.342 Zeilen fuer 1.245 neue Spieler angelegt** (DB 111k → 118,7k Zeilen); Dortmund 22 → 36 Zeilen. Es fehlten ausschliesslich Nachwuchs-/Reservespieler ohne Marktaktivitaet — deren Slugs sind nicht ableitbar (`felix-kalu-nmecha`). Laeuft ab jetzt taeglich 04:00 UTC.
- **Updater pflegt zusaetzlich Verein, Liga, Liga-Land und Position mit** (`anyPlayer.activeClub` + `anyPositions` im bestehenden Query, kein Zusatz-Call). Ein voller Sweep dauert 2–3 Tage; bis dahin gemischte Alt-/Neu-Liganamen.
- **„Players Tracked" sinkt während des Saisonflips — KEIN Bug (geklärt 27.07.):** Die Marktseite zählt nur Zeilen mit fmv/floor/sale. Seit dem BUG-011-Fix werden In-Season-Zeilen der neuen Saison ehrlich geleert (keine Sales, oft kein Listing) statt Altwerte zu konservieren → ~19,8k sichtbar von 53,7k In-Season-Zeilen (27.07.). Die Spieler sind alle noch in der DB; die Zahl steigt von selbst, sobald 26/27-Karten gehandelt/gelistet werden. Classic wächst parallel (45k sichtbar).

**Sorion-UI (alles live auf sorion.pro):**
- Markt: **seit 19.08. serverseitig** — der Server liefert genau eine Tabellenseite (50 Zeilen), Aggregate aus den RPCs `market_overview`/`market_facets`/`market_leagues`, Movers und Trefferzahl als eigene Kleinabfragen. **~100 KB statt ~15 MB pro Besuch** (Faktor 150; Egress-Limit damit ~50.000 statt ~340 Besuche/Monat). Wichtige Details: "Players Tracked" behaelt bewusst die alte Definition (FMV ODER Floor ODER Sale — sonst spraenge die Zahl); die Trefferzahl-Zaehlung laeuft GETRENNT von der Seitenabfrage (`Prefer: count=exact` an der or()-Sichtbarkeitsregel lief in den Timeout, 5,0 s → 500) und wird je Filter gecacht nachgereicht; Suche debounced 300 ms; veraltete Antworten werden per Sequenznummer verworfen. Ausloeser war Jonas' Beobachtung „nur noch 913 Spieler getrackt" — das war der alte Zaehler, der beim 30-Sekunden-Laden hochzaehlte. In-Season/Classic-Toggle, ehrliche 24h/7d-Prozente, 7d-Marktbewegungs-Chips an den Avg-FMV-Boxen (Vollmarkt-Snapshots aus `market_daily`, deckungsgleich mit der angezeigten Avg-Zahl), Accuracy-Zeile, Movers, Platzhalterbild für Karten ohne Foto
- Portfolio (öffentlich per Manager-Slug, kein Login): P&L pro Karte (Kaufpreis via tokenOwner), Karten-Detail-Modal (Position/Markt/7d/30d-Trend+Sparkline), Filter (Rarity, Eligibility, Herkunft, Winners/Losers) + Sortier-Button-Leiste, 10-Min-Cache, FMV-Batches mit Retry
- **Filter (31.07.):** Club (Eingabe mit Vorschlagsliste), **Land → Liga** (verschachteltes Menue mit Flagge + Anzahl), Position (GK/DF/MD/FW — Gruppe erscheint erst, wenn die Spalte gefuellt ist). Kombinierbar mit Rarity/Eligibility/Suche. **Land ist Pflicht bei mehrdeutigen Liganamen** (24 erkannt: „Bundesliga" DE+AT, „Premier League" in 9 Laendern) — sonst wuerden fremde Klubs mitgefiltert; bei eindeutigen Namen bleiben Zeilen ohne Land erhalten.
- **Liga-Ranking (31.07.):** rechte Spalte neben Hero/Movers, buendig abschliessend (Inhalt absolut positioniert, sonst blaeht die Liste die Rasterzeile auf). Ligen nach FMV-Summe, gruppiert nach **Land+Liga**, mit eigenem Rarity-Umschalter (ALL/L/R/SR) — Titel nennt den Stand. Klick filtert die Tabelle. Bewusst FMV-Summe statt FMV×Stueckzahl (`available_supply` deckt nur ~40 % ab).
- Scrollbalken seitenweit im Sorion-Lila statt grau/weiss (Firefox + Chromium)
- **Trade History (01.08.):** Neuer Tab im Portfolio mit den REALISIERTEN Trades eines Managers. Quellen sind oeffentlich (kein Login): `soldSingleSaleTokenOffers` (Karte in `senderSide.anyCards`, Preis in `receiverSide.amounts`), `boughtSingleSaleTokenOffers`, `wonTokenAuctions` — gebuendelt in der neuen Action **`user_trades`** der Function `sorare-oauth` (kind: sold|bought|won; **Seitengroesse 30 ohne SORARE_APIKEY**, sonst Complexity 606 > 500). Zuordnung ueber den Karten-Slug, Einstand = letzter Kauf VOR dem Verkauf. Tabelle je Trade + Gesamtuebersicht (Einsatz, realisierter Gewinn, Gebuehren, Trefferquote), gesteuert vom NET/GROSS-Toggle. Verkaeufe ohne Kaufbeleg (Reward/Craft/Tausch) werden separat ausgewiesen statt als Gewinn gebucht.
- **Sorare-Marktgebuehr (31.07.):** Sorare zieht beim Verkauf ~5 % ab (Screenshot Jonas: "Marktgebuehr inkl. Steuerabgaben, falls zutreffend" -> "Du erhaeltst"). Unsere Sale-Preise und der FMV sind **brutto**; die Gebuehr steckt NICHT in der API (die `Fee`-Typen im Schema sind Blockchain-/Auszahlungsgebuehren). Zentrale Konstante `MARKET_FEE = 0.05` in portfolio.html. Portfolio hat einen Toggle **NET -5% / GROSS** (Standard NET, Label nennt die Basis; wirkt auf Kacheln, Sortierung, Stats), das Karten-Detail zeigt Erloes, **Break-even-Ask (Kauf / 0,95)** und Gebuehrenbetrag. **FMV bleibt bewusst brutto** (sonst nicht mehr mit Floor/letztem Sale vergleichbar); Accuracy-Tracking unberuehrt. **Noch nicht umgestellt:** Top/Flop der Manager Search (Markt- und Portfolioseite) rechnen weiter brutto — und ein spaeterer Calculator muss dieselbe Konstante nutzen.
- Manager Search (Scout): Nav-Punkt auf Markt- UND Portfolio-Seite — Kompakt-Bilanz (Stats + Top/Flop 3 Kauf→FMV) vor dem Voll-Portfolio
- Accounts: Signup (Double-Opt-In via Resend, Absender noch @craftlog.pro), Login, Reset, E-Mail-Wechsel, Profil (Sorare-Slug → Portfolio), DSGVO-Export + Konto-Löschung (`delete-account`). Header: `◉ <slug>` → Profil
- Einheitliches SORION_-Textlogo (blinkender Cursor), Startseite 38px
- Sprachen: Markt+Portfolio Englisch, Profil Deutsch — i18n später als eigenes Projekt, neue Texte auf Englisch (außer legal.html)

**Sicherheit:** SEC-001 geschlossen (neue sb_-Keys, Legacy disabled, alter Key verifiziert tot). Sorare-OAuth läuft mit dem alten, nie geleakten Secret; die App liegt auf einem unbekannten Account (TODO: neue App beantragen).

**Handel & Kosten (01.–02.08.):**
- **Sorare-Marktgebuehr (5 %)** wird beruecksichtigt: NET/GROSS-Umschalter im Portfolio (Standard netto), Break-even-Ask im Karten-Detail (`Kauf / 0,95`, NICHT `Kauf x 1,05`). Zentrale Konstante `MARKET_FEE` in portfolio.html. **FMV bleibt bewusst brutto** — sonst nicht mehr mit Floor und letztem Sale vergleichbar; Accuracy-Tracking unberuehrt.
- **Trade History** mit realisierten Trades (Kauf/VK brutto+netto, Haltedauer, Gewinn) und Gesamtuebersicht. **Toggle „Free cards"** blendet Verkaeufe ohne Kaufbeleg ein (Reward/Craft/Tausch): Einstand „—", P&L 0, ihr Erloes wird separat ausgewiesen und geht NICHT in Rendite-% und Trefferquote ein (Division durch null Kapital).
- **Verkaufsdatum** `last_sale_at`: Die Marktseite zeigt jetzt das Alter des letzten Verkaufs („3d ago", frisch gruen, alt gedaempft). Vorher war unklar, ob „LAST SALE 6,72 €" von gestern oder von vor drei Wochen stammte.

**Verbrauch & Speicher (01.–02.08.) — der groesste Umbau des Tages:**
- **Portfolios werden gespiegelt** (`manager_*`-Tabellen + Function `sync-portfolio`). Anzeigen kostet **0 Sorare-Anfragen** (vorher 6–16 pro Aufruf, bei jedem Neuladen erneut). Die Sperre haengt am MANAGER-SLUG: ein Manager wird hoechstens 1x/24 h geholt, egal wie viele — auch anonyme — Leute ihn ansehen. Eigener Sync-Knopf: 10-Minuten-Cooldown, nur fuer den eingeloggten Besitzer (Abgleich gegen `profiles.sorare_slug`, sonst 403).
- **`price_history` nur noch bei Preisaenderung** statt taeglich pro Karte. Vorher ~100.000 Zeilen/Tag (~3 Mio/Monat), fast alles Wiederholungen. Der Updater laedt die Historie jetzt EINMAL (45-Tage-Fenster) und leitet daraus sowohl die Schreibentscheidung als auch die 24h/7d-Prozente ab — **eine DB-Abfrage weniger** pro Karte.
- **Datenbank war ueber dem Free-Limit** (531/500 MB). Zwei verwaiste Indizes auf `price_history` entfernt (stammten vom 30./31.07., Zweck entfallen) → **422 MB**. Wichtig: `drop index` loescht keine Daten, und der Platz wird sofort frei — `vacuum full` ist dabei unnoetig und sperrt nur.
- **Marktseite serverseitig vorbereitet:** RPCs `market_leagues`, `market_facets`, `market_overview` + Index angelegt. Ziel: 15 MB und 199 Anfragen pro Besuch → ~50 KB. **Frontend-Umbau steht noch aus.**

**Konten (01.08.):**
- **Sorare-Verknuepfung** (`link_sorare`): Der Sorare-Login ist KEIN zweiter Kontotyp, sondern der Nachweis fuer den Managernamen — ein Premium-Modell braucht EINE Konto-Identitaet. Prueft Sorion-JWT UND Sorare-Token, 409 bei fremd verknuepftem Slug, setzt `sorare_verified`. Von Jonas getestet: funktioniert, **die bestehende OAuth-App deckt beide Domains ab**.
- **Header-Fehler behoben:** Beim Ansehen fremder Portfolios uebernahm der Header den fremden Manager, und `sorion_manager` wurde ueberschrieben — „Portfolio" landete danach beim Fremden. Identitaet (`sorion_own`) und Ansicht sind jetzt getrennt.
- **DSGVO nachgezogen:** `delete-account` ruft `purge_manager_data(slug)` (haengt am Slug, CASCADE greift dort nicht), Export enthaelt `portfolio: { sync, cards, trades }`, legal.html Abschnitt 5b beschreibt die Spiegelung.

**Messung (30.–31.07.):** Eigenes cookiefreies Analytics statt Plausible (Abo abgelaufen). Beacon-Function `track`, Auswertung nur fuer Admin (`is_analytics_admin`, Mail `jonas.rehr@outlook.de`), Dashboard `UI/stats.html` **nicht im oeffentlichen Repo**. Custom Events: manager_search, portfolio_view, card_detail, elig_toggle, scarcity_switch, signup_done, login_done, trade_history.

**Kapazitaet — gemessen 02.08.:** Gleichzeitigkeit ist NICHT das Problem (8 Besucher parallel → 2,4 s, null Fehler). Der Flaschenhals ist der Traffic: Marktseite ~15 MB pro Besuch → auf dem Free-Tarif (5 GB) nur ~340 Aufrufe/Monat. Portfolio dagegen 25 KB. Deshalb ist der serverseitige Umbau der Marktseite die wichtigste offene Aufgabe vor jeder Promotion.

**Durchsatz des Preis-Updaters (18.08.) — kein zweiter API-Key nötig, live gemessen:**
- **Ausgangsfrage:** Hilft ein zweiter Sorare-API-Key (200 req/min), um Limited tagesaktuell zu bekommen? **Nein.** Gemessen lief der Updater bei 31 req/min je Service, drei Services = 93 von 200 erlaubten. Wir waren nie rate-limitiert, sondern **selbst gedrosselt** — durch `DELAY_MS` und das 12-h-Cron-Fenster.
- **Der Denkfehler, der fast teuer wurde:** Zuerst gerechnet „120 Karten x 1,5 s sleep = 3 min, also passt BATCH_SIZE=190". Falsch — pro Karte kommen ~200-430 ms Netzwerk dazu (Sorare-Call + zwei Supabase-Writes). Der sleep ist NICHT die Taktzeit. 190 haette den 5-min-Slot gesprengt, Railway haette den naechsten Tick UEBERSPRUNGEN und der Durchsatz waere *gesunken*.
- **Loesung statt Raten:** `MAX_RUN_MS` (255 s) als harte Zeitbremse. `BATCH_SIZE` ist damit nur noch eine Obergrenze; nicht bearbeitete Zeilen behalten ihr `updated_at` und kommen beim naechsten Tick zuerst dran (die Batch ist nach Alter sortiert). Der Lauf loggt jetzt Laufzeit und ms/Karte.
- **Gewichtete Queue** (`IN_SEASON_SHARE=0.80`): Vorher zog die Queue stur die aeltesten Zeilen — In-Season und Classic bekamen gleich viele Slots, obwohl Classic eine FMV-Halbwertszeit von 14 Tagen hat. Zwei getrennte Queries + Auffuellung, falls eine Sorte ihr Kontingent nicht ausschoepft. Der Index `(scarcity, updated_at)` traegt den Eligibility-Filter mit (580-920 ms).

| gemessen 18.08. | vorher | nachher |
|---|---|---|
| Karten/Lauf | 120 | **200** |
| ms/Karte | 1.760 | **1.200** |
| Laufzeit | 3:30 | 4:03 (Slot: 5:00) |
| In-Season-Anteil | 50 % | **80 %** (exakt 160/40) |
| In-Season voll durch | alle 2,4 Tage | **alle 0,89 Tage** |
| Classic voll durch | alle 2,4 Tage | alle 3,5 Tage |
| Anfragen/min (3 Services) | 93 | 148 von 200 |

- **Env in Railway** (je Service Updater Limited/Rare/SR): `DELAY_MS=1000`, `BATCH_SIZE=200`, `IN_SEASON_SHARE=0.80`. `MAX_RUN_MS` = Default 255000.
- **Railway ist schneller als lokal gemessen:** 1.200 statt vorhergesagter 1.430 ms/Karte — die Latenz zu Supabase ist von Railway aus geringer. Deshalb greift die Zeitbremse gar nicht (`Skipped: 0`), BATCH_SIZE ist die bindende Grenze.
- **Club_Rosters von 04:00 auf 07:00 verschoben:** 04:00 lag im Updater-Fenster, zusammen ~183 der 200 req/min. Freie Stunden sind 5-15 und 21; 05:30 belegt der Harvester (**eigenes Railway-Projekt**, gleicher Key).
- **Ein zweiter Key wuerde erst zaehlen**, wenn ALLE 122k Zeilen taeglich sollen: 85 req/min ueber 24 h bzw. 170/min im 12-h-Fenster.

**FMV-Genauigkeit — Befund 20.08. (Jonas: „30 % Abweichung empfinde ich als viel"):**
Erste Auswertung der wiederhergestellten fmv_accuracy (14.968 Zeilen vom ersten Tag, 1.000-Zeilen-Stichprobe signiert analysiert):
- **Der Fehler ist GERICHTET, nicht zufaellig:** Limited in-season verkauft sich im Median **+46,7 % UEBER unserem FMV, 87 % aller Sales liegen drueber**. Schlimmster Bereich: FMV 1–5 EUR → Median **+71,5 %**. Teure Karten (>20 EUR) nur +11 %. Classic limited nur +6,5 % → das Problem haengt an in-season.
- **Hauptverdaechtige (in dieser Reihenfolge):** (1) `SELL_CAP = Floor x 1,05` — FMV wird am NIEDRIGSTEN Ask gedeckelt, Verkaeufe liegen naturgemaess darueber → eingebaute Unterschaetzung, im Billigsegment brutal. (2) `FLOOR_BLEND 0,35` zieht zusaetzlich Richtung Floor. (3) Saisonstart = steigender Markt (Avg +53 % seit Flip); ein nachlaufender Schaetzer (HL 3d) hat im Aufwaertstrend systematisch positiven Bias — Classic (stabiler Markt) zeigt entsprechend kaum Bias. (4) Serial-Premiums maesten den rechten Rand (P95 ~+300 %).
- **Vorsicht:** Tag-1-Daten in einer heissen Marktphase — NICHT blind kalibrieren. Beschlossener Weg: **Backtest-Harness** (Offline-Script im Updater-Repo: spielt FMV-Varianten gegen echte Verkaufssequenzen durch — Cap 1,05/1,25/ohne, Floor-Blend 0,35/0,20/liquiditaetsgewichtet — und misst Median-Fehler + Bias je Variante). Erst dessen Zahlen entscheiden die Formelaenderung. Accuracy parallel ~1 Woche weitersammeln.
- **UI-Entscheidung Jonas (20.08.): Accuracy runter von der Marktseite.** „Wir werden nie einen wunderschoenen Wert hinbekommen" — die drei Boxen sind jetzt eine eigene Seite `accuracy.html` hinter dem Fusszeilen-Link „FMV Accuracy": Transparenz fuer Suchende, kein Aushaengeschild. Die Seite erklaert die MESSUNG (Median-Gap zu echten Sales, 48h/30d) und warum FMV besser ist als Floor/Durchschnitt — **verraet aber bewusst KEINE Formel-Zutaten** (zweite Vorgabe Jonas: Rezept bleibt geheim, siehe Security-Prep 27.07.). SEASON/CLASSIC-Labels stehen dort direkt am Wert.

**DB-Diaet abgeschlossen (20.08.): 515 → 225 MB.** price_history per Tabellen-Swap dedupliziert (1,69 Mio → ~520k Zeilen, 346 → <100 MB), tote Indizes weg (pkey 37 MB/0 Scans u. a.), Wochen-Rollup `price_history_rollup(90)` laeuft montags 06:30 via pg_cron (greift ab ~Ende Oktober). Drei Editor-Fallen dokumentiert in BUG-018 — die wichtigste: **Migrationen im SQL-Editor immer „ohne RLS" ausfuehren.** Free-Tarif hat damit wieder ~275 MB Luft.

**INFRASTRUKTUR-ENTSCHEIDUNG 25.08.: Supabase Pro.** Nach zwei Totalausfaellen in drei Tagen (INC-005, INC-006) und trotz erheblicher Optimierung (Marktseite 15 MB → 100 KB, DB 515 → 225 MB, Materialized Views, Updater-Fenster halbiert). INC-006 hat bewiesen, dass KEIN Programmierfehler dahintersteckt, sondern das erschoepfte Burst-Budget der NANO-Instanz (Kante bei 04:55: 0,14 s → 43,9 s ohne Uebergang, exakt am Ende des naechtlichen Updater-Fensters). Neuer Kostenrahmen: ~37 EUR/Monat statt ~14 (Railway 6 $ + Domains ~100 EUR/Jahr + Supabase Pro 25 $). Monatlich kuendbar — bei erfolgreicher Index-Bereinigung ist ein Rueckschritt zu pruefen.

**Budget-Rahmen (20.08., Jonas):** Laufende Kosten heute: Railway 6 $/Monat (Updater-Crons) + ~100 EUR/Jahr Domains (sorion.pro + craftlog.pro) = **~170 EUR/Jahr, ~14 EUR/Monat**. Mit Supabase Pro (25 $/Monat) waeren es **~38 EUR/Monat, ~450-470 EUR/Jahr** — eine bewusste Ausgabe, solange die Seite nichts verdient. Deckungsmarken fuer Monetarisierung: laufend ~14 EUR/Monat (~7 Nutzer a 2 EUR), Vollstack ~38 EUR/Monat (~19 Nutzer a 2 EUR). Regel fuer alle kuenftigen Sessions: **kostenlose Loesungen zuerst ausschoepfen** (Indizes, Snapshots, Diaet, Rollup — siehe BUG-015 bis -017), das Upgrade an messbare Signale koppeln (Fehlergraph rot trotz Fixes, 500-MB-Marke trotz Diaet nicht haltbar), und bei neuen Features die Folgekosten (Egress/Compute/Speicher) benennen. Gemessene Einordnung: Free-Tarif traegt den JETZIGEN Stand; Pro traegt das 100- bis 500-Fache des heutigen Traffics und 3,5-10+ Jahre Datensammlung (je nach Rollup).

## Squad-Manager (IDEA-003) — Vorarbeiten 20.08.

- **Konzept:** [IDEAS.md](IDEAS.md) → IDEA-003 (Leaderboard nach Ø-Punkten, Aufstellungs-Snapshots, Player-Cap-Überwachung). Feature landet auf **sorion.pro** (Entscheidung Jonas 20.08.), Weg über OAuth.
- **API-Wissen** (Squad-Typen, was öffentlich geht, kein Abgabe-Zeitstempel): Ökosystem-HANDOFF `C:/craft-log/docs/HANDOFF.md` → „API-Wissen: Squads".
- **Erledigt:** (1) Neue Action `squad_board` in `sorare-oauth` (deployed, Negativ-Test ✅) — liefert Squad-Stammdaten + Squad-Board-Steps inkl. `squadLeaderboardLineups`. (2) Sorion-OAuth-Flow bewahrt jetzt das Sorare-Token auf (`callback.html` → `sorare_access_token`/`sorare_refresh_token` in localStorage; Logout in `profile.html` räumt beide) — vorher wurde es nach `link_sorare` verworfen. Deployed nach sorion.pro (Commit `f1f8c8b` in sorion-ui).
- **✅ Erster Live-Test 20.08. (Jonas' Token, Squad „Handpicked", 10/10, Captain jr3hr):** Funktioniert. Board = **Level-Steps** mit Punktezielen (700→980→1060→1140→1280), Staten CLAIMED/CLAIMABLE/PRE_MATCHDAY_LOCKED/LOCKED; pro abgeschlossenem Step komplettes 10er-Ranking. **Squad-Lineups sind `ThresholdPickerTaskLineup`** (Task-Lineup, NICHT So5Lineup — deshalb war das erste Appearance-Fragment leer). Action um `... on TaskLineupInterface { aasmState score taskAppearances { captain locked lockedAt score player anyCard } } }` erweitert + deployed. `lockedAt` = Deadline pro Karte → perfekt für den Snapshot-Poller. **`nextMatchdayAt` = 21.08. 07:00Z (09:00 dt.)** — Squad-Spieltag ≠ Classic-GW-Start (16:00)!
- **✅ Re-Test 20.08. mit TaskLineup-Fragment: voller Erfolg.** Pro Lineup kommen Spieler (Slug), Karte (Step 700 lief mit common-Karten), Captain, Einzel-Scores, `aasmState` und **`lockedAt` pro Karte** (= Anpfiff des jeweiligen Spiels — Lock ist pro Spieler, nicht global; Poller muss vor dem frühesten Anpfiff laufen).
- **✅ Player-Cap-Auswertung rückwirkend verifiziert (Konsolen-Auswertung über `taskAppearances`):** Step 700: **Messi 6× aufgestellt (Cap 4 gerissen)** — MaisonPanda, FFGAJ, ParisBoemboem, Sorare_Jens, McBeast, JR3HR. Step 980: kein Verstoß, aber 4 Spieler exakt am Limit (Ginter, Kökçü, Nübel, Trossard). Wer chronologisch der 5./6. war, ist rückwirkend nicht feststellbar → dafür braucht es den Snapshot-Poller.
- **✅ Sichtbarkeits-Frage BEANTWORTET (21.08. live):** `PRE_MATCHDAY_LOCKED` heißt „Aufstellen noch nicht möglich" — der Step öffnet erst zum Matchday-Start (09:00). VOR 09:00 kann also niemand aufstellen (Snapshots 00:07 + 09:02: 0 Lineups). **Sobald ein Lineup gesetzt ist, ist es SOFORT per API sichtbar** (09:15, Minuten nach Jonas' Aufstellung: Step-State `LINEUP_SET`, `lineups=1 [JR3HR]`) — lange vor den per-Karte-Locks (Anpfiff). → **Der Poller kann die Aufstellungs-Reihenfolge live erfassen; der 5. Manager beim Player-Cap ist identifizierbar.** Sorare Inside („Squad Insights": Player/Team/Captain-Exposure, Manager-Häkchen, Risk Flags) bestätigt die Live-Sichtbarkeit — zeigt aber weder Reihenfolge noch Ø-Punkte-Historie: das bleibt Sorions Mehrwert.
- **Test-Infrastruktur 21.08. (lokal), Poller v2:** Script `C:\craft-log\tools\squad-snapshot.mjs` (Snapshot + Kurzlog nach `C:\craft-log\_squad_snapshots\`; Token in `~\.claude\secrets\sorare_token.json`, Refresh automatisch). Scheduled Tasks (Claude-App, laufen NUR bei offener App; verpasste Läufe werden nur einmalig nachgeholt): Snapshots **alle 15 Min rund um die Uhr** (Vorgabe Jonas 21.08.: Spiele können nachts liegen, MLS ~04:30 — feste Endzeit wäre falsch). Poller stoppt NICHT zeitgesteuert, sondern über Marker `SNAPSHOT_DONE`, sobald ≥9/10 Lineups **gesperrt** sind — ein Lineup gilt als gesperrt, sobald EIN `taskAppearance` `locked` ist (erster Anpfiff = keine Umstellung mehr). Beim DONE-Lauf fährt derselbe Task direkt die Abschluss-Auswertung (First-seen-Reihenfolge inkl. Umstellungen, Cap-Check, schreibt hierher, deaktiviert sich). Die frühere fixe 21:10-Auswertung („squad-tag-auswertung") ist GELÖSCHT. Reaktivierungs-Task täglich 08:45. Hinweis: Die 09:05-Morgen-Auswertung lief, hat aber nichts geschrieben (vermutlich Permission-Stopp im Scheduled-Run) — Befund manuell dokumentiert.
- **✅ Holo/Skin wird jetzt erfasst (21.08., deployed):** Holo ist KEINE Rarity, sondern ein Skin (`enum Skin { STANDARD HOLO SHINY FULL_ART }`; alle Squad-Karten sind `rarityTyped: common`). `squad_board`-Query um `anyCard.skinEdition { skin skinName bonus }` erweitert — live verifiziert: JR3HR spielt Raya als HOLO (bonus 1.1), ParisBoemboem Shiny-Raya + Shiny-Ødegaard (bonus 1.05). Snapshots vor dem Deploy (21.08. ~09:43) enthalten das Feld nicht rückwirkend.
- **Cap-Rätsel Step 700:** Messi 6× trotz „Cap 4“ — entweder galt die Regel dort noch nicht oder sie greift erst ab einem bestimmten Step. Squad-intern klären.
- **✅ CLOUD-POLLER LIVE (21.08., Entscheidung Jonas):** Edge Function **`squad-poll`** + `pg_cron` `*/15 * * * *` (Job `squad-poll-15min`) → läuft unabhängig von Jonas' Rechner, auch nachts (MLS-Locks ~04:30).
  - **Tabellen** (Migration `migrations/2026-08-21_squad_manager.sql`, eingespielt via `supabase db push`; alle RLS ohne Policies = nur Service-Role): `squad_tokens` (1 Zeile; Tokens in DB statt Secret, weil Sorare Refresh-Tokens ROTIERT — Function schreibt frische Werte zurück), `squad_snapshots` (Roh-Snapshot je Poll des offenen Steps, TTL 24 h, löscht die Function selbst), `squad_lineup_log` (dauerhaft: Step×Manager×Spieler mit first/last_seen_at, removed_at, captain, skin, locked → First-seen-Reihenfolge für den Player-Cap), `squad_step_scores` (dauerhaft: Punkte/Ranking je Step×Manager → Ø-Leaderboard).
  - **Auth:** `verify_jwt = false` + Header `X-Cron-Secret` gegen Secret `SQUAD_CRON_SECRET` (Muster wie CRON_SECRET; Klartext NUR bei Jonas lokal in `~\.claude\secrets\squad_cron_secret.txt` + im pg_cron-Job; Migration im Repo enthält Platzhalter `__SQUAD_CRON_SECRET__`). Actions: `poll` (Default), `seed_tokens`, `status`. Negativ-Test 403 ✅.
  - **Erster Poll 21.08. 10:01 verifiziert:** 110 lineup_log-Zeilen (inkl. Gratis-Backfill der fertigen Steps 700/980 — Messi-6×-Beleg jetzt dauerhaft in DB), 22 score-Zeilen, Snapshot geschrieben. Achtung: `first_seen_at` der Backfill-Steps = Poll-Zeitpunkt, nicht echte Reihenfolge.
  - **UI-Zugriff später NICHT per Direkt-Read** (RLS zu!), sondern über eigene Function/RPC.
- **Lokale Scheduled Tasks (Claude-App):** nach Verifikation des ersten pg_cron-Laufs deaktiviert — Cloud übernimmt. First-seen des heutigen Steps aus der lokalen Phase: 1. JR3HR ~09:10, 2. ParisBoemboem ~09:43 (lokale Snapshots in `C:\craft-log\_squad_snapshots\` bleiben als Beleg).
- **✅ Neue Action `cap_report` in `squad-poll` (21.08., deployed):** je Spieler alle Manager in First-seen-Reihenfolge inkl. **Skin + Karten-Slug** (`{action:'cap_report', limit?:4, step_id?}`), auch entfernte Einträge. **Erster aufgeklärter Cap-Fall (Step 1060): Gouiri 5×** — 5. war `sorare_jens` 14:45 dt., ABER mit **HOLO** (wertvollste Version im Konflikt; davor 3× STANDARD jr3hr/andreihaha/ffgaj + SHINY mcbeast). Grenzen: 15-min-Raster (gleicher Snapshot = nicht ordnbar), first_seen vor Cloud-Start trägt den Backfill-Zeitpunkt (10:01).
- **⚠️ Squad-Regel präzisiert (Jonas, 21.08.) — Cap ist wertigkeitsbasiert, nicht rein zeitlich:** Eine Chroma-Version (Skin > Standard) darf den 5. Slot **claimen**; am Ende bleibt die **wertvollste Karte** stehen, sofern genug Basics (Standard) im Konflikt sind — weichen muss der niedrigste Skin (bei Gleichstand: zuletzt gekommen / wer nicht geclaimt hat). Gouiri-Fall: Jens' Holo bleibt, sein Versäumnis war nur das fehlende Claimen; einer der 3 Basic-Halter muss weichen — laut Jonas trifft es ihn selbst (jr3hr). Die genaue Weich-Reihenfolge unter gleichwertigen Basics (ältester? jüngster? Absprache?) beim UI-Bau mit Jonas klären. **Gelebter Präzedenzfall Raya:** ffgaj kam 12:15 als 5. (SHINY), der einzige Basic-Halter mcbeast nahm seinen um 13:30 raus → 4. Skin-Rangfolge aus API-`bonus`: STANDARD 1 < SHINY 1.05 < HOLO 1.1 (< FULL_ART). → Die UI-Cap-Ampel muss zweistufig sortieren: Skin-Wert desc, dann first_seen asc, und „wer muss weichen“ ausweisen.
- **✅ Discord-Bot live (22.08.):** Webhook „Pick" in `#🚨alert🚨`, Meldungen auf **Englisch** (Squad ist international). Events: Aufstellung gesetzt · ⚠️ Cap erreicht (mit „wer muss noch stellen") · 🚨 Verstoß inkl. Reihenfolge, Bonus je Karte und Begründung wer weichen muss · ✅ Konflikt gelöst. Dedup über `squad_notifications` (event_key als PK → jede Meldung genau einmal). Secret `DISCORD_WEBHOOK_URL`.
- **✅ Cap-/Claiming-Regel final (22.08., offizieller Squad-Regeltext):** Ein Spieler darf von max. 4 Managern aufgestellt werden. Stellt ein **5.** auf, gilt: Er **behält** seine Karte, wenn (a) sein Bonus höher ist als der eines der ersten vier UND (b) er das **vor 19:00 am Lock-Tag** in `#📈-lineups` postet und den Betroffenen @mentioned — dann weicht **der Gemeinte**. Ohne gültigen Claim (kein Post, zu spät, oder Bonus nicht höher) muss **der 5. selbst** einen anderen Spieler aufstellen, unabhängig von der Seltenheit. **Bonus = Gesamtbonus abzüglich Captain** (fix +50 %, verifiziert als `captainBasisPoints`=5000); Bestandteile Skin/Collection/XP — Skin allein genügt nicht (Messi 22.08.: jr3hr Holo+Collection **+15 %**, paris/mcbeast Shiny +10 %, ffgaj Standard +3 %). ⚠️ **Den Claim-Post kann der Bot NICHT prüfen** (Webhook sendet nur) — die Meldung zeigt deshalb beide möglichen Ausgänge. Automatische Prüfung bräuchte einen echten Discord-Bot mit Leserecht auf `#📈-lineups`.
- **✅ Claim-Deadline-Erinnerung (22.08.):** An Spieltagen um **18:30 Berliner Zeit** postet der Bot `@everyone` mit dem Hinweis, dass Claims nach 19:00 ungültig sind — inklusive der **konkret offenen Konflikte** (wer wen claimen müsste, oder dass kein gültiger Claim möglich ist) und der Spieler, die bereits bei 4/4 stehen. Umsetzung im Poller selbst (läuft `*/15`, trifft 18:30 exakt) statt als eigener Cron — damit ist die Sommer-/Winterzeit automatisch korrekt (`Europe/Berlin`). Guard: nur bei offenem Step und **solange noch nichts gesperrt ist**; Dedup über `claimdeadline:<step>:<YYYY-MM-DD>` (genau eine Erinnerung pro Tag). `allowed_mentions.parse=['everyone']` für den echten Ping.
- **✅ Bot IST der Claim (Entscheidung Jonas 22.08.):** Der Bot-Post in `#📈-lineups` **gilt als Claim** — der Manager muss nichts mehr selbst posten („alles andere wäre doppelt gemoppelt"). Damit ist der Fall eindeutig entscheidbar: 5. Kopie mit höherem Bonus **vor 19:00** → gültiger Claim, der Schwächste der ersten vier wird angepingt und muss tauschen. Höherer Bonus, aber **nach 19:00** → „Too late to claim", der Späte stellt um. Kein höherer Bonus → „no claim possible", der Späte stellt um. Ziel-Channel über Secret `DISCORD_LINEUPS_WEBHOOK_URL` (Fallback: Haupt-Webhook). **Anzeigename:** kein `username`-Override im Code (22.08. entfernt) - es gilt der in Discord am Webhook hinterlegte Name (aktuell beide 'Coordinator Bot'). Umbenennen also direkt in Discord, ohne Deploy.
- **INTERN, nicht kommunizieren (Jonas 22.08.):** Auf die 19:00-Frist liegt eine **stille Toleranz von 10 Minuten** (`CLAIM_GRACE_MIN` in `squad-poll`). Grund: Sorare liefert keinen Abgabe-Zeitstempel, wir sehen nur den ersten Poller-Kontakt (im Claim-Fenster bis zu 5 Min spaeter) - ohne Puffer wuerde jemand faelschlich als 'zu spaet' gelten. Alle Discord-Texte nennen weiterhin ausschliesslich 19:00, damit die gefuehlte Frist nicht auf 19:10 wandert.
- **WICHTIG - Claim-Frist haengt am Anpfiff, nicht nur an 19:00 (Befund Jonas 22.08.):** Ein Lineup ist gesperrt, sobald die ERSTE Karte darin anpfeift. Wer tauschen soll, braucht davor noch **30 Minuten** (`SWAP_MIN`). Effektive Frist gegen Manager X = frueher von (19:00 + stille Toleranz) und (Anpfiff von X minus 30 Min). **Klarstellung Jonas 22.08.: Die Anpfiff-Regel macht die Frist nur FRUEHER, nie spaeter - 19:00 bleibt harte Obergrenze** (entspricht dem Regeltext 'no later than 7 PM'). Bei einem 21:30-Anpfiff gilt also weiterhin 19:00, bei 13:30-Anpfiff dagegen 13:00. Gemessen am 22.08.: 6 Manager sperrten schon **13:30** (Frist 13:00), maisonpanda 21:30, sorare_jens **01:30 nachts** - eine pauschale 19:00-Frist waere fuer die einen zu spaet und fuer die anderen zu frueh gewesen. Quelle: `taskAppearances.lockedAt` (= Anpfiff des jeweiligen Spiels), seit 22.08. als Spalte `locked_at` gespeichert.
- **Erinnerung ist dynamisch UND bedingt:** feuert 30 Min vor der FRUEHESTEN echten Frist des Steps (nicht mehr fix 18:30) und nennt Fristzeitpunkt und ersten Anpfiff. **Sie kommt nur, wenn es etwas zu tun gibt** (Vorgabe Jonas 22.08.): offener Konflikt (5. Kopie) ODER ein Spieler steht bereits bei 4/4, waehrend noch jemand aufstellen muss - dann waere der Naechste automatisch der 5. Stehen alle Spieler unter dem Cap, gibt es keinen @everyone-Ping.
- **Zeitliche Genauigkeit an der Frist:** Zusätzlicher Cron `squad-poll-claimwindow` (`*/5`), der über `window:'claim'` außerhalb **17:30–19:15 Berlin** sofort verworfen wird → Unschärfe am 19:00-Stichtag nur noch 5 statt 15 Minuten, ohne den ganzen Tag häufiger zu pollen (Lehre aus INC-005).
- **Echte @-Pings:** Tabelle `squad_discord_users` (sorare_slug → discord_id) + Action `set_discord_ids` (`{map:{slug:id}}`). Ohne hinterlegte ID fällt die Meldung auf reinen Text `@slug` zurück (kein Ping). **✅ Alle 10 IDs hinterlegt (22.08.)**, Quelle/Pflege: `~\.claude\secrets\discord_ids.txt`, Einspielen per `set_discord_ids`. Ping live getestet. Zweiter Webhook `DISCORD_LINEUPS_WEBHOOK_URL` → Channel `1528702115005071430` (Claims), Alerts weiterhin im Haupt-Channel.
- **Stolperfalle beim Testen:** Discord antwortet auf Requests mit Pythons Standard-User-Agent (`Python-urllib/*`) mit **403**. Lokale Testskripte brauchen einen eigenen `User-Agent`-Header; `curl` und die Edge Function (Deno-fetch) sind nicht betroffen.
- **Leaderboard-Auswertung (22.08.) - NUR auf Abruf, der Bot postet es NICHT** (Vorgabe Jonas: das Leaderboard pflegt er selbst in Claude Cowork, eigener Kanal). Action `leaderboard` rechnet den offiziellen Season-Modus: Platzierungspunkte je Game Week (1.=10, 2.=9, 3.=8, 4.=6, 5.=5, 6.=4, 7.=3, 8.=2, 9.=1, 10.=0), **Stage-Bonus = Stage-Nummer fuer die Raenge 1-3** einer geschafften Stage (nur die drei zaehlen zum Squad Score - bestaetigt: Step 700 totalScore 1212,7 = Summe der Top 3), **-5 fuer fehlendes Lineup** (Abmeldungen kann der Bot nicht kennen -> per `excuse:["step_id:slug"]` erlassbar), `seed:{slug:punkte}` fuer Altbestand. Die 5 Board-Steps (700/980/1060/1140/1280) SIND die Stages 1-5.
- **Stage-Nummern je Board (Fix 22.08.):** `board_id` wird jetzt in `squad_step_scores` mitgeschrieben. Ohne das waeren die Stage-Nummern beim naechsten Board von 1..10 weitergelaufen statt zweimal 1..5 - der Stage-Bonus haette still falsch gerechnet. (Die 'STAGE CLEARS'-Zeile im Board von Jonas zeigt, dass es im Set schon ~5 Boards gab.)
- **!! OFFEN & WICHTIG (23.08. mit Jonas): Der Cap-Tie-Break nutzt die FALSCHE Rangfolge.** Bei gleichem Bonus entscheidet laut Regel der Leaderboard-Platz. `lbPos` in `squad-poll` rechnet dafuer aktuell den **Durchschnitt aus unseren gespeicherten Steps** - das offizielle Board rankt aber nach **Punkten** (Platzierung + Stage-Bonus - Strafen) ueber das ganze Set. Die Reihenfolgen weichen ab (z. B. Namiunk_022: Platz 6 nach Punkten, Platz 9 nach Ø). Folge: Bei einem Gleichstand-Claim kann der Bot den Falschen benennen. Zu tun: (a) `lbPos` auf die Punktewertung der `leaderboard`-Action umstellen, (b) Seeding der Set-Historie, sonst bleibt die Rangfolge unvollstaendig. Jonas erklaert am 23.08. die Regel-Details.
- **Offen fuers Leaderboard:** (1) Seeding ohne Doppelzaehlung - unklar, welche Stages im 21.08.-Stand schon enthalten sind. (2) Laufende Game Weeks sind vorlaeufig (Platzierungen aendern sich bis zum letzten Anpfiff).
- **✅ Captain-Ausnahmen (23.08.):** Tabelle `squad_overrides` + Action `override` (`{op:'add'|'list'|'remove', kind, value, step_id?, note?}`). Vier Arten: **`position_exempt`** (Position vom Cap ausnehmen - Anlass: zu wenige Torhueter fuer den GK-Cap), **`player_exempt`** (einzelner Spieler), **`cap`** (abweichende Cap-Zahl fuer den Step), **`excuse`** (Manager, kein -5 fuers fehlende Lineup). `step_id = null` macht daraus eine Dauerregel. Wirkt auf Cap-Meldungen, Claims UND die Erinnerung. Jonas nennt die Ausnahme, ich setze sie - ein Discord-Slash-Command braeuchte einen echten Bot statt des Webhooks.
- **⚠️ Neuer Step-State `FAILED` (23.08. erstmals gesehen):** Stage 5 des ersten Boards wurde verpasst. `isActive` schloss zunaechst nur CLAIMED/CLAIMABLE aus - der Bot haette fuer den toten Step weiter gemeldet. Jetzt `DONE_STATES = [CLAIMED, CLAIMABLE, FAILED]`. Stage-Bonus zaehlt FAILED korrekt nicht als geschafft.
- **Board-Wechsel live bestaetigt (23.08.):** Neues Board gestartet, Stage-Nummern laufen korrekt wieder bei 1 los statt durchzuzaehlen - der `board_id`-Fix von gestern hat sofort getragen.
- **✅ Saison-Stand geseedet (24.08.):** Jonas' History liegt in `C:\craft-log\_squad_snapshots\Bot Data\` (runden.csv = 21 Runden je Manager, leaderboard-daten.json = Stand/Strafen/Meta, handpicked-leaderboard.md = Arbeitskopie). **Regelwerk gegen die History verifiziert: 21 Runden nachgerechnet, 0 Abweichungen** - Placement Points, Stage-Bonus (nur bei cleared, nur Top 3), Strafen, Ø inkl. 0-Runden und Beitrittsrunden (McBeast/Sorare|MA ab R2 = 20 Runden) stimmen exakt mit Punkten UND Durchschnitten des offiziellen Boards ueberein.
- **Neue Tabellen:** `squad_season_state` (Punkte, score_sum, rounds_played, joined_at_round, unexcused_missed je Manager - geseedet bis R21) und `squad_rounds` (Rundenergebnisse ab API-Tracking). Actions: `seed_season`, `season` (Ausgabe sortiert nach Regel 8: Punkte, dann Ø).
- **✅ Widerspruch Claim-Verfahren GEKLAERT (Squad-Ansage 24.08.):** Der **Bot claimt automatisch** - manuelle Posts entfallen, Spec 6.3 ist insoweit ueberholt (dort vermerkt). Bestaetigt wurden ausserdem: Claims nach **Bonus %** statt Seltenheit, bei Gleichstand die **Leaderboard-Platzierung**, Frist 19:00 bzw. **30 Min vor dem Anpfiff des Betroffenen** (Beispiel B der Ansage entspricht exakt der gebauten Logik: maßgeblich ist der Anpfiff des Tauschpflichtigen, nicht der des Spielers). Cap-Strafe ab 24.08. **-5**.
- **✅ Tie-Break korrigiert (24.08.):** `lbPos` nutzt jetzt den offiziellen Stand aus `squad_season_state` (Punkte, dann Ø) statt des Ø aus den gespeicherten Steps. Vorher haette der Bot bei Bonus-Gleichstand den Falschen benannt (z. B. Namiunk_022: offiziell Platz 10, nach Ø waere er 9. gewesen).
- **OFFEN (Rest):** SQUAD_LEADERBOARD.md 6.3/6.4c verlangt einen **manuellen** Post des Managers; ohne gueltigen Claim zahlt **jeder** ab Position 5 eine eigene Strafe (belegt in den Altfaellen R17: MaisonPanda -2 als 5., Sorare_Jens -2 als 6.). Jonas' muendliche Entscheidung vom 22.08. lautete dagegen: **der Bot-Post GILT als Claim**. Aktuell ist Letzteres gebaut (Auto-Claim, keine Strafverbuchung). Muss geklaert werden - falls die Spec gilt, braeuchte der Bot **Lesezugriff auf #Lineup** (Webhook kann nur senden -> echter Discord-Bot noetig).
- **Neue Action `overview` (24.08.):** Cap-Lage der laufenden Runde auf Knopfdruck - wer tauschen muss (inkl. Claimer, Bonuswerte und Frist), wer keinen gueltigen Claim hat, wer am Limit steht, welche Ausnahmen gelten. Nutzt dieselbe Kettenlogik wie die Live-Meldungen. `{post:true}` schickt sie in den Lineups-Channel. Erstmals genutzt am 24.08.
- **Vorgemerkt (nicht in Arbeit): Captain-Seite auf sorion.pro** fuer die Sonderregeln - Details und Begruendung in [IDEAS.md](IDEAS.md) IDEA-004. Sie ist zugleich der Bruecken-Baustein zur Mandantenfaehigkeit (fremde Squads koennen sich Regeln nicht von Claude setzen lassen).
- **✅ Lesender Zugang fuer den Cowork-Chat (24.08.):** `GET .../squad-poll?key=<SQUAD_READ_TOKEN>&action=report` liefert die komplette Rundenauswertung als JSON - Scores je Manager, Platzierung, Placement Points, Stage-Bonus, Squad Score (Top 3), cleared/failed, Cap-Verstoesse (Ausnahmen beruecksichtigt) und den Saison-Stand. **Ersetzt die taeglichen Screenshots.** Eigener Token `SQUAD_READ_TOKEN` (Klartext in `~\.claude\secrets\squad_read_token.txt`), **rein lesend**: GET erlaubt nur `report`, `season`, `leaderboard` - kein Schreiben, kein Discord-Post. Ohne Key 403.
- **Achtung bei der Nutzung:** Waehrend `state=LIVE` aendern sich die Scores noch. Fuer die Auswertung erst abrufen, wenn die Runde durch ist (state CLAIMED/CLAIMABLE/FAILED).
- **Kanal-Aufteilung (Vorgabe Jonas 25.08.):** `#📈-lineups` enthaelt **nur handlungsfordernde Einzeiler** ("@X - you have to swap out Y" / "...line up a different player") mit Ping. **Alles andere geht in den Alert-Channel**: die Detail-Karte zum Claim (Bonuswerte, Begruendung, wer noch drin ist), Aufstellungs-Meldungen, Cap-erreicht, Fristerinnerung, Entwarnungen, Fix-Meldungen und die Uebersicht. Umsetzung: je Claim zwei `notifyOnce`-Aufrufe (`claim:...` = Einzeiler nach LINEUPS_WEBHOOK, `detail:claim:...` = Embed nach WEBHOOK).
- **Entwarnung repariert (25.08.):** Der Block suchte noch nach `viol:`-Schluesseln, die es seit dem Umbau nicht mehr gibt - er lief ins Leere. Jetzt liest er die `claim:`-Meldungen, zieht das Ziel aus dem Schluessel und postet einmalig, sobald dieses Ziel den Spieler nicht mehr aufgestellt hat. **Namen in GROSSBUCHSTABEN fett, ohne @** (Bestaetigung, kein Ping). Text unterscheidet jetzt korrekt zwischen "back within the cap" und "still over the cap - one more copy has to go" (vorher stand faelschlich "back within the cap (5/4)").
- **Erinnerung hat zwei Varianten (26.08.):** Bei Handlungsbedarf wie bisher `@everyone` mit Konfliktliste und Pings an Nachzuegler. **Ist alles in Ordnung**, kommt jetzt eine ruhige Meldung **ohne Ping**: "All good - no changes needed" plus der Hinweis, dass bis zur Frist noch umgestellt und **geclaimt** werden darf, dazu die Liste der Spieler am Limit. So weiss der Squad, dass geprueft wurde, statt zu raten, ob der Bot stumm ist - ohne Ping ohne Anlass. Beide Varianten bekommen ein Meme.
- **Erinnerung pingt jetzt auch die Nachzuegler (25.08.):** Abschnitt "No line-up yet - please submit" mit echten @-Pings fuer jeden Manager ohne Aufstellung. Sie feuert ausserdem **auch dann**, wenn es gar keinen Cap-Konflikt gibt, aber noch Aufstellungen fehlen (vorher nur bei Konflikt oder Spieler am Limit). Anlass: Sorare | MA hat am 25.08. erst kurz vor Schluss gestellt.
- **Meme-Pool fuer die Erinnerung (25.08.):** Tabelle `squad_memes` + Action `memes` (`{op:'add'|'list'|'remove'|'toggle', urls?, id?, active?}`). Der Bot haengt an jede Claim-Erinnerung **ein zufaellig gewaehltes aktives Meme** an. Aktuell 10 Stueck (Klipy-Links von Jonas). **Zwei getrennte Sammlungen** (Spalte `kind`, Vorgabe Jonas 26.08.): `reminder` (12 Memes, Handlungsbedarf) und `allclear` (10 Memes, Entwarnung) - sie duerfen sich nicht mischen. **Keine direkte Wiederholung:** das zuletzt gezogene Meme faellt aus der Auswahl (Spalte `last_used_at`), ausser es ist das einzige. Rein kosmetisch - ist der Pool leer, bleibt die Erinnerung unveraendert. **Achtung:** Die Anbindung an die Erinnerung fehlte zunaechst - eine Ersetzung hatte still nicht gegriffen, die Memes lagen in der DB, wurden aber nie gezogen (gefixt 25.08.). **Hinweis:** Klipy-Links betten sich in Discord nicht zuverlaessig als Vorschau ein; Tenor/Giphy oder direkte `.gif`-Adressen rendern besser. Falls die GIFs nur als blauer Link erscheinen, dort tauschen.
- **Neue Action `fix_note`:** postet eine kurze Fix-Meldung (was behoben, was war falsch, wie rechnet es jetzt) in den Alert-Channel. Dedup ueber `fix:<id>`.
- **✅ Saison-Stand aktualisiert auf R23 (26.08.):** Der Tie-Break rechnete noch mit dem Stand nach R21 - seither gespielte Runden fehlten, weil die Fortschreibung nie gebaut wurde. Neue Werte stammen aus der Leaderboard-Engine in `C:\craft-log\squad-bot\leaderboard\` (Gegenprobe: **alle 23 Runden reproduziert**). Auswirkung war real: Sorare | MA #2→#4, Sorare_Jens #3→#2, McBeast #8→#6, FFGAJ #5→#7.
- **⚠️ WARNUNG zur Leaderboard-Engine:** Ihr `README_1.md` sagt, `index.ts` gehoere nach `supabase/functions/squad-poll/`. **Das wuerde den laufenden Bot loeschen** - die neue Datei hat 290 Zeilen und enthaelt NICHTS von Claims, Cap-Kette, Discord-Meldungen, Fristen oder Poller (27 entsprechende Stellen in der laufenden 1204-Zeilen-Function, 0 in der neuen). Empfehlung: als **eigene Function** deployen (z. B. `squad-board`), `squad-poll` bleibt der Claim-/Cap-Bot. Beide koennen dieselbe DB nutzen.
- **Zweites Datenmodell:** Die Engine bringt eigene Tabellen mit (`managers`, `rounds`, `round_results`, `penalties`, `lineup_entries`, `cap_instructions`, `cap_overrides`) - inhaltlich parallel zu den bestehenden `squad_*`-Tabellen. Vor dem Einspielen klaeren, welches Modell fuehrend ist, sonst laufen zwei Wahrheiten nebeneinander.
- **Neu in der Engine, noch nicht im Bot:** Blockregel des neuen Sets ab 08.09. (14-Tage-Bloecke, Mindestbeteiligung 60 %, Strikes) und `first_poll`-Kennzeichnung fuer unzuverlaessige Zeitstempel (in R23 hatten drei Manager denselben first_seen = erster Poll des Tages).
- **Neue Actions in `squad-poll`:** `cap_report` (Cap-Lage je Spieler mit Reihenfolge, Skin, Bonus, Captain) und **`bonus_report`** (exakte Bonus-Aufschlüsselung je Karte über `currentUser.step(id)` + `powerBreakdown` — eigene schlanke Query, weil die Board-Query mit powerBreakdown Sorares Komplexitätslimit von 30.000 reißt; `top: 20`→`10` gesenkt — **10 ist Sorares harte Obergrenze fuer Squad-Mitglieder** (`Squad.maxMembersCount`), also keine Einschraenkung, sondern der exakte Vollausbau).
- **✅ Saison-Stand dauerhaft geloest (27.08., BUG-030):** Der Punktestand wird **nicht mehr gespeichert**, sondern bei **jedem Abruf** aus den Rohdaten gerechnet (`computeStandings()` in `squad-poll`). Quellen: `squad_history_rounds`/`squad_history_results` (R1-R23, unveraenderlich geseedet), `squad_step_scores` verknuepft ueber `squad_step_rounds` (Rundennummer wird beim Rundenende **automatisch** vergeben, einmalig je Step) und `squad_penalties`. **Alle drei Verbraucher lesen dieselbe Berechnung:** `season`, `report.standings` und der Tie-Break im Poller - Abweichungen wie bei BUG-022/023/024 sind damit strukturell ausgeschlossen. `squad_season_state` und `seed_season` sind abgeloest (Tabelle bleibt als Archiv, Action gibt 410).
- **Verifiziert (27.08.):** Berechnung ohne laufende Runden reproduziert R23 exakt (Paris 152 P / Ø 353,25). Nach Zuordnung von R24: Paris 164 P - erwarteter Wert. Poll-Lauf danach vergab **keine** neuen Nummern (idempotent). Neue Actions: `steps` (Diagnose Step->Runde), `seed_history`, `map_round`, `penalty`.
- **Gegenprobe gegen das offizielle Board (27.08., Stand nach R24):** Jonas hat die offizielle Tabelle geliefert - **alle 10 Manager deckungsgleich** in Punkten und Ø, dazu **Stage Clears 6/5/5/3/1** und **Squad Ø 1168,63** exakt. Auch die Auf-/Absteiger-Spalte stimmt (FFGAJ ▲1, MaisonPanda ▲1, McBeast ▼2, Namiunk_022 ▲1, Enexxx ▼1). Damit ist die zustandslose Berechnung gegen 24 Runden verifiziert, nicht nur gegen die geseedete Historie.
- **Neu in `season`:** `squadAvg` (Ø des Squad Scores ueber alle gewerteten Runden), `lastRound` und je Manager `move` (Rangaenderung gegenueber dem Stand vor der letzten Runde; > 0 = aufgestiegen). `move` entsteht aus einem zweiten, identisch gerechneten Durchlauf ohne die letzte Runde - keine gespeicherte Vorrunden-Tabelle.
- **Was das am Rundenende bedeutet:** nichts zu tun. Sobald ein Step auf CLAIMED/CLAIMABLE/FAILED springt, bekommt er seine Nummer und faellt in die Wertung. Kein Nachpflegen, kein Seed, kein Screenshot.
- **✅ Phantom-Entwarnungen behoben (27.08., BUG-031):** Die "you don't have to swap X any more"-Meldung leitete den Empfaenger aus dem Dedup-Schluessel ab. Bei **abgelehnten** Claims endet der aber ebenfalls auf den Ziel-Manager, obwohl dort der **Claimer** angeschrieben wird - also bekamen Unbeteiligte eine Entwarnung. Jetzt entscheidet die gespeicherte Nutzlast (`target` vorhanden = gueltiger Claim). Wer selbst getauscht hat, bekommt nur die gruene Meldung.
- **Kanal-Aenderung (27.08.):** **Beide** Fristmeldungen gehen jetzt in den **Lineup-Kanal** - die Entwarnung ("All good - no changes needed", ohne Ping) *und* die Variante mit Handlungsbedarf (@everyone + Konfliktliste + Meme). Begruendung Jonas: "im alert hat sie keiner wahrgenommen". Der Alert-Kanal behaelt die Detailkarten, Tausch-Bestaetigungen und Uebersichten.
- **Testauslauf (27.08.):** `poll` mit `{"test_reminder": true}` schickt die Fristmeldung sofort - **derselbe Code, dieselben Live-Daten**, nur ausserhalb des Zeitfensters und mit eigenem Dedup-Schluessel (`claimdeadline-test:<step>:<timestamp>`). Der echte Abendlauf bleibt unberuehrt, der Test ist beliebig oft wiederholbar. Damit laesst sich pruefen, was abends tatsaechlich rausgeht, statt eine Kopie von Hand zu posten.
- **✅ Live-Punktestand (27.08.):** Neue Action `live` (auch lesend ueber den Read-Token). Sie liest die Scores, die der Poller ohnehin alle 10 min in `squad_step_scores` mitschreibt, und liefert: Punktestand je Manager mit Live-Platzierung und vorlaeufigen Platzierungspunkten, Squad Score (Top 3), Abstand zum Ziel, Anzahl der Manager ohne Punkte (= noch nicht gespielt) sowie eine **Projektion der Saisontabelle** ("waere jetzt Schluss"). Die Projektion nutzt `computeStandings(extra)` - die laufende Runde wird nur versuchsweise mitgerechnet und **nirgends gespeichert**.
- **Einschraenkung der Projektion:** Solange Manager bei 0 stehen, ist die Reihenfolge unter ihnen willkuerlich (gleicher Score), und die Platzierungspunkte darunter sind entsprechend vorlaeufig. Aussagekraeftig wird die Projektion erst, wenn alle Spiele laufen bzw. gelaufen sind.
- **✅ Liveticker (27.08., IDEA-005):** Eigener Discord-Kanal, meldet **Ueberholvorgaenge im Tagesergebnis** der laufenden Stage (nicht das Saison-Leaderboard). Laeuft im bestehenden Poll mit - kein zusaetzlicher Sorare-Aufruf, keine zusaetzliche Schreiblast. Schwelle: ab 5 Managern mit Punkten. Sortierung deterministisch (Punkte, dann Slug), sonst melden Punktgleichstaende Phantom-Wechsel. Webhook als Secret, siehe **SEC-005** (Rotation empfohlen).
- **✅ Erster vollstaendiger Durchlauf (27.08., R25):** Ziel 1140 sicher erreicht, 🎯-Meldung mit Meme um 23:10 Berlin, Runde danach **automatisch** als R25 gewertet. Saisonstand jetzt 25 Runden: ParisBoemboem 176 P (Ø 360.24) vor Sorare_Jens 150 und andreihaha 148; MaisonPanda ▲1 auf 6, FFGAJ ▼1 auf 7. Squad Ø 1174.48, Stage Clears 6/5/5/4/1. **Kein manueller Eingriff noetig.**
- **⚠️ Cowork-URLs haben ein Pfad-Suffix (27.08., BUG-032):** `.../squad-poll/v2?key=…&action=report`. Der Abrufweg des Cowork-Bots hatte die Antwort **14 Stunden** zwischengespeichert und Query-Parameter dabei ignoriert. Ein anderer PFAD ist fuer den Cache eine neue Ressource - dieselbe Function, kein zweiter Deploy. Faellt es erneut auf: Suffix hochzaehlen. Fertige URLs in `_squad_snapshots\Bot Data\cowork-urls.txt`. Neu: `report` liefert `generatedAt`, damit veraltete Antworten sofort auffallen.
- **⚠️ INC-007 (29.08.):** Der Bot war ~12 h stumm - `appDone` wurde oberhalb seiner Definition benutzt, jeder Poll mit offener Runde brach mit HTTP 500 ab. Behoben; zusaetzlich liegt der **komplette Ticker-Block jetzt in try/catch**, damit ein Fehler im Beiwerk nie wieder die Cap-/Claim-Logik mitreisst. **Merke:** Deploys ausserhalb der Spielzeit testen den Pfad "offene Runde" nicht.
- **✅ Sorare-Aussetzer abgefangen (31.08., BUG-035):** Sorare lieferte jr3hrs Aufstellung von 11:30 bis 15:40 nicht mehr aus. Der Bot deutete das als fuenf Auswechslungen und meldete zwei Tausche, die nie stattfanden. Jetzt gilt: Fehlt ein Manager **komplett** im Board, bleiben seine Zeilen unangetastet und es wird nichts gemeldet. Der Bot selbst lief die ganze Zeit fehlerfrei durch.
- **✅ Startelf-Warnung VOR Anpfiff (04.09., IDEA-006):** `Game.homeFormation/awayFormation { startingLineupAvailable startingLineup bench }` liefert die veroeffentlichte Elf, sobald der Anbieter sie hat (~1 h vor Anpfiff). Der Poller pingt im Lineup-Kanal, wenn ein Spieler des Squads auf der Bank oder nicht im Kader steht - **nur Manager, deren Aufstellung noch nicht gesperrt ist** (Sorare sperrt die ganze Aufstellung mit der ersten angepfiffenen Partie, Karte `locked`). Kann niemand mehr tauschen, gibt es keine Meldung. Einmal je Spieler und Runde (`s11:<step>:<player>`). Gegenprobe 04.09.: Dembele (jr3hr) Bank, aber jr3hrs Sulc spielte bereits -> korrekt keine Meldung. Action `availability` (auch lesend) zeigt starter/bench/not_in_squad/pending + `canSwap`. **Fundweg fuer kuenftige Feldfragen:** Introspection ist abgeschaltet, aber das komplette Schema liegt unter `https://api.sorare.com/graphql/schema` (39.180 Zeilen) - ZUERST dort suchen statt Feldnamen zu raten. Ebenfalls dort: `Player.playingStatus` (genereller Status, nicht fixture-bezogen), `PlayerGameStats.footballPlayingStatusOdds`, `projectedScore(so5LeaderboardSlug)` (die "35 Punkte" der App).
- **✅ 0-Runden im Durchschnitt (05.09., BUG-037):** Wer in einer live erfassten Runde nicht aufgestellt hat, zaehlt jetzt mit 0 Punkten (Regel 8). Strafe -5 dafuer bleibt Captain-Entscheidung ueber `penalty`; fuer R29 (Sorare | MA) eingetragen. Neue lesende Action `rounds` (Einzelscores je Runde, `?round=N`), `steps` lesend freigegeben. Cowork-Bot nutzt inzwischen Pfad-Suffix /v4 - jedes Suffix ist erlaubt.
- **Befund 05.09. "Der Bot stand" - war Sorares API:** Cron und Function liefen luckenlos (pg_cron `succeeded`, HTTP 200 `ok:true` um 07:00, 07:10, 07:20, 07:30 UTC). Die Antworten enthielten aber **nur das alte Board** (R27-R30); das neue Board (Ziel 700) tauchte in `currentUser.boards(mode: SQUAD, ...)` erst um **07:40 UTC = 09:40 Berlin** auf - mit da schon 3 Aufstellungen. Die App zeigt ein neues Wochen-Board also frueher als die API. **Gemessene Wechselzeiten** (erstes Sehen des neuen Boards durch den 10-min-Cron, Berlin): 24.08. **09:10**, 29.08. **10:20**, 05.09. **09:40** - unregelmaessig, immer nach 09:00. `endDate` ist an allen Boards null, ein Cutover-Zeitpunkt ist nicht ablesbar. `nextMatchdayAt` (= 09:00 Berlin) gilt nur fuer Stages INNERHALB eines Boards; die sind als LOCKED-Steps schon vorher sichtbar und werden puenktlich erfasst. Betroffen ist ausschliesslich der Board-Wechsel nach einer verlorenen Stage. Folge: In diesem Fenster ist `first_seen` fuer alle bis dahin gesetzten Aufstellungen identisch - die Reihenfolge fuer den Cap-Streit ist dann nicht bestimmbar, es entscheidet faktisch der Bonus. Naechste Beobachtung 12.09. Beleg abrufbar ueber die neue Action **`cron_runs`** (pg_cron-Laeufe + pg_net-Antworten inkl. Body, ~6 h Vorhalt; RPCs `squad_cron_runs`, `squad_http_responses`, `squad_cron_jobs`).
- **pg_net-Timeout 5 s -> 30 s (05.09.):** Die Function braucht 4-7 s; am 05.09. 07:40 UTC lief der Cron-Aufruf in den 5-s-Timeout (Function lief trotzdem zu Ende, darauf ist aber kein Verlass). Beide Jobs per `cron.alter_job` auf `timeout_milliseconds := 30000`, verifiziert.
- **✅ Slug-Aliase (05.09., BUG-039):** Jonas hat sich bei Sorare umbenannt (JR3HR -> R3HR), Sorare vergab einen neuen Slug, der Bot sah einen elften Manager und zaehlte seine Spieler doppelt fuer den Cap. Jetzt `squad_slug_aliases` + Kanonisierung an der Eingangstuer. **Kanon ist auf Wunsch von Jonas `r3hr`** (Migration 20260905123000 zog alle Tabellen inkl. Notification-Schluessel um); `jr3hr` in aelteren Eintraegen = derselbe Manager. **Ablauf bei jeder Umbenennung:** Alias eintragen, `purge_manager all_steps`, Poll.
- **Offen:** (1) UI-Seite auf sorion.pro (Leaderboard Ø-Punkte aus `squad_step_scores`, Cap-Ampel + Timeline aus `squad_lineup_log`/`cap_report`; Zugriff via neuer Function/RPC). (2) Langfristig: Token-Bindung an Jonas' Account — bei Sorare-Re-Login/Widerruf muss `seed_tokens` neu befüllt werden (Ablauf dokumentieren).

## 🔴 AKUT (25.08.): DB-Totalausfall Nr. 2 — siehe [INCIDENTS.md](INCIDENTS.md) INC-006

Der Squad-Bot hat am 25.08. nichts gemeldet, weil REST/Auth seit ~07:45 UTC nicht antworten.
**Nicht** der Cache-Waermer (seit 22.08. aus), sondern die **Updater selbst**: `Updater Limited`
lief 22:00-05:16 (>7 h statt ~4 min) bei Cron `*/5` — die Laeufe ueberlappen sich.
`Club_Rosters` hing im Dauer-Retry und wurde 08:12 per `railway down` gestoppt.

**Zu tun, in dieser Reihenfolge:** (1) Projekt neu starten. (2) Updater von `*/5` auf `*/30`
und Fenster verkuerzen. (3) `Club_Rosters` aus lassen bzw. woechentlich. (4) Kapazitaets-
entscheidung: NANO/Free traegt die Schreiblast nicht — Frequenz runter oder Supabase Pro.

**AUFGELOEST (25.08. nachmittags):** Ursache bewiesen (Burst-Budget-Klippe 04:55, kein
Programmierfehler — Details INC-006 in INCIDENTS.md), **Supabase Pro + Micro-Compute aktiv**.
Cron-Kernjobs wiederhergestellt (accuracy 05:45, refresh 09:20, rollup Mo 06:30); Squad-Poller
bewusst noch aus. Erste Nacht auf Micro = Belastungsprobe, Cron-Historie am 26.08. pruefen.

## 🔍 Verkaufsarten in Sorares Preishistorie (Befund 05.09.2026)

**Sorares `tokenPrices` mischen drei Dinge**, erkennbar an `deal.__typename`:
`TokenPrimaryOffer` (Sofortkauf VON Sorare), `TokenAuction` (Sorare-Auktion),
`TokenOffer` (Manager zu Manager = der eigentliche Zweitmarkt). Wir warfen den Typ
bisher weg und bewerteten alles gleich.

**Messung (180 liquideste Spieler, 3.598 Sales, tools/2026-09-05_deal-type-share.mjs):**

| Segment | Sorare selbst | Manager→Manager | Zweitmarkt vs Sofortkauf | vs Auktion |
|---|---|---|---|---|
| In-Season Limited | 79 % | 21 % | −12 % | −3 % |
| In-Season Rare | 51 % | 49 % | −34 % | −10 % |
| Classic Limited | 0 % | 100 % | (kein Sorare-Verkauf) | |

**Bedeutung:** Classic ist ein reiner Zweitmarkt, dort ist FMV am staerksten gegen den
Floor. In-Season Limited besteht zu 4/5 aus Sorares Listenpreisen; unser FMV (und jeder
Verkaufsschnitt, auch Sorares Anzeige) schaetzt dort ueberwiegend Sorares Preis, nicht
den Zweitmarkt. Vermutlich der Grund, warum avg5 und FMV dort gleichauf liegen: gleicher
Topf, gleicher Fehler.

**Stand:** Updater holt und loggt `deal_type` (Commit 05.09.), Migration offen (Punkt F).
FMV-Formel UNVERAENDERT bis Entscheidung Jonas. RPC `accuracy_by_deal(p_days)` liefert
Median-Abweichung je Segment x Verkaufsart x Schaetzer.

**Haltung (Jonas, 05.09., bindend fuer Texte und Features):** Sorion ist **Verbuendeter
von Sorare, nicht Gegner**. Sorare verdient an Handel; Sorion soll Handel interessanter
und sicherer machen. Befunde wie dieser werden als Marktkunde formuliert ("zwei Maerkte,
so unterscheidet man sie"), nie als Vorwurf ("Sorare verschleiert"). Gilt fuer den
geplanten Knowledge-Artikel ebenso wie fuer Discord und Reddit.

## 🥊 Wettbewerb (06.09.2026) — siehe [WETTBEWERB.md](WETTBEWERB.md)

Zwei sichtbare Konkurrenten entdeckt: **sowizzy.com** (breiteres Toolset: Craft Assist,
Price Alerts, Lineups; Preismethode = gewichteter Verkaufsschnitt + Ratios fuer duenne
Scarcities) und **sorareterminal.com** (Market-Cap-Dashboard). **Beide verlangen einen
Login, bevor man etwas sieht** (sowizzy sogar Sorare-OAuth). Sorion braucht keinen.

Sowizzys Methode ist exakt unser `avg_sales`-Benchmark. Messung 06.09.: bei Limited
liegt der einfache Schnitt hauchduenn vorn, bei Rare sind wir 4-5 Punkte besser, bei
Super Rare Faktor 2,7 (22,1 % vs 60 %). Details und Konsequenzen in WETTBEWERB.md.

**Market Cap koennen wir NICHT sauber rechnen:** nur 16.624 von 126.360 card_prices-Zeilen
haben `available_supply` (13 %). Waere ein eigenes Vorhaben (Supply flaechendeckend erfassen).

## 🎯 LANGFRISTZIEL FMV (festgelegt von Jonas, 26.08.): den Last-5-Schnitt schlagen

Sorare Inside nutzt den ungewichteten Durchschnitt der letzten 5 Verkaeufe. Vergleich
auf 11.917 identischen Walk-Forward-Zielen (tools/2026-08-26_avg5-vergleich.mjs):
v3.3 ist praktisch gleichauf (gesamt ±23,9 vs ±23,7), gewinnt Rare (±24,1 vs ±25,1)
und Classic, verliert Limited/in_season knapp (±24,5 vs ±23,5). Unsere strukturellen
Vorteile: Bias ±2 % statt −4…−7 %, Ausreisser-Trimmen, Abdeckung ohne frische Sales.

**ZIEL: FMV schlaegt avg5 im Median in ALLEN Segmenten, bei |Bias| < 5 %.**
Messlatte: das Vergleichs-Skript nach jeder Formel-Aenderung neu laufen lassen
(gleiche Zielmenge = fairer Vergleich). Kandidat v3.4: bei sehr frischer, sehr
liquider Basis die juengsten Verkaeufe noch staerker gewichten (kuerzere effektive
Halbwertszeit bei hoher Sales-Dichte) — wie immer erst Backtest, dann Deploy.

## FMV-Faktoren-Analyse (25.08.) — ✅ v3.3 DEPLOYED (25.08. abends)

**Deploy-Status:** Von Jonas freigegeben ("hau in die tasten"). lib/fmv.mjs ersetzt
(Commit `eba6fc7`, Randfaelle vor dem Push per Smoke-Test verifiziert: liquide
ungedeckelt, duenn/alt weiter gedeckelt, ohne Sales null, Floor zieht nie hoch),
Railway deployt automatisch; change_guard-Migration von Jonas ausgefuehrt
(Cut 26.08. korrekt: erster v3.3-Snapshot entsteht 26.08. 05:30). market_move
liefert erwartungsgemaess leer bis ~02.09. (leer statt falsch). **Nachkontrolle
~01.09.:** `select * from accuracy_benchmark(7);` — faellt der Bias im Segment
<2 EUR Richtung null, hat v3.3 live gehalten, was der Backtest verspricht.

Auftrag aus `Sorion_FMV_Faktoren_Analyst.json` abgearbeitet (Checkpoints 1–3 mit Jonas durchlaufen). Vollbericht **[FMV_FAKTOREN_ANALYSE.md](FMV_FAKTOREN_ANALYSE.md)** (INTERN — Formel-Zutaten!). Kurzfassung:

- **Datenlauf:** 800 Karten geschichtet (limited+rare), 11.917 Walk-Forward-Ziele aus Sorare-Verkaufssequenzen + fmv_accuracy-v3.2-Ära (n=19.226). Skript `tools/2026-08-25_factor-data.mjs` (nur lesend, anonym — kein Key-Verbrauch). Rohdaten in `tools/analysis-out/` (~80 MB, NICHT committen).
- **Hauptbefund:** v3.2 unterschaetzt Karten mit FMV <2 € um +23…+75 % (live gemessen); Ursache ist der IMMER aktive SELL_CAP 1,50 — gedeckelte Vorhersagen Bias +35…+71 pp, ungedeckelte ~0. **Score-Niveau, Einsatzquote, Liga und Club erklaeren nach Preisniveau-Kontrolle NICHTS Robustes** (waren Preis-Proxys; Details und p-Werte im Bericht). J1-Ausreisser (+221 % am 24.08.) = Update-Latenz beim Saison-Regimesprung, kein Formelfehler — FMV holte binnen 24 h auf.
- **v3.3-Vorschlag (Backtest-belegt):** Cap nur noch bei duenner ODER alter Sales-Basis (<3 Sales im Fenster oder juengster Sale aelter als halfLife). Walk-Forward 11.917 Ziele: Median 33,3→23,9 %, Bias +22,2→+1,7 %, ±20 % 34→44 %; je Rarity limited i.s. 33,3→24,5, classic 31,4→22,4, rare 35,7→24,1. Duenne Schichten bleiben per Design EXAKT v3.2 (BUG-010-Schutz unangetastet). Reproduzierbar offline: `node tools/2026-08-25_fmv-v33-backtest.mjs`.
- **Deploy (falls Jonas freigibt, Reihenfolge!):** (1) `migrations/2026-08-25_fmv_v33_change_guard.sql` — Cut-Datum auf den echten Deploy-Tag setzen, „ohne RLS" ausfuehren (zwei Schnittkanten: 22.08. + v3.3-Tag). (2) `tools/2026-08-25_fmv-v33-proposal.mjs` nach `lib/fmv.mjs` kopieren, pushen. (3) Werte liquider Karten steigen einmalig — kein Marktereignis; 7d-Chip ~7 Tage leer. (4) CUT in `tools/accuracy-briefing.mjs` nachziehen, nach ~1 Woche `accuracy_benchmark(7)` pruefen (Bias <2 € muss Richtung 0, avg10-Vergleich).
- **Bewusst NICHT umgesetzt:** Liga-/Club-/Score-Terme (kein Effekt), Momentum-Term (auf J1-Einzelereignis gefittet), Form-Faktor L5/L40 (p=0,059, nur ein Datensatz — bei mehr Live-Aera-Daten erneut pruefbar).

## ⚠️ Offene Aktionen für Jonas

H. ✅ **Berlin-Zeit-Migration ausgefuehrt (06.09. 14:19 Berlin, per CLI durch Claude).** Verifiziert:
   alle 6 Auswertungs-Funktionen mit TimeZone=Europe/Berlin, Spalten-Default auf Berlin, 85 von
   1.731 Ereignis-Zeilen auf den richtigen Tag verschoben, 0 verbleibend. stats.html einmal neu laden.


G. ✅ **edge_cache-Migration ausgefuehrt (05.09. ~13:05 UTC).** Verifiziert: 1. Aufruf miss 1,75 s,
   danach hit ~0,3 s; 5 parallele Aufrufe alle hit 0,24 bis 0,32 s. accuracy.html haengt damit nicht
   mehr an der Live-RPC. Eine RPC je 10 Minuten statt einer je Besucher.


F. 🔴 **NEU (05.09.) — Migration `2026-09-05_accuracy_deal_type.sql` ausfuehren** (Spalte
   `fmv_accuracy.deal_type` + RPC `accuracy_by_deal`). Der Updater ist bereits so deployed,
   dass er die Verkaufsart loggt, SOBALD die Spalte da ist (Probe, kein Crash ohne). Danach
   **Formel-Entscheidung** auf Basis von `select * from accuracy_by_deal(3)` nach 2 bis 3 Tagen
   Daten: FMV nur aus Manager-Verkaeufen (TokenOffer), gewichtet, oder zwei Werte anzeigen?
   Siehe Abschnitt "Verkaufsarten" unten und `docs/2026-09-05_VERKAUFSARTEN_MESSUNG.md`.


E. **Zweiter Sorare-API-Key: NICHT nutzen (Pruefung der Sorare-Bedingungen 05.09.).**
   `sync-portfolio` kann technisch ueber `SORARE_APIKEY_2` rotieren, aber:
   - API Terms 2.1: Lizenz ist "non-transferable"; 3.2: Keys "must not share or disclose
     them to third parties". Ein Key aus einem FREMDEN Konto ist damit fuer beide Seiten
     ein Verstoss.
   - T&C 5.1 "Multiple Accounts": "You may not create more than one User account and/or
     manage the User account(s) of other Users (even if given explicit permission)".
     Ein EIGENES Zweitkonto ist selbst schon der Verstoss; 5.2 verbietet zudem das
     Zusammenwirken mehrerer Konten, um die Terms zu umgehen (hier: das Rate Limit aus 3.1).
   - Sanktion (API 5.1): Sperre des API-Zugangs, also aller Updater. T&C-Verstoss kann das
     Hauptkonto treffen. Nutzen (60 statt 30 neue Portfolios/min) steht in keinem Verhaeltnis.
   Einzig denkbarer sauberer Weg: pruefen, ob das EIGENE Konto unter sorare.com/settings/developer
   mehrere Keys ausgeben darf (README: "generate an extra API Key") und ob das Limit je Key gilt.
   Selbst dann Grauzone zu 3.1. Empfehlung: bei einem Key bleiben; bei Bedarf Sorare um ein
   hoeheres Limit bitten. Der Code bleibt drin, ist ohne gesetzte Variable wirkungslos.
   Nebenbefund API 2.3: kommerzielle Nutzung erlaubt, Vorabgenehmigung nur fuer Sublizenz/
   Weiterverkauf/Verteilung der API-Inhalte. Sorion (kostenlos, Spenden) faellt nicht darunter.


D. ✅ **Discord-Service LIVE (04.09., Ko-fi-Test erfolgreich):** Railway-Service
   `sorion-discord`, Domain `sorion-discord-production.up.railway.app`, Webhook
   `/kofi` bei Ko-fi eingetragen, Token + Discord-Webhook ("Penny") als Railway-Variablen.
   Offen (optional): Migration `2026-09-04_kofi_events.sql` ausfuehren + `SUPABASE_URL`/
   `SUPABASE_SERVICE_KEY` am Service setzen → Dedup ueberlebt Neustarts, Footer-Zaehler moeglich.
   Empfohlen: Webhook-URL und Ko-fi-Token neu erzeugen (standen im Chat), dann Variablen aktualisieren.


0. **price_history-Lockdown — ✅ ERLEDIGT (27.07.):** Bulk-Abgriff geschlossen. Nötig war die Korrektur-Migration `migrations/2026-07-27_price_history_lockdown_FIX.sql` (`revoke select from anon,authenticated` + alle Policies droppen — `enable RLS` allein hatte nicht gereicht). Live verifiziert: direkter `price_history`-Zugriff per publishable Key → 401 „permission denied"; RPC `player_history` liefert weiter spielergenau; `card_prices` bleibt öffentlich lesbar. (Ökosystem-BUGS BUG-012.)
0c. ✅ **Deploy-Crash behoben (28.07.):** Index `card_prices(scarcity,updated_at)` eingespielt + verifiziert (limited-Query 3,28 s → <1 s), Code-Resilienz deployed. BUG-012 geschlossen.
0d. ✅ **ERLEDIGT (30.07.) — Avg-FMV-Bewegung auf echten Vollmarkt umgestellt.** Eingespielt: `2026-07-30_market_daily_v3.sql` + Nachtrag `..._v3b_basket.sql`. **Live verifiziert:** `market_move` antwortet in 0,29 s; Snapshot-Werte deckungsgleich mit der UI (4,5181 € ↔ angezeigt „€4.52", Rare 14,7322 € ↔ „€14.73"); n über die Tage stabil (6.217 heute vs. 5.532 vor 7 Tagen); Werte plausibel: In-Season Limited +53,0 %, Rare +56,1 %, Classic Limited −0,4 %, Classic Rare −6,5 %. **Einordnung:** Der große In-Season-Anstieg ist überwiegend ein **Zusammensetzungs-Effekt** des Saisonflips (billige Karten ohne Sales verlieren ihren FMV und fallen aus der Bewertung; Median Limited nur 1,51 € bei Avg 4,52 €) — Classic ist das nüchterne Marktsignal. Optionaler Folgeschritt: Chip auf Median umstellen (ab 31.07. für alle neuen Tage verfügbar, in der Auffüllung nicht rekonstruierbar). Ab jetzt schreibt der Harvester täglich 05:30 den Snapshot; `market_daily` ist zugleich die Basis für die geplante 30-Tage-Marktbewegung.
   <details><summary>Details der Umstellung (Historie unten)</summary>
   Migration v3/v3b: Vorgabe Jonas: „realer Wert, keine Stichproben, tagesaktuell". Umsetzung: neue Mini-Tabelle `market_daily` + `snapshot_market_daily()` schreibt 1×/Tag den **Vollmarkt**-Avg/Median aus `card_prices` (das ist die Basis der angezeigten Zahl); Harvester ruft sie täglich 05:30 auf (Code deployed); `market_move` liest nur noch Snapshots → Vollmarkt vs. Vollmarkt, instant, und verweigert die Aussage, wenn die Stichprobengrößen um >25 % abweichen. Die Migration füllt zusätzlich die letzten 14 Tage per Carry-Forward auf (letzter bekannter Preis je Spieler ≤ Tag), damit der Chip sofort statt erst in einer Woche funktioniert. Verifikation: `select day,scarcity,eligibility,avg_fmv,n from market_daily order by day desc limit 12;` + RPC-Test (in der Datei dokumentiert). Historie der Fehlversuche unten.
   **Warum es zwei Fehlversuche gab (Lektion):**

   (a) Ursprünglich zeigte der Chip den **Median der Einzelkarten-7d-Änderungen** → ein anderer Wert als der angezeigte Durchschnitt (Avg 2,22→4,52 €, Chip −1,4 %; Rare konstant 0,0 %) → wirkte „random" (Dev-Feedback 30.07.).
   (b) 1. Versuch `market_avg_history` (Tagesdurchschnitte aus `price_history`) war **ebenfalls falsch, mit Echtdaten belegt:** n schwankte 537…5102, „Tagesdurchschnitt" sprang 6,38…25,96 € — man vergleicht rotierende Stichproben. Dazu Timeout bei `limited`.
   (c) 2. Versuch `market_move` v2 (gepaarter Korb aus `price_history`) war rechnerisch valide, aber immer noch nicht der von Jonas gewünschte **reale Vollmarkt-Wert**.
   (d) **v3 (final):** Vollmarkt-Snapshot aus `card_prices` → `market_daily`.
   **Lektion:** `price_history` ist ein **Änderungs-Log** (Zeile nur bei Updater-Berührung), NICHT ein Tages-Snapshot des Markts. Für Marktaggregate immer `card_prices` als Vollbestand nehmen und Snapshots davon persistieren. Aggregat-Kennzahlen vor dem Ausliefern gegen Echtdaten plausibilisieren. Eine dritte Naht lag in der Auffüllung selbst (unterschiedliche Grundgesamtheit) → v3b vereinheitlicht den Korb.
   </details>

   Zusätzlich am 30.07. erledigt (live): Tabellen-Header 9→11 px + helleres Lila (Dev-Feedback „too small"), Footer der Marktseite englisch, 8 deutsche Portfolio-Sortier-Tooltips englisch. Die Avg-Boxen zeigen den Chip nur, wenn vergleichbare Snapshots existieren — sonst bewusst leer (leer statt falsch).
0e. ✅ **ERLEDIGT (30.–31.07.) — eigenes Analytics statt Plausible:** Plausible-Abo ist abgelaufen, Ersatz gebaut (Vorgabe Jonas: keine laufenden Kosten). Tabelle `analytics_events` + Auswertungs-RPCs mit **Admin-Gate** (`is_analytics_admin()`, prüft die JWT-E-Mail — Admin-Liste dort pflegen). Edge Function `track` ist deployed, Tracking-Snippet auf allen Sorion- UND CraftLog-Seiten live (vorher lief Plausible auf Sorion nur auf der Startseite!). **Bis die Migration läuft, werden keine Daten gespeichert** (Beacon antwortet 500). Verifiziert 30.07.: Beacon → 200 (Daten fließen), `analytics_events` per publishable Key → 401 permission denied, Müll-Event → 400, Bot → ignoriert. **Nachtrag `2026-07-30_analytics_hardening.sql` eingespielt + verifiziert:** Die RPCs waren ohne Login noch aufrufbar (leere Antwort, HTTP 200), weil Postgres EXECUTE automatisch an PUBLIC vergibt und `revoke from anon` das nicht entfernt (gleiche Fehlerklasse wie beim price_history-Lockdown; Daten waren durch den Admin-Gate nie exponiert). Danach live geprueft: alle 4 Auswertungs-RPCs ohne Login → **401**, Beacon weiterhin 200, `market_move` unbeeintraechtigt. **Admin-Mail korrigiert** auf `jonas.rehr@outlook.de` (Migration `2026-07-30_analytics_admin_email.sql`).
   - **Dashboard:** `UI/stats.html` — bewusst NICHT im öffentlichen Repo (steht in `sorion-ui/.gitignore`).
     **Aufruf seit 05.09.: https://sorion-discord-production.up.railway.app/stats** (Lesezeichen). Der
     Railway-Service `sorion-discord` liefert die Seite unter `/stats` aus (Route in
     `services/discord/server.mjs`, Kopie in `services/discord/static/`). Nach Aenderungen an
     `UI/stats.html`: `node tools/build-stats-page.mjs` und `railway up services/discord --service
     sorion-discord --path-as-root`. NICHT mehr als file:// oeffnen: Chrome speichert dort keine
     Passwoerter, der Sitzungsspeicher ist unzuverlaessig (Ursache des staendigen Neu-Anmeldens).
     Supabase-Functions scheiden aus: der Gateway erzwingt fuer HTML `text/plain` (gemessen 05.09.).
     Die Seite erneuert das Zugangstoken jetzt selbst (ensureSession) und ist ein echtes <form>,
     damit Passwort-Manager greifen. Login mit dem Supabase-Konto (jonas.rehr@outlook.de). Zeigt Besucher/Aufrufe, Verteilung über die Seiten, Feature-Nutzung, Herkunft/Land/Gerät.
   - **Events (Monetarisierungs-Nachweis):** manager_search, portfolio_view, card_detail, elig_toggle, scarcity_switch, signup_done, login_done. Neue Events müssen in die Whitelist in `supabase/functions/track/index.ts` — sonst 400.
   - **Datenschutz:** keine Cookies, kein Dritt-Dienst; IP + User-Agent gehen nur flüchtig in einen täglich rotierenden Hash ein und werden nicht gespeichert; Aufbewahrung 400 Tage (`analytics_prune()`). Die `legal.html` beider Produkte wurde umgeschrieben (Plausible-Abschnitt raus, eigene Reichweitenmessung beschrieben).
   - **Warum die Function bewusst ohne Auth-Gate läuft** (`verify_jwt = false`, dokumentiert in `supabase/config.toml`): ein Beacon muss von jedem Besucher absendbar sein — wie bei jedem Analytics-Anbieter. Schutz: strikte Whitelists für site/path/event, Längenlimits, CORS auf die eigenen Domains, Bot-Filter; die Function liest nichts und gibt nur `{ok}` zurück.
0f. ✅ **ERLEDIGT (31.07.) — Filter, Liga-Ranking, Kader-Abgleich.** Migrationen `2026-07-30_position_and_league.sql` eingespielt; Railway-Service „Club Rosters" von Jonas angelegt und erstmals manuell gelaufen. Alle UI-Aenderungen im lokalen Browser klick-getestet (siehe BUGS.md BUG-013 — drei Fehler, die nur im Browser sichtbar waren).
   **Wiedervorlage 01.09.:** `railway-rosters.toml` auf woechentlich (`0 4 * * 1`) umstellen, im Januar-Transferfenster zurueck auf taeglich.
0g. ✅ **ERLEDIGT (01.08.) — Gespeicherte Portfolios statt Live-Abruf.** Ziel: API-Verbrauch senken, BEVOR ein zweiter Sorare-Key beantragt wird. Heute kostet jeder Portfolio-Aufruf 6–16 Sorare-Anfragen; kuenftig kommt die Anzeige aus der DB (0 Anfragen), geholt wird nur beim Sync.
   **Fertig:** Migration `migrations/2026-08-01_manager_portfolios.sql` (Tabellen `manager_sync`/`manager_cards`/`manager_trades`, oeffentlich lesbar, Schreiben nur per Service-Key, plus `purge_manager_data()` fuer die Kontoloeschung) + Edge Function **`sync-portfolio`** deployed. Schutz verifiziert: ungueltiger Slug 400, `force` ohne Login 403.
   **Kernidee:** Die Sperre haengt am MANAGER-SLUG, nicht am Betrachter — ein Manager wird hoechstens 1x/24 h von Sorare geholt, egal wie viele (auch anonyme) Leute ihn ansehen. Eigener Sync-Knopf: `force` mit 10-Minuten-Cooldown, nur fuer den eingeloggten Besitzer des Slugs (Abgleich gegen `profiles.sorare_slug`).
   **Live verifiziert (01.08.):** Erster Sync jr3hr = 267 Karten + 9 Trades in 2,3 s; zweiter Aufruf sofort danach blockiert (`skipped: fresh`, next in 1440 min); Schreibversuch von aussen -> RLS 42501; Portfolio-Aufruf danach mit **0 Sorare-Requests** (Browser-Netzwerkpanel), Werte identisch zum frueheren Live-Abruf. Trade History liest ebenfalls aus `manager_trades`.
   **Bug beim Umbau gefunden und behoben:** `updateStats()` lief vor `loadFMV()` -> Wert und P&L blieben leer; jetzt wird nach dem FMV-Nachladen neu gerendert.
   **NOCH OFFEN:** (a) `delete-account` um `purge_manager_data` ergaenzen, DSGVO-Export um manager_cards/-trades erweitern, legal.html anpassen. (b) Eigenes Portfolio hinter Login legen (aktuell weiterhin per Slug offen). (c) Erst danach zweiten Sorare-Key beantragen — Frage an Sorare: gilt 200/min pro Key oder pro Account?
0h. ✅ **ERLEDIGT (01.08.) — Sorare-Verknuepfung, von Jonas getestet:** Der Sorare-Login ist bei Sorion KEIN eigener Kontotyp, sondern der Nachweis fuer den Manager-Slug. Grund (Entscheidung Jonas): ein spaeteres Premium-Modell braucht EINE Konto-Identitaet, sonst zahlt jemand auf Konto A und nutzt Konto B. Der CraftLog-Login legt dagegen Konten unter `<slug>@sorare.craftlog.pro` an — genau dieses Zweitkonto wollen wir bei Sorion nicht.
   Umgesetzt: Action `link_sorare` in `sorare-oauth` (prueft Sorion-JWT UND Sorare-Token, 409 wenn der Slug schon einem anderen Konto verifiziert gehoert), `sorion.pro/auth/callback` angelegt, profile.html zeigt den Verbindungsstatus und sperrt das Slug-Textfeld bei verifizierter Verbindung (vorher konnte jeder jeden fremden Slug eintragen). Schutz getestet: ohne access_token 400, ohne Sorion-Login 401.
   **Testergebnis Jonas (01.08.): Verknuepfung funktioniert.** Damit ist geklaert: `sorion.pro/auth/callback` ist in der BESTEHENDEN OAuth-App registriert (Client-ID `JwRtoOAB…`) — **eine neue App ist NICHT noetig**, die Antragsmail enthielt beide Callback-URLs. Ebenfalls bestaetigt: Sync-Knopf laeuft und die 10-Minuten-Pause greift.
   Merke fuer kuenftige Pruefungen: Ob eine redirect_uri registriert ist, laesst sich NICHT von aussen testen — Sorare liefert fuer jede Variante dieselbe SPA-Huelle (alle Testvarianten exakt 20.058 Byte). Nur der echte Klick zeigt es.
1. ✅ **ERLEDIGT (21.08.):** Impressum vollstaendig — Betreiberangaben + Hosting/Region auf BEIDEN Seiten eingetragen. **Der letzte Launch-Blocker ist damit gefallen.**
2. SQL-Bereinigung BUG-011 ausführen (Session 26.07.: konservierte Alt-FMVs nullen) — falls noch nicht geschehen
3. ✅ **ERLEDIGT (04.09.):** `SORARE_APIKEY` ist als Supabase-Secret gesetzt (aus Railway durchgereicht, Wert nie angezeigt). Genutzt von der neuen Edge Function **`so5-results`** (C:\craft-log\supabaseunctions\so5-results\, verify_jwt=false in config.toml, deployed): Proxy fuer die GW-Historie — Modus 1 ohne fixture = letzte 8 Fixtures (previous-Kette ab `so5Fixture(type:LIVE_OR_PAST)`; allSo5Fixtures kann kein 'letzte N vergangene'), Modus 2 mit manager+fixture = kompakte Ergebnisse (Lineups, Raenge, typisierte Rewards). Schutz: CORS (sorion.pro + localhost:8123 fuer Tests), Slug-Whitelist, Memory-Cache + Cache-Control. ACHTUNG: Functions-Ordner ist KEIN Git-Repo — Aenderungen nur auf Platte + deployed.
4. ~~Neue Sorare-OAuth-App beantragen~~ — **erledigt sich (01.08.):** Die bestehende App deckt beide Domains ab (live getestet). Offen bleibt nur die Secret-Hygiene, falls die App tatsaechlich auf einem fremden Account liegt.
6. 🔴 **NEU (25.08.) — zwei Migrationen ausfuehren** (SQL-Editor, „ohne RLS", in dieser Reihenfolge):
   (a) `2026-08-25_facets_club_league.sql` — BUG-025/Filter-Umbau: Club-Facetten bekommen ihre Liga, Club-Suche nach Liga-Wahl vorgefiltert.
   (b) `2026-08-25_player_nation.sql` — neue Spalte `player_nation` (Nationalitaet des Spielers, NICHT Liga-Land) + Facetten-Set 'nation' + Teilindex. Ersetzt die Facetten-MV erneut — (b) allein genuegt daher auch, falls (a) uebersprungen wurde. ✅ beide am 25.08. ausgefuehrt.
   (c) `2026-08-25_age_and_gameplay_tier.sql` — Spalten `player_age` (Sorare age-Feld, wird bei jeder Beruehrung frisch geschrieben — Jonas wollte bewusst KEIN Geburtsdatum) und `gameplay_tier` (GOAT/STAR/IMPACT/ROSTER/DNP — Sterne-Klassifizierung des SPIELERS; nicht verwechseln mit der tier-Spalte = CardQuality T0-T5 aus dem Seed, die kein Skript mehr schreibt). UI: FMV-Doppelslider (sofort aktiv, logarithmisch), Alters-Doppelslider und Tier-Dropdown erscheinen von selbst, sobald die Spalten befuellt sind.
   Danach einmal `select public.refresh_market_aggregates();`. Anschliessend beide Repos pushen (sorion-ui + sorion-updater; Updater-Fix + Nation-Befuellung werden erst mit dem Railway-Deploy wirksam!). Das neue UI faellt ohne Migration/Daten sauber zurueck (Club-Liste ungefiltert, Nation-Knopf verborgen bis ≥100 Karten Nation tragen); Nation fuellt sich In-Season in ~1 Tag, Classic in ~3-4 Tagen.
   **Nachtrag 25.08. abends:** Alle drei Migrationen ausgefuehrt, beide Repos deployt, Erstbefuellung per `tools/backfill-player-meta.mjs` erledigt (7.847 Spieler). Liga-Menue auf **Sorares Marktplatz-Struktur** umgebaut (Quelle `so5.so5Competitions`): benannte Top-Wettbewerbe, Gruppen Champion/Contender (Kopf-Klick filtert die ganze Gruppe per or-Baum aus (Name,Land)-Paaren), Under 23 (setzt den Alters-Slider), Rest of the World nach Kartenzahl absteigend. Liga-Klicks filtern alle NAMENS-ALIASSE zugleich (Sorare-Umbenennungen: MLS/Major League Soccer, HNL/SuperSport HNL ...) — Aliastabelle im UI-Code (SORARE_TOP/SORARE_GROUPS), bei kuenftigen Umbenennungen dort ergaenzen. Noch offen: einmal `refresh_market_aggregates` NACH dem Backfill, damit das Nation-Dropdown erscheint.
5. ✅ **Dubletten `limited/rare/sr/` entfernt (28.07.):** Per Railway Settings→Build bestätigt, dass alle 3 Services (Update Limited/Rare/SR) aus dem Repo-Root via `/railway-<s>.toml` bauen (keine Root Directory gesetzt). Ordner per `git rm -r` gelöscht → `update-scarcity.mjs`/`lib/fmv.mjs` existieren nur noch einmal (Root). Keine 4-fach-Sync mehr.

## 📣 Soft Launch r/Sorare, Sa 05.09.2026 (Fazit, 22:11 UTC)

**Post:** "I built a free Sorare value tracker", 11:20 lokal (Entscheidung Jonas: Samstag statt
Montag, F2P-Set am Di). Nach 12 h: 796 Ansichten, 9 Upvotes (100 %), 1 Kommentar, Platz 2 des
Tages (zeitweise Platz 1). **0 fremde Kommentare** (der eine gezaehlte war Jonas' eigener CTA).
Laender: GB 19 %, DE 11 %, US 9 %.

**Unsere Seite (Tag):** 111 Pageviews, **47 Besucher** (Vortag 35 / 6, also 8x Besucher).
Klickrate Reddit -> Seite ~6 % (Werbeblocker untertreiben). Seiten: Start 53, **Profil 26**,
Accuracy 16, Portfolio 16. Ereignisse: 19 Manager-Suchen, 15 Portfolio-Ansichten, 12 Karten-
Details, **7x Top Earners**, 3 Support-Klicks, 2 Discord-Beitritte. **2 neue Konten**, beide mit
Sorare-Portfolio verknuepft (cmarshhh, tomneg). Ko-fi: 0 Spenden.

**Technik:** null Fehler, null Sync-Fehler, alle Antworten unter 1 s, Sorare-Kontingent nie
kritisch. Zwei Fixes am Tag selbst: morgens BUG-036 (Erstaufruf-Wettlauf/429, vor dem Post),
mittags Cache-Function fuer accuracy_benchmark (edge_cache), nachdem die RPC zweimal ins
3-s-Limit lief. Monitor: tools/monitor-launch.mjs.

**Bewertung:** Soft Launch hat genau das geliefert, was er sollte: echter Traffic ohne Flut,
Technik bewiesen, erste Konversionen. Schwach: Engagement (0 fremde Kommentare in 12 h, auch
nicht auf den CTA; eine Tool-Vorstellung wird zur Kenntnis genommen, nicht diskutiert. Kommentare
entstehen bei Meinung, Geschichte, Fehlern -> Lern-Artikel ist die Korrektur, nicht nur die naechste Idee) und Reichweite (Samstag = Spieltage;
~800 Ansichten fuer Platz 1-2 zeigen die Wochenendgroesse des Subreddits).

**Konsequenzen:** Kein Montag-Post (waere Repost). Naechster Post = Fehler-/Lern-Artikel in
1-2 Wochen, werktags 15:00, mit offener Frage am Ende. Discord-Communities mit Tools-Kanal
als kostenlose Reichweite. Beobachten: kommen die 2 neuen Konten wieder (analytics_retention)?
Tracking-Bug BUG-037 (Ereignisse bei Klick statt Erfolg) behoben; Zahlen bis 05.09. leicht zu hoch.

## Roadmap → Launch (Plan: Saisonstart + ~3 Tage stabil)

**Lasttest 05.09. (vor Reddit):** Erstaufruf-Wettlauf und fehlende 429-Behandlung in
`sync-portfolio` gefunden und behoben (BUG-036). Danach: 6 parallele Erstaufrufe ->
1 Sorare-Fetch, 0 Fehler; Cache-Pfad ~170 ms. Statische Seite kann nicht crashen; der
einzige Engpass ist das Sorare-Kontingent bei vielen NEUEN Slugs pro Minute (~30/min
mit einem Key, ~60/min mit zwei). Reddit-Post geplant Mo 08.09. 15:00 (docs/reddit/).


| Schritt | Status |
|---|---|
| Betrieb stabilisieren, Accuracy-Zahlen täglich prüfen (Bias!) | 🔄 läuft |
| Season-Flip überstehen (ligaweise im Gang) | 🔄 läuft |
| Impressum | ✅ 21.08. |
| Watchlist mit Zielpreisen (Stufe 2; Tabelle existiert) | ⬜ nächstes Feature |
| OG-Tags/Favicon + Erstbesucher-Erklärtext | ⬜ Launch-Kosmetik |
| Mobile-Durchgang | ⬜ vor Launch |
| Notifications (Stufe 3, braucht OAuth-App) | ⬜ nach Launch |
| 30d-Marktbewegung ergänzen | ⬜ ab ~20.08. (History reicht dann) |

## 💎 PRO-FEATURES — vollstaendige Referenz (Stand 06.09.2026)

Alles zu Bezahl-Features an EINER Stelle. Wer ein neues Bezahl-Feature baut, liest zuerst
die Checkliste weiter unten.

### Was ist heute kostenpflichtig?

| Feature-Key | Was | Stufe | Wo |
|---|---|---|---|
| `leaderboard_cash` | Cash-Punkteschwelle UND Cash-Team-Kosten auf der Leaderboards-Seite | `pro` | rewards.html |

Frei bleiben dort bewusst: Essence-Schwelle, Essence-Team-Kosten, bezahlte Raenge, Top-Score,
Lineup-Zahlen. Grund: Die Seite muss ohne Konto genug zeigen, um ueberhaupt zu ueberzeugen.

### Grundregel (gilt fuer JEDES Bezahl-Feature)

**Ein CSS-Blur ist KEIN Schutz.** Wer F12 drueckt oder die REST-Adresse aufruft, liest den
Wert im Klartext. Der Wert darf den Server gar nicht erst verlassen. Deshalb immer:
Tabelle sperren -> gefilterte RPC ausliefern -> im Frontend nur einen Platzhalter blurren.
Verifiziert am 06.09.: `cash_score` steht bei Nicht-Pro auch im Browser-Speicher auf `null`.

### Datenmodell (Migration `2026-09-06_user_tiers_feature_access.sql`)

- **`user_tiers`** — je Konto ein Schalter pro Ko-fi-Stufe: `supporter` (0,50 EUR),
  `pro` (5,00), `vip` (25,00). Dazu:
  - `valid_until` (NULL = unbefristet) — verhindert, dass ein gekuendigtes Abo ewig gilt.
  - `creator` — Betreiber-Zugang (siehe unten), unabhaengig von den Stufen.
  - `source` — manual | kofi | key | code.
  RLS: jeder liest NUR die eigene Zeile; es gibt KEINE Schreib-Policy (nur Service-Key
  und die Funktionen unten schreiben).
- **`feature_access`** — Feature -> Mindeststufe (`free | supporter | pro | vip`).
  Oeffentlich lesbar, damit die UI weiss, was gefordert ist. **Ein Feature laesst sich hier
  OHNE Deploy verschieben oder freigeben:**
  `update feature_access set min_tier = 'free' where feature_key = 'leaderboard_cash';`
- **`app_secrets`** — nur der SHA-256-Hash des Creator-Schluessels. Kein anon/authenticated.
- **`redeem_codes`** — Gutschein-Codes: nur Hash, Stufe, Laufzeit (Monate), `max_uses`,
  `used_count`, Ablaufdatum. Kein anon/authenticated.

### Die einzige Entscheidungsstelle

`has_feature(key)` beantwortet "darf dieser Nutzer das?" — Rangfolge vip > pro > supporter
ueber `tier_rank()` / `my_tier_rank()`, abgelaufene Stufen zaehlen nicht, unbekanntes Feature
= gesperrt (**fail closed**, ein Tippfehler verschenkt kein Bezahl-Feature).
**Diese Logik NIE an anderer Stelle nachbauen** — Lehre aus BUG-022/023/024, wo dieselbe
Regel dreifach implementiert war und dreimal unterschiedlich falsch.

### Stufe vergeben — drei Wege

1. **SQL-Editor (manuell, heute der Normalfall):**
   `select set_user_tier('mail@example.com', 'pro');`
   aus: `select set_user_tier('mail@example.com', 'pro', false);`
   befristet: `select set_user_tier('mail@example.com', 'pro', true, '2026-12-31');`
2. **Gutschein-Code (fuer Unterstuetzer, schliesst die Ko-fi-Luecke):**
   Anlegen nur im SQL-Editor:
   `select create_redeem_code('SORION-PRO-7QX2', 'pro', 1);`
   mehrfach nutzbar: `select create_redeem_code('LAUNCH25', 'supporter', 3, 25);`
   Eingeloest wird im **Profil unter "Gutschein einloesen"**. Eine laufende Stufe wird
   VERLAENGERT, nicht ueberschrieben. **Damit braucht die Ko-fi-Anbindung keine E-Mail** —
   der Code kommt in die Ko-fi-Dankesnachricht; die Datenschutz-Entscheidung vom 04.09.
   (keine Mail speichern) bleibt unangetastet.
3. **Creator-Schalter im Profil** (nur Betreiber, zum Testen) — siehe naechster Punkt.

### Creator-Zugang (Testen der Bezahl-Features)

Zweck: Jonas schaltet die Stufen an seinem eigenen Konto an und aus, um beide Ansichten zu
pruefen. Zwei Wege hinein, beide serverseitig geprueft:
- fest hinterlegter Betreiber: `is_analytics_admin()` (E-Mail `jonas.rehr@outlook.de`), oder
- **Schluessel** aus dem Passwort-Manager, eingegeben im **Gutschein-Feld** (bewusst getarnt,
  Wunsch Jonas). `redeem_code()` prueft zuerst den Creator-Schluessel, dann die Gutscheine.
  Einmalig setzen (nur SQL-Editor): `select set_creator_key('...mind. 24 Zeichen...');`
  In der DB liegt nur der Hash. Gesetzt am 06.09. (57 Zeichen).
  `lock_creator()` gibt den Zugang samt Stufen wieder ab.
- Danach erscheint im Profil die Karte **"Creator-Schalter"** mit je einem Knopf pro Stufe.
  `set_my_tier(tier, on)` ist doppelt gesperrt: Betreiber ODER Creator-Konto, UND es fasst
  ausschliesslich die eigene Zeile an.

### Checkliste fuer ein NEUES Bezahl-Feature

1. Feature-Key in `feature_access` eintragen (`insert ... on conflict do nothing`).
2. Datenquelle sperren: `revoke select` auf der Tabelle, RLS-Policy entfernen.
3. RPC bauen, die den Wert nur bei `has_feature('<key>')` liefert, sonst `null`, plus ein
   `*_locked`-Flag fuer die Anzeige. **`round(x::numeric, 2)`** nicht vergessen.
4. Aendert sich der Rueckgabetyp einer bestehenden RPC: `drop function` davor (42P13).
5. Frontend: Platzhalter blurren (NIE den echten Wert), Upsell-Hinweis, Ko-fi-Link.
6. **Alle Client-Pfade auf 401 pruefen** (siehe Falle unten).
7. Von aussen verifizieren (Rezept unten), erst dann als fertig melden.

### Fallen (alle real aufgetreten)

- **Abgelaufene Sitzung leerte die Seite (06.09., gefixt):** Nach dem Sperren der Tabelle
  schickte die Leaderboards-Seite ein abgelaufenes Token, bekam 401, und der Rueckfall griff
  auf die inzwischen gesperrte Tabelle zu -> gar keine Anzeige. Anonyme Besucher waren nie
  betroffen, deshalb fiel es beim Test nicht auf. **Regel: Wer eine Tabelle sperrt, muss
  JEDEN Client-Pfad durchgehen, der sie bisher las** — und eingeloggte Nutzer separat testen.
  Loesung: Token erneuern (`grant_type=refresh_token`), sonst anonym erneut anfragen und die
  freie Ansicht zeigen statt nichts.
- **42P13** bei geaendertem RPC-Rueckgabetyp -> `drop function` davor.
- **42883** `round(double precision, int)` gibt es nicht -> `round(x::numeric, 2)`.

### Verifikation (von aussen, ohne Login)

```
curl ".../rest/v1/reward_thresholds?select=cash_score&limit=1" -H "apikey: <anon>"   # 401
curl -X POST ".../rest/v1/rpc/leaderboard_thresholds" -H "apikey: <anon>" -d '{}'    # cash_score null, cash_locked true
curl -X POST ".../rest/v1/rpc/has_feature" -d '{"p_feature":"leaderboard_cash"}'     # false
curl -X POST ".../rest/v1/rpc/set_user_tier" ... / set_my_tier / redeem_code / set_creator_key   # alle 401
```
Stand 06.09.: alle Punkte geprueft und bestanden.

### Offen

- **Ko-fi-Automatik:** Der Weg steht (Codes), es fehlt nur die Ausgabe in der
  Ko-fi-Dankesnachricht bzw. ein Vorrat generierter Codes.
- **Produktfrage:** einen Wettbewerb als Kostprobe offen lassen, damit die Seite auf Reddit
  ihren Aufhaenger behaelt?
- **Preisfrage:** Partner-API-Staffel (29-49 EUR/Monat) ist nur Konzept, siehe
  MONETARISIERUNG.md.

## Reward-Schwellen (06.09.) — rewards.html, Tabelle reward_thresholds

Wunsch Jonas: "Wie viele Punkte braucht man im Schnitt fuer Geld/Essence je Leaderboard?" —
nur Saison 26/27 (GW1 ab 31.07.), als Tabelle je Wettbewerb, auf der Seite visualisiert.
- **Quelle:** je abgeschlossenem Spieltag (`so5Fixtures`, aasmState=closed) die Wettbewerbs-
  Leaderboards; je Leaderboard `rewardsConfig.ranking[]` mit `toSo5Ranking.score` = Score des
  letzten belohnten Rangs je Stufe. Anonym Komplexitaet 500 -> 1 Call je Leaderboard (~40/Spieltag).
  Arena/PvP/Cap/Beginner-Raeume werden ausgeschlossen (Slug-Regex in `isCompetition`).
- **Pipeline:** `tools/sync-reward-thresholds.mjs` (idempotent, ueberspringt fertige Spieltage) ->
  Tabelle `reward_thresholds` (Migration `2026-09-04_reward_thresholds.sql`, oeffentlich lesbar) ->
  `UI/rewards.html` aggregiert clientseitig je Wettbewerb x Rarity (Ø Cash-/Essence-Schwelle mit
  Min-Max, bezahlte Raenge, Top-Score, Lineups, Cash-Trend-Sparkline). Footer-Link auf der Marktseite.
- **Cron:** `railway-rewards.toml` (taeglich 09:00 UTC) — Railway-Service muss Jonas wie
  Club_Rosters anlegen. Bis dahin manuell: `railway run node tools/sync-reward-thresholds.mjs`.
- **Status 06.09.:** Code/Seite deployt, Migration + Erst-Sync (~400 Calls, ~8 min) OFFEN.
- Hinweis: Laenderspielpausen (z. B. GW10, 1.-4.9.) haben nur ~13 Leaderboards — deshalb
  ungleiche GW-Zahlen je Zeile; ist erklaert auf der Seite.

## Live-Daten im Spieler-Modal (04.09.) — Edge Function `player-live`

`C:\craft-log\supabase\functions\player-live\index.ts` (deployed, `verify_jwt=false`, CORS
auf eigene Domains, 10-min-Cache je Slug): liefert je Spieler Startelf-Quote, Punkte-Prognose
A–F (+ projizierte Punkte), Spielstatus und SO5-Form (L5/L10/L40 + Einsaetze). Quelle der
Quote/Prognose ist **Sorare Inside via Sorare** (`providerRedirectUrl`) — im Modal
attributiert. Bewusst LIVE statt in card_prices: die Werte erscheinen wenige Tage vor dem
Spieltag und verschwinden danach (Jonas 04.09.). Felder: `nextClassicFixturePlayingStatusOdds`
(nur auf Typ `Player`, Fragment noetig!), `nextClassicFixtureProjectedGrade` — "Classic"
heisst hier WOCHEN-Spieltag, nicht unsere Classic-Eligibility. Ein Sorare-Call je
Modal-Oeffnung (Cache daempft); SORARE_APIKEY als Supabase-Secret wuerde das Limit heben.

## 📣 Patch Notes -> Discord (eingerichtet 01.09., Regeln BINDEND)

Der Sorion-Discord bekommt oeffentliche Patch Notes ueber einen Webhook.
**Regeln von Jonas: (1) KEINE Interna** — keine Formel-Zutaten, keine
Sicherheits-/Incident-Details, keine Infrastruktur/Kosten/Dateipfade; nur was
Nutzer sehen. **(2) Jonas muss VOR jeder Veroeffentlichung den Entwurf
freigeben** — erst nach dem "Go" posten, nie automatisch.

Ablauf: Entwurf nach docs/patchnotes/JJJJ-MM-TT_<titel>.md (erste Zeile =
# Titel), Jonas im Chat zeigen, nach Freigabe:
`node --env-file=.env tools/publish-patchnotes.mjs docs/patchnotes/<datei>.md`
Webhook-URL liegt NUR in der lokalen .env (git-ignoriert), niemals ins Repo.

## 💰 Discord-Service: Ko-fi → „💰・donations" (LIVE seit 04.09.2026)

**Was:** `services/discord/` nimmt Ko-fi-Webhooks an und postet Spenden und
Mitgliedschaften als gestaltete Nachricht (Sorion-Lila, VIP Magenta, keine
„Donation-Bot"-Optik). Eigener Railway-Web-Service via `railway-discord.toml`
(`node services/discord/server.mjs`, Healthcheck `/health`). Keine
Sorare-API-Aufrufe. Details, Einrichtung, Erweiterungspfade: `services/discord/README.md`.

**Tiers (Ko-fi, Stand 04.09.):** Supporter 0,50 €/M · Pro-Supporter 5 €/M · Sorion VIP 25 €/M
(`lib/tiers.mjs`; Erkennung ueber Name, Rueckfall Betrag).

**Aufbau (erweiterbar):** Pipeline `dedup → enrich → announce → persist` in
`handlers/kofi.mjs`. Neue Faehigkeit = neue Funktion in `STEPS`. Optionales
Event-Log `kofi_events` + RPC `kofi_stats()` (Migration
`2026-09-04_kofi_events.sql`) ist die Basis fuer Milestones/Statistik/Footer-Zaehler.

**Getestet (04.09., Mock-Discord, ohne Aussenwirkung):** 5 Ereignisarten,
falsches Token abgelehnt, Duplikat uebersprungen, E-Mail nie im Ausgang.
Zwei Fehler dabei gefunden und behoben: Tier-Teilstring („Pro-Supporter" →
„Supporter") und Dedup-Zeitpunkt (ID erst am Pipeline-Ende gemerkt).

**Rollenvergabe: macht Ko-fis eigener Discord-Bot** (Jonas 04.09.) — nicht
unser Service. Ko-fi liefert `discord_userid` mit; wir verlinken den Spender
damit in der Nachricht (ohne Ping) und speichern sie fuer Statistik.
**Noch nicht gebaut (bewusst, Community klein):** Milestones, Monatsrueckblick.

**Deploy-Weg (04.09.):** per CLI, Service `sorion-discord`, Domain
`sorion-discord-production.up.railway.app`. Aus `services/discord/`:
`railway up . --service sorion-discord --path-as-root`. OHNE `--path-as-root`
laedt die CLI das ganze Repo hoch → startet `update.mjs` → Crash (so passiert
beim ersten Versuch). Ko-fi-Webhook-URL: `https://sorion-discord-production.up.railway.app/kofi`.

**Manuelle Schritte Jonas:** siehe „Offene Aktionen" Punkt D.

## SO5-Historie: eigener Speicher (04.09.2026)

`so5_lineups` + `so5_card_earnings` (Migration 2026-09-04_so5_history_store.sql)
halten die Gameweek-Historie dauerhaft. Die Edge Function `so5-results` liest
zuerst dort und holt nur FEHLENDE Gameweeks bei Sorare — jede (Manager, GW)
kostet damit weltweit genau EINEN API-Aufruf, danach kommt alles aus der DB
(gemessen: 1307 ms -> 235 ms). Gespeichert werden nur `aasmState = closed`
Gameweeks; sie sind unveraenderlich, also wird jede Zeile genau einmal
geschrieben (keine periodische Last, kein Cron — Lehre INC-005/006).

**Schluessel ist Sorares `lineup_id`**, nicht (Manager, GW, Wettbewerb): Ein
Manager kann im selben Wettbewerb MEHRERE Aufstellungen haben (jr3hr hatte in
GW9 zwei "Bundesliga – Limited"); der erste Entwurf scheiterte daran still.

Stand 04.09.: jr3hr komplett erfasst — 84 Gameweeks, 257 Karten, zurueck bis
zum Sorare-Start im September 2025. Ausbaustufe (Ziel Jonas): ueber
`so5_card_earnings` laesst sich kuenftig fuer JEDE Karte ausweisen, was sie
JEMALS erspielt hat — ueber alle Manager hinweg.

## 🛠 Migrationen laufen jetzt per CLI (seit 06.09.2026)

Kein Kopieren in den SQL-Editor mehr. Aus `C:\craft-log\supabase` (dort ist die CLI eingeloggt
und das Projekt verknuepft):

    npx supabase db query --linked --file "C:/craft-log/Sorion_pro/migrations/<datei>.sql"
    npx supabase db query --linked --output-format json "select ..."      # Verifikation

Laeuft ueber die Management-API mit dem CLI-Login, kein DB-Passwort noetig. Regeln dazu:
Vor dem Ausfuehren sagen, was die Migration tut (Lehre INC-005/006: die DB ist gedrosselt,
schwere Migrationen koennen sie lahmlegen); danach immer mit einer Abfrage verifizieren;
Admin-gegatete RPCs (is_analytics_admin) liefern ueber die API leere Ergebnisse, das ist
kein Fehler. `analytics_prune` bleibt ohne Zeitzonen-Setting (400-Tage-Loeschung, 2 h egal).

## Werkzeuge in tools/ (bei Bedarf per `railway run node tools/<name>.mjs` ausloesen)

- `backfill-player-meta.mjs` — laedt gameplay_tier/player_age/player_nation fuer ALLE Spieler ueber die ~250 Club-Kader (Einmal-Befuellung; die laufende Pflege machen Updater + Kader-Abgleich von selbst — In-Season taeglich, Classic alle 3-4 Tage)
- `repair-club-leagues.mjs` — vereinheitlicht Liga-Felder von Clubs mit gemischten Zeilen auf den Sorare-Live-Stand (BUG-025-Aufraeumer; nach dem Updater-Fix normalerweise unnoetig)
- `fmv-backtest.mjs` — Walk-Forward-Backtest der FMV-Formel (Grundlage von v3.2)
- `2026-08-25_factor-data.mjs` — Datenlauf der FMV-Faktoren-Analyse (nur lesend, anonym; Ausgaben in `tools/analysis-out/`, ~80 MB, nicht committen)
- `2026-08-25_fmv-v33-backtest.mjs` — offline-Reproduktion des v3.3-Backtests (liest analysis-out, kein API/DB-Zugriff)
- `2026-08-25_fmv-v33-proposal.mjs` — VORSCHLAG: kompletter Ersatz fuer `lib/fmv.mjs` (bedingter SELL_CAP); Einbau nur durch Jonas nach Checkliste oben
- KEINEN Cron fuer diese Werkzeuge anlegen — Einmal-/Diagnose-Skripte, Dauerlast war die INC-005/006-Falle
- Vorbereiteter Analyse-Auftrag: `C:\Users\Jonas\Documents\Bot2B\07_Wissen\Prompts_Bibliothek\Sorion_FMV_Faktoren_Analyst.json` — prueft, ob Score/Einsatzquote/Liga/Club die VERKAUFSPREISE ueber v3.2 hinaus erklaeren (Ziel: Formel-Faktoren fuer v3.3+). Kernregeln stehen im Prompt: gegen Verkaeufe messen (nie gegen den eigenen FMV — zirkulaer), Walk-Forward-Backtest vor jedem Formel-Vorschlag, DB nur lesend.

## Team-Kosten je Wettbewerb (06.09.) — was kostet eine Gewinner-Aufstellung?

Wunsch Jonas: "Was kostet ein Team im Durchschnitt je Wettbewerb, um Cash bzw. Essence
abzuraeumen?" **Nur diese Saison** (ab GW1, 31.07.2026) — Spielmechaniken aendern sich je
Saison, aeltere Aufstellungen taugen nicht als Massstab.

- **Quelle:** `so5Leaderboard.so5RankingsPaginated(page, pageSize)` -> `so5Lineup.so5Appearances`
  mit `anyPlayer{slug}` + `anyCard{rarityTyped inSeasonEligible}`. **Braucht den APIKEY**
  (Tiefe 8 noetig, anonym nur 7). Gemessen: 50 Aufstellungen je Aufruf in ~0,4 s.
- **Stichprobe statt Vollerhebung:** je Leaderboard drei gezielte SEITEN — Spitzenfeld, die
  Seite an der Cash-Grenze, die Seite an der Essence-Grenze. Seitennummern erlauben den
  direkten Sprung, kein Durchblaettern. Ergebnis GW10: 1.160 Aufstellungen aus 26 Aufrufen.
- **Bepreisung:** Karten gegen `card_prices` (player_slug + scarcity + eligibility) zu
  HEUTIGEN FMV. Bewusst nicht historisch: die nuetzliche Frage ist "was kostet so ein Team
  jetzt". Nur vollstaendig bepreiste Aufstellungen zaehlen (5/5), sonst waere die Summe
  systematisch zu niedrig; Abdeckung GW10 lag bei 76 %.
- **Kennzahl:** Median, nicht Mittelwert — einzelne Sammler-Aufstellungen mit teuren Karten
  wuerden den Schnitt verzerren. GW10 gesamt: Cash-Team 184,60 EUR, Essence-Team 76,08 EUR
  (ueber alle Rarities gemischt; die Seite zeigt es je Wettbewerb und Rarity).
- **Bausteine:** Migration `2026-09-06_lineup_costs.sql` (Tabelle `lineup_costs`, nicht
  oeffentlich lesbar; `leaderboard_thresholds()` um `cash_cost`/`essence_cost` erweitert),
  Sync `tools/sync-lineup-costs.mjs` (idempotent, ueberspringt fertige Leaderboards).
  Aufruf: `railway run -s "Updater Limited" node tools/sync-lineup-costs.mjs`.
- **Gate (Details: Abschnitt PRO-FEATURES):** Die CASH-Kosten haengen am selben Schalter wie die Cash-Schwelle
  (`leaderboard_cash`), Essence-Kosten sind frei. Umstellen ohne Deploy ueber
  `feature_access`.
- **Kadergroesse:** All Star und Under 23 spielen mit SIEBEN Karten, alle anderen mit fuenf.
  Team-Kosten sind daher nur INNERHALB eines Wettbewerbs vergleichbar (Hinweis steht auf der Seite).
- **Falle (06.09., gefixt):** `so5RankingsPaginated` zaehlt Seiten **ab 0**. Mit 1-basierter
  Rechnung fehlten in allen 178 Leaderboards die Raenge 1-50 — also genau die Cash-Gewinner,
  weshalb die Cash-Team-Spalte leer blieb. Nach dem Fix: niedrigster Rang 1, 95 von 190
  Leaderboard-Wochen mit Cash-Kosten, 3.826 Cash-Aufstellungen (vorher 1.662).
- **Offen:** Cron einrichten (analog `railway-rewards.toml`), damit neue Spieltage
  automatisch nachlaufen.

## Sorare-API: Merkzettel (ZUERST hier nachsehen, nicht im Schema stochern)

Diese Liste existiert, weil ich am 06.09. zweimal in Limits gelaufen bin, die bereits
dokumentiert waren. **Regel: Bei jeder Sorare-API-Frage erst diesen Abschnitt und die
Suche in HANDOFF.md, dann das Schema.**

- **Limits:** anonym Tiefe 7 / Komplexitaet 500. Mit `APIKEY`-Header Tiefe 13 /
  Komplexitaet 30.000, 200 Anfragen/Minute (geteilt von ALLEN Diensten!).
- **Wo liegt der Schluessel:** nur auf den Railway-Diensten `Updater Limited`,
  `Update Rare`, `Updater SR` — NICHT auf `Club_Rosters`/`sorion-updater`. Zusaetzlich als
  Supabase-Secret (seit 04.09.). Lokal ausfuehren:
  `railway run -s "Updater Limited" node tools/<skript>.mjs`
- **Schema herunterladen:** `curl -sL https://api.sorare.com/graphql/schema -o schema.graphql`.
  Introspektion (`__type`) ist ABGESCHALTET, die Schema-Datei ist der einzige Weg.
- **Feldnamen-Fallen (live gelernt):**
  - Karte: `rarityTyped`, NICHT `rarity`. Dazu `seasonYear`, `inSeasonEligible`.
  - Spieler: `age`, `country{code}`, `gameplayTier` (GOAT/STAR/IMPACT/ROSTER/DNP),
    `eligibleSo5Competitions`, `averageScore(type: LAST_FIVE_SO5_AVERAGE_SCORE)`,
    `lastFiveSo5Appearances`. `birthDate` gibt es nicht (`birthDay`).
  - Startelf-Quote/Prognose haengen am Typ `Player`, nicht am Interface: Fragment
    `... on Player { playingStatus nextClassicFixturePlayingStatusOdds { ... } }` noetig.
    "Classic" heisst dort WOCHEN-Spieltag, nicht unsere Classic-Eligibility.
  - `so5Fixtures(...)` kennt KEIN Argument `type` (nur `so5Fixture` hat es). Zustand ueber
    `aasmState` (opened|started|closed) filtern; die Liste kommt NEUESTE zuerst.
  - `So5Leaderboard` hat KEIN Feld `so5Competition` — Zuordnung ueber den Slug
    (`...-seasonal-<key>-(in_season|all_seasons)_<key>_<rarity>`).
  - "Champion" existiert nur als `all_seasons`-Wettbewerb, es gibt kein In-Season-Pendant.
- **Postgres-Falle bei RPC-Aenderungen:** Kommt eine SPALTE zur Rueckgabe dazu, scheitert
  `create or replace function` mit 42P13 ("cannot change return type"). Immer
  `drop function if exists <name>(<argtypen>);` davor. Der SQL-Editor laeuft als EINE
  Transaktion, die Funktion ist also nie wirklich weg. Aufgetreten bei market_facets (25.08.)
  und leaderboard_thresholds (06.09.) — beide Male dieselbe Ursache.
- **`round(x, 2)` gibt es nur fuer `numeric`:** `percentile_cont` liefert
  `double precision` -> 42883. Immer `round(wert::numeric, 2)` schreiben.
- **Ranglisten und Aufstellungen (06.09. verifiziert, braucht APIKEY):**
  `so5.so5Leaderboard(slug).so5Rankings(first, after)` — Cursor-Pagination, ODER
  `so5RankingsPaginated(page, pageSize)` — mit SEITENNUMMER, damit sind gezielte Raenge
  ohne Durchblaettern erreichbar. Je Ranking: `ranking`, `score`, `so5Lineup.so5Appearances`
  mit `anyPlayer{slug}`, `anyCard{rarityTyped inSeasonEligible}`, `captain`. Das mappt
  direkt auf `card_prices(player_slug, scarcity, eligibility)`.
- **Preisgelder und Punkte-Schwellen:** `so5Leaderboard.rewardsConfig.ranking[]` mit
  `fromRank/toRank`, `usdAmount`, `cardShardRewardConfigs` (Essence), `cards`, und
  `fromSo5Ranking/toSo5Ranking { ranking score }` = die SCORE-Grenzen der Preisstufen.
  `totalRewards { prizePool prizePoolCurrency }` = Preisgeld je Leaderboard.

## Architektur-Wissen (Kern)

**Manager-Identitaet (seit 06.09., BUG-039):** Sorare-Slugs sind aenderbar. Bei Umbenennung
bleibt der alte Slug als Alias, der neue bekommt eine UUID angehaengt. Bei uns hat jeder
Manager EINEN Primaer-Slug (`manager_sync.sorare_slug`, unter dem alle Daten liegen) plus
`sorare_user_id` (Sorare "User:<uuid>", stabil), `current_slug`, `nickname`. Eingaben werden
mit `resolve_manager(input)` (RPC, oeffentlich) auf den Primaer-Slug aufgeloest; die Seiten
schreiben die URL darauf um. `sync-portfolio` speichert die Identitaet bei jedem Sync und
fragt Sorare mit `current_slug` ab. Daten werden NIE umgehaengt. Sorare kann NICHT per ID
suchen; die ID dient nur dem Abgleich in unseren Tabellen. Offen: so5-results/squad_* siehe BUG-039.


**Edge Functions haben KEINEN wirksamen Speicher-Cache (gemessen 05.09.):** Supabase gibt
jeder Anfrage praktisch eine frische Instanz; 7 von 7 Aufrufen einer Function mit
modul-globaler Map waren Cache-Misses, auch bei 5 gleichzeitigen. Was zwischen Anfragen
ueberleben soll, gehoert in die DB (`edge_cache` generisch, oder eine fachliche Tabelle
wie `so5_lineups`). Der "Memory-Cache" in `so5-results` ist damit wirkungslos, aber
harmlos, weil der eigentliche Cache dort die DB-Tabellen sind.
**Statement-Timeout:** Mit dem oeffentlichen Schluessel brechen RPCs nach ~3 s ab (HTTP 500);
mit dem Service-Schluessel nicht. Schwere Aggregate (Perzentile ueber 80k Zeilen) deshalb
hinter eine Function mit Service-Key + DB-Cache legen, nicht direkt aus der Seite rufen.


- **SO5-Ergebnisse je Manager sind OEFFENTLICH abrufbar (entdeckt 01.09.):** `so5.so5Fixture(slug:"football-<tag>-<monat>-<jahr>").userFixtureResults(userSlug:"<slug>")` -> `so5LeaderboardContenders` -> je Lineup `so5Appearances` (Spieler, Kapitaen, gewichtete Scores) + `so5Rankings` (Rang, rankingRatio, Score) + `so5Rewards.rewards`; **Wertung: nur `substitutionState` STARTER und SUBBED_IN zaehlen zum Lineup-Score — ON_BENCH und SUBBED_OUT (Startelf, aber nicht gespielt -> ersetzt) NICHT** (Hinweis Jonas 04.09., live verifiziert: Summe der zaehlenden Scores trifft den Ranking-Score auf 0,01) (`... on CardShardsReward{quantity rarity}`, `... on InGameCurrencyReward{coinAmount config{currency}}` — ACHTUNG (Korrektur Jonas 01.09.): `coinAmount` ist nur die MENGE; die Waehrungsart steht in `config.currency` (Enum: LIMITED_XP, RARE_XP, LIMITED_ENERGY, Craft-Clues, Wheel-Tickets, ...). Ohne dieses Feld wird XP faelschlich als 'Coins' angezeigt). Fixture-Slugs nach Muster `football-28-aug-1-sep-2026`; Leaderboard-Slugs enthalten `all_seasons` = Klassisch. ACHTUNG: anonym gilt Query-Tiefe max 7 / Komplexitaet 500 — fuer die volle Abfrage den SORARE_APIKEY nutzen (liegt in den Railway-Updater-Services; lokal: `railway run --service "Updater Limited" node ...`). Feature-Idee: SO5-Ergebnis-Ansicht im Portfolio (Raenge, Scores, Gewinne je GW).

- **Sorare GraphQL**: anonym Depth 7 / Complexity 500; mit APIKEY-Header Depth 13 / Complexity 30.000. Jonas' Key: 200 req/min (geteilt von allen Nutzern des Keys!). `@example.com`-Testmails lehnt Resend ab (GoTrue → „Error sending confirmation email")
- **Kaufdaten sind öffentlich**: `card.tokenOwner { amounts, from, transferType }` (SHARDS=gecraftet, REWARD, SINGLE_SALE_OFFER…) — Grundlage des P&L-Portfolios
- **Karten-Slug-Format**: `<player>-<jahr>-<rarity>-<serial>` (rarity auch `super_rare` MIT Unterstrich!)
- **Bewertung pro Karte**: `inSeasonEligible ? in_season : classic`-Zeile; Cache-Keys `slug_rarity_eligibility`
- **Spieler-Vollstaendigkeit**: Seed/Harvester/Portfolio finden nur Spieler mit MARKTAKTIVITAET. Nachwuchs-/Reservespieler ohne Handel fehlten dauerhaft (Dortmund: 14 von 38 fehlten, aufgedeckt durch den Club-Filter 30.07.). Slugs sind nicht ableitbar (`felix-kalu-nmecha`, `daniel-svensson-2002-02-12`) → nur ueber den Club-Kader. `football.club(slug)` haengt unter FootballRoot; Club-Slugs haben die Form `borussia-dortmund-dortmund`; Trainer (`anyPositions: [Coach]`) ueberspringen.
- **Update-Queue**: ältestes `updated_at` zuerst; epoch = Sofort-Priorität. 0-Sales-Zeilen werden voll verarbeitet (nicht konserviert!)
- **`price_history` ≠ Marktsnapshot:** Es ist ein **Änderungs-Log** — eine Zeile entsteht nur, wenn der Updater die Zeile in dem Lauf anfasst (voller Sweep dauert Tage). Tagesweise Aggregate daraus sind rotierende Stichproben und NICHT vergleichbar. Für Marktaggregate: `card_prices` (Vollbestand) → Tages-Snapshot in `market_daily` (`snapshot_market_daily()`, vom Harvester 05:30 gerufen). Einzelspieler-Verläufe (Sparklines) kommen weiter aus `price_history` via `player_history`-RPC.
- **GitHub Pages CDN cached ~10 Min** — nach Deploys `?v=x` zum Testen; Nutzer heilen sich von selbst
- **UI-Änderungen lokal servieren und KLICKEN (31.07., BUG-013):** Syntaxprüfung findet keine ReferenceErrors, keine CSS-Klassenkollisionen und keine Stapel-Fehler. Schnelltest: Node-Einzeiler als Static-Server auf `C:\craft-log\sorion-ui`, im Browser öffnen, Filter/Menüs klicken, Überlagerungen mit `document.elementFromPoint` prüfen.
- **Stapel-Kontexte:** `.controls` (z-index 40) liegt über `.table-wrap` (z-index 1) — beide erzeugen durch die `fadeUp`-Animation einen eigenen Kontext; ohne explizite z-index gewinnt der spätere im DOM.
- **UI-Patches**: Dateien haben teils CRLF (git autocrlf) — Patch-Scripts per Write-Tool als .mjs-Datei schreiben (NIE komplexe Templates durch bash/sed quälen), vorher LF-normalisieren, Anker prüfen, `new Function`-Syntaxcheck; UI-Interaktionen danach im Browser WIRKLICH klicken (Lektion BUG-009)
- **FMV-Historie**: v1 Index-Gewichte → v2 steiler → v3 Zeit-Decay+Cap → v3.1 (Ask≠Preis, Floor nur abwärts) → v3.2 (Cap 1,50, Blend 0) → **v3.3 vorgeschlagen 25.08.** (Cap nur bei duenner/alter Basis, siehe FMV_FAKTOREN_ANALYSE.md). Formel lebt an genau EINER Stelle: `lib/fmv.mjs` (Root)

## Regeln

- **Zeitbasis der Statistik: Europe/Berlin, Tag beginnt 00:00 Berlin** (Jonas 05.09.). Alle
  Auswertungs-Funktionen laufen mit `set timezone = 'Europe/Berlin'`; Fenster sind Kalendertage
  inklusive heute, nie rollierende 24 h. Neue Auswertungen halten sich daran. (Preis-Snapshots
  market_daily / fmv_accuracy_daily bleiben bei 05:30 UTC = 07:30 Berlin, gleiches Datum.)

- FMV-Logik nur in `lib/fmv.mjs` (Repo-Root — es gibt keine Ordner-Kopien mehr)
- UI-Änderung = kanonische Datei + `sorion-ui`/`Craft_log` + `Sorion_pro/UI` synchron pushen
- Keine Secrets in Code/Git; Keys mit `sb_`-Präfix sind aktuell, alles mit `eyJ` ist Legacy/tot
- Neue UI-Texte Englisch (legal.html Deutsch)
- **Nach JEDER Session: HANDOFF-Stand + BUGS/INCIDENTS aktualisieren** (Verstoß am 22.–26.07. → Jonas musste es anmahnen)
