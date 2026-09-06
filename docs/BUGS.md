# SORION — Bug-Archiv

> Format: ID · Titel · Symptom · Ursache · Fix · Status
> Sicherheitslücken & Crashes gehören nach [INCIDENTS.md](INCIDENTS.md).
> ⚠️ Nummern-Kollision: Das Ökosystem-Register (`C:/craft-log/docs/BUGS.md`) vergibt EIGENE BUG-007–010 (Review-Befunde 08.07.) — bei Referenzen immer den Dateipfad nennen.

---

## BUG-001 — FMV massiv über Marktpreis (Maza 7,15 € vs. Markt ~5 €)

- **Symptom:** Spieler wurden mit FMV weit über dem Preis angezeigt, zu dem Karten tatsächlich angeboten/verkauft wurden.
- **Ursache (3-fach):**
  1. `player.floor_price ?? sorted[0]?.eur` — Nullish-Coalescing fror den Floor beim ersten gesetzten Wert für immer ein
  2. Floor kam aus letzten *Sales* statt aus aktiven *Listings*
  3. Sale-Gewichte zu flach: Sales 6–20 hatten zusammen ~40 % Gewicht → alte teure Verkäufe dominierten bei illiquiden Karten
- **Fix:** Floor live aus `lowestPriceAnyCard.publicMinPrices` (ein kombinierter GraphQL-Call); FMV v3 mit Zeit-Decay statt Index-Gewichten (`lib/fmv.mjs`)
- **Status:** ✅ behoben 2026-07-06 (Commits `c7f2f22`, FMV v3)

## BUG-002 — Floor Price = letzter Verkauf statt aktuelles Angebot

- **Symptom:** „Floor" in UI entsprach nicht dem günstigsten Kaufangebot auf Sorare.
- **Ursache:** Scripts kannten nur `tokenPrices` (Sales). Kein Query auf aktive Listings.
- **Fix:** `anyPlayer(slug).lowestPriceAnyCard(inSeason: true, rarity).publicMinPrices.eurCents`, Fallback günstigster letzter Sale.
- **Status:** ✅ behoben 2026-07-06

## BUG-003 — Drei konkurrierende FMV-Formeln

- **Symptom:** FMV eines Spielers sprang je nachdem, welcher Codepfad zuletzt schrieb.
- **Ursache:** Formel dupliziert in `update-*.mjs` (Railway), `add-missing-players` (alte Gewichte, min(sales) als Floor) und `update-prices` (ganz andere Gewichte, sorarehoops-Daten).
- **Fix:** Eine Formel in `lib/fmv.mjs`. `add-missing-players` berechnet nichts mehr (Slim-Insert mit `updated_at=epoch`). `update-prices` wurde 2026-07-21 tatsächlich auf Metadaten-only umgebaut (vorher schrieb sie doch noch FMV/Floor/Sales mit eigener Formel!) und setzt kein `updated_at` mehr (störte die Queue).
- **Status:** ✅ behoben 2026-07-21 — Functions deployed

## BUG-004 — 24h-/7d-Prozente ohne Zeitbezug

- **Symptom:** „24h %" verglich Sale 1 mit Sale 2, „7d %" Sale 1 mit Sale 5 — bei illiquiden Karten sind das ggf. Wochen alte Verkäufe. Faktisch falsche Zahlen.
- **Ursache:** UI rechnete aus `sale_1/sale_2/sale_5` statt aus `price_history` (die existiert und täglich befüllt wird).
- **Fix:** Update-Script berechnet `change_24h`/`change_7d` aus `price_history` (FMV heute vs. vor 1/7 Tagen) und speichert sie in `card_prices`. UI liest nur noch die Spalten. Migration: `migrations/2026-07-06_add_change_columns.sql`.
- **Status:** ✅ behoben 2026-07-21 — Migration ausgeführt

## BUG-005 — updated_at wird auch bei Fehlschlag gesetzt

- **Symptom:** Spieler, deren API-Abruf dauerhaft scheitert (falscher Slug, keine Daten), verschwinden unbemerkt ans Queue-Ende. Keine Sichtbarkeit über tote Einträge.
- **Ursache:** Fail-Pfad schreibt `updated_at = now()` ohne Fehlerzähler.
- **Fix (vorgeschlagen):** Spalte `fail_count int default 0`; bei Fehlschlag inkrementieren, bei Erfolg auf 0. UI/Query kann dann `fail_count > 5` als tot markieren.
- **Status:** 🔴 offen

## BUG-006 — Hardcodiertes Jahr „2025" in Pool-URLs

- **Symptom (erwartet):** Tier-/Pool-Daten brechen beim Saisonwechsel August 2026.
- **Ursache:** `update-pool`/`update-prices` fetchen `footballRewardPool2025<Rarity>.json` von sorarehoops.vercel.app — Jahr fest verdrahtet, Dritt-Site-Abhängigkeit.
- **Fix:** Jahr dynamisch (aktuelles Jahr, Fallback Vorjahr) in `update-pool` + `update-prices` (2026-07-21). Stand 21.07. existieren nur 2025-Files → nach Erscheinen der 26/27-Files verifizieren, dass das Namensschema gleich bleibt.
- **Namensschema-Check erledigt 2026-08-06:** `footballRewardPool2026Limited.json` → **404**; `…2025Limited.json` → **200, 4,9 MB, `fetchedAt: 2026-08-06`**. sorarehoops behält den „2025"-Namen für die laufende Saison bei. Der Cron läuft also aktuell **nur dank des `thisYear-1`-Fallbacks**. Funktioniert, bleibt aber von fremder Namenskonvention abhängig — bricht, sobald sie umbenennen (oder wenn ein Jahr mehr Abstand entsteht).
- **✅ Echter Ausweg gefunden (2026-08-06): Sorare liefert den Pool selbst.** `cardShardsPool(quality: CardQuality!, rarity: Rarity!, sport: FOOTBALL)` → `[PlayerWithSupply { slug, availableSupply, anyPlayer{...} }]`, **öffentlich ohne Auth** (live getestet: TIER_1/limited = 140 Spieler mit Slug, Supply, Name, Club). Dazu `cardShardsPoolComputedAt(sport)` als Frische-Stempel. `quality` (TIER_0…) entspricht der sorarehoops-Tier-Einteilung → 1:1-Ersatz.
- **Empfehlung:** `update-pool`/`update-prices` auf `cardShardsPool` umstellen → Dritt-Anbieter-Abhängigkeit (und damit dieser Bug) verschwindet vollständig. Siehe Feature-Spec im Ökosystem-HANDOFF.
- **Status:** 🟡 Workaround deployed 21.07. (läuft via Fallback) — **Umstellung auf Sorare-API empfohlen** (Ökosystem-HANDOFF: „Craft-Pool & aktueller Craft direkt von Sorare")

## BUG-007 — CraftLog-Login tot nach Key-Umstellung (Dreifach-Ursache)

- **Symptom:** Nach SEC-001-Abschluss (21.07.): „Login with Sorare" → „Verbindung fehlgeschlagen".
- **Ursachen (3 Schichten, nacheinander gefunden):**
  1. Function-Aufrufe in callback.html/index.html schickten nur `Authorization: Bearer <key>` ohne `apikey`-Header — mit dem alten JWT-anon-Key ok, mit dem neuen publishable Key (kein JWT) lehnt das Gateway ab
  2. Beim Function-Deploy ging eine lokal vorbereitete, nie deployte SEC-gehärtete `sorare-oauth`-Version live (Login verlangt `access_token` zur Identitäts-Verifikation) — Callback kannte den neuen Vertrag nicht
  3. index.html machte nach dem Callback einen eigenen Login mit **abgeleitetem Passwort** (`sorare_<slug>_craftlog`) — genau die Lücke, die die Härtung schließt (Zufallspasswort pro Login). Die gültige Session aus dem Callback wurde ignoriert, weil `sb_user` nie gespeichert wurde
- **Fix:** apikey-Header ergänzt; Callback sendet `access_token` mit, speichert die komplette Session (`sb_token`/`sb_refresh`/`sb_user`) und wirft bei fehlender Session einen Fehler statt „erfolgreich"; index.html übernimmt die Session direkt, Passwort-Ableitung nur noch toter Fallback.
- **Lektion:** Halbe Deployments vermeiden — die gehärtete Function lag wochenlang lokal ohne Deploy UND ohne Frontend-Anpassung. Function-Contract-Änderungen immer zusammen mit den Clients ausrollen.
- **Nachtrag:** Ursache 4 — `get-analytics` + `sorare-proxy` hatten ebenfalls hardcodierte Legacy-Keys, standen aber nie auf der SEC-001-Deploy-Liste → seit Key-Disable tot, Crafts-Laden schlug fehl. 21.07. auf Deno.env umgestellt + deployed. Jetzt alle 7 Functions sauber.
- **Status:** ✅ behoben 2026-07-21 (Repo `R3HR/Craft_log` + Functions)

## BUG-008 — Crafthelper-Daten seit April eingefroren

- **Symptom:** Pool-/Tier-Daten im Crafthelper (CraftLog) monatelang veraltet (Cache-Stand 05.04.).
- **Ursache (2-fach):** (1) `update-pool` upsertete ohne `on_conflict=rarity` — der Merge zielte auf den Primärschlüssel, jeder Lauf nach dem ersten scheiterte mit „duplicate key". (2) Es gab ohnehin keinen Zeitplan — niemand rief die Function regelmäßig auf.
- **Fix:** `?on_conflict=rarity` im Upsert (deployed 21.07.); Harvester ruft `update-pool` + `update-prices` jetzt täglich nach seinem Lauf auf.
- **Status:** ✅ behoben 2026-07-21 — Cache wieder aktuell (1.189 Spieler/Rarity)

## BUG-009 — Marktseite: Ladefehler + still kaputter Eligibility-Toggle

- **Symptom:** „ERROR: failed to load market data" nach der Ladezeit-Optimierung; außerdem reagierte der In-Season/Classic-Toggle seit 21.07. nicht (still, ohne Fehlermeldung).
- **Ursachen:** (1) `count=exact` über 80k Zeilen konnte in Timeout laufen; (2) Tippfehler `filterData()` statt `filterTable()` — im neuen Loader UND im Toggle vom 21.07. (dort crashte jeder Klick lautlos).
- **Fix:** Loader ohne count-Abfrage (lädt bis unvolle Seite, Fehler degradieren zu Teildaten statt Crash); Funktionsname korrigiert. Live verifiziert inkl. Toggle (8.183 Classic-Zeilen).
- **Lektion:** UI-Interaktionen nach Einbau einmal wirklich KLICKEN (Toggle war nie getestet worden); `node --check`/`new Function` fängt nur Syntax, keine ReferenceErrors.
- **Nachtrag (v3):** Die parallelen OFFSET-Abfragen liefen bei tiefen Offsets in DB-Timeouts → stillschweigend Teildaten (9k statt 38k). Umgestellt auf ID-Bereichs-Abfragen (PK-Index, konstant schnell). Dazu Seitenwechsel-Cache: Marktdaten 5 Min in der Cache API, Portfolio 10 Min in sessionStorage — Wechsel Markt↔Portfolio lädt nichts neu. Live verifiziert (38.303 Zeilen, Cache-Hit in <1s).
- **Status:** ✅ behoben 2026-07-22

## BUG-010 — Fantasie-Listing wurde zum FMV (Corrie Ndaba, 731 €)

- **Symptom:** Corrie Ndaba (Kilmarnock) stand mit FMV 731,23 € an der Spitze der Markttabelle — reale Sales lagen bei 2,94 €.
- **Ursache (2 Formel-Löcher):** (1) Ohne qualifizierte Sales (alle älter als das Fenster) fiel `calculateFMV` blind auf den Floor zurück — der Floor war aber ein 731-€-Wunschpreis-Listing. (2) Auch mit Sales konnte ein überteuerter Floor den Blend nach OBEN ziehen (35 % Gewicht).
- **Fix (FMV v3.1):** Ohne Sales → FMV null (ein Ask ist kein Marktpreis; Floor wird separat angezeigt). Floor ≥ Sales-Wert → Floor ignorieren (man verkauft durch Unterbieten zum Sales-Wert); Floor wirkt nur noch nach unten (Blend + Cap).
- **Hinweis:** Betroffene Alt-Werte korrigieren sich beim nächsten Queue-Durchlauf der jeweiligen Zeile; das Accuracy-Tracking wird den Effekt der Regeländerung im Bias sichtbar machen.
- **Status:** ✅ behoben 2026-07-22 (deployed)

## BUG-011 — Konservierte Troll-FMVs bei Spielern ohne Verkäufe

- **Symptom:** Rare-In-Season-Tabelle führte Karteileichen mit absurden Werten an (Denholm FMV = Floor = 1.999,99 € bei letzten Sales um 5 €; Fati 847 € ohne jeden Sale) — massiver Glaubwürdigkeitsschaden.
- **Ursache:** Der Updater behandelte „0 Sales von der API" als Fehlschlag und bumpte nur `updated_at` — alte FMV/Floor-Werte (aus der Vor-v3.1-Ära mit Floor-als-FMV) blieben dadurch für immer konserviert. Betraf v. a. frisch geflippte In-Season-Zeilen (neue Saison = noch keine Verkäufe).
- **Fix:** Leere Sales = gültiger Marktzustand → Zeile wird voll verarbeitet und überschrieben (fmv wird via v3.1 zu null, Floor aktualisiert, Sales-Spalten geleert). Nur echte API-Fehler bumpen noch konservierend.
- **Sofort-Bereinigung (SQL, optional):** siehe Session 26.07. — Verdachtszeilen nullen + epoch.
- **Status:** ✅ Code deployed 2026-07-26 — Bestandszeilen korrigieren sich beim nächsten Queue-Durchlauf

## BUG-012 — Nächtlicher „Deploy Crashed" (Railway, v. a. Update Rare)

- **Symptom:** Railway schickte jede Nacht eine „Deploy Crashed"-Mail für den Service `Update Rare` (Projekt faithful-gentleness). Andere Scarcities meist unauffällig.
- **Ursache:** Der Updater holt seine Batch mit `card_prices where scarcity=$1 order by updated_at asc limit $2`. Für diese WHERE+ORDER-Kombi fehlte ein Index → Postgres filtert+sortiert ~36k Zeilen. Unter nächtlicher Parallel-Last (3 Updater + Harvester gleichzeitig, `*/5`-Cron) lief die Query in den **statement_timeout (57014)**. Das Script wertete den Query-Fehler als fatal → `process.exit(1)`; wegen `restartPolicyType="never"` meldete Railway „Crashed". Am 28.07. live reproduziert (order-by-updated_at auf `limited` → 57014). Code/Config/Datenmenge aller drei Services sind identisch (~36k je Scarcity) → „nur Rare" liegt an Timing/Last bzw. evtl. einer rare-spezifischen Railway-Env-Var (BATCH_SIZE im Dashboard) — nicht am Code.
- **Fix:** (1) **Index** `card_prices(scarcity, updated_at)` → Queue-Query wird Index-Scan, kein Timeout mehr (eigentlicher, code-unabhängiger Fix): `migrations/2026-07-28_card_prices_queue_index.sql`. (2) **Resilienz:** Batch-Query mit 3× Retry+Backoff, danach sauberer `return` (exit 0) statt `process.exit(1)` — ein transienter DB-Timeout meldet keinen Crash mehr, der nächste Cron-Tick übernimmt. In Root + limited/rare/sr-Kopien synchron deployed.
- **Index eingespielt + verifiziert (28.07.):** Nach `create index idx_card_prices_scarcity_updated` fiel die Queue-Query bei `limited` von 3,28 s (vorher Timeout) auf konstant <1 s, alle Scarcities gleichauf (Rest = HTTP-Overhead). Root-Cause behoben; Code-Resilienz (Retry statt exit) zusätzlich deployed.
- **Cleanup erledigt (28.07.):** Die Alt-Ordner `limited/rare/sr/` (Voll-Kopien) wurden entfernt, nachdem via Railway Settings→Build bestätigt war, dass alle 3 Services aus dem Repo-Root bauen. `update-scarcity.mjs` existiert nur noch einmal → keine Mehrfach-Sync mehr.
- **Status:** ✅ geschlossen 2026-07-28 (Code deployed + Index eingespielt & verifiziert)

## BUG-018 — Diaet-DELETE starb still am Editor-Zeitlimit (1,17 Mio Duplikate blieben)

- **Symptom:** Nach der Diaet v2 fiel die DB nur 511→459 MB. Diagnose-Abfrage: 1.691.974 Zeilen, davon **1.171.962 Duplikate (69 %)** noch da — das DELETE war nie durchgelaufen, die Index-Drops danach schon.
- **Ursache (korrigiert nach dem 3. Fehlversuch):** Der SQL-Editor schickt einen Lauf als **EINE implizite Transaktion** — scheitert ein Statement, wird ALLES zurueckgerollt. Diaet v2 starb am grossen DELETE → auch Index-Drops/Rollup/pg_cron wurden zurueckgerollt. Der Swap starb am 2BP01 (Alt-Tabelle hat ein Vor-Repo-SERIAL; "including defaults" kopierte den nextval auf die ALTE Sequenz) → auch die fertige Kopie verschwand wieder ("price_history_neu does not exist" beim FIX-Versuch). Die beobachteten −52 MB kamen vom manuell gelaufenen VACUUM FULL (Index-Defragmentierung), nicht von der Diaet — diese Fehlzuordnung hat die Diagnose verzoegert.
- **Fix:** `2026-08-20_price_history_swap.sql` — bei 69 % Muell ist KOPIEREN billiger als Loeschen: Behalter in frische indexlose Tabelle, Tausch, zwei lebendige Indizes neu, Lockdown exakt reappliziert. Ersetzt auch das VACUUM FULL (neue Tabelle ist kompakt geboren — das VACUUM auf 296 MB waere vermutlich am selben Limit gescheitert).
- **Lektionen:** (1) Massen-DELETEs im SQL-Editor sind eine Falle — ab ~Hunderttausenden Zeilen: Tabellen-Swap oder Batches. (2) Nach JEDER Editor-Migration das Ergebnis MESSEN statt auf "Success" vertrauen — die Diagnose-Abfrage (Zeilen/Groesse/Duplikate) haette den Fehlschlag sofort gezeigt. (3) Eine neue Tabelle bekommt Default-Rechte — Lockdowns (BUG-012) muessen beim Swap explizit mitwandern.
- **Fix final:** `2026-08-20_price_history_swap_v2.sql` — ALLES in einem Lauf; die Ein-Transaktions-Eigenschaft macht den Swap atomar: Kopie, Default-Abloesung VOR dem Drop, Tausch, eigenes Identity, 2 Indizes, Lockdown, card_prices-Praefix-Drop, Rollup-Funktion + pg_cron. `_swap.sql` und `_swap_FIX.sql` sind obsolet.
- **Lektion dazu:** Editor-Laeufe sind atomar — Teil-Erfolge gibt es nicht, und was nach einem Fehler "noch da" wirkt, stammt aus einem ANDEREN Lauf. Effekte messen und einem konkreten Lauf zuordnen.
- **Vierte und letzte Erkenntnis — der eigentliche Ausloeser aller drei Fehlversuche:** Der Editor-Dialog **„mit RLS / ohne RLS"**. Jonas waehlte stets „mit RLS" — und `price_history` ist seit BUG-012 die einzige Tabelle mit VOLLVERRIEGELUNG (`force row level security`, null Policies). Im mit-RLS-Modus war sie fuer die Migrationen unsichtbar/gesperrt: die Diaet fand nichts zu loeschen, der Swap kopierte null Zeilen, alles rollte zurueck. **Unser eigener Sicherheitszaun sperrte den Admin aus.** Mit „ohne RLS" lief der v2-Swap in einem Durchgang.
- **Ergebnis (20.08., verifiziert):** DB **515 → 225 MB**. Lockdown auf der NEUEN Tabelle geprueft: anon → 401 permission denied ✓; `player_history` liefert lueckenlos bis 23.07. ✓; market_overview 339 ms ✓; market_leagues nach Swap-bedingtem Kalt-Ausreisser (1x 3,5 s) stabil 400–450 ms.
- **Lektion final:** Migrationen im SQL-Editor IMMER „ohne RLS" ausfuehren — der mit-RLS-Modus simuliert einen API-Besucher, und gegen abgeriegelte Tabellen scheitern dann sogar SELECTs, ohne dass die Fehlermeldung RLS erwaehnt.
- **Status:** ✅ gefixt und live verifiziert 2026-08-20. **Wiedervorlage erledigt 21.08.:** Historie-Punkte vom 20.08. (nach dem Swap) und 21.08. vorhanden (Yamal/Wirtz/Bellingham via player_history) — der Updater schreibt sauber in die neue Tabelle, kein Log-Check mehr noetig.

## BUG-017 — Hero-Boxen leer unter echtem Traffic (Overview + Zaehlung kalt ueber Timeout)

- **Symptom (Jonas, 20.08.):** "Players Tracked", beide Avg-FMV-Boxen und alle drei Accuracy-Boxen zeigen "—". Movers, Ranking, Chips, Tabelle laufen.
- **Zwei GETRENNTE Ursachen:**
  1. `market_overview` kalt 3,49 s → 500, Players-Tracked-Zaehlung kalt 3,61 s → 500. Beide lagen am Vortag knapp UNTER der 3-s-Grenze; mit dem ersten echten Traffic (19./20.08., 143 Views auf der Marktseite) ist der Cache umkaempfter und der jeweils erste Besucher reisst sie. Lehre zu BUG-015/016 dazu: **"knapp unter dem Timeout" ist kein Zustand, sondern eine Frist.**
  2. `fmv_accuracy_stats` → **404: View UND Basistabelle `fmv_accuracy` existieren nicht mehr.** Keine Migration im Repo enthaelt ein drop — die Tabelle wurde ausserhalb geloescht (vermutlich Dashboard). Als INC-004 dokumentiert, Klaerung mit Jonas offen.
- **Fix (zu 1):** `migrations/2026-08-20_overview_snapshot_and_visible_index.sql` — market_overview liest nur noch den Tages-Snapshot `market_daily` (6 Zeilen/Tag statt Live-Mittel ueber 20k Zeilen; Avg damit exakt deckungsgleich mit der Chip-Basis). Teil-Index `idx_cp_visible` exakt auf der Sichtbarkeitsregel macht die Zaehlungen zu Index-Only-Scans.
- **Status:** 🟡 Migration geschrieben — einspielen, danach Kalt-Messung; Accuracy haengt an INC-004

## BUG-016 — Marktseite bricht nach Server-Umbau sporadisch ab (fehlende Filter-Indizes)

- **Symptom (Jonas, 19.08.):** Kartenladen zeigt „öfters mal" error; Liga-Filter bricht ab. Reproduzierbar als Kalt/Warm-Muster.
- **Messung:** Standard-Seite 1 **kalt 3.653 ms** (→ ueber dem statement_timeout der anon-Rolle → 500), warm 661 ms. Der Fehler trifft den jeweils ersten Besucher nach Cache-Verfall — deshalb „sporadisch".
- **Ursache:** Jede Abfrage war ein Volldurchlauf ueber ~122k Zeilen. (a) Die or()-Sichtbarkeitsregel wird vom partiellen Index (`WHERE fmv is not null`) nicht impliziert → Postgres darf ihn nicht nutzen, selbst die Startseite scannt alles. (b) Liga/Verein/Namenssuche hatten gar keinen Index.
- **Fix:** `migrations/2026-08-19_market_filter_indexes.sql` — Voll-Index (eligibility, scarcity, fmv desc) OHNE Praedikat (liest ~50 statt 122k Zeilen), Liga- und Team-Indizes, Movers-Index (change_7d), Trigramm-Indizes (pg_trgm) fuer die ilike-Suche auf Name+Verein. Der alte partielle Index bleibt fuer die Markt-RPCs.
- **Lektion (Antwort auf Jonas' Frage „wir haben doch alle Spieler in Listen"):** Eine Tabelle ist eine Liste, kein Register. **Jede Spalte, nach der die Marktseite filtert oder sortiert, braucht ihren Index** — das gilt auch fuer kuenftige Filter (Nation, Alter): Spalte fuellen UND Index anlegen, sonst Volldurchlauf und ab kaltem Cache Timeout. Merksatz aus BUG-015 ergaenzt: Messungen kalt UND warm lesen — 661 ms warm verdeckte 3,7 s kalt.
- **Verifiziert nach Einspielen (19.08.):** Classic SR (vorher 500 nach 4,8 s) → 560–690 ms; Liga-Filter ~300 ms; Suche ~290 ms; Movers ~250 ms — davon je ~200–300 ms Netzwerklaufzeit. Kein 500 mehr. Achtung Messfalle: Die ERSTEN Aufrufe nach der Migration lagen bei 1,8–2,5 s (Erstberührung der frischen Index-Seiten) — erst die Wiederholung zeigt den Dauerzustand. Restrisiko: Sortierungen ohne eigenen Index (z. B. 24h %) brauchen kalt ~2,4 s, bleiben aber unter der Grenze; falls der Fehlergraph dort wieder ausschlaegt, Index nachziehen.
- **Status:** ✅ gefixt und live verifiziert 2026-08-19

## BUG-015 — Markt-RPCs seit 02.08. unbenutzbar (coalesce blockierte den Index)

- **Symptom:** `market_overview`, `market_leagues` und `market_facets` antworteten NIE — alle drei liefen in den statement_timeout (4,0–5,2 s, 57014). Der serverseitige Umbau der Marktseite lag deshalb zwei Wochen still, ohne dass es jemandem auffiel: die RPCs waren angelegt, aber nie benutzt.
- **Ursache 1 (die eigentliche):** Alle drei filterten mit `coalesce(cp.eligibility,'in_season') = p_elig`. Ein Funktionsaufruf auf der Spalte macht `idx_card_prices_elig_scarcity_fmv` unbenutzbar → Full Scan über ~122k Zeilen bei JEDEM Aufruf. Das `coalesce` schützte vor einem Fall, den es nicht gibt: **0 Zeilen haben `eligibility IS NULL`** (Schlüssel ist slug×scarcity×eligibility, der Updater setzt die Spalte immer).
- **Ursache 2:** `market_facets` scannte die Tabelle DREIMAL (drei `union all` über dieselben Zeilen). Gemessen kostet ein Scan **warm ~250 ms, kalt ~2,4 s** — bei den aktuellen Besucherzahlen ist der Cache meistens kalt, drei Scans rissen die Grenze also zuverlässig. Jetzt ein Durchlauf via `grouping sets`.
- **Ursache 3:** `market_overview` benutzte `percentile_cont(0.5)`, das je Gruppe alle Zeilen sortiert. Der Median kommt jetzt aus `market_daily` (dort ohnehin täglich berechnet).
- **Fix:** `migrations/2026-08-18_market_rpc_coalesce_fix.sql` (ersetzt die nie eingespielte `2026-08-02_market_overview_fix.sql`). Beim Einspielen kam **42P13** — `market_overview` gab vier Spalten zurück, die neue Fassung fünf (`median_as_of`); `create or replace` darf den Rückgabetyp nicht ändern, also `drop function` davor (ohne CASCADE, damit unerwartete Abhängigkeiten laut scheitern).
- **Verifiziert (18.08., als anon über PostgREST):** overview 2.497 ms kalt / **273 ms warm**, facets 2.239 / **522 ms**, leagues 478 / **351 ms** — alle auch kalt unter der 3-Sekunden-Grenze. Werte plausibel (Limited 7.488 Karten, Avg 5,89 €, Median 2,03 € vom 18.08.).
- **Lektion 1:** `coalesce()`/`lower()`/Casts auf einer gefilterten Spalte machen jeden normalen Index blind. Defensives `coalesce` gegen NULLs, die es gar nicht gibt, kostet dann den kompletten Index.
- **Lektion 2:** `explain analyze select * from <sql_function>()` zeigt nur `Function Scan` — der innere Plan bleibt verborgen. Zum Prüfen die Rumpf-Query direkt erklären oder über PostgREST **zeiten**.
- **Lektion 3:** Kalter Cache ist bei wenig Traffic der Normalfall, nicht der Ausreißer. Messungen immer kalt UND warm lesen; ein „warm 250 ms" verdeckt ein „kalt 2,4 s".
- **Status:** ✅ gefixt und live 2026-08-18. **Folgeaufgabe:** Marktseite im Frontend auf diese RPCs umstellen (15 MB → ~50 KB pro Besuch).

## BUG-014 — Header zeigt fremden Manager statt des eingeloggten Nutzers (Marktseite)

- **Symptom (Jonas, 18.08.):** Nach einer Manager-Suche steht oben rechts der Name des ANGESEHENEN Portfolios statt des eigenen — **ueber Sitzungen hinweg**, nicht nur bis zum Neuladen.
- **Ursache:** `index.html` malte den Profil-Button aus `localStorage['sorion_manager']` — das ist per Definition der **zuletzt angesehene** Manager, nicht die Identitaet. `msOpenFull()` schreibt bei jeder Suche den fremden Slug dorthin. Weil localStorage die Sitzung ueberlebt, blieb der fremde Name dauerhaft stehen.
- **Warum es durchrutschte:** Die Trennung `sorion_own` (Identitaet) vs. `sorion_manager` (Ansicht) kam am 01.08. — aber nur in `portfolio.html`. Die Marktseite hat ihren eigenen Header-Code; der alte Kommentar dort lautete noch woertlich „Profil-Button zeigt den gemerkten Manager". Der Fix vom 01.08. war also **unvollstaendig, nicht falsch**.
- **Fix (18.08.):** `index.html` liest jetzt `sorion_own`, faellt ohne Wert auf „◉ Profile" zurueck (nie auf einen fremden Namen) und gleicht bei bestehender Session gegen `profiles.sorare_slug` ab — dieselbe Logik wie `resolveOwnSlug()` im Portfolio. Malt sofort aus dem Cache, verfeinert danach.
- **Verifiziert:** Die gepatchte Funktion gegen einen Fake-localStorage laufen lassen — (a) verknuepft → eigener Name, (b) nur fremder Manager gemerkt → „◉ Profile", (c) leer → „◉ Profile". Alle drei bestanden. Keine Datenmigration noetig: `sorion_own` war nie falsch befuellt, nur die Lesestelle war die falsche.
- **Lektion:** Beide UI-Seiten haben eigenen Header-Code. Wer die Identitaetslogik anfasst, muss `index.html` UND `portfolio.html` pruefen — `grep -n "sorion_own\|sorion_manager" *.html` vor dem Abhaken.
- **Status:** ✅ gefixt 2026-08-18 (beide Kopien: `Sorion_pro/UI/` + `sorion-ui/`)

## BUG-013 — Liga-Filter/Ranking: vier UI-Fehler, die nur im Browser sichtbar waren

- **Symptom (Jonas, 31.07.):** (a) Liga-Dropdown ohne Funktion, (b) Ranking-Panel nur als leerer Platz, (c) Klick auf 🇩🇪 Bundesliga lieferte auch Sturm Graz und Rapid Wien, (d) Menü öffnete **hinter** der Spielertabelle.
- **Ursachen:**
  1. **CSS-Namenskollision:** Das neue Ranking-Panel nutzte `.lg-panel` — dieselbe Klasse wie das Länder-Dropdown, das `display:none` als Grundzustand hat. Panel blieb unsichtbar, obwohl die Rasterspalte Platz reservierte.
  2. **Doppelte `pickLeague`-Definition:** Die spätere (neue) Version überschrieb die des Dropdowns und referenzierte nie deklarierte Variablen (`leagueCountry`, `leagueSet`) → ReferenceError bei jedem Klick.
  3. **Land wurde verworfen:** `pickLeague` setzte `countryFilter = league ? null : country`, `filterTable` verglich nur den Namen. Sorare vergibt denselben Liganamen mehrfach (24 Fälle: „Bundesliga" DE+AT, „Premier League" in 9 Ländern).
  4. **Stapel-Kontext:** `.controls` und `.table-wrap` erzeugen beide über die `fadeUp`-Animation (transform) einen Stapel-Kontext auf Ebene 0. Ohne z-index gewinnt der spätere im DOM → Tabelle über dem Menü, unabhängig vom z-index *innerhalb* des Dropdowns.
- **Fix:** eigene `lr-*`-Klassen fürs Ranking; Duplikat entfernt und ans bestehende Auswahl-Modell angebunden; Land wird mitgeführt und geprüft — **Pflicht nur bei mehrdeutigen Liganamen** (zur Laufzeit aus den Daten ermittelt), bei eindeutigen Namen bleiben Zeilen ohne Land erhalten und werden im Ranking zugeschlagen; `.controls` bekommt `position:relative; z-index:40`, `.table-wrap` `z-index:1`.
- **Lektion (Wiederholung von BUG-009):** `new Function`/`node --check` finden weder ReferenceErrors noch CSS-Kollisionen noch Stapel-Fehler. **UI-Änderungen an dieser Seite ab jetzt lokal servieren und wirklich klicken** (`node`-Einzeiler als Static-Server + Browser). Verifiziert wurde: DE 236 Treffer (nur `de`), AT 90 (nur `at`), NL 188; Ranking-Klick filtert und hebt auf; `elementFromPoint` im gesamten Überlappungsbereich → Menü oben.
- **Status:** ✅ behoben 2026-07-31 (alle vier Punkte live verifiziert)

---

## BUG-020 — Herz-und-Nieren-Pruefung 22.08.: Sparkline-Nachlader, Kalt-Abfragen, Sync-Knopf (behoben)

- **Anlass:** Jonas: „Pruefe alle Funktionen auf Herz und Nieren." 54 API/Sicherheits-Pruefungen (anonymer Schluessel) + Live-Klicktests aller Seiten + CraftLog.
- **Funde (Frontend, alle behoben 22.08.):**
  1. `loadVisibleHistory` stuerzte bei Re-Render ab (`closest('td')` null → Uncaught TypeError in der Live-Konsole). Auf dem **Handy** traf es den ersten Treffer (Karten haben kein `<td>`) → **gar keine Sparkline-Nachladung auf Mobil**. Dazu liefen alte Schleifen weiter: 76 statt 50 `player_history`-Aufrufe pro Sitzung. Fix: Generationszaehler + Null-Guards + Karten-Host.
  2. „Last Updated"-Abfrage (`order=updated_at.desc` ueber die ganze Tabelle) **4,5 s kalt → 500**. Fix: `scarcity=eq.limited` nutzt den Index `(scarcity, updated_at)`.
  3. Exakte Trefferzahl (`count=exact`) **4,2 s kalt → 500** und IO-Fresser. Fix: `count=estimated` (exakt bei kleinen Mengen, Planer-Schaetzung bei grossen).
  4. Movers bei Rarity „All"/Classic **4,6 s kalt → 500** (kein Index mit eligibility-Praefix). Fix: `idx_cp_elig_ch7` (in der SEC-004-Migration).
  5. Portfolio: Sync-Knopf fuer Fremde sichtbar → „Sync failed"-Alert. Fix: nur fuer den eingeloggten Besitzer.
- **Sicherheit:** SEC-004 (interne Funktionen anonym aufrufbar) → INCIDENTS.md, eigene Migration.
- **Kosmetik, nicht behoben (Edge Functions, Deploy noetig):** `sorare-oauth` unbekannte Action → 500 statt 400; `userinfo` ohne Token → 200 `{}`; `add-missing-players` leerer Body → 200; `get-pool` ohne JSON-Body → 500 „Unexpected end of JSON input". Alle harmlos.
- **Beobachtung (Produktentscheidung, offen):** `profile.html` ist komplett deutsch (Anmelden/Registrieren/Passwort), der Rest von sorion.pro englisch. CraftLog hat keine Discord-Fahne.
- **Gruen (zur Ehrenrettung):** Tabellen-Lockdowns (price_history/analytics 401; profiles/sorare_users/watchlist/squad_tokens/squad_discord_users leer per RLS), MVs nicht direkt lesbar, OpenAPI-Wurzel verlangt Secret-Key, Edge Functions (update-pool/update-prices 403, squad-poll 403, get-analytics/delete-account 401, sync-portfolio force 403, track Whitelist 400). Marktseite: alle Filter, Suche, Sortierung inkl. Umschaltung, Pagination, Modal, Classic/In-Season, Rarity „All". Portfolio: 297 Karten, Stats, NET/GROSS (Wert bewusst brutto, nur P&L netto — dokumentierte Entscheidung), Trade History, Winners-Filter, Karten-Detail mit Break-even, Manager-Suche, Header-Identitaet. CraftLog: Login-Gate, Sprachen, Impressum-Link, keine Fehler.
- **Status:** ✅ alles behoben und live verifiziert 2026-08-22 (inkl. SEC-004)

## BUG-019 - Discord-Meldung geht verloren, wenn Discord drosselt (22.08.) - BEHOBEN

- **Symptom:** 10 von 10 Aufstellungen standen korrekt in der DB, der Bot meldete aber nur 9 - `andreihaha` fehlte komplett (Befund Jonas).
- **Ursache:** `notifyOnce` in `squad-poll` schrieb den Dedup-Schluessel in `squad_notifications` **vor** dem Discord-Post und pruefte die Antwort **nicht**. Discord drosselt Webhooks bei ~5 Anfragen/2 s; beim ersten Lauf gingen ~10 Nachrichten im Schwall raus. Ein Post lief in ein **429**, der Schluessel war aber schon gesetzt -> nie wieder ein Versuch. Die Meldung war dauerhaft verloren.
- **Nicht betroffen:** Die Datenerfassung. Alle 10 Manager waren korrekt in `squad_lineup_log` - nur die Benachrichtigung fehlte.
- **Fix (deployed 22.08.):** `notifyOnce` prueft jetzt die Discord-Antwort. Bei 429 wird `retry_after` abgewartet und **einmal erneut** gesendet; schlaegt es endgueltig fehl, wird der **Schluessel wieder geloescht**, sodass der naechste Poll es erneut versucht. Zusaetzlich 400 ms Pause nach jedem erfolgreichen Post, um Schwaelle zu entzerren.
- **Lektion:** Ein Dedup-Schluessel darf erst dann endgueltig gelten, wenn die Zustellung bestaetigt ist - sonst wird aus "genau einmal senden" ein "hoechstens einmal senden".

---

## BUG-020 - Beim Spieltagsstart fielen Snapshot und Discord-Meldung aus (23.08.) - BEHOBEN

- **Symptom:** Neue Stage 5 war offen, `parisboemboem` wurde vom Cron um 09:50 (Berlin) korrekt ins Log geschrieben - aber es gab **weder Snapshot noch Discord-Meldung**. Die Meldung kam erst um 09:57 durch einen manuellen Poll.
- **Ursache:** Snapshot, Umstellungs-Erkennung und Benachrichtigungen haingen an `step.state === 'LINEUP_SET'`. Das Log-Schreiben dagegen nicht. Beim Uebergang von einer abgeschlossenen zur naechsten Stage steht der Step kurz in einem anderen Zustand - in diesem Fenster wurden Lineups erfasst, aber nicht gemeldet.
- **Auswirkung:** Bis zu 10 Minuten Verzug (der naechste Cron-Lauf holt es nach), kein Datenverlust.
- **Fix (deployed 23.08.):** Gemeinsames Kriterium `isActive = state !== 'CLAIMED' && state !== 'CLAIMABLE'` fuer alle drei Stellen. Damit greifen Snapshot und Meldung, sobald ein Step Lineups hat und noch nicht abgeschlossen ist - unabhaengig vom genauen Zwischenzustand.
- **Nebenbefund (Diagnose-Falle):** Die Function speichert und meldet in **UTC**; Berlin ist +2. Bei der Fehlersuche wirkten die Zeiten dadurch zunaechst falsch (07:50 UTC = 09:50 Berlin, passend zur Aufstell-Freigabe um 09:00). `status` gibt weiterhin UTC aus - beim Lesen umrechnen.
- **Zusaetzlich:** `status` liefert jetzt die letzten 15 gesendeten Meldungen (`lastNotifications`), damit sich so etwas kuenftig in einem Aufruf pruefen laesst.

---

## BUG-021 - Cap-Stau: derselbe Manager wurde mehrfach angezaehlt (24.08.) - BEHOBEN

- **Symptom (Jonas):** Budimir stand in **7** Lineups. Der Bot benannte bei jedem der drei Uebercap-Aufsteller **denselben** Manager (Enexxx) als denjenigen, der tauschen muss - obwohl der nur einmal tauschen kann.
- **Ursache:** Fuer jeden Spaetaufsteller wurde unabhaengig "der Schwaechste der ersten vier" berechnet. Ohne Gedaechtnis darueber, wer bereits verdraengt wurde, faellt die Wahl jedes Mal auf dieselbe Person. Bei genau 5 Kopien fiel das nicht auf - erst ab 6 entsteht der Stau.
- **Fix (deployed 24.08.):** Aufloesung als **Kette**. Der sichere Kreis ist jetzt veraenderlich: Ein gueltiger Claim entfernt den Verdraengten und nimmt den Claimenden auf. Der naechste Claim trifft damit automatisch den **naechstschwaechsten**. Jeder Manager wird genau einmal benannt.
- **Verifiziert per Simulation (7 Kopien):** drei verschiedene Betroffene, Endzustand = die vier hoechsten Boni. Tie-Break bestaetigt: bei gleichem Bonus weicht der **besser** platzierte Manager (Platz 8 vor Platz 10).
- **Nebeneffekt:** Die Meldung zeigt jetzt zusaetzlich das Feld "Currently in" - wer nach dem Claim tatsaechlich noch drin ist. Damit ist der Stand nach jeder Kettenstufe im Channel nachvollziehbar.

---

## BUG-022 - Erinnerung widersprach der Claim-Meldung (24.08.) - BEHOBEN

- **Symptom (Jonas):** Die 30-Minuten-Erinnerung schrieb *"Budimir - ffgaj (14%) must claim @McBeast (5%) now, or line up someone else"* - obwohl (a) der Bot laut Ansage selbst claimt und (b) die `overview`-Ausgabe zur selben Zeit **maisonpanda** als Betroffenen nannte, nicht McBeast.
- **Ursache 1 (falsche Namen):** Der Erinnerungs-Block rechnete noch mit der alten Formel `weakest = min(effBonus) der ersten vier` - **ohne Kette** (BUG-021) und **ohne Leaderboard-Tie-Break**. Zwei Codestellen, die dasselbe berechnen sollten, waren auseinandergelaufen.
- **Ursache 2 (falscher Text):** Die Formulierung stammte aus der Zeit vor der Auto-Claim-Entscheidung vom 22.08. und forderte den Manager auf, selbst zu claimen.
- **Ursache 3 (gefunden bei der Analyse):** Der Dedup-Schluessel der Claim-Meldung lautete `claim:<step>:<spieler>:<claimer>` - **ohne Ziel**. Tauscht der Betroffene und rueckt ein anderer Manager als Schwaechster nach, aendert sich das Ziel, aber es ging **keine neue Meldung** raus. Der Claim blieb auf dem alten Namen stehen.
- **Fix (deployed 24.08.):** (1) Erinnerung nutzt jetzt exakt dieselbe Kettenlogik inkl. Tie-Break. (2) Text umgestellt auf *"claimed by X → @Y has to swap out"* bzw. *"has no valid claim → line up someone else"*, Ueberschrift jetzt "Cap conflicts - action needed before the deadline". (3) Dedup-Schluessel um das Ziel erweitert: `claim:<step>:<spieler>:<claimer>:<ziel>` - ein Zielwechsel loest eine neue Meldung aus.
- **Lektion:** Dieselbe Regel an zwei Stellen implementiert = garantierte Divergenz. Kuenftig fuer Cap-Aufloesung nur EINE Funktion, die Claim-Meldung, Erinnerung und `overview` gemeinsam nutzen.

---

## BUG-023 - Tie-Break war stillschweigend AUSSER KRAFT (24.08.) - BEHOBEN

- **Symptom (Jonas):** Der Bot forderte **McBeast** auf, Budimir zu tauschen. Richtig waere **MaisonPanda** gewesen: beide hatten exakt +5 % Bonus, MaisonPanda steht im Leaderboard auf **Platz 7**, McBeast auf **Platz 8** - bei Gleichstand muss der BESSER platzierte weichen (Regel 6.2).
- **Ursache:** Tippfehler mit stiller Wirkung. Im Claim-Block stand `lbPos(r) < lbPos(m)` - `lbPos` erwartet aber einen **Slug**, nicht den Datensatz. Beide Aufrufe lieferten den Fallback **999**, `999 < 999` ist falsch, also blieb schlicht das erste Element der Liste stehen - der zeitlich Erste (McBeast). **Der Tie-Break war damit komplett wirkungslos, ohne dass ein Fehler auftrat.** Die Reihenfolge sah nur zufaellig plausibel aus.
- **Warum es nicht auffiel:** Die `overview`-Action und der Erinnerungs-Block hatten die Zeile korrekt (`lbPos(r.manager_slug)`) und lieferten die richtige Antwort - nur die Claim-Meldung nicht. Dieselbe Divergenz-Falle wie BUG-022.
- **Fix (deployed 24.08.):** `lbPos(r.manager_slug) < lbPos(m.manager_slug)`.
- **Folge fuer Runde 22 (23./24.08.):** Budimir blieb mit **5 aktiven Aufstellungen** gesperrt (mcbeast +5 %, maisonpanda +5 %, parisboemboem +10 %, namiunk_022 +13 %, ffgaj +14 %). Weichen haette **maisonpanda** muessen; angesprochen wurde McBeast. **Captain-Entscheidung noetig**, wie die Runde gewertet wird - der Bot hat den Falschen informiert.
- **Lektion:** Ein Fallback-Wert (`?? 999`) macht aus einem Typfehler ein stilles Fehlverhalten. Bei Nachschlage-Funktionen kuenftig entweder streng typisieren oder unbekannte Schluessel protokollieren, statt sie stumm auf einen Standardwert zu setzen.

---

## BUG-024 - Leaderboard-Tie-Break galt nicht fuer die Claim-Berechtigung (25.08.) - BEHOBEN

- **Symptom (Jonas):** Zwei Meldungen, beide falsch. Der Bot forderte **namiunk_022** auf, Patrick Berg und Jens Petter Hauge zu ersetzen ("bonus is not higher"). Richtig: Nami **darf beide behalten**, weil er im Leaderboard auf **Platz 10** steht und damit bei gleichem Bonus jeden schlaegt. Weichen mussten **andreihaha** (#4, Patrick Berg) und **jr3hr** (#6, Hauge).
- **Ursache:** Die Claim-Berechtigung pruefte nur `effBonus(spaeter) > effBonus(ziel)` - also **echt hoeher**. Der Leaderboard-Tie-Break wurde ausschliesslich INNERHALB der sicheren Vier angewandt (wer von ihnen der Schwaechste ist), nicht fuer die Frage, ob der Spaete ueberhaupt claimen darf. Bei Gleichstand fiel der Claim damit immer aus - obwohl Regel 6.2 die Platzierung als vollwertigen Tie-Break vorsieht.
- **Fix (deployed 25.08.):** Eine zentrale Funktion `beats(a, b)` = "a darf gegenueber b behalten": hoeherer Bonus gewinnt; bei gleichem Bonus behaelt der im Leaderboard **tiefer** platzierte. Sie wird jetzt an **allen drei** Stellen genutzt (Claim-Meldung, Erinnerung, `overview`) - zusammen mit `weakestIn()` und `whyNot()` fuer die Begruendungstexte.
- **Verifiziert:** Simulation beider Faelle liefert exakt das von Jonas genannte Ergebnis; die Live-`overview` nach dem Deploy ebenfalls (andreihaha und jr3hr muessen weichen).
- **Lektion (dritter Fall dieser Art nach BUG-022/BUG-023):** Dieselbe Regel war an drei Stellen implementiert. Erst BUG-022 (unterschiedliche Formeln), dann BUG-023 (Tippfehler in einer Kopie), jetzt BUG-024 (unvollstaendige Regel in allen Kopien). **Konsequenz: Wertigkeits-Vergleiche laufen ab sofort ausschliesslich ueber `beats()`/`weakestIn()`** - keine lokalen Neuimplementierungen mehr.

---

## BUG-025 - Liga blieb beim Vereinswechsel am alten Verein kleben (25.08.) - BEHOBEN

- **Symptom (Nutzer-Report via Jonas):** Unter Englands Ligen wirkten 1. und 2. Liga vermischt: Im Premier-League-Filter standen Spieler von Championship-Clubs und umgekehrt. Befund: Leicester-Spieler verteilt auf "Premier League", "Championship", "Ligue 1" und "1. SNL"; weltweit **228 Clubs** mit gemischten Liga-Zeilen.
- **Ursache 1 (Kern):** Der Updater schrieb `team_name`/`league_name`/`league_country` einzeln und jeweils **nur wenn nicht-null**. Sorare liefert fuer manche Clubs (Leicester, Wigan, alle U19/U21/II-Teams) `activeClub.domesticLeague = null`. Wechselte ein Spieler dorthin, wurde das Team aktualisiert, die **Liga des ALTEN Vereins blieb stehen** - Ipswich-Abgaenge standen als Leicester-Spieler in der Premier League, Swansea-Abgaenge in der Championship.
- **Ursache 2 (Nebenbefund):** Sorare hat Ligen teils **umbenannt** (HNL -> SuperSport HNL, MLS -> Major League Soccer, Ligue 2 -> Ligue 2 BKT, First Division A -> Jupiler Pro League ...). Altbestand trug die alten Namen, frisch beruehrte Zeilen die neuen -> zusaetzliche Mischung. Club- und Spieler-Endpunkt liefern dabei nicht mal denselben Namen.
- **Fix (25.08.):** (1) `update-scarcity.mjs`: Sobald ein `activeClub` vorhanden ist, werden Team UND Liga-Felder **als Einheit** geschrieben - auch auf null. Liga-Land faellt auf das Club-Land zurueck, damit ligalose Clubs unterm Land filterbar bleiben. (2) Gleicher Fallback in `sync-club-rosters.mjs`. (3) Einmal-Reparatur `tools/repair-club-leagues.mjs` (via `railway run`): setzt alle Zeilen der 228 gemischten Clubs auf die Live-Wahrheit von Sorare. Danach heilt der normale Tageszyklus jede kuenftige Drift selbst.
- **Verifiziert:** DB-Werte fuer Burnley/Ipswich/Leeds gegen Sorare live geprueft (identisch); UI-Filterpfade (Menue, Ranking, eq-Query) waren korrekt - es war ein reines Datenproblem.
- **Lektion:** "Nur schreiben wenn nicht-null" konserviert bei zusammengehoerigen Feldern (Team+Liga) einen halb-alten Zustand. Zusammengehoerige Felder immer als Einheit schreiben, Leerwerte eingeschlossen.

---

## BUG-025 - Entwarnung blieb aus, wenn ein ANDERER den Konflikt loeste (26.08.) - BEHOBEN

- **Symptom (Jonas):** Sorare | MA haette laut Bot den Torwart wechseln muessen. Stattdessen zog **jr3hr** seine eigene Kopie - der Cap war damit ebenfalls eingehalten. Der Bot meldete dazu **nichts**: kein Hinweis, dass der Fall erledigt ist.
- **Ursache:** Die Entwarnung prueft nur, ob **genau der angesprochene Manager** den Spieler nicht mehr aufgestellt hat (`rows.some(r => r.manager_slug === tgt)` -> `continue`). Loest jemand anderes den Konflikt, bleibt das Ziel in der Liste und der Block bricht ab. Die Sicht war "hat der Angesprochene gehorcht?" statt "ist das Problem geloest?".
- **Fix (deployed 26.08.):** Der Konflikt gilt als geloest, wenn **entweder** der Angesprochene getauscht hat **oder** der Spieler wieder im Rahmen ist - unabhaengig davon, wer umgestellt hat. Der tatsaechliche Verursacher wird aus dem juengsten `removed_at`-Eintrag ermittelt und in der Meldung genannt. Loeste jemand anderes den Fall, ergaenzt der Text ausserdem: "<ZIEL> did not have to swap in the end." - damit der Angesprochene sieht, dass fuer ihn nichts mehr zu tun ist.
- **Lektion:** Bestaetigungen sollten am **Zustand** haengen, nicht am erwarteten Verhalten einer bestimmten Person. Sonst bleibt jede Loesung unbemerkt, die anders zustande kommt als vorgesehen.

---

## BUG-026/027 - Entwarnung nannte den falschen Tauscher (26.08.) - BEHOBEN

- **Symptom (Jonas):** Zwei identische Entwarnungen fuer Kylian Mbappe - beide "PARISBOEMBOEM swapped out Kylian Mbappe", einmal mit "Claim by FFGAJ", einmal mit "Claim by SORARE-MA". Der Spieler wurde aber nur **einmal** getauscht.
- **Ursache:** Die Entwarnung lief **je Claim-Meldung**. Standen fuer einen Spieler zwei Claims offen (weil zwei Manager ueber dem Cap waren), erzeugte ein einziger Tausch zwei Meldungen mit identischem Inhalt.
- **Fix (deployed 26.08.):** Ist der Spieler nach dem Tausch **wieder im Rahmen**, gibt es genau **eine** Meldung pro Spieler (Schluessel `done:<step>:<player>`), die alle offenen Claims gebuendelt als erledigt ausweist ("Claims by FFGAJ, SORARE-MA settled"). Nur solange der Spieler **weiterhin ueber dem Cap** ist, wird weiter je Angesprochenem gemeldet - dort ist die Einzelmeldung ja die Information, dass noch jemand dran ist.
- **Zusatz:** Wer trotz Claim nicht tauschen musste, wird namentlich entlastet ("X and Y did not have to swap in the end").

**Nachtrag 26.08. (Korrektur des ersten Fixes):** Mein erster Ansatz - alle Claims zu EINER Meldung zusammenzufassen - war falsch. Jonas: *"Wenn es 2 Claims gab, haette es auch 2 Meldungen geben muessen. Aber nicht 2x vom gleichen Spieler. Paris kann nicht 2x Mbappe ausgewechselt haben."*

**Der eigentliche Fehler:** Die Schleife lief ueber die **Claims** und setzte fuer jede denselben "letzten Tauscher" (`removedRows[0]`) als Verursacher ein. Bei zwei Claims stand deshalb zweimal PARISBOEMBOEM, obwohl er nur einmal getauscht hatte.

**Richtige Loesung:** Die Meldung haengt jetzt am **Tausch-Vorgang**, nicht am Claim - ein Tausch, eine Meldung, mit dem tatsaechlichen Namen (Schluessel `done:<step>:<player>:<manager>`). Zwei Tausche ergeben zwei Meldungen mit den jeweils richtigen Namen; ein Tausch ergibt eine. Zusaetzlich:
- Wer ohne Aufforderung getauscht hat, bekommt "Solved without being asked - thanks."
- Wer angesprochen wurde, aber dank eines anderen nicht mehr tauschen muss, bekommt eine eigene Entlastung (`spared:<step>:<player>:<manager>`): "you don't have to swap X any more."

**Lektion:** Ereignismeldungen an das **tatsaechliche Ereignis** haengen (hier: der Tausch), nicht an den Ausloeser, der es angefordert hat. Sonst stimmt die Anzahl der Meldungen, aber nicht ihr Inhalt.

---

## BUG-028 - Entwarnung meldete jeden spaeteren Kaderwechsel (26.08.) - BEHOBEN

- **Symptom (Jonas):** Zwei Entwarnungen fuer Marc Cucurella mit dem Zusatz "is back within the cap (**2/4**)". Bei 2 von 4 ist der Konflikt laengst erledigt - die Meldungen waren voellig irrelevant.
- **Ursache:** Die Schleife lief ueber **alle** Entfernungen eines Spielers, fuer den es irgendwann Claims gab. Cucurella stand einmal ueber dem Cap (2 Claims), danach tauschten nach und nach vier Manager - und **jede** dieser Entfernungen loeste eine Entwarnung aus, obwohl der Konflikt nach den ersten beiden geloest war. Der Bot unterschied nicht zwischen "Tausch, der den Konflikt aufloest" und "normaler Kaderwechsel danach".
- **Fix (deployed 26.08.):** Es werden nur noch so viele Tausche gemeldet, wie es Claims gab (`removedRows.slice(0, claims.length)`) - genau die Anzahl, die zur Aufloesung noetig war. Alles danach ist ein gewoehnlicher Wechsel und bleibt unerwaehnt.
- **Lektion (vierter Fall in dieser Ecke, nach BUG-025/026/027):** Die Meldungslogik braucht nicht nur den richtigen **Ausloeser**, sondern auch ein **Ende**. Ein Ereignis, das einmal relevant war, bleibt es nicht dauerhaft - jede Ereignismeldung braucht eine Bedingung, ab wann sie nicht mehr gilt.

---

## BUG-029 - report lieferte eine zufaellige Runde (27.08.) - BEHOBEN

- **Symptom (Jonas):** Der Cowork-Leaderboard-Bot konnte die Runde nicht auswerten - "die Stage wurde wohl nicht als beendet deklariert". Jonas musste wieder Screenshots schicken.
- **Ursache:** Die `report`-Action waehlte die Runde mit `rowsAll[0].step_id` bei `order=updated_at.desc`. Der Poller aktualisiert aber bei **jedem** Lauf die Zeilen **aller** Steps - alle haben damit praktisch denselben `updated_at`, die Sortierung ist ein Gleichstand und der Gewinner **zufaellig**. Mal kam die fertige Runde, mal die noch laufende (ohne Scores, state LINEUP_SET). Genau das sah der Cowork-Bot als "nicht beendet".
- **Fix (deployed 27.08.):** Die Runde wird jetzt deterministisch gewaehlt - die **zuletzt gespielte abgeschlossene** Runde, bestimmt ueber den juengsten Aufstellungszeitpunkt (`max(first_seen_at)` je Step aus `squad_lineup_log`). Abgeschlossen = CLAIMED, CLAIMABLE oder FAILED. Gibt es keine fertige Runde, faellt der Report auf die offene zurueck.
- **Zusaetzlich:** Zwei neue Felder fuer den Cowork-Bot - **`round.final`** (boolean: darf gewertet werden, ohne auf einzelne Zustandsnamen pruefen zu muessen) und **`openRound`** (die gerade laufende Runde, falls vorhanden). Damit muss der Konsument keine Zustandslogik nachbauen.
- **Verifiziert:** drei Abrufe hintereinander liefern identisch Stage 3, `final=true`, 10/10 Scores - vorher wechselte das Ergebnis.
- **Lektion:** `order by` auf einem Feld, das fuer alle Zeilen gleichzeitig geschrieben wird, ist keine Sortierung, sondern ein Muenzwurf. Fuer "die neueste Runde" braucht es einen Zeitstempel, der die Runde beschreibt - nicht einen, der beim letzten Schreibvorgang entsteht.

---

## BUG-030 - Saison-Stand veraltete strukturell (27.08.) - DAUERHAFT BEHOBEN

- **Symptom (Jonas):** "kann es sein, dass der bot mit veralteten leaderboard Daten arbeitet?" - Der Stand war bis Runde 21 geseedet, seither gespielte Runden fehlten. Betroffen war nicht nur die Anzeige, sondern der **Tie-Break bei gleichem Bonus**: Bei Gleichstand behaelt der schlechter Platzierte den Spieler - stand die Platzierung falsch, entschied der Bot **falsch**, wer tauschen muss.
- **Ursache (die eigentliche):** Der Punktestand war ein **gespeicherter Wert** (`squad_season_state`), der nach jeder Runde haette fortgeschrieben werden muessen. Genau diese Fortschreibung existierte nie. Jede Korrektur hielt nur bis zur naechsten Runde - das Problem kam garantiert wieder. BUG-023 war derselbe Fehler an anderer Stelle.
- **Fix (deployed 27.08.):** Der Punktestand wird **nicht mehr gespeichert**, sondern bei **jedem Abruf** aus den Rohdaten neu gerechnet (`computeStandings()`):
  - `squad_history_rounds` / `squad_history_results` - Historie R1-R23, einmalig geseedet, unveraenderlich
  - `squad_step_scores` + `squad_step_rounds` - jede abgeschlossene Runde, Nummer automatisch beim Rundenende vergeben
  - `squad_penalties` - Cap-Strafen des Captains
  - Regeln: Platzierungspunkte [10,9,8,6,5,4,3,2,1,0], Stage-Bonus nur fuer Top 3 und nur bei geschafftem Ziel, Ø ueber alle gewerteten Runden, Sortierung Punkte -> Ø
- **Alle drei Verbraucher lesen jetzt dieselbe Berechnung:** `season`, `report.standings` und der **Tie-Break im Poller**. Damit kann keiner mehr abweichen (die Ursache von BUG-022/023/024 war genau diese Mehrfach-Implementierung).
- **Verifiziert:** Ohne laufende Runden reproduziert die Berechnung R23 exakt (ParisBoemboem 152 P / Ø 353,25; Sorare | MA auf Platz 4). Nach Zuordnung der Runde 24 steht Paris bei 164 P - der erwartete Wert. Ein Poll-Lauf danach vergab **keine** neuen Nummern (idempotent).
- **Gegenprobe am offiziellen Board (27.08., nach R24):** alle 10 Manager deckungsgleich in Punkten und Ø, Stage Clears 6/5/5/3/1 und Squad Ø 1168,63 exakt, Auf-/Absteiger-Pfeile identisch. Die Berechnung ist damit gegen 24 Runden geprueft.
- **Lektion:** Ein gespeicherter abgeleiteter Wert braucht einen Pfleger. Fehlt der, veraltet er nicht *vielleicht*, sondern **sicher**. Wo die Rohdaten vorhanden sind, ist Neuberechnung bei Abruf die einzige Bauart, die nicht veralten kann.

---

## BUG-031 - Entwarnung ging an Manager, die nie aufgefordert wurden (27.08.) - BEHOBEN

- **Symptom (Jonas):** "Die blauen nachrichten machen kein sinn. beide manager mussten nicht wechseln. wurden auch nicht vom bot zum wechseln alamiert." PARISBOEMBOEM bekam "you don't have to swap **Raphinha** any more", ENEXXX dasselbe fuer **Lamine Yamal** - beide hatten nie eine Aufforderung erhalten.
- **Ursache:** Die Entwarnung leitete den Empfaenger aus dem **Dedup-Schluessel** ab (`claim:<step>:<player>:<claimer>:<target>`, letztes Segment = Ziel). Der Schluessel endet aber **auch bei abgelehnten Claims** auf den Ziel-Manager - obwohl dort der **Claimer** angeschrieben wird ("line up a different player") und das Ziel **gar nichts erfaehrt**. Jeder abgelehnte Claim erzeugte damit einen Phantom-Empfaenger.
- **Belegt an den echten Daten:** Raphinha hatte zwei Claims - abgelehnt (sorare-ma, Bonus niedriger als parisboemboem) und gueltig (andreihaha → ffgaj). Angeschrieben wurden sorare-ma und ffgaj, **beide tauschten**. Die Entwarnung ging trotzdem an parisboemboem, weil dessen Name im Schluessel des **abgelehnten** Claims stand. Bei Lamine Yamal dasselbe Muster (abgelehnt: sorare-ma, Phantom: enexxx), ebenso bei Ginter um 11:30 (Phantom: ffgaj).
- **Fix (deployed 27.08.):** Der Empfaenger kommt jetzt aus der gespeicherten **Nutzlast**, nicht aus dem Schluessel. Sie unterscheidet sauber: gueltiger Claim → `{claimer, target}`, abgelehnt → `{claimer, reason}`. Daraus ergibt sich pro Claim, **wer handeln sollte**: bei gueltigem Claim das Ziel (muss tauschen), bei abgelehntem der Claimer (muss anders aufstellen). Zusaetzlich bekommt niemand eine Entwarnung, der selbst getauscht hat - dafuer gibt es die gruene Meldung. Keine Schluessel geaendert, also keine Nachzuendung alter Meldungen.
- **Lektion:** Der Dedup-Schluessel ist eine **Identitaet**, keine Datenquelle. Wer Fachlogik aus Schluesselbestandteilen rekonstruiert, uebernimmt stillschweigend auch die Faelle, fuer die das Segment etwas anderes bedeutet. Die Nutzlast, die beim Senden ohnehin mitgeschrieben wird, sagt eindeutig, was gemeint war.
- **Nebenbei:** `status` akzeptiert jetzt `like`, `payload` und `limit` - damit lassen sich Meldungen gezielt nachvollziehen (genau so wurde dieser Bug belegt).

---

## BUG-032 - Cowork-Bot las eine 14 Stunden alte Antwort (27.08.) - UMGANGEN

- **Symptom:** Der Cowork-Leaderboard-Bot meldete abends unveraendert Runde 22 (Stage 1, Ziel 700), Standings mit 23 Runden und ParisBoemboem bei 152, dazu "das Feld `final` existiert nicht". Er schloss daraus auf einen fehlgeschlagenen Deploy und empfahl einen Blick in die Function-Logs.
- **Gegenprobe:** Drei bzw. sechs Abrufe des Endpoints von hier - jedes Mal Stage 4, `final: true`, Standings mit 25 Runden, ParisBoemboem 176 P. Neu eingefuegtes Feld **`generatedAt`** wechselte im Sekundentakt. `CF-Cache-Status: DYNAMIC`, also kein Cloudflare-Cache.
- **Beweis der Herkunft seiner Daten:** Der von ihm gemeldete `squadScore` **1151.24** ist exakt die Top-3-Summe von R22 (411.70 + 381.40 + 358.14). Es waren also echte Daten unseres Endpoints - nur aus einer Antwort von **heute frueh (updatedAt 07:10:02)**, vor allen Korrekturen des Tages. Dazu passt, dass `final` fehlte (kam erst mittags) und die Standings noch aus der inzwischen abgeloesten `squad_season_state` stammten.
- **Ursache:** Der Abrufweg des Cowork-Bots cacht die Antwort und **ignoriert dabei Query-Parameter** - `&nocache=1&ts=...` erzeugte denselben Body. Sein eigener Befund "vier URLs, byte-identischer Body" war der Beleg dafuer, nicht dagegen.
- **Umgehung (deployed 27.08.):** Die Cowork-URLs nutzen jetzt ein **Pfad-Suffix**: `/functions/v1/squad-poll/v2?...`. Supabase leitet jeden Pfad unterhalb des Function-Namens an dieselbe Function; fuer einen Cache ist es eine neue Ressource. **Kein zweiter Deploy, kein duplizierter Code** - die Alternative "unter neuem Namen deployen" haette die 1900-Zeilen-Function verdoppelt und damit gegen die Regel verstossen, dass jede Logik genau einmal existiert. Faellt es wieder auf, wird das Suffix hochgezaehlt.
- **Dauerhafte Absicherung:** `report` liefert jetzt `generatedAt`. Damit ist Staleness in jeder Antwort selbst erkennbar, statt aus Inhalten erschlossen werden zu muessen.
- **Lektion:** Eine plausible Diagnose von aussen ("dein Deploy haengt") ist eine Hypothese, keine Tatsache. Entscheidend war ein **Fingerabdruck in den Daten** - eine Zahl, die nur zu einem bestimmten Zeitpunkt gehoeren kann. Die 1151.24 hat die Frage in einem Schritt geklaert, nachdem zwei Runden Log-Suche nichts ergeben haetten.

---

## BUG-033 - Dieselbe Tausch-Aufforderung ein zweites Mal (28.08.) - BEHOBEN

- **Symptom (Jonas):** Harry Kane stand in 6 Aufstellungen. sorare_jens wurde um 10:50 zum Tausch aufgefordert (Claim von maisonpanda, 5. Kopie), jr3hr um 11:10 (Claim von ffgaj, 6. Kopie). Nachdem **jr3hr** getauscht hatte, kam um 11:20 **erneut** eine Aufforderung an sorare_jens. "warum?"
- **Ursache:** Der Dedup-Schluessel enthielt den **Claimer**: `claim:<step>:<player>:<claimer>:<target>`. Nach jr3hrs Tausch waren es noch 5 Kopien; der Bot rechnete neu, der verbliebene Ueberzaehlige war jetzt ffgaj, und der traf wieder auf sorare_jens als Schwaechsten. `…:maisonpanda:sorare_jens` und `…:ffgaj:sorare_jens` sind verschiedene Schluessel - also ging dieselbe Aufforderung ein zweites Mal raus. Fuer den Empfaenger ist es aber **dieselbe Pflicht**; wer sie ausgeloest hat, ist ihm gleichgueltig.
- **Fix (deployed 28.08.):** Der Schluessel haengt jetzt am **Adressaten**: `claim:<step>:<player>:<recipient>`. Adressat ist bei einem gueltigen Claim das Ziel (muss tauschen), sonst der Claimer (muss anders aufstellen) - genau die Person, die die Nachricht bekommt. Wechselt dagegen das **Ziel**, ist es ein anderer Adressat und damit zu Recht eine neue Meldung; die Absicht von BUG-022 bleibt erhalten.
- **Gegenprobe an den echten Ereignissen:** Alte Logik 3 Meldungen (sorare_jens doppelt), neue Logik 2 - an sorare_jens und jr3hr je genau eine.
- **Lektion (dritter Fall dieser Art, nach BUG-022 und BUG-031):** Der Dedup-Schluessel muss die **Identitaet des Ereignisses aus Sicht des Empfaengers** abbilden. Steht Material darin, das den Empfaenger nicht betrifft - hier der Auslöser -, wiederholt sich die Nachricht, sobald sich nur dieses Material aendert.

---

## BUG-035 - Sorare-Aussetzer als Auswechslungen fehlgedeutet (31.08.) - BEHOBEN

- **Symptom (Jonas):** "Es gab bei Sorare heute einen Bug bei den Squad aufstellungen. Scheinbar hat dieser auch unseren Bot gekillt."
- **Befund - der Bot lief durchgehend:** 71 Roh-Snapshots von 07:10 bis 16:30 ohne Luecke, 36 Meldungen ueber den Tag, Ticker zuletzt 16:30:09 aktualisiert, Fristerinnerung um 18:30 Berlin korrekt in der Handlungs-Variante (eine Aufstellung fehlte). Er war nicht tot, sondern **getaeuscht**.
- **Der Aussetzer, aus den Snapshots rekonstruiert:** Die Zahl der Aufstellungen im Board lief 2 → 3 → … → 9 (10:50), fiel um **11:30 auf 8** und stieg um **15:40 zurueck auf 9**. Verschwunden und zurueckgekehrt ist genau **jr3hr** - vier Stunden lang lieferte Sorare dessen Aufstellung nicht mehr aus.
- **Ursache im Bot:** Die Umstellungs-Erkennung vergleicht die gespeicherten Zeilen mit dem, was das Board gerade liefert. Fehlt eine Zeile, gilt der Spieler als herausgenommen. Verschwindet eine **ganze Aufstellung**, sieht das aus wie fuenf gleichzeitige Auswechslungen. Der Bot meldete daraufhin "JR3HR swapped out Pedri" und "JR3HR swapped out Eric García" - beides ist nie passiert - und schickte andreihaha eine Entwarnung, die auf derselben Fehlannahme beruhte.
- **Fix (deployed 31.08.):** Fehlt ein Manager **komplett** im Board, obwohl er gespeicherte Zeilen hat, werden seine Zeilen unangetastet gelassen und keine Auswechslung gemeldet. Begruendung: Niemand nimmt sein halbes Team binnen zehn Minuten heraus, und selbst dann bliebe die Aufstellung im Board. Der Fall wird als Warnung geloggt.
- **Lektion:** Aus "Daten fehlen" folgt nicht "der Nutzer hat gehandelt". Wo ein Bot aus **Abwesenheit** auf eine Handlung schliesst, muss er unterscheiden, ob eine Einzelheit fehlt (echte Aenderung) oder ein ganzer Block (Aussetzer der Quelle). Dieselbe Wurzel wie BUG-031: dort wurde aus einem Schluesselbestandteil, hier aus einer Luecke etwas geschlossen, das die Quelle nie behauptet hat.

---

## BUG-036 - Jubel blieb aus: Ziel-Pruefung nur bei aktiver Runde (31.08.) - BEHOBEN

- **Symptom (Jonas):** "wieso hat der bot nicht gejubelt?" - Stage 4 (Ziel 1060) stand auf CLAIMABLE, aber keine 🎯-Meldung.
- **Ursache:** Die Pruefung "garantierte Summe >= Ziel" sass ausschliesslich im Ticker-Block, der nur bei `isActive` laeuft. Sorare setzt die Stage aber auf CLAIMABLE, **sobald das Ziel erreicht ist** - oft bevor alle Partien als `played` gelten. Der Step verliess den aktiven Zustand, ehe die Untergrenze das Ziel ueberschritt; die Frueherkennung bekam nie ein Zeitfenster. Ausgerechnet die Vorsicht ("erst melden, wenn es nicht mehr kippen kann") verhinderte den Jubel - spiegelbildlich zur Missed-Meldung vom 28.08.
- **Fix (deployed 31.08.):** Zweiter Ausloeser: Sorare setzt CLAIMED/CLAIMABLE -> `postHit(..., confirmed=true)` ("Stage cleared"). Beide Ausloeser teilen sich `postHit()` und den Schluessel `target-hit:<step>`, also genau eine Meldung. Nachgeholt fuer R29: 1247.27 / 1060 um 00:27.
- **Lektion:** Ein Zustandsuebergang der Quelle (hier: Sorare schliesst den Step) kann die eigene Fruehregel ueberholen. Jede Meldung braucht neben dem fruehen auch einen **formalen** Ausloeser am Endzustand.

---

## BUG-037 - Verpasste Aufstellung schoente den Durchschnitt (05.09.) - BEHOBEN

- **Symptom (gemeldet vom Cowork-Leaderboard-Bot ueber Jonas):** Sorare | MA stand bei 156 P / Ø 347.88, richtig sind 151 / 335.89 - "weil die R29-Strafe und die 0-Runde dort fehlen".
- **Ursache:** (1) Die -5 fuer die verpasste Aufstellung in R29 (31.08., nur 9 Aufstellungen) war nie in `squad_penalties` eingetragen - Captain-Entscheidung, die nicht nachgezogen wurde. (2) `computeStandings()` zaehlte nur Runden mit vorhandener Score-Zeile. Wer nicht aufgestellt hatte, hatte keine Zeile - die Runde fehlte im Nenner. Regel 8 verlangt aber "Ø inkl. 0-Runden". Gegenrechnung: 347.88 x 28 = 9740.6, / 29 = 335.9 ✓; 156 - 5 = 151 ✓.
- **Fix (deployed 05.09.):** Strafe R29 eingetragen (`penalty`). In `computeStandings()` bekommt jedes Squad-Mitglied ohne Zeile in einer live erfassten Runde einen 0-Eintrag - die Runde zaehlt als gespielt, 0 Platzierungspunkte, Schnitt sinkt. Historie (R1-R23) ist davon unberuehrt, dort sind 0-Runden bereits geseedet. Verifiziert: 151 / 335.89 / 29 Runden, exakt der Sollwert.
- **Zusaetzlich:** Neue lesende Action **`rounds`** (Einzelscores je Runde, `?round=29` filtert) und `steps` lesend freigegeben - der Cowork-Bot bekam auf `action=rounds` 403, weil es die Action nicht gab, und musste Einzelwerte aus Durchschnitten rueckrechnen.
- **Lektion:** "Keine Zeile" ist ein Datum, kein Nichts. Wo eine Regel Abwesenheit bewertet (0-Runde, verpasste Frist), muss die Berechnung Abwesenheit explizit erzeugen, statt sie stillschweigend wegzulassen. Dieselbe Familie wie BUG-035 (Abwesenheit falsch gedeutet) - nur andersherum.

---

## BUG-036 - Erstaufruf eines Portfolios: Wettlauf und Drosselung endeten als "Manager not found" (05.09.) - BEHOBEN

- **Symptom:** Lasttest vor dem Reddit-Launch (Frage Jonas: "koennte unsere Seite crashen?"). Sechs gleichzeitige Erstaufrufe desselben Portfolios: einer lieferte 322 Karten, fuenf antworteten `500 card insert failed`. Die Seite zeigte dafuer "Manager not found".
- **Ursache 1 (Wettlauf):** `sync-portfolio` prueft die Frische, holt bei Sorare und schreibt dann `DELETE` + `INSERT`. Bei parallelen Aufrufen desselben Slugs kollidierten die Inserts am Primaerschluessel. Kein Claim, kein Upsert.
- **Ursache 2 (Drosselung):** Der Sorare-Aufruf kannte keinen Umgang mit 429/5xx. Jede Drosselung wurde zu einem generischen Fehler, den die Seite ebenfalls als "not found" anzeigte. Bei ~30 neuen Besuchern pro Minute (5 bis 10 Sorare-Anfragen je neuem Slug, Kontingent 200/min) waere das im Launch passiert.
- **Fix (deployed 05.09., Function `sync-portfolio`):**
  1. **Atomarer Claim:** Vor dem Sorare-Abruf setzt der Aufrufer `last_error='in_progress'` per bedingtem PATCH (bzw. INSERT mit ignore-duplicates). Nur wer die Zeile bekommt, holt; alle anderen erhalten `skipped:'in_progress'` und die Seite liest nach 4 s aus der DB. Claim aelter als 2 Minuten gilt als haengen geblieben und wird uebernommen. Bei Fehler wird der alte Stand zurueckgeschrieben.
  2. **Upsert statt Insert** (`on_conflict` + `merge-duplicates`) als zweiter Boden.
  3. **Retry mit Key-Wechsel:** 429/5xx -> bis 3 Versuche mit Backoff, dabei rotierend ueber `SORARE_APIKEY` und optional `SORARE_APIKEY_2` (verdoppelt das Kontingent, Angebot Jonas).
  4. **Fehlerklassen:** 404 `not found` (Manager gibt es nicht), 503 `busy` + `retry_after_s` (Sorare drosselt), sonst 500. Die Seite (`portfolio.html`, `firstSync`) zeigt jetzt passende Texte, wiederholt bei busy einmal sichtbar und wartet bei in_progress.
- **Verifikation (railway run, jr3hr kuenstlich 2 Tage alt):** 6 parallel -> 1x synced, 5x in_progress, 0 Fehler. Danach 4 parallel -> alle `fresh` in ~170 ms, `last_error` null. Unbekannter Slug -> 404.
- **Erster Fix-Versuch reichte nicht:** Ein Lesen-dann-Schreiben-Claim liess im Millisekunden-Burst trotzdem alle sechs durch (alle lasen den alten Stand, bevor der erste schrieb). Erst der bedingte Schreibzugriff, bei dem die DB entscheidet, machte es dicht.
- **Lektion:** Ein "einmal pro TTL"-Schutz, der aus Lesen und spaeterem Schreiben besteht, ist unter Gleichzeitigkeit kein Schutz. Der Claim muss in EINEM Schreibbefehl mit Bedingung erfolgen. Und: die Seite darf nicht jeden Backend-Fehler auf dieselbe Nutzer-Meldung abbilden, sonst versteckt "not found" Drosselung und Wettlaeufe.

---

## BUG-037 - signup_done / login_done zaehlten Versuche statt Erfolge (05.09.) - BEHOBEN

- **Symptom (Jonas, Launch-Tag):** Monitor meldete 4 `signup_done`, die Konten-Kachel zeigte scheinbar keine neuen Konten. Tatsaechlich: 2 neue Konten (auth.users 13:15 und 14:08 UTC, beide bestaetigt, beide mit Sorare-Slug), aber 4 Ereignisse (eines doppelt binnen 2 s, eines um 12:05 ohne angelegtes Konto). `login_done` feuerte 4x in 16 Sekunden.
- **Ursache:** In `profile.html` standen `track('signup_done')` und `track('login_done')` in der ERSTEN Zeile von doSignup/doLogin, vor Pflichtfeld-Pruefung und vor der Supabase-Anfrage. Jeder Klick zaehlte, auch Fehlversuche und Doppelklicks.
- **Fix:** Beide Ereignisse feuern erst nach erfolgreicher Antwort (Signup: Konto angelegt, egal ob Token oder Bestaetigungsmail; Login: Token erhalten). Historische Zahlen bis 05.09. sind entsprechend zu hoch.
- **Nebenbefund Panel:** Die Konten-Kachel zeigte nur "+N in 30d"; zwei neue Konten an einem Tag gehen darin unter. Jetzt zusaetzlich "+N today" (zweiter Aufruf von analytics_accounts mit p_days=1).
- **Lektion:** Konversions-Ereignisse gehoeren hinter die Erfolgsantwort, nie an den Anfang des Handlers. Und ein Launch-Dashboard braucht die Tageszahl, nicht nur das Monatsfenster.

---

## BUG-038 - Statistik mischte drei Zeitbasen (UTC-Tag, rollierende 24 h, current_date) (05.09.) - BEHOBEN

- **Symptom (Jonas, Launch-Abend):** "Ich habe das Gefuehl, dass unterschiedliche Anzeigen unterschiedliche Zeiten nutzen." Tageskurve, Konten-Kachel und Retention widersprachen sich um Mitternacht.
- **Ursache:** (1) `track` setzte `analytics_events.day` als UTC-Datum -> Besuche zwischen 0 und 2 Uhr Berlin zaehlten zum Vortag. (2) `analytics_daily/pages/events_top/sources` filterten mit `current_date` der DB (UTC) und ueber N+1 Tage. (3) `analytics_accounts` ("+N today") und `analytics_retention` (active_1d/7d/30d) rechneten rollierend `now() - interval`, nicht nach Kalendertag.
- **Fix (06.09.):** Vorgabe Jonas: **alles Berliner Zeit, neuer Tag um 00:00 Europe/Berlin.** `track` berechnet den Tag (und den Tages-Salt des Besucher-Hashs) in Europe/Berlin (deployed). Migration `2026-09-06_analytics_berlin_time.sql`: Altdaten `day` aus `created_at` neu gesetzt, Spalten-Default auf Berlin, alle sechs Auswertungs-Funktionen mit `set timezone = 'Europe/Berlin'` und **Kalendertagen inklusive heute** (p_days=1 = nur heute, 30 = heute + 29). Interne Monitor-/Probe-Skripte ebenfalls auf Berlin.
- **Nebenwirkung:** Der Besucher-Hash rotiert jetzt um Mitternacht Berlin statt 02:00; am Umstellungstag koennen Besucher um diese Zeit einmal doppelt gezaehlt sein. Historische Hashes lassen sich nicht neu berechnen.
- **Lektion:** Eine Zeitbasis fuer das ganze Dashboard festlegen und in den Funktionen erzwingen (`set timezone`), statt in jeder Abfrage neu zu entscheiden. "Heute" heisst Kalendertag, nicht letzte 24 Stunden.

---

## BUG-039 - Umbenennung bei Sorare = neuer Slug = elfter Manager (05.09.) - BEHOBEN

- **Symptom (Jonas):** "Ich habe meinen Sorare namen von JR3HR zu R3HR [geaendert], wurde aber jetzt als zusaetzlicher manager erkannt." Um 12:00 meldete der Bot "R3HR has set their line-up - 5/10", obwohl JR3HR um 11:30 laengst gemeldet war.
- **Ursache:** Sorare vergibt bei einer Umbenennung einen **neuen Slug** (`jr3hr` -> `r3hr-7625d620-89dc-4c09-86b1-d1b3a7f21c50`, mit angehaengter UUID). Der Bot identifiziert Manager ueber den Slug - der neue war fuer ihn ein Unbekannter. Folgen: doppelte Aufstellungsmeldung; im Lineup-Log des Steps standen **beide** Slugs mit je fuenf Spielern (die alten Zeilen blieben absichtlich stehen, weil der Aussetzer-Schutz aus BUG-035 einen komplett fehlenden Manager nicht als Tausch wertet) - **jeder Spieler von Jonas zaehlte fuer den Cap doppelt**; Saisonstand mit elf Eintraegen; Discord-Ping und Strafen haetten ins Leere gezeigt.
- **Fix (deployed 05.09.):** Tabelle **`squad_slug_aliases`** (alias -> canonical). Der Bot kanonisiert **alles, was von Sorare kommt**, bevor es verarbeitet wird (`canonizeBoards()` auf Squad-Mitglieder und alle Lineups der Board-Antwort; ebenso in `s11Snapshot`). Historischer Kanon bleibt `jr3hr`, der Anzeigename kommt weiterhin aus Sorares Nickname (jetzt "R3HR"). Neue Admin-Actions **`find_manager`** (wo taucht ein Slug auf) und **`purge_manager`** (Zeilen eines falschen Slugs entfernen, je Step oder `all_steps`). Dubletten von heute entfernt: 5 Log-Zeilen, 1 Score-Zeile, 1 Snapshot-Zeile; danach Poll -> Step sauber mit `jr3hr`/"R3HR".
- **Lektion:** Ein Identifikator, den die Quelle bei Umbenennung neu vergibt, ist kein stabiler Schluessel. Wer trotzdem darauf baut, braucht eine Alias-Schicht an der Eingangstuer - nicht verstreute Sonderfaelle. **Bei jeder kuenftigen Umbenennung im Squad:** neuen Slug in `squad_slug_aliases` eintragen, `purge_manager` fuer den neuen Slug, ein Poll.
- **Nachtrag (05.09.):** Auf Wunsch von Jonas ist der Kanon jetzt **`r3hr`** ("mein eigentlicher name"), nicht mehr `jr3hr`. Migration `20260905123000` zieht alle Tabellen um - Historie, Strafen, Scores, Log, Snapshots, Discord-Zuordnung, Overrides **und die Dedup-Schluessel/Nutzlasten in `squad_notifications`** (sonst waere `lineup:<step>:r3hr` ein neuer Schluessel und die laufende Stage haette eine zweite Aufstellungsmeldung bekommen). Aliase: `jr3hr -> r3hr`, `r3hr-7625d620-... -> r3hr`. Aeltere Doku-Eintraege nennen weiterhin `jr3hr` - das ist derselbe Manager.

---

## BUG-039 - Umbenennung bei Sorare sperrte den Manager aus (06.09.) - BEHOBEN

- **Symptom (Jonas):** Nach Umbenennung JR3HR -> R3HR bei Sorare "kann ich mich nicht mehr einloggen". Eingabe "r3hr" -> "Manager not found"; eigenes Portfolio nach Sorare-Neuverifizierung nicht mehr ladbar.
- **Befund:** Sorare behaelt den alten Slug "jr3hr" als Alias (API liefert weiter das Konto), der neue kanonische Slug ist "r3hr-7625d620-89dc-4c09-86b1-d1b3a7f21c50" (Nickname + UUID, 41 Zeichen); "r3hr" existiert als Slug nicht. Drei Fehler bei uns: (1) die Neuverifizierung schrieb den UUID-Slug ins Profil, (2) `sync-portfolio` lehnte Slugs ueber 40 Zeichen ab ("invalid slug"), (3) es gab keine Zuordnung zwischen altem und neuem Namen. Sorare bietet keine Suche per Nutzer-ID an.
- **Fix (06.09., Migration `2026-09-06_manager_identity.sql`, per CLI ausgefuehrt):** Modell "Primaer-Slug": jeder Manager hat EINEN Slug (`manager_sync.sorare_slug`), unter dem alle Daten liegen; dazu `sorare_user_id`, `current_slug`, `nickname`. RPC `resolve_manager(input)` loest alten Slug, neuen Slug oder eindeutigen Nickname auf den Primaer-Slug auf. `sync-portfolio`: Regex bis 80 Zeichen, Eingabe aufloesen (DB, sonst Sorare-Identitaet per ID abgleichen), Sorare mit dem aktuellen Slug abfragen (Rueckfall Primaer), Identitaet bei jedem Sync speichern, Antwort traegt `slug` + `nickname`. `sorare-oauth link_sorare`: Primaer-Slug ueber Sorare-ID/aktuellen Slug bestimmen statt Sorares Slug blind zu schreiben; `profiles.sorare_user_id`. Seiten: Eingabe vor dem Laden aufloesen, URL auf den Primaer-Slug umschreiben (portfolio, Manager-Suche, Profil-Eingabe). Keine Datenzeile wurde umbenannt (2.955 Ertragszeilen, 487 Aufstellungen bleiben unter jr3hr).
- **Nebenfehler behoben:** Claim-Ruecknahme in `sync-portfolio` schrieb bei nie synchronisierten Slugs `synced_at = null` (not null) und scheiterte still -> Zeile blieb 2 Minuten "in_progress". Jetzt wird die Zeile geloescht.
- **Verifikation:** r3hr, R3HR, UUID-Slug, jr3hr -> alle `slug=jr3hr, nickname=R3HR`; unbekannt -> 404; neuer Manager (andreihaha) -> synchronisiert mit Identitaet.
- **Offen:** `so5-results` fragt Sorare weiter mit dem Primaer-Slug (Alias) ab; falls Sorare Aliasse einmal verwirft, dort `current_slug` nutzen. Squad-Tabellen (`squad_*`) kennen nur den Slug; Umbenennungen dort noch nicht abgefangen. Der OAuth-Pfad wurde nicht live durchgespielt (braucht Jonas' Sorare-Login), Code-Pfad geprueft.
- **Lektion:** Fremde Bezeichner, die der Nutzer aendern kann, taugen nicht als einziger Schluessel. Feste ID mitfuehren, eigenen stabilen Schluessel behalten, Eingaben aufloesen statt Daten umzuhaengen.
