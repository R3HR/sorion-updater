# SORION — Incidents: Fatal Errors, Crashes, Sicherheitslücken

> Nur schwerwiegende Vorfälle: Datenverlust, Ausfälle, Sicherheitslücken, Crashes.
> Normale Bugs → [BUGS.md](BUGS.md)

---

## SEC-001 — Service-Role-Key & OAuth-Secret im Klartext in Git 🔴 KRITISCH

- **Entdeckt:** 2026-07-06
- **Betroffen:**
  - Supabase **service_role** JWT hardcodiert in `supabase/functions/get-pool/index.ts`, `update-pool/index.ts`, `update-prices/index.ts` — dieser Key umgeht Row-Level-Security komplett (DB-Vollzugriff: lesen, schreiben, löschen)
  - Sorare **OAuth Client Secret** hardcodiert in `supabase/functions/sorare-oauth/index.ts` (+ Kopien `C:\craft-log\index.ts`, `Sorion_pro/sorare-oauth.ts`)
  - **Git-History-Analyse (2026-07-06):** Der service_role-Key liegt in der GitHub-History (Initial Commit `0054ce9`, `update.mjs` — abrufbar via `git show 0054ce9:update.mjs`). Ursache: Erste Prototyp-Version mit gepasteten Credentials; Commit `2547094 "move keys to env vars"` hat nur die Dateien gefixt, nicht die History. Das Sorare Client Secret war dagegen **nie auf GitHub** (nur lokale Dateien + Supabase-Deployment) → geringeres Risiko, Rotation trotzdem empfohlen
- **Risiko:** Jeder mit Repo-Zugriff (oder bei public Repo: jeder) kann die komplette DB manipulieren/löschen und sich als die Sorare-OAuth-App ausgeben.
- **⚠️ Verschärfung (2026-07-08):** Repo ist verifiziert **PUBLIC** (raw.githubusercontent.com ohne Login abrufbar) → der service_role-Key in der Git-History ist für **jeden im Internet** lesbar, nicht nur für Collaborators. Key-Rotation ist damit DRINGEND. Siehe auch SEC-003.
- **Fix Code-Seite:** ✅ 2026-07-06 — alle Functions nutzen jetzt `Deno.env.get('SERVICE_ROLE_KEY')` bzw. `Deno.env.get('SORARE_CLIENT_SECRET')`
- **Fix durch Jonas (OFFEN):**
  1. Supabase Dashboard → Settings → API → **JWT Secret rotieren** (⚠️ invalidiert auch den anon-Key → neuen anon-Key in `UI/index.html` und `Craft_Log UI/index.html` eintragen!)
  2. Sorare-Dashboard (OAuth-App) → **Client Secret neu generieren**
  3. Secrets setzen: `supabase secrets set SERVICE_ROLE_KEY=<neu> SORARE_CLIENT_SECRET=<neu>`
  4. Functions deployen: `supabase functions deploy get-pool update-pool update-prices sorare-oauth add-missing-players`
  5. Railway-Services: `SUPABASE_SERVICE_KEY` auf neuen Key ändern
  6. Optional (empfohlen wenn Repo je public war/wird): Git-History mit `git filter-repo` säubern
- **Status:** ✅ **GESCHLOSSEN 2026-07-21** — Projekt auf neue Supabase-API-Keys umgestellt (`sb_publishable`/`sb_secret` in UIs, Railway ×4, Edge-Function-Secrets), alle 5 Functions neu deployed, danach Legacy-JWT-Keys via „Disable JWT-based API keys" deaktiviert. Verifiziert: alter Key → HTTP 401, neue Keys + Functions + Updater laufen. **Restpunkte:** (1) Sorare Client Secret noch regenerieren + `SORARE_CLIENT_SECRET` setzen — bis dahin ist der CraftLog-Login down, da die deployte Function das Secret aus dem Env erwartet! (2) CraftLog-UI-Hosting braucht die neue index.html (alter Key darin ist tot)

## SEC-002 — Anon-Key + Supabase-URL in Frontends (Info, kein Fix nötig)

- Anon-Keys sind by design öffentlich; Zugriff wird über Row-Level-Security begrenzt.
- **Aufgabe daraus:** RLS-Policies prüfen! `card_prices`/`price_history` müssen für `anon` **read-only** sein. Wenn `anon` schreiben darf, wäre das eine echte Lücke. → Supabase Dashboard → Authentication → Policies prüfen.
- **Status:** 🟡 Prüfung offen

## INC-001 — Stille Fehlschläge bei Sorare-API (potenzieller schleichender Ausfall)

- **Beobachtung:** Alle Fetch-Fehler werden mit `catch { return null }` verschluckt. Kein Logging der Ursache, kein 429-Handling, kein Backoff.
- **Risiko:** Nach dem Voll-Seed (tausende Spieler) können Rate-Limits große Teile der nächtlichen Runs stillschweigend scheitern lassen — die UI zeigt dann tagelang alte Preise, ohne dass es jemand merkt.
- **Gegenmaßnahmen (offen):**
  - HTTP-Status + GraphQL-Error im Log ausgeben (Railway-Logs werden sonst wertlos)
  - Bei 429: Backoff (z. B. 30 s warten, Delay verdoppeln)
  - `fail_count`-Spalte (siehe BUG-005)
  - Sorare API-Key beantragen (höhere Limits)
- **Status:** 🔴 offen

## INC-002 — Season-Rollover August 2026 (vorhersehbarer Ausfall)

- **Szenario:** Beim Saisonwechsel liefern alle `IN_SEASON`-Queries plötzlich Daten der neuen Saison. Neue Karten haben 0 Sales-Historie → FMV fällt auf Floor zurück oder wird null. Hardcodierte 2025-Pool-URLs (BUG-006) brechen.
- **Plan:** Siehe HANDOFF.md TODO #1/#2. Vor dem Rollover testen: Was liefert `tokenPrices(seasonEligibility: IN_SEASON)` in den ersten Tagen der neuen Saison?
- **Status:** 🔴 offen — Deadline Anfang August 2026

## SEC-003 — Repo public: FMV-Formel & gesamter Code öffentlich lesbar

- **Entdeckt:** 2026-07-08 (Frage von Jonas, verifiziert per anonymem HTTP-Abruf)
- **Betroffen:** `github.com/R3HR/sorion-updater` ist public — `lib/fmv.mjs` (Kern-IP: FMV-Formel mit allen Parametern), alle Scripts, Configs und die komplette Git-History sind ohne Login lesbar.
- **Risiko:** Kein technischer Angriff, aber Geschäftsrisiko: Konkurrenten können die Formel 1:1 kopieren. Zusätzlich verschärft es SEC-001 (Key in History öffentlich).
- **Fix durch Jonas:** GitHub → Repo → Settings → General → Danger Zone → **Change visibility → Private**. Railway-Deploys funktionieren weiter (GitHub-App-Anbindung deckt private Repos ab). Danach trotzdem Key-Rotation aus SEC-001 durchziehen — die History war öffentlich, der Key gilt als kompromittiert.
- **Folgewirkung:** Repo privat gestellt 2026-07-08 → GitHub Pages (Website) ging offline, da Pages bei privaten Repos Bezahlplan braucht. Lösung: UI in separates public Repo `sorion-ui` ausgelagert (`C:\craft-log\sorion-ui`, enthält nur HTML/anon-Key, keine Formel). Sync-Regel in HANDOFF.
- **Status:** 🟡 Repo privat ✅ (2026-07-08) · Key-Rotation SEC-001 weiterhin offen

## INC-004 — Tabelle fmv_accuracy samt View ausserhalb der Migrationen geloescht (offen)

- **Entdeckt:** 20.08.2026 beim Debuggen leerer Accuracy-Boxen (BUG-017): `fmv_accuracy_stats` UND die Basistabelle `fmv_accuracy` antworten 404 (PGRST205, nicht im Schema-Cache).
- **Befund:** Kein einziges `drop table`/`drop view` in `migrations/` — die Loeschung kam von aussen, wahrscheinlich manuell im Supabase-Dashboard (moeglicherweise beim Aufraeumen der DB-Groesse Anfang August; die Tabelle wuchs mit jedem Sale-Abgleich).
- **Auswirkung:** Accuracy-Anzeige der Marktseite dauerhaft "—" (faellt sauber zurueck, kein Fehler). Der Updater ueberspringt das Accuracy-Logging kontrolliert (Probe + Warnung, seit 22.07. so gebaut). **Die historischen Accuracy-Daten (seit 22.07.) sind verloren** — sie waren der Beleg "FMV ± x %" auf der Marktseite.
- **Offene Klaerung mit Jonas:** Wurde die Tabelle bewusst geloescht (Platz)? Dann Accuracy-Boxen aus der UI entfernen. Falls nicht bewusst: Tabelle+View aus `migrations/2026-07-22_accuracy.sql` neu anlegen — fuellt sich dann binnen ~30 Tagen wieder; zusaetzlich pruefen, WER/WAS geloescht hat.
- **Lektion:** Objekte, die ausserhalb der Migrationen verschwinden, fallen erst auf, wenn ein Feature stirbt. Bei DB-Aufraeumaktionen im Dashboard vorher gegen `migrations/` abgleichen.
- **Klaerung (20.08., Jonas):** NICHT bewusst geloescht; Accuracy soll weiter getrackt werden. Wiederherstellung in `migrations/2026-08-20_accuracy_restore_and_tracked_snapshot.sql` (exakte Definition vom 22.07.; Updater loggt per Probe automatisch wieder). Wie es zur Loeschung kam, bleibt ungeklaert — bei kuenftigen Dashboard-Aufraeumaktionen vorher gegen `migrations/` abgleichen.
- **Status:** 🟡 Wiederherstellung geschrieben, Einspielen offen

---

## INC-005 — Kompletter Supabase-Ausfall: Disk-IO-Budget erschöpft 🔴 LAUFEND (2026-08-22)

- **Symptom:** Ab ca. **07:37 UTC** antworten **alle** Supabase-Dienste des Projekts nicht mehr — REST, RPC (`market_move`), Auth und Storage laufen in Timeouts (HTTP 000 nach 20 s). Dashboard-Status: **Unhealthy** für Database, PostgREST, Auth, Storage; **Healthy** nur Realtime + Edge Functions (die brauchen die DB-Platte nicht).
- **Ursache (Dashboard-Meldung):** *„Project is depleting its Disk IO Budget"* — Free Plan / **NANO**-Instanz. Ist das Burst-Budget aufgebraucht, drosselt AWS auf einen sehr niedrigen Basiswert; die DB ist dann praktisch unerreichbar. **Kein** Supabase-weiter Incident (die Statusseite meldete zwar „Partially Degraded", das betrifft aber ein JWT-Thema).
- **Nutzer-Auswirkung:** sorion.pro und craftlog.pro laden als statische Seite weiter, zeigen aber **keine Daten** (Markt, Portfolio) und **kein Login**. Der Squad-Poller (`squad-poll`) fällt ebenfalls aus → Datenlücke in `squad_lineup_log` für die Zeit des Ausfalls.
- **WICHTIG — nicht zu verwechseln mit dem Speicher-Problem vom 01.08.:** Damals war die **Disk-GRÖSSE** über dem Free-Limit (531/500 MB → durch Löschen zweier verwaister `price_history`-Indizes auf 422 MB). Das ist ein anderer Wert als das **Disk-IO-BUDGET** (Schreib-/Lesedurchsatz, IOPS). Größe ist laut Dashboard heute unkritisch (0 GB/500 MB); erschöpft ist der **Durchsatz**. Der Fix vom 01.08. war also korrekt und ist nicht zurückgefallen — er adressiert schlicht eine andere Metrik.
- **Wahrscheinlichste Ursache (Stand 22.08., aus Code + Zeitachse): der Cache-Wärmer `warm_market_aggregates`.** Angelegt am **21.08. ~11:12** (Migration `2026-08-21_warm_market_aggregates.sql`), läuft per pg_cron **alle 10 Minuten rund um die Uhr** und führt je Lauf **6 Vollaggregate** über `card_prices` aus (`market_leagues`/`market_facets`/`market_overview` × in_season/classic) — laut eigenem Migrationskommentar **3–3,5 s kalt**. Das sind ~864 Aggregat-Abfragen/Tag als **Dauerlast**. Auf einer NANO-Instanz (wenig RAM) passt das Working Set nicht dauerhaft in den Cache → jeder Lauf liest erneut von der Platte. Ein **Burst-Budget erschöpft sich kumulativ**: Deployment 21.08. mittags → Zusammenbruch 22.08. 07:37, also nach ~20 h Dauerlast. Passt exakt.
- **Verdacht auf Todesspirale:** pg_cron startet den Job alle 10 Min **unabhängig davon, ob der vorige noch läuft**. Unter IO-Drosselung dauert ein Lauf statt 3 s viele Minuten → Läufe überlappen → noch mehr parallele Full-Scans. Das erklärt, warum sich die DB seit dem Ausfall **nicht von selbst erholt**, obwohl von außen kaum Last kommt.
- **Entlastet:** `sync-club-rosters` (läuft seit Wochen täglich ohne diesen Effekt) und der Speicher-Fix vom 01.08. (andere Metrik). Der Squad-Poller lief den ganzen 21.08. mit, ohne dass etwas passierte.
- **Ironie/Lektion:** Der Wärmer war selbst ein Fix (Kaltstart-Timeouts der Marktseite). Er tauscht ein UX-Problem gegen Dauer-IO. Bessere Lösung: seltener wärmen (30–60 Min), nur zu aktiven Zeiten, oder die fertigen `market_daily`-Snapshots nutzen statt live zu aggregieren.
- **Frühere Hypothese (verworfen/abgeschwächt):** `sync-club-rosters.mjs` (Railway, **täglich 07:00 UTC** — exakt 37 Minuten vor dem Ausfall). Das Skript (a) liest zum Dedupe **alle** `card_prices`-Schlüssel in einen Set (Full Table Read) und (b) upsertet je Spieler **6 Zeilen** (3 Scarcities × 2 Eligibilities) mit `ignoreDuplicates` — bei ~6.000 Spielern also ~36.000 Unique-Index-Prüfungen pro Lauf. Das ist für eine NANO-Instanz ein IO-Hammer.
- **🔬 BEWEIS aus den Railway-Logs (22.08., via CLI):** Die Updater-Läufe des Nachtfensters endeten um **05:00–05:07 UTC** mit `canceling statement due to statement timeout` bzw. `upstream request timeout` — und mit **5.065 / 5.396 / 11.254 ms pro Karte** statt der normalen **~1.200 ms** (Faktor 4–9), Laufzeiten 339/308/**754 s** statt ~243 s. **Die DB war also bereits um 05:00 UTC schwer degradiert — zwei Stunden BEVOR der Kader-Abgleich (07:00) startete.** Damit ist der Kader-Abgleich als Auslöser widerlegt; er war Opfer (loggte ab 07:00 nur noch Cloudflare-522-Seiten) und hielt die Instanz danach zusätzlich unter Last, weil er über eine Stunde weiter retryte. Am 22.08. 08:26 UTC per `railway down` gestoppt (Container beendet).
- **Zweiter Verstärker:** Die Updater laufen `*/5`, brauchten aber 339–754 s pro Lauf — **länger als das Cron-Intervall**. Dadurch überlappen sich Läufe und erzeugen noch mehr parallele Last. Die Zeitbremse `MAX_RUN_MS=255000` hat das nicht verhindert (754 s Laufzeit).
- **Gesamtbild:** Cache-Wärmer (21.08. 11:12) zieht das Burst-Budget rund um die Uhr leer → Nachtfenster der Updater (22–04 UTC) trifft auf leeres Budget → Läufe werden 4–9× langsamer und überlappen → 05:00 erste Statement-Timeouts → 07:00 Kader-Abgleich kommt obendrauf → 07:37 Totalausfall.
- **Verifikation vor jedem Fix:** Supabase-Dashboard → Reports → Database → *Disk IO Budget/Consumption*. Zeigt der Verlauf einen **Tagesspike um 07:00 UTC**, ist der Kader-Abgleich der Treiber; ist es eine **gleichmäßige Dauerlast**, sind es eher die Preis-Updater (`*/5` in 22–04 und 16–20 UTC = 11 h/Tag Dauerschreiben) oder Autovacuum/Bloat. Danach `pg_stat_statements` prüfen (Top-Statements nach `shared_blks_*`).
- **Sofortmaßnahme:** Last wegnehmen, damit das Budget sich erholt — Railway-Service „Kader-Abgleich" stoppen; falls die DB bis 16:00 UTC nicht zurück ist, auch die Preis-Updater pausieren (Fenster 16–20 UTC).
- **Fixes (Vorschlag, noch nicht umgesetzt):**
  1. Kader-Abgleich auf **wöchentlich** stellen (war für nach dem Deadline Day 01.09. ohnehin geplant) — sofort umsetzbar, größter Hebel.
  2. Full-Table-Read streichen: `on conflict do nothing` erledigt das Dedupe in der DB, der Vorab-Read ist überflüssig.
  3. Nur die tatsächlich benötigten Scarcity/Eligibility-Kombinationen anlegen statt aller 6.
  4. Erst wenn das nicht reicht: Pro-Plan (größeres IO-Budget) erwägen — Kostenpunkt gegen Jonas' Kostenfreiheits-Ziel abwägen.
- **✅ BEWIESEN durch `cron.job_run_details` (22.08.):** Der Wärmer sollte laut eigener Migration **3–3,5 s kalt / 400 ms warm** brauchen. Tatsächliche Laufzeiten am 22.08.: **1 bis 12,6 Minuten** je Lauf (z. B. 08:30→08:40:56 = 10:56 min; 08:00→08:11:09; 07:40→07:52:39) — bei einem **10-Minuten-Intervall**. Die Läufe überlappten sich also nachweislich, und **jeder einzelne endete mit `failed`**: Der Wärmer hat also stundenlang nichts gewärmt, sondern nur Platten-IO verbrannt. `pg_stat_statements` war durch den Neustart zurückgesetzt und lieferte keine Historie mehr — die Job-Historie genügt aber als Beleg.
- **Behebung (22.08.):** `cron.unschedule('warm_market_aggregates')` + `cron.unschedule('squad-poll-15min')` im SQL-Editor, Kader-Abgleich per `railway down` gestoppt, Projekt neu gestartet. Danach kam die DB zurück: Auth zuerst, dann REST (Antwortzeit von >20 s Timeout auf **0,25 s**). Gesamtausfall ~07:37–09:00 UTC.
- **Folgeentscheidungen:** (1) Cache-Wärmer bleibt **dauerhaft aus** — Kaltstarts fängt seit 20.08. ohnehin der Client-Wachhund mit Backoff ab. (2) Squad-Poller wieder aktiv, aber auf ***/15 statt */10** (Migration `2026-08-22_squad_poll_reenable.sql`). (3) Offen: die drei Markt-RPCs (`market_leagues`/`market_facets`/`market_overview`) sind mit 3–3,5 s kalt zu teuer — richtiger Fix wäre eine **Materialized View**, die ein- bis zweimal täglich aktualisiert wird; die RPC-Signaturen blieben gleich, das Frontend müsste nicht angefasst werden.
- **Materialized-View-Umbau gebaut (22.08., Markt-Session):** `migrations/2026-08-22_market_materialized_views.sql` — `mv_market_leagues` + `mv_market_facets`, RPCs lesen nur noch daraus (Signaturen unveraendert, Frontend unberuehrt), Refresh 2x taeglich (06:15/14:00 UTC, ausserhalb aller Cron-Fenster) mit **Advisory-Lock gegen Ueberlappung** — die direkte Lehre aus diesem Incident. Last-Bilanz: 2 Aggregat-Laeufe/Tag statt 864 (Waermer) bzw. 1 pro Besucher. Die Waermer-Migration ist im Repo stillgelegt (`…_RETIRED_INC-005.sql`, auskommentiert).
- **Status:** 🟢 behoben, Ursache belegt. MV-Migration geschrieben, Einspielen durch Jonas offen. Railway: Updater schreiben seit 22.08. 16:00 UTC wieder (572 Zeilen/15 min gemessen); Club_Rosters am 23.08. 07:00 pruefen.

## SEC-004 — Interne Schwerlast-/Loesch-Funktionen anonym aufrufbar (22.08.2026)

- **Gefunden:** Herz-und-Nieren-Pruefung 22.08. (54 Pruefungen mit dem anonymen Schluessel). `warm_market_aggregates()` → HTTP 204 (LIEF — die INC-005-Last, fuer jeden ausloesbar), `snapshot_fmv_accuracy()` → 200 (Voll-Perzentil + DELETE), `snapshot_market_daily()`/`refresh_market_aggregates()`/`price_history_rollup()` → 57014 (liefen an; der Rollup LOESCHT Zeilen), `analytics_prune()` → 200. Kein Datenleck, aber ein **DoS-Hebel auf die IO-Drossel** (INC-005 per Knopfdruck) und ungewollte Mutationen.
- **Ursache:** Supabase-Default-Privileges vergeben EXECUTE auf neue Funktionen explizit an `anon`/`authenticated`; `revoke ... from public` (so stand es in den Migrationen) entfernt diese expliziten Grants NICHT. Spiegelbild der Analytics-Lektion vom 30.07. — dort fehlte das `from public`, hier das `from anon, authenticated`. **Regel ab jetzt: beides, immer.**
- **Fix:** `migrations/2026-08-22_SEC-004_internal_functions_lockdown.sql` — Waermer-Funktion gedroppt, fuenf interne Funktionen explizit gesperrt, **Default-Privileges geaendert: neue Funktionen starten privat** (oeffentliche RPCs brauchen ihren expliziten Grant — alle Markt-RPC-Migrationen tun das bereits; Hinweis an alle Sessions im HANDOFF). Verifikationsabfrage listet alles, was anon noch ausfuehren darf.
- **Entlastung:** Tabellen-Seite sauber — price_history/analytics_events 401, profiles/sorare_users/watchlist/squad_tokens/squad_discord_users liefern leer (RLS greift), MVs nicht direkt lesbar, OpenAPI-Wurzel verlangt Secret-Key. Edge Functions: update-pool/update-prices 403, squad-poll 403 ohne Cron-Secret, get-analytics/delete-account 401, sync-portfolio force 403.
- **Verifiziert 22.08. (nach Einspielen, von aussen als anon):** warm_market_aggregates 404 (gedroppt), snapshot_fmv_accuracy/refresh_market_aggregates/price_history_rollup/analytics_prune/snapshot_market_daily je **401**; oeffentliche RPCs (overview/leagues/facets/player_history) weiter 200.
- **Status:** ✅ geschlossen 2026-08-22

---

## INC-006 · Erstbefund (08:15 UTC) — Zweiter Totalausfall der DB (2026-08-25) — ⚠️ frühe Momentaufnahme, ersetzt durch den Abschlussbericht INC-006 weiter unten

- **Symptom:** Ab spaetestens **07:45 UTC** antworten REST und Auth nicht mehr (HTTP 000 nach 20-25 s). Die Edge Function `squad-poll` scheitert mit `Unexpected token '<', "<!DOCTYPE "... is not valid JSON` - sie bekommt eine **Cloudflare-Fehlerseite statt JSON**. Folge: Der Squad-Bot hat am 25.08. **keine einzige Meldung** abgesetzt, obwohl bereits drei Aufstellungen standen (Befund Jonas).
- **Abgrenzung zu INC-005 (22.08.):** Gleiche Fehlerklasse, **anderer Ausloeser**. Der Cache-Waermer ist seit 22.08. abgeschaltet und scheidet aus.
- **Belege aus den Railway-Logs (25.08., via CLI):**
  - Alle drei Updater: `Batch-Query nach 3 Versuchen fehlgeschlagen - upstream request timeout` bzw. `<!DOCTYPE html>`.
  - **`Updater Limited` lief von 22:00:06 bis 05:16:43 - ueber SIEBEN STUNDEN** fuer einen Lauf, der normal ~4 Minuten braucht (200 Karten, ~1.200 ms/Karte). Bei Cron `*/5` ueberlappen sich die Laeufe dadurch massiv. Dieselbe Todesspirale wie INC-005, diesmal von den Updatern selbst getragen.
  - `Club_Rosters` lief ab 07:45 im Dauer-Retry gegen die tote DB (letzter Log 08:08). **Am 25.08. 08:12 per `railway down` gestoppt** - wie am 22.08. der Job, der die Erholung blockierte.
- **Aktuelle Cron-Lage (Railway):** `Updater Limited` / `Update Rare` / `Updater SR` je `*/5 22-23,0-4 * * *` (= alle 5 Min ueber 6 Nachtstunden). `Club_Rosters` und `sorion-updater` ohne Cron.
- **Sofort noetig (Captain/Coder):**
  1. **Projekt neu starten** (Dashboard → Settings → General → Restart project). Ohne Neustart kommt die Instanz nicht hoch.
  2. **Updater entschaerfen** - Vorschlag: `*/30` statt `*/5` und das Fenster verkuerzen. Preisdaten werden seltener frisch, die Instanz ueberlebt aber die Nacht.
  3. `Club_Rosters` bleibt aus; wenn wieder an, dann **woechentlich** (`0 7 * * 1`) - 36.000 Unique-Index-Pruefungen je Lauf sind fuer NANO grenzwertig.
- **Strukturelle Bewertung:** **Zweiter Totalausfall in vier Tagen.** Eine NANO-Instanz auf dem Free-Tarif traegt diese Schreiblast nicht dauerhaft. Entweder die Updater-Frequenz deutlich runter oder Supabase Pro (25 $/Monat; Kostenrahmen dann ~38 EUR/Monat statt ~14 - siehe MONETARISIERUNG.md). Das ist keine Randnotiz mehr, sondern eine Kapazitaetsentscheidung.
- **Nicht die Ursache:** Die Supabase-Statusseite meldete erneut „Partially Degraded Service". Das war schon am 22.08. eine falsche Faehrte - die Symptome sind projektspezifisch (eigene Jobs im Timeout, Dashboard-SQL blockiert).
- **Offene Diagnose nach dem Neustart:** `select jobname, schedule from cron.job` (laeuft noch etwas Unerwartetes?) und `cron.job_run_details` auf Laufzeiten pruefen - genau die Abfrage, die INC-005 aufgeklaert hat.
- **Status:** 🔴 offen - DB zum Zeitpunkt der Dokumentation (08:15 UTC) weiterhin nicht erreichbar, Neustart steht aus.

## INC-006 — Zweiter Totalausfall binnen drei Tagen (2026-08-25) — Abschlussbericht 🟢 BEHOBEN

- **Symptom (25.08., 08:21 UTC):** REST, RPC und Auth antworten gar nicht mehr (HTTP 000 nach 12–20 s). Edge Functions weiter 200 — sie brauchen die DB-Platte nicht. sorion.pro und craftlog.pro laden als Seite, zeigen aber keine Daten und kein Login. Um 08:31 unveraendert; die DB erholt sich NICHT von selbst.
- **Railway als Ursache ausgeschlossen (per CLI geprueft):** alle Dienste **0/1 running**, kein Container rennt gegen die tote DB an (anders als bei INC-005, wo der Kader-Abgleich eine Stunde weiter retryte). Die Updater-Fenster sind bereits von 11 h auf 6 h gekuerzt (`*/5 22-23,0-4`), naechster Lauf erst in 13 Stunden. **Die Last kommt von innen.**
- **Vorgeschichte:** INC-005 am 22.08. (Cache-Waermer, behoben). Am 22.08. abends erneute Warnung „depleting its Disk IO Budget" TROTZ totem Waermer. Am 25.08. der zweite Totalausfall. **Das Muster ist damit nicht mehr ein einzelner Fehler, sondern ein struktureller Kapazitaetsengpass.**
- **Wahrscheinliche Treiber (in dieser Reihenfolge, noch nicht per Messung belegt — die DB ist unerreichbar):**
  1. **Schreibverstaerkung durch Indizes.** Auf `card_prices` liegen inzwischen **zwoelf** Indizes, darunter **zwei GIN-Trigramm-Indizes** (Namenssuche, ~23 MB). Die drei Updater aendern ~79.000 Zeilen/Tag; das ergibt bis zu ~950.000 Index-Aktualisierungen taeglich, jede davon Schreib-IO. GIN-Indizes sind dabei besonders teuer (Pending-List + schubweise Merges), und viele Indizes verhindern die guenstigen HOT-Updates. **Diese Indizes stammen aus BUG-016 (19.08.) und SEC-004 (22.08.) — also aus meiner Optimierung der LESE-Seite.**
  2. **Squad-Poller im 15-Minuten-Takt** (andere Session). Exakt derselbe Mechanismus wie der Waermer bei INC-005: unter Drosselung dauert ein Lauf laenger als das Intervall, die Laeufe stapeln sich, pg_cron hat keinen Ueberlappungsschutz.
  3. **Morgendlicher Cron-Pulk** 05:30 Harvester, 05:45 Accuracy-Snapshot, 06:15 Materialized-View-Refresh, 07:00 Kader-Abgleich — dicht gedraengt, direkt nach dem naechtlichen Updater-Fenster.
- **Wiederanlauf:** `migrations/2026-08-25_INC006_recovery.sql` — erst Projekt-Neustart im Dashboard (sonst antwortet nicht einmal der SQL-Editor), dann ALLE Cron-Jobs deaktivieren und nur das Noetigste zurueckholen (Accuracy-Snapshot, Markt-Refresh auf 1x taeglich 09:20, Wochen-Rollup). Squad-Poller bleibt aus, bis der Takt geklaert ist. Enthaelt am Ende die Diagnose-Abfragen (Index-Nutzung, HOT-Anteil).
- **Einordnung (ehrlich):** Diese Woche wurde bereits erheblich optimiert — Marktseite 15 MB → 100 KB, DB 515 → 225 MB, Live-Aggregate → Materialized Views, Updater-Fenster halbiert. Die Instanz faellt trotzdem um. Der NANO-Basiswert von 5 MB/s liegt schlicht unter dem, was dieser Datenbestand (122k Zeilen, ~79k Updates/Tag, mehrere Aggregat-Jobs) braucht. **Damit ist das im HANDOFF festgelegte Upgrade-Signal erreicht** („Fehlergraph rot trotz aller Fixes"). Reihenfolge bleibt trotzdem: erst Indizes messen und ausduennen (kostenlos, potenziell grosser Hebel), dann Pro entscheiden.
- **🔬 URSACHE BEWIESEN (cron.job_run_details, 25.08.):** Die pg_cron-Jobs (5- und 10-Minuten-Takt der Squad-Session) liefen die ganze Nacht mit **0,10–0,19 s** je Lauf und Status `succeeded` — bis **04:50**. Der Lauf um **04:55 schlug fehl und brauchte 43,9 s**; ab 05:00 scheiterte alles, mit steigenden Laufzeiten (1–8 min). **Faktor 300 innerhalb von fuenf Minuten, ohne Uebergang.**
- **Das ist die Signatur eines erschoepften Burst-Budgets**, nicht die eines entgleisten Jobs: volle Geschwindigkeit bis zum Aufbrauchen der Guthaben, dann schlagartig der NANO-Grundwert von 5 MB/s. Ein einzelner Ausreisser haette sich als schleichende Verlangsamung oder als EIN auffaelliger Job gezeigt.
- **04:55 ist exakt das Ende des naechtlichen Updater-Fensters** (`*/5 22-23,0-4` → letzter Start 04:55). Verbraucher ist also die Nachtschicht selbst: ~6 Stunden Dauerschreiben durch drei Updater, jede geaenderte Zeile durch **zwoelf Indizes** auf card_prices gejagt (~79.000 Zeilen-Updates/Tag).
- **ENTLASTET (wichtig fuer die Zuordnung):** (a) Der Materialized-View-Refresh um 06:15 — er scheiterte nach 03:42 wie alles andere, er war Opfer. (b) Die 5-/10-Minuten-Jobs der Squad-Session — sie liefen die ganze Nacht mit 0,14 s. (c) Railway — alle Dienste 0/1 running, per CLI geprueft. **Es gibt in diesem Vorfall keinen Programmierfehler zu finden.**
- **KONSEQUENZ — Upgrade auf Supabase Pro (Entscheidung 25.08.):** Das im HANDOFF definierte Signal ist erreicht und der Vorbehalt („Pro wuerde nur einen Fehler verdecken") ist durch den Beweis oben ausgeraeumt. NANO traegt diesen Datenbestand nicht mehr; Micro verdoppelt den Grundwert und vervielfacht das Burst-Guthaben. Kosten: 25 $/Monat, Gesamtstack dann ~37 EUR/Monat. Monatlich kuendbar — falls die Index-Bereinigung genug bringt, ist ein Rueckschritt moeglich.
- **Weiterhin offen (jetzt Optimierung statt Rettung):** Index-Messung nach vollem Tageszyklus (Erinnerung fuer 26.08. gesetzt; der Projekt-Neustart hatte alle pg_stat-Zaehler zurueckgesetzt, `card_prices` zeigte 0 Updates). Ziel: die zwei GIN-Trigramm-Indizes und ungenutzte Indizes entfernen, um die Schreiblast dauerhaft zu senken. Zusaetzlich: Advisory-Lock fuer die 5-Minuten-Jobs der Squad-Session — sie waren zwar unschuldig, verwandeln aber jede Verlangsamung in eine Spirale (Lehre aus INC-005).
- **Upgrade durchgefuehrt (26.08., Jonas):** Supabase Pro ist aktiv. WICHTIG: Der Plan-Wechsel allein stellt die Instanz NICHT um — unter Settings → Compute and Disk muss die Groesse auf **Micro** stehen (das 10-$-Guthaben im Pro-Plan deckt genau das; die Umstellung macht einen kurzen Neustart). Erst NACH bestaetigtem Micro die Updater-Fenster wieder oeffnen — sonst wiederholt sich die INC-006-Nacht auf der alten Nano.
- **Fenster-Wiederherstellung:** Die railway-*.tomls im Repo tragen weiterhin die vollen Fenster (`*/5 22-23,0-4,16-20`); die 6-h-Kuerzung lebt nur in den Railway-Einstellungen. Ein Redeploy je Updater-Service stellt die vollen Fenster wieder her (Config-as-code gewinnt).
- **Status:** 🟢 behoben, Ursache bewiesen, Pro aktiv seit 26.08. Offen: Micro bestaetigen → Fenster oeffnen · Squad-Poller wiederherstellen (command aus Block-1-Sicherung oder Squad-Session) · Index-Messung 26.08.


---

## SEC-005 — Discord-Webhook des Ticker-Kanals im Klartext geteilt (27.08.2026) 🟡 ROTATION EMPFOHLEN

- **Was:** Die Webhook-URL des neuen Liveticker-Kanals wurde im Chat im Klartext uebermittelt. Sie liegt damit im Sitzungsprotokoll auf Jonas' Rechner (`~\.claude\projects\C--craft-log\*.jsonl`) und ist ueber den Modellanbieter gelaufen. **Nicht** oeffentlich, **nicht** in Code oder Git (per `grep` geprueft).
- **Sofort getan:** URL nach `C:\Users\Jonas\.claude\secrets\discord_ticker_webhook.txt` (ausserhalb aller Repos) und als Supabase-Secret `DISCORD_TICKER_WEBHOOK_URL` gesetzt. Der Code liest ausschliesslich `Deno.env.get`.
- **Risiko, konkret:** Wer die URL hat, kann **in genau diesen einen Kanal schreiben** - beliebiger Text, Einbettungen, Dateien, unter frei gewaehltem Namen und Avatar (also auch als "Coordinator Bot"). Je nach Kanalrechten auch mit `@everyone`. Ausserdem laesst sich der Webhook **loeschen**, dann schweigt der Ticker.
- **Was ausdruecklich NICHT geht:** nichts lesen (keine Nachrichten, keine Mitgliederliste, keine anderen Kanaele), nicht dem Server beitreten, keine Rechte aendern, kein Zugriff auf Sorare-Daten, Datenbank oder andere Secrets. Ein Webhook ist **schreibend, auf einen Kanal begrenzt**.
- **Empfehlung:** Webhook in Discord loeschen und neu anlegen (Kanal → Einstellungen → Integrationen → Webhooks). Die **neue** URL nicht in den Chat, sondern direkt in `discord_ticker_webhook.txt` schreiben - dann wird nur noch aus der Datei gelesen.
- **Regel fuer kuenftige Secrets:** Datei anlegen, Pfad nennen. Der Wert selbst gehoert nie in eine Nachricht. So wurde es bei `discord_webhook.txt` und `discord_lineups_webhook.txt` bereits gehandhabt ("Die txt liegt jetzt im Ordner") - genau richtig.

---

## INC-007 — Squad-Bot 12 h stumm: Helfer vor seiner Definition benutzt (29.08.2026) 🟢 BEHOBEN

- **Symptom (Jonas):** "der bot läuft nicht". Heute frueh keine einzige Meldung, obwohl sieben Manager bereits aufgestellt hatten.
- **Befund:** Der pg_cron-Lauf funktionierte durchgehend (Snapshots 09:30, 09:40, 09:50). Der Poller schrieb also Rohdaten, brach aber danach ab: `POST /poll` antwortete **HTTP 500 — "Cannot access 'appDone' before initialization"**.
- **Ursache (selbst verursacht, 28.08. nachts):** Beim Einbau der Frueherkennung "Ziel nicht mehr erreichbar" wurde der Helfer `appDone` an einer Stelle **oberhalb** seiner eigenen Definition verwendet. In JavaScript ist ein `const` bis zu seiner Definition gesperrt (Temporal Dead Zone) - der Zugriff wirft. Der Fehler lag im Ticker-Block, der nur bei **offener Runde** laeuft; beim Deploy um 23:30 waren alle Runden abgeschlossen, deshalb blieb er beim Testen unsichtbar und schlug erst am naechsten Morgen zu.
- **Auswirkung:** Rund 12 Stunden ohne Aufstellungs- und Cap-Meldungen. Rohdaten gingen **nicht** verloren (Snapshot und Log werden vor dem Ticker geschrieben). Beim ersten erfolgreichen Lauf wurden die sieben Aufstellungsmeldungen nachgeholt; Cap-Verstoesse hatte es in der Zeit keine gegeben, nur Messi bei 4/4.
- **Fix (deployed 29.08.):** (1) `appDone` steht jetzt ganz oben bei den uebrigen Helfern, vor jeder Verwendung. (2) **Der gesamte Ticker-Block liegt in try/catch.** Der Ticker ist Beiwerk, die Cap- und Claim-Logik ist der Kern - ein Fehler im Beiwerk darf den Kern nie mitreissen.
- **Lektion:** Ein Codepfad, der nur unter bestimmten Bedingungen laeuft (hier: offene Runde), ist beim Deploy ausserhalb dieser Bedingungen **nicht getestet**, auch wenn der Aufruf "ok" zurueckgibt. Nach einem Deploy ausserhalb der Spielzeit gehoert ein Testlauf mit offener Runde dazu - oder die riskanten Teile werden so gekapselt, dass ihr Ausfall folgenlos bleibt.

## INC-003 — Plattform-Risiko: Totalabhängigkeit von Sorare-API (Risiko-Analyse)

- **Erfasst:** 2026-08-26 (Frage von Jonas: „Was wenn Sorare den API-Zugriff entzieht?")
- **Lage:** Preis-Daten laufen anonym über die öffentliche GraphQL-API (kein Key im Spiel); OAuth-App (CraftLog-Login) ist separat. Sorare kann jederzeit: Auth-Pflicht einführen, Rate-Limits verschärfen, Railway-IPs blocken, OAuth-App sperren.
- **Auswirkung je Szenario:** OAuth-Sperre → nur CraftLog-Login tot. API-Einschränkung → keine frischen Preise; Seite läuft mit letztem DB-Stand weiter.
- **Kritisch:** `price_history` ist NICHT rekonstruierbar (Sorare liefert nur letzte 20 Sales) — nach einer Lücke ist die Historie für immer weg.
- **Abfederung:** (1) DB-Backups automatisieren — wichtigste Einzelmaßnahme; (2) offiziellen API-Key beantragen (HANDOFF TODO #8) = registrierter statt anonymer Nutzer; (3) Rate-Limits respektieren (Delays/Backoff seit 2026-07-06 drin); (4) Plan B: API-Calls über User-OAuth-Tokens der eigenen Nutzer verteilen (CraftLog-Flow ausbaubar).
- **Status:** 🟡 dauerhaftes Restrisiko — Backups + API-Key als nächste Schritte
