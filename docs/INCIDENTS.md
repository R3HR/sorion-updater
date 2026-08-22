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
- **Status:** 🟢 behoben, Ursache belegt. Offen: Materialized-View-Umbau der Markt-Aggregate + Railway-Wiederherstellung (siehe HANDOFF).
