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
