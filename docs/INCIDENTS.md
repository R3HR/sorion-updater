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
- **Fix Code-Seite:** ✅ 2026-07-06 — alle Functions nutzen jetzt `Deno.env.get('SERVICE_ROLE_KEY')` bzw. `Deno.env.get('SORARE_CLIENT_SECRET')`
- **Fix durch Jonas (OFFEN):**
  1. Supabase Dashboard → Settings → API → **JWT Secret rotieren** (⚠️ invalidiert auch den anon-Key → neuen anon-Key in `UI/index.html` und `Craft_Log UI/index.html` eintragen!)
  2. Sorare-Dashboard (OAuth-App) → **Client Secret neu generieren**
  3. Secrets setzen: `supabase secrets set SERVICE_ROLE_KEY=<neu> SORARE_CLIENT_SECRET=<neu>`
  4. Functions deployen: `supabase functions deploy get-pool update-pool update-prices sorare-oauth add-missing-players`
  5. Railway-Services: `SUPABASE_SERVICE_KEY` auf neuen Key ändern
  6. Optional (empfohlen wenn Repo je public war/wird): Git-History mit `git filter-repo` säubern
- **Status:** 🟡 Code gefixt, Rotation offen

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
