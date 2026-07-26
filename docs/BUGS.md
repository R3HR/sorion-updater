# SORION — Bug-Archiv

> Format: ID · Titel · Symptom · Ursache · Fix · Status
> Sicherheitslücken & Crashes gehören nach [INCIDENTS.md](INCIDENTS.md).
> ⚠️ Nummern-Kollision: Das Ökosystem-Register (`C:craft-logdocsBUGS.md`) vergibt EIGENE BUG-007–010 (Review-Befunde 08.07.) — bei Referenzen immer den Dateipfad nennen.

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
- **Status:** ✅ deployed 2026-07-21 — offen nur: Namensschema-Check wenn sorarehoops die 26/27-Files publiziert

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
