# SORION / CraftLog — Monetarisierungsbericht

> Erstellt 20.08.2026 · Rolle: externer Product Lead · Basis: vollständige Analyse von
> `Sorion_pro/docs/HANDOFF.md`, `docs/HANDOFF.md` (Ökosystem), `IDEAS.md`, UI-Code
> (Markt/Portfolio/Profil/Stats), Edge Functions (track, get-analytics, sync-portfolio,
> sorare-oauth u. a.), Analytics-Migrationen und Betriebsdaten (Budget, Egress, API-Limits).
> Kennzeichnung durchgängig: **[Fakt]** = aus dem Projekt belegt · **[Schluss]** = meine
> Ableitung · **[Hypothese]** = muss getestet werden.

---

# 1. Produktverständnis

**Was ist das Produkt?** Ein Zwei-Produkt-Ökosystem für Sorare-Football-Manager:

- **Sorion (sorion.pro)** — „Sorare Market Intelligence": eigener Fair-Market-Value (FMV v3.1
  mit Zeit-Decay, öffentlich sichtbarem Accuracy-Tracking), serverseitige Marktseite mit
  ~122k getrackten Karten-Zeilen (3 Scarcities × In-Season/Classic), Filter (Club, Land→Liga,
  Position), Movers, Liga-Ranking, Vollmarkt-Tagessnapshots (`market_daily`). Dazu ein
  **öffentliches Portfolio pro Manager-Slug** (P&L mit 5-%-Marktgebühr NET/GROSS,
  Karten-Detail mit Sparklines, Break-even-Ask), **Trade History** mit realisierten Gewinnen
  und Trefferquote sowie **Manager Search** (Scout). Accounts mit Sorare-Verknüpfung
  (`link_sorare`, `sorare_verified`) existieren bereits. **[Fakt]**
- **CraftLog (craftlog.pro)** — bewusst simples Craft-Tool: Craft Tracker, Craft Helper mit
  Pool/Tiers/Essence-Bewertung, seit 06.08. vollständig ohne Dritt-Anbieter (sorarehoops
  ersetzt). Positionierung 22.07.: kein Feature-Ausbau. **[Fakt]**

**Welches Problem löst es?** Sorare selbst zeigt Floor-Preise, aber keinen belastbaren
fairen Wert, keine Portfolio-P&L-Rechnung inkl. Gebühren und keine realisierte
Trade-Bilanz. Sorion beantwortet die drei Kernfragen eines Sorare-Traders: *Was ist die
Karte wirklich wert? Wie steht mein Depot da? Habe ich mit meinen Trades tatsächlich Geld
verdient?* CraftLog beantwortet: *Lohnt sich dieser Craft?* **[Schluss]**

**Wichtigste Nutzergruppen** **[Schluss]**:

1. **Aktive Trader** — kaufen/verkaufen regelmäßig, brauchen FMV, Movers, Break-even. Höchste Zahlungsbereitschaft.
2. **Sammler/Manager** — schauen v. a. auf ihr eigenes Depot (Portfolio-Aufrufe sind laut Analytics ein Kern-Event). Mittlere Zahlungsbereitschaft.
3. **Scouts/Neugierige** — schauen fremde Portfolios und Manager an (manager_search). Geringe Zahlungsbereitschaft, aber wichtig für Reichweite.
4. **Crafter** — CraftLog-Nische, klein (6 Sorare-Konten **[Fakt]**), loyal.

**Zentraler kostenloser Nutzen:** Marktpreise/FMV für jeden Spieler + öffentliches
Portfolio per Slug ohne Login. Genau das bringt heute den Traffic über Google und ChatGPT
**[Fakt laut Jonas]** — es ist der Akquisekanal und darf nicht angetastet werden.

**Wichtiger Kontext, der alles rahmt** **[Fakt]**:
- **Noch nicht gelauncht** (Impressum = Launch-Blocker; Launch-Plan: Saisonstart + 3 Tage stabil).
- **Kostenbasis winzig:** ~14 €/Monat heute, ~38 €/Monat mit Supabase Pro. Deckung = 7 bzw. ~19 Nutzer à 2 €.
- **Harte Ressourcen-Limits:** 1 Sorare-API-Key (200 req/min, von allen geteilt), Supabase Free (500 MB, 5 GB Egress — nach dem serverseitigen Umbau ~50.000 Marktseiten-Besuche/Monat möglich).
- **Plattformrisiko:** Sorare selbst stand 2025 wirtschaftlich unter Druck (Presse: „close to the red"). Der Gesamtmarkt ist deutlich kleiner als 2021/22. **[Fakt, extern]**

**[Schluss]** Das Ziel der Monetarisierung ist in Phase 1 daher nicht „Umsatz maximieren",
sondern: **(a) Kosten decken, (b) Zahlungsbereitschaft beweisen, (c) den Vertrauens- und
Datenvorsprung ausbauen** (die eigene Preishistorie wird mit jedem Tag wertvoller und ist
nicht nachbaubar).

---

# 2. Analyse des bestehenden Produkts

| Feature | Nutzerwert | Nutzungspotenzial | Monetarisierungspotenzial | Kostenlos lassen? | Begründung |
|---|---|---|---|---|---|
| Marktseite (FMV-Tabelle, Filter, Suche) | Sehr hoch | Sehr hoch | Indirekt (Akquise) | **Ja, dauerhaft** | Kern-Nutzen + SEO/ChatGPT-Traffic-Quelle. Paywall hier würde das Wachstum töten. |
| FMV + öffentliches Accuracy-Tracking | Sehr hoch | Hoch | Indirekt (Vertrauen) | **Ja, dauerhaft** | Die sichtbare Accuracy ist DAS Differenzierungsmerkmal gegenüber „Bauchgefühl-Preisen". Vertrauen ist die Vorbedingung jeder späteren Zahlung. |
| Movers, Liga-Ranking, Avg-FMV-Chips | Mittel | Mittel | Gering | Ja | Engagement-Features, kein eigenständiger Zahlungsanlass. |
| Portfolio-P&L per Slug (inkl. NET/GROSS, Break-even) | Sehr hoch | Sehr hoch (Kern-Event portfolio_view) | Indirekt | **Ja (Basis)** | Öffentlich + loginfrei = viraler Einstieg („schau dir mein Depot an"). Kostet dank Spiegelung 0 API-Calls. |
| Trade History (realisierte P&L, Trefferquote) | Hoch | Mittel–hoch | **Mittel** | Ja (Basis) | Wer seine realisierte Bilanz regelmäßig prüft, ist ein Trader — die Zielgruppe für Pro. Vertiefungen (Export, Steuer-Report, lange Historie) sind Premium-fähig. |
| Manager Search / Scout | Hoch | Hoch (Kern-Event) | Gering–mittel | Ja | Discovery-Feature; API-seitig ohnehin auf öffentliche Daten beschränkt. |
| Karten-Detail (Sparklines, 7d/30d) | Hoch | Hoch (card_detail-Event) | **Mittel** | Ja (Basis) | Basis-Historie frei; *lange* Historie in voller Granularität wird durch den Wochen-Rollup ab ~Ende Oktober ohnehin knapp — voller Verlauf ist ein ehrlicher Premium-Kandidat. |
| Watchlist (Tabelle existiert, Feature offen) | Hoch (erwartet) | Hoch (erwartet) | **Hoch** | Ja (Basis-Kontingent) | Watchlist = erklärte Kaufabsicht. Zielpreise + Benachrichtigungen sind der klassischste, ehrlichste Premium-Mehrwert in dieser Produktklasse. |
| Sorare-Verknüpfung (`link_sorare`) | Niedrig (heute) | — | **Sehr hoch (Enabler)** | Ja | Kein Feature, sondern die Konto-Identität, auf der Premium technisch aufsetzt. Von Jonas genau dafür gebaut (Entscheidung 01.08.). |
| IDEA-001: Rendite-Suite (Preisgeld + Essence als Ertrag) | **Sehr hoch (potenziell)** | Mittel–hoch | **Sehr hoch** | Nein (Premium-Flaggschiff) | API-seitig NUR mit eigenem OAuth-Token möglich **[Fakt]** → kein Wettbewerber kann es ohne dieselbe Hürde; kein kostenloses Kernfeature wird beschnitten. |
| CraftLog (Tracker + Helper) | Hoch (Nische) | Niedrig (6 Nutzer) | Gering | **Ja, komplett** | Zu kleine Basis; bewusst simpel positioniert. Monetarisierung hier wäre Aufwand ohne Ertrag. |
| Eigenes Analytics + stats.html | — (intern) | — | Indirekt | — | Gutes Fundament; für Monetarisierungsentscheidungen fehlen aber Retention- und Funnel-Metriken (s. Abschnitt 9). |

---

# 3. Nutzer- und Zahlungslogik

**Wer könnte zahlen?** **[Schluss]** Die einzige realistisch zahlende Gruppe sind **aktive
Trader mit nennenswertem Depotwert**. Wer 500–5.000 € in Karten hält, für den sind 4–5 €/Monat
eine triviale Absicherung („ein einziger besser getimter Verkauf zahlt das Jahr"). Indiz im
Produkt selbst: Features wie Break-even-Ask, NET/GROSS-Toggle und Trefferquote werden nur von
Leuten verstanden und gebraucht, die wirklich handeln.

**Wofür?** Nicht für Daten-*Anzeige* (die ist Standard und frei verfügbar), sondern für:

1. **Nicht-selbst-machbar:** Rendite-Rechnung inkl. Preisgeld/Essence (geht nur mit verknüpftem Konto — technisch exklusiv).
2. **Zeitersparnis/Automatisierung:** Alerts bei Zielpreis statt täglich selbst nachschauen.
3. **Tiefe:** volle Preishistorie in Tagesgranularität, Exporte, Depot-Vergleiche über Zeit.

**Warum?** Weil jeder dieser Punkte direkt an Geld hängt, das der Nutzer bereits investiert
hat. Die Zahlungslogik ist „Absicherung + Rendite meines Sorare-Kapitals", nicht „nettes Tool".

**Wer sollte niemals zahlen müssen?**
- Gelegenheitsnutzer, die einen Spielerpreis nachschlagen (= der Google/ChatGPT-Traffic).
- Manager, die ihr eigenes Portfolio und ihre Basis-P&L sehen wollen.
- Scouts, die fremde Manager ansehen.
- CraftLog-Nutzer komplett.

**[Schluss]** Realistische Größenordnung: Bei dem heutigen Traffic-Niveau (Free-Tier-Rahmen,
Pre-Launch) sind in den ersten 6 Monaten **eher 10–50 zahlende Nutzer** erreichbar als
Hunderte. Das reicht exakt für das erklärte Ziel (Kostendeckung ~38 €/Monat) — jede Strategie,
die mehr verspricht, wäre unseriös.

---

# 4. Monetarisierungsoptionen

Skalen: 1–10 (bei Entwicklungsaufwand und Risiko ist 10 = hoch/schlecht).

## Option A — Freemium „Sorion Pro" (Abo) · **Empfehlung: JA, als Hauptmodell**

- **Modell:** Kostenlose Vollversion bleibt; Pro-Abo für Rendite-Suite, Alerts, volle Historie, Exporte.
- **Zielgruppe:** Aktive Trader mit verknüpftem Sorare-Konto.
- **Konkreter Mehrwert:** (1) Rendite-Suite (IDEA-001): Gesamtrendite = Wertänderung + Preisgeld + Essence — die Frage, die kein anderes Tool je Manager beantworten kann, technisch nur mit OAuth-Link möglich **[Fakt]**. (2) Zielpreis-Alerts auf der Watchlist (E-Mail, später Discord). (3) Preisverlauf in voller Granularität über den 90-Tage-Rollup hinaus (der Datenvorteil wächst automatisch **[Fakt: Rollup ab ~Ende Okt.]**). (4) CSV-Export von Trades/Portfolio (Steuer/Buchhaltung).
- **Preisstruktur [Hypothese]:** 3,99 €/Monat · 39 €/Jahr (Anker: SorareData Pro 4,99 €/Star 9,99 € — Sorion ist jünger und schmaler, muss darunter bleiben).
- **Technische Komplexität:** Mittel. Stripe + `profiles`-Flag ist einfach; Alerts brauchen einen Notification-Runner (Railway-Cron reicht); Rendite-Suite braucht den 5-Minuten-API-Test aus IDEA-001, dann paginierte Historie je Nutzer (API-Budget beachten: 200 req/min geteilt!).
- **Erwartetes Potenzial:** 10–50 Abos in 6 Monaten [Hypothese] → 40–200 €/Monat. Deckt Kosten, beweist das Modell.
- **Risiken:** Rendite-Suite steht und fällt mit dem API-Test (kann mit Token trotzdem leer sein); Alerts erzeugen laufende API-/Compute-Last; Sorare-ToS für kommerzielle API-Nutzung ungeprüft (**offene Info, s. u.**).
- Nutzerwert **9** · Zahlungsbereitschaft **7** · Aufwand **6** · Umsatzpotenzial **6** · Risiko **5**

## Option B — Supporter/Donations + Founding Member · **Empfehlung: JA, sofort (Brücke)**

- **Modell:** Ko-fi/BuyMeACoffee-Link + „Founding Supporter" (z. B. 24 €/Jahr, Discord-Rolle, Name auf der Seite, späterer Pro-Rabatt lebenslang).
- **Zielgruppe:** Die ersten loyalen Nutzer (Discord existiert seit 18.08. **[Fakt: discord.js + discord_join-Event]**).
- **Mehrwert:** Ideell + Frühzugang. Ehrlich kommuniziert als „hilf, die Serverkosten zu decken" — bei 14–38 €/Monat glaubwürdig.
- **Preis:** frei / 2–5 €. **Komplexität:** trivial (1 Tag). **Potenzial:** 5–30 €/Monat — aber es ist der **erste echte Zahlungsbereitschafts-Datenpunkt**.
- **Risiken:** praktisch keine; einzige Gefahr ist, es als Endzustand misszuverstehen.
- Nutzerwert **3** · Zahlungsbereitschaft **4** · Aufwand **1** · Umsatzpotenzial **2** · Risiko **1**

## Option C — Alerts als eigenständiges Bezahl-Feature (ohne volles Pro)

- **Modell:** Watchlist frei (z. B. 10 Plätze), Alerts nur bezahlt.
- **Bewertung:** Als *Bestandteil* von Pro richtig; als *alleiniges* Bezahlprodukt zu schmal — ein einzelnes Feature rechtfertigt schwer ein Abo, und Alert-Infrastruktur lohnt sich erst mit dem Pro-Bündel. **Empfehlung: in Option A aufgehen lassen.**
- Nutzerwert **7** · Zahlungsbereitschaft **6** · Aufwand **5** · Umsatzpotenzial **4** · Risiko **3**

## Option D — Daten-API / B2B-Datenprodukt · **Empfehlung: NEIN (jetzt)**

- FMV-Feed für andere Tools/Bots. Der `price_history`-Lockdown (27.07.) zeigt: Bulk-Abgriff-Interesse existiert vermutlich. Aber: Markt winzig, Abnehmer wären Konkurrenten, Sorare-ToS-Risiko maximal, Support-Aufwand hoch, und es kannibalisierte den einzigen Moat (eigene Historie). Frühestens neu bewerten, wenn Sorion selbst etabliert ist.
- Nutzerwert **5** · Zahlungsbereitschaft **5** · Aufwand **6** · Umsatzpotenzial **3** · Risiko **8**

## Option E — Werbung/Sponsoring · **Empfehlung: NEIN**

- Bei realistisch wenigen tausend Pageviews/Monat sind das Cent-Beträge; Ads zerstören die
  Terminal-Ästhetik und das Vertrauens-Positioning („cookiefrei, kein Tracking" steht in der
  legal.html **[Fakt]** — Ad-Netzwerke würden das brechen). Einzige Ausnahme später:
  ein einzelner, handverlesener Community-Sponsor im Discord.
- Nutzerwert **1** · Zahlungsbereitschaft — · Aufwand **3** · Umsatzpotenzial **1** · Risiko **7**

## Option F — Credits/Einmalkäufe · **Empfehlung: NEIN**

- Der Wert von Sorion ist kontinuierlich (Preise ändern sich täglich) — Einmalkäufe passen
  strukturell nicht und erzeugen Buchhaltungs-Overhead ohne wiederkehrenden Umsatz.
- Nutzerwert **2** · Zahlungsbereitschaft **3** · Aufwand **4** · Umsatzpotenzial **2** · Risiko **4**

## Option G — Tradingbot (geplant) als Premium-Produkt · **Empfehlung: SPÄTER, separat**

- Steht als Zukunftsidee im Ökosystem-HANDOFF **[Fakt]**. Höchster denkbarer Zahlungsanlass
  (direkter Geldhebel), aber: noch nichts gebaut, ToS-/Fairness-Fragen, Betriebsrisiko
  (ein Bot, der Fehltrades macht, vernichtet Vertrauen sofort). Nicht Teil der 6-Monats-Planung.
- Nutzerwert **8** · Zahlungsbereitschaft **9** · Aufwand **10** · Umsatzpotenzial **7** · Risiko **9**

---

# 5. Empfohlenes Geschäftsmodell

**Freemium mit einem einzigen Pro-Tier**, gebaut auf der Konto-Identität, die dafür bereits
angelegt wurde (Entscheidung Jonas 01.08.: „ein Premium-Modell braucht EINE Konto-Identität"
**[Fakt]**).

**Dauerhaft kostenlos (unantastbar):**
- Marktseite komplett: FMV, Accuracy, Filter, Suche, Movers, Liga-Ranking
- Portfolio per Slug inkl. P&L, NET/GROSS, Karten-Detail mit Basis-Sparkline (90 Tage)
- Trade History (Anzeige)
- Manager Search
- Watchlist mit Basis-Kontingent (z. B. 10 Spieler) — *ohne* automatische Alerts
- CraftLog vollständig

**Sorion Pro (nur echte Zusatzwerte):**
1. **Rendite-Suite** — Preisgeld + Essence als Ertrag, Gesamtrendite aufs eingesetzte Kapital; später Karten-Ebene (anteilig nach Punkten, Regel aus IDEA-001). *Begründung: technisch exklusiv, ohne Free-Beschneidung.*
2. **Zielpreis-Alerts** (E-Mail/Discord) + unbegrenzte Watchlist. *Begründung: Automatisierung/Zeitersparnis, klassischer Komfort-Mehrwert.*
3. **Volle Preishistorie** (Tagesgranularität > 90 Tage, sobald der Rollup greift). *Begründung: echter Datenmehrwert, der ohne Kunstgriff entsteht — Free behält die vollen letzten 90 Tage.*
4. **CSV-Exporte** (Trades, Portfolio, Steuer-Sicht mit Gebühren). *Begründung: Komfort für genau die Zielgruppe.*
5. Optional später: **gecraftete Karten mit Essence-Einstand** (IDEA-002) als Teil der Rendite-Suite.

**Darf niemals hinter eine Paywall:** FMV selbst, Accuracy-Anzeige, das öffentliche
Portfolio per Slug, Basis-P&L. Das sind Akquise, Vertrauen und der Grund, warum ChatGPT/Google
die Seite empfehlen.

**Ausdrücklich verworfen als Premium:** schnellerer Portfolio-Sync als Bezahlfeature
(klingt naheliegend, wäre aber eine künstliche Verknappung — der 24-h-/10-min-Rhythmus ist
eine technische Schutzmaßnahme am geteilten API-Key, kein Mehrwert-Feature; zudem würden
zahlende Vielnutzer das gemeinsame 200-req/min-Budget der Free-Nutzer auffressen).

---

# 6. Preisstrategie

Alle Preise **[Hypothese]** — Anker ist SorareData (Free „Rookie" / Pro 4,99 € / Star 9,99 €
pro Monat; Jahrespreis = 10 Monate). Sorion ist jünger, schmaler, aber mit zwei
Alleinstellungen (Accuracy-Transparenz, Rendite-Suite). Einstieg **unter** dem Anker:

| Stufe | Monat | Jahr | Begründung |
|---|---|---|---|
| **Free** | 0 € | 0 € | Vollwertig (s. o.). Muss so gut bleiben, dass man freiwillig dauerhaft bleibt — das ist die Wachstumsmaschine. |
| **Founding Supporter** (nur Phase 1–2, limitiert z. B. 50 Plätze) | — | 24 €/Jahr | Vor dem Pro-Launch. Preis bewusst = „2 €/Monat", exakt die interne Deckungsmarke **[Fakt: Budget-Rahmen 20.08.]**. Enthält späteres Pro auf Lebenszeit dieses Preises → belohnt Frühvertrauen, liefert den ersten harten Zahlungsbeweis. |
| **Pro** | 3,99 € | 39 € (≙ 2 Monate frei) | Unter SorareData Pro (4,99), über der Schmerzgrenze der Beliebigkeit (1,99 wirkt wertlos und deckt nach Gebühren kaum etwas). 10 Abos ≈ 40 €/Monat = Vollstack gedeckt. |
| ~~Star/Teams~~ | — | — | **Bewusst keine zweite Bezahlstufe.** Bei <100 zahlenden Nutzern verwirrt jede weitere Stufe mehr, als sie bringt. Erst ab klarer Pro-Traktion prüfen. |

Praktisch: Stripe Payment Links reichen für den Start (kein Checkout-Eigenbau); Preise in EUR;
Jahresabo aktiv bewerben (Cashflow + Commitment); 7–14 Tage Pro-Trial nach Sorare-Verknüpfung
[Hypothese: Trial hebt Link-Rate — messen].

---

# 7. Monetarisierungs-Roadmap

**Phase 1 — sofort (vor/zum Launch)** · Impact hoch / Aufwand klein / Risiko null
1. **Impressum füllen** (bestehender Launch-Blocker **[Fakt]**) — ohne Launch keine Monetarisierungsdaten.
2. **IDEA-001-Machbarkeitstest** (der im HANDOFF beschriebene 5-Minuten-Test mit Jonas' OAuth-Token). Ergebnis entscheidet, ob das Premium-Flaggschiff existiert. **Vor** jeder weiteren Premium-Planung.
3. **Sorare-API-ToS prüfen** (kommerzielle Nutzung abgeleiteter Daten + OAuth-Daten). Schriftlich festhalten. (Fehlende Info, s. Abschnitt 9.)
4. Donation-/Supporter-Link (Ko-fi o. ä.) auf Seite + Discord.
5. Analytics-Events fürs Monetarisierungs-Funnel ergänzen (Liste in Abschnitt 9).

**Phase 2 — Monat 1–2** · Impact hoch / Aufwand mittel / Risiko klein
1. **Watchlist mit Zielpreisen bauen — kostenlos** (steht ohnehin als „nächstes Feature" in der Roadmap, Tabelle existiert **[Fakt]**). Jeder gesetzte Zielpreis ist ein gemessenes Alert-Bedürfnis.
2. **Fake-Door „SORION PRO"**: Pricing-Seite + gesperrte Preview-Kacheln (Rendite-Suite, Alerts, Historie) mit „Notify me"-Waitlist. Zwei Preisvarianten anzeigen (s. Abschnitt 8).
3. **Founding-Supporter-Angebot** an Discord + Waitlist (echtes Geld, 24 €/Jahr, limitiert).
4. Retention messbar machen (Login-basiert; das tägliche Hash-Rotieren verhindert Wiederkehrer-Messung, s. Abschnitt 9).

**Phase 3 — Monat 3–6** · Impact hoch / Aufwand hoch / Risiko mittel — **nur bei bestandener Validierung** (Kriterien in Abschnitt 8)
1. **Pro v1 bauen:** Rendite-Suite auf Depot-Ebene + Zielpreis-Alerts (E-Mail via Resend, Runner als Railway-Cron) + unbegrenzte Watchlist + CSV-Export.
2. Stripe-Integration (Payment Links → `profiles.pro_until`).
3. Supabase Pro erst upgraden, wenn zahlende Nutzer die 38 €/Monat tragen oder die messbaren Signale aus dem Budget-Rahmen (20.08.) es erzwingen — Regel „kostenlose Lösungen zuerst" gilt weiter **[Fakt]**.
4. Alert-API-Budget einplanen: Alerts laufen gegen `card_prices` (eigene DB, 0 Sorare-Calls) — nur die Rendite-Suite-Syncs kosten API-Kontingent; pro Pro-Nutzer deckeln (z. B. 1 Voll-Sync/Tag).

**Phase 4 — danach**
- Rendite auf Karten-Ebene (Punkte-anteilige Zuordnung, IDEA-001-Ausbaustufe) + Essence-Einstand für Crafts (IDEA-002).
- 30d/-Langfrist-Marktanalysen aus wachsender `market_daily`-Historie als Pro-Deep-Dives.
- Neu bewerten: zweite Preisstufe, Daten-API, Bot — jeweils nur mit Traktionsbeweis.

**Explizit zurückgestellt:** zweiter Sorare-API-Key (erst wenn Pro-Syncs das Budget sprengen — die Messung vom 18.08. zeigt 148/200 req/min Auslastung **[Fakt]**), i18n, Mobile-Apps, jede B2B-Idee.

---

# 8. Validierung

Grundsatz: **kein Premium-Feature bauen, bevor Geld oder ein starker Proxy geflossen ist.**

| Experiment | Umsetzung | Kennzahl | Erfolgsschwelle [Hypothese] |
|---|---|---|---|
| **Fake-Door Pro-Seite** | /pro mit Feature-Preview + „Get notified"-Button | `pro_page_view` → `pro_interest_click` CTR | ≥ 8 % der eingeloggten Besucher klicken |
| **Waitlist** | E-Mail-Feld nach Interest-Click | Einträge absolut + Anteil verifizierter Sorare-Nutzer | ≥ 30 Einträge in 4 Wochen nach Launch |
| **Preisexperiment (statisch)** | Zwei Varianten der Pro-Seite (3,99 vs. 5,99) per URL-Parameter/50:50-JS-Split | Interest-CTR je Preis | kein Einbruch > 30 % bei 5,99 → höherer Preis tragfähig |
| **Founding Supporter (echtes Geld)** | Stripe Payment Link, 24 €/Jahr, 50 Plätze | zahlende Käufe | ≥ 10 Käufe = grünes Licht für Pro-Bau; < 5 = Stopp und neu denken |
| **Watchlist-Signal** | Watchlist frei launchen | `watchlist_add`, gesetzte Zielpreise pro Nutzer | ≥ 25 % der eingeloggten Nutzer setzen ≥ 1 Zielpreis |
| **Premium Preview / Konzept-Test** | Rendite-Suite einmalig manuell für 3–5 Discord-Power-User rechnen (nach IDEA-001-Test) und Screenshot teilen | qualitative Reaktion + „würdest du dafür zahlen?" | mind. 3 von 5 sagen unaufgefordert ja |
| **Trial→Paid (später)** | 14-Tage-Trial nach Sorare-Link | Trial-Start-Rate, Trial→Paid-Conversion | Trial→Paid ≥ 15 % |

Wichtig bei Fake-Door: nach dem Klick ehrlich sein („Pro ist in Arbeit — du stehst auf der
Liste"). Bei diesem Vertrauens-Positioning wäre alles andere Selbstbeschädigung.

---

# 9. Analytics

**Vorhanden [Fakt]:** cookiefreies Eigen-Analytics (`analytics_events`, `track`-Function,
stats.html-Dashboard) mit Events: pageview, manager_search, portfolio_view, card_detail,
market_search, elig_toggle, scarcity_switch, signup_done, login_done, discord_join,
trade_history. Herkunft/Land/Gerät. RPCs admin-gated.

**Strukturelle Lücke [Schluss]:** `visitor_hash` rotiert täglich → **Wiederkehrer und
Retention sind prinzipiell nicht messbar**, dabei ist Retention DIE Kennzahl vor jeder
Abo-Entscheidung. Lösung ohne Datenschutz-Bruch: für **eingeloggte** Nutzer eine stabile
pseudonyme ID (Hash der User-ID) mitloggen, oder Salt wochenweise statt täglich rotieren
(grobe Wochen-Retention, weiterhin cookiefrei).

**Neue Events (Whitelist in `track/index.ts` erweitern):**
`pro_page_view`, `pro_interest_click` (mit Preisvariante im path, z. B. /pro-a, /pro-b),
`waitlist_join`, `supporter_click`, `watchlist_add`, `watchlist_target_set`,
`sorare_link_done`, `sync_click`, `csv_export_click` (Fake-Door im Trade-Tab),
`history_range_switch` (wer will > 90 Tage sehen?).

**KPIs fürs Monetarisierungs-Dashboard:**
1. Wöchentlich aktive eingeloggte Nutzer (WAU) + Wochen-Retention (Kohorten)
2. Funnel: Besuch → Signup → Sorare-Link → Watchlist-Nutzung (`signup_done` → `sorare_link_done` → `watchlist_target_set`)
3. Power-User-Anteil: Nutzer mit ≥ 3 aktiven Tagen/Woche (die Pro-Zielgruppe in Zahlen)
4. Interest-CTR und Waitlist-Größe (Abschnitt 8)
5. Traffic-Quellen-Split (ChatGPT/Google/Discord/direkt) — schützt die Entscheidung, die Marktseite offen zu halten
6. Kosten-Wächter: Egress/Monat, DB-Größe, req/min-Auslastung (Budget-Regel 20.08.)

**Fehlende Informationen, die ich nicht aus dem Projekt ziehen konnte:**
- Konkrete aktuelle Zahlen (Besucher, Event-Häufigkeiten) — das Dashboard ist admin-gated; für die nächste Iteration bitte einen 30-Tage-Export bereitstellen.
- Sorare-API-ToS zur kommerziellen Nutzung (Key wurde persönlich vergeben — Bedingungen prüfen, ggf. bei Sorare anfragen; dieselbe Mail kann die offene Frage „200 req/min pro Key oder pro Account?" klären **[Fakt: offene Frage im HANDOFF]**).
- Depotwerte der bisherigen Nutzer (Proxy für Zahlungsbereitschaft) — grob aus `manager_sync`-Daten ableitbar.

---

# 10. Was wir NICHT tun sollten

1. **Marktseite oder Portfolio hinter Login/Paywall legen.** Tötet den ChatGPT/Google-Kanal und widerspricht dem erklärten Produktversprechen. (Auch kein „Login ab Seite 2".)
2. **FMV-Genauigkeit oder Datenaktualität künstlich staffeln** („Free sieht Preise 24 h verzögert"). Das ist genau die künstliche Verknappung, die das Vertrauen zerstört — und Accuracy-Transparenz ist das Markenzeichen.
3. **Werbung/Ad-Netzwerke.** Cent-Erträge, Bruch des Cookiefrei-Versprechens in der legal.html.
4. **Schnelleren Sync verkaufen.** Technische Schutzgrenze ≠ Premium-Feature; frisst zudem das geteilte API-Budget.
5. **Daten-API/Bulk-Zugang verkaufen (jetzt).** Kannibalisiert den einzigen Moat, maximales ToS-Risiko — der Lockdown vom 27.07. war die richtige Richtung.
6. **Premium komplett bauen vor der Validierung.** Die Rendite-Suite ist verführerisch — aber erst der 5-Minuten-Token-Test, dann Fake-Door/Founding-Verkäufe, dann Code.
7. **Mehrere Preisstufen oder Credits zum Start.** Komplexität ohne Gegenwert bei < 100 Zahlern.
8. **CraftLog monetarisieren.** 6 Nutzer, bewusst simpel — jede Paywall dort wäre reiner Vertrauensschaden.
9. **In Vorleistung skalieren** (zweiter API-Key, Supabase Pro, bezahlte Tools) bevor zahlende Nutzer oder harte Messsignale es verlangen — die Budget-Regel vom 20.08. gilt auch für Premium-Träume.

---

# 11. Konkrete Empfehlung

**Empfohlenes Modell:** Freemium mit einem Pro-Tier (3,99 €/Monat · 39 €/Jahr) um
Rendite-Suite + Alerts + volle Historie + Exporte — validiert über Founding-Supporter-Verkäufe
und Fake-Door, **bevor** eine Zeile Premium-Code entsteht. Brücke bis dahin: Donations.

**„Wenn ich dieses Projekt heute als Product Lead übernehmen würde, würde ich als Nächstes genau diese 5 Dinge tun:"**

1. **Impressum füllen und launchen.** Alles andere ist Theorie, solange die Seite nicht offiziell live ist und Traffic sammelt — der Launch-Blocker steht seit Wochen im HANDOFF und kostet einen Abend.
2. **Den IDEA-001-Test machen (5 Minuten):** `user.rewardedRankings` einmal mit dem eigenen OAuth-Token abfragen. Kommen Daten, existiert ein technisch exklusives Premium-Flaggschiff; kommen keine, wird Pro um Alerts + Historie herum gebaut — diese eine Abfrage steuert die gesamte Premium-Architektur.
3. **Watchlist mit Zielpreisen als kostenloses Feature bauen** (steht ohnehin als nächstes an, Tabelle existiert) und mit den neuen Events instrumentieren. Gesetzte Zielpreise sind der ehrlichste Frühindikator für Alert-Zahlungsbereitschaft.
4. **Fake-Door „/pro" + Founding Supporter (24 €/Jahr, 50 Plätze) live stellen** und über den frischen Discord ausspielen. Entscheidungsregel vorab festlegen: ≥ 10 Founding-Käufe → Pro v1 bauen; < 5 → Modell überdenken, weiter kostenlos wachsen.
5. **Retention messbar machen** (stabile pseudonyme ID für eingeloggte Nutzer bzw. Wochen-Salt) und einen 30-Tage-Analytics-Export ziehen. Ohne Wiederkehrer-Zahlen ist jede Abo-Prognose geraten — und die aktuelle Tages-Rotation macht genau diese Zahl blind.

---

*Quellen extern: SorareData-Preise ([Substack-Ankündigung](https://soraredata.substack.com/p/introducing-soraredata-memberships), [soraregoat.com](https://www.soraregoat.com/what-is-soraredata/)); Sorare-Wirtschaftslage ([SBC News, 06/2025](https://sbcnews.co.uk/featurednews/2025/06/13/sorare-close-to-the-red/)). Alle Projektfakten aus den HANDOFF-/IDEAS-/BUGS-Dateien und dem Code im Ordner `C:\craft-log`.*
