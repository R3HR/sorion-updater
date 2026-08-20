# SORION / CraftLog — Produktvision & Roadmap

> Erstellt 20.08.2026 · Rolle: Product Lead · Lebendes strategisches Dokument — bei jeder größeren Session gegen den aktuellen Stand prüfen und fortschreiben.
> Basis: vollständige Analyse von `Sorion_pro/docs/HANDOFF.md`, `docs/HANDOFF.md` (Ökosystem), `BUGS.md`/`INCIDENTS.md` (beide Ebenen), `IDEAS.md`, `MONETARISIERUNG.md` (20.08.), UI-Code (Markt/Portfolio/Profil/Stats), Edge Functions (track, get-analytics, sync-portfolio, sorare-oauth, update-pool u. a.), `lib/fmv.mjs`, Migrationen und Betriebsdaten (Budget, Egress, API-Limits).
> Kennzeichnung durchgängig: **[Fakt]** = aus dem Projekt belegt · **[Schluss]** = Ableitung · **[Hypothese]** = muss getestet werden.
> Schwesterdokument: [MONETARISIERUNG.md](MONETARISIERUNG.md) — dieses Dokument übernimmt dessen Ergebnisse und ordnet sie in die Gesamt-Roadmap ein.

---

# 1. Aktueller Produktzustand

## Was existiert?

**Sorion (sorion.pro)** — Sorare-Market-Intelligence, technisch weit gediehen **[Fakt]**:

- **Daten-Pipeline:** Eigener FMV (v3.1, Zeit-Decay, „sellable FMV"-Philosophie, genau eine Formelquelle `lib/fmv.mjs`), 3 Railway-Updater (200 Karten/Lauf, 148/200 req/min Auslastung), Harvester, Kader-Abgleich (228 Clubs, ~7.200 aktive Spieler), ~122k Karten-Zeilen (3 Scarcities × In-Season/Classic), Vollmarkt-Tagessnapshots (`market_daily`), Accuracy-Tracking gegen echte Sales.
- **Marktseite:** seit 19.08. serverseitig (~100 KB statt ~15 MB pro Besuch → ~50.000 statt ~340 Besuche/Monat im Free-Tier), Filter (Club, Land→Liga, Position), Movers, Liga-Ranking, Accuracy-Anzeige, 7d-Marktbewegungs-Chips.
- **Portfolio per Manager-Slug** (öffentlich, ohne Login): P&L inkl. 5-%-Marktgebühr (NET/GROSS), Karten-Detail mit Sparklines, Break-even-Ask; dank Spiegelung (`manager_*`-Tabellen) 0 Sorare-Requests pro Aufruf.
- **Trade History:** realisierte Trades mit Einstand, Gebühren, Trefferquote; Free-Cards sauber getrennt.
- **Manager Search / Scout**, Accounts (Signup/Login/DSGVO-Export/Löschung), **Sorare-Verknüpfung** (`link_sorare`, `sorare_verified`) — die Konto-Identität, auf der ein späteres Pro aufsetzt.
- **Eigenes cookiefreies Analytics** (`track`-Function, `analytics_events`, Admin-Dashboard `stats.html`).
- **Betrieb:** DB-Diät abgeschlossen (515 → 225 MB), Wochen-Rollup ab ~Ende Okt., Kosten ~14 €/Monat.

**CraftLog (craftlog.pro)** — bewusst simples Craft-Tool, vollständig funktionsfähig, seit 06.08. ohne Dritt-Anbieter (sorarehoops ersetzt). ~6 echte Sorare-Konten. Positionierung 22.07.: **eingefroren, kein Ausbau** **[Fakt]**.

**Tradingbot** — reine Zukunftsidee, null Code **[Fakt]**.

## Was funktioniert?

Die gesamte Kern-Kette läuft und ist live verifiziert **[Fakt]**: Pipeline → FMV → Marktseite → Portfolio → Trade History. Die teuersten Betriebsprobleme (Egress-Explosion, DB-Limit, API-Durchsatz) wurden in den letzten drei Wochen systematisch gelöst. Sicherheitslage solide: SEC-001–005 geschlossen, `price_history`-Bulk-Abgriff dicht, Analytics-RPCs admin-gated.

## Was ist unfertig?

1. **Der Launch selbst.** Impressum-Platzhalter = Launch-Blocker, steht seit Wochen offen **[Fakt]**. Das Produkt ist faktisch im Pre-Launch — alles Folgende hängt daran.
2. **Retention ist prinzipiell nicht messbar:** `visitor_hash` rotiert täglich **[Fakt, track/index.ts]** — Wiederkehrer sind unsichtbar. Für eingeloggte Nutzer wird keine stabile ID geloggt.
3. **Messfehler im Tracking (neu gefunden, 20.08.):** `portfolio.html` sendet `track('trade_history')`, aber `trade_history` fehlt in der Event-Whitelist der `track`-Function — die Function stuft unbekannte Events still auf `pageview` herab (`EVENTS.includes(body.event) ? body.event : 'pageview'`). **Die Trade-History-Nutzung wird also seit dem 01.08. als Pageview mitgezählt statt als Feature-Event.** Umgekehrt steht `market_search` in der Whitelist, wird aber von keiner UI gesendet. **[Fakt, Code-Abgleich]**
4. **Watchlist:** Tabelle existiert, UI fehlt komplett — im HANDOFF als „nächstes Feature" markiert **[Fakt]**.
5. **Launch-Kosmetik:** OG-Tags/Favicon, Erstbesucher-Erklärtext, Mobile-Durchgang **[Fakt, Roadmap-Tabelle HANDOFF]**.
6. **Offene Härtung:** BUG-008 (Parameter-Injection-Whitelists) offen; BUG-009 halb offen (`sorare-proxy` rechnet noch v1-FMV); OAuth-App liegt auf unbekanntem Account; `SORARE_APIKEY` fehlt als Supabase-Secret **[Fakt]**.
7. **Sprachen gemischt** (Markt/Portfolio Englisch, Profil Deutsch) — bewusst zurückgestellt **[Fakt]**.

## Wichtigste aktuelle Nutzerflows

1. Besucher → Marktseite → Spieler suchen/filtern → Karten-Detail (FMV, Sparkline, Accuracy).
2. Besucher → Portfolio per Slug (auch fremde) → P&L → Trade History → Karten-Detail.
3. Scout: Manager Search → Kompakt-Bilanz → Voll-Portfolio.
4. Registrierter Nutzer: Signup → Profil → Sorare-Verknüpfung → eigener Sync-Knopf.
5. CraftLog: Login → Craft Tracker / Craft Helper (separater, kleiner Kreis).

**Was wir über die tatsächliche Nutzung wissen: fast nichts Belastbares.** Das Analytics läuft erst seit 30.07., das Dashboard ist admin-gated, konkrete Zahlen (Besucher, Event-Häufigkeiten, Quellen-Split) liegen dieser Analyse nicht vor — **Das wissen wir noch nicht.** Herausfinden: 30-Tage-Export aus `stats.html` ziehen (steht bereits als offene Info im Monetarisierungsbericht). Bekannt ist nur: CraftLog ~6 Konten, Discord seit 18.08., Traffic-These „Google/ChatGPT empfehlen die Seite" stammt von Jonas und ist unquantifiziert **[Fakt/Hypothese]**.

## Größte technische und produktseitige Risiken

| Risiko | Art | Einordnung |
|---|---|---|
| **Pre-Launch-Dauerschleife:** Produkt wird immer besser, aber niemand darf es offiziell sehen | Produkt | Größtes Risiko überhaupt. Jede weitere Woche Feature-Arbeit vor dem Launch ist Optimierung ohne Feedback. **[Schluss]** |
| **Blindflug bei Retention** (rotierender Hash, fehlende stabile ID, Whitelist-Lücke) | Produkt/Messung | Ohne Wiederkehrer-Zahlen ist jede Priorisierung und jede Abo-Entscheidung geraten. **[Fakt/Schluss]** |
| **Sorare-Plattformrisiko:** restrukturiert, Profitabilitätsziel Ende 2026, UKGC-Prozess 06/2027; API ohne SLA (SorareData-Todesursache) | Extern | Nicht akut, aber real. Gegenmittel: eigene Historie, winzige Kostenbasis, Sorare-Inside-Kontakt. Re-Check spätestens Q1 2027. **[Fakt, Memory sorare-lage]** |
| **Ein geteilter API-Key (200 req/min):** 148/200 bereits belegt | Technisch | Jedes neue API-lastige Feature (Rendite-Suite-Syncs!) muss gegen dieses Budget gerechnet werden. **[Fakt]** |
| **Ein-Personen-Projekt + handgepflegte Deploy-Wege** (UI dreifach synchron pushen, Secrets nur in Railway) | Technisch | Bekannte Fehlerquelle (BUG-013, CRON_SECRET-Lektion). Tragbar, aber diszipliniert halten. **[Fakt]** |
| **Free-Tier-Grenzen** (500 MB, 5 GB Egress) | Technisch | Nach Diät + Server-Umbau entschärft; Kosten-Wächter-KPIs weiterführen. **[Fakt]** |

---

# 2. Produktverständnis

**Welches Problem löst das Produkt?** Sorare zeigt Floor-Preise, aber keinen belastbaren fairen Wert, keine ehrliche Portfolio-P&L inkl. Gebühren und keine realisierte Trade-Bilanz. Sorion beantwortet die drei Kernfragen eines Sorare-Traders: *Was ist die Karte wirklich wert? Wie steht mein Depot da? Habe ich mit meinen Trades tatsächlich Geld verdient?* CraftLog beantwortet: *Lohnt sich dieser Craft?* **[Schluss]**

**Für wen?**

1. **Aktive Trader** — brauchen FMV, Movers, Break-even; höchste Zahlungsbereitschaft.
2. **Sammler/Manager** — schauen v. a. aufs eigene Depot.
3. **Scouts/Neugierige** — fremde Portfolios; Reichweite, kaum Zahlungsbereitschaft.
4. **Crafter** — kleine, loyale CraftLog-Nische.

**Warum sollte jemand regelmäßig wiederkommen?** Weil sich Preise täglich ändern und der Nutzer Geld im Spiel hat. Der natürliche Wiederkehr-Anlass ist: *„Was ist mein Depot heute wert, und haben sich meine Beobachtungsspieler bewegt?"* — Ersteres bedient das Portfolio bereits, Letzteres ist genau die fehlende Watchlist. **[Schluss]**

**Aktueller Core Value:** Vertrauenswürdige, überprüfbare Preise (FMV mit öffentlich sichtbarer Accuracy — das Differenzierungsmerkmal) plus die ehrlichste P&L-Rechnung im Sorare-Umfeld (Gebühren, Free-Cards, Break-even). Die Marktlücke ist real: **SorareData, das dominante Community-Tool, wurde Mai 2025 eingestellt; Sorare hat weiterhin ~378k aktive Nutzer** **[Fakt, Memory sorare-lage]**.

---

# 3. Produktvision

> **Sorion ist der vertrauenswürdige Marktstandard für Sorare-Preise: das Tool, das jeder Sorare-Manager öffnet, bevor er kauft oder verkauft — mit nachprüfbar ehrlichen Preisen, der ehrlichsten Depot-Rechnung und einer Preishistorie, die niemand nachbauen kann.**

**Was soll das Produkt in 12 Monaten sein?**

- Die selbstverständliche Antwort auf „was ist diese Karte wert?" — verlinkt in Discords, empfohlen von Google/ChatGPT, mit öffentlich belegter FMV-Genauigkeit.
- Ein Produkt mit messbarem, wiederkehrendem Kern: Nutzer mit Depot und Watchlist, die mehrmals pro Woche zurückkommen.
- Selbsttragend: laufende Kosten (~14–38 €/Monat) durch ~10–20 zahlende Pro-Nutzer gedeckt — validiert, nicht erhofft.
- Besitzer eines wachsenden, nicht nachbaubaren Datenbestands (tägliche FMV-Historie seit 07/2026).

**Was soll es ausdrücklich NICHT sein?**

- Kein Tradingbot und keine Trade-Empfehlungsmaschine (Vertrauensrisiko, ToS-Risiko, null Code — nicht Teil der 12-Monats-Planung).
- Kein Daten-API-/B2B-Anbieter (kannibalisiert den einzigen Moat).
- Kein werbefinanziertes Portal (bricht das Cookiefrei-/Vertrauens-Positioning).
- Kein Feature-Bauchladen à la SorareData in Spätphase — Sorion bleibt schmal: Markt, Depot, Rendite.
- Kein Multi-Sport-/Multi-Plattform-Tool und keine App — Web, Fußball, Sorare.
- CraftLog bleibt eingefroren und kostenlos.

---

# 4. North Star

**North-Star-Metrik: Wöchentlich wiederkehrende Kern-Nutzer** — Anzahl identifizierter Nutzer (eingeloggt oder wochenstabiler Hash), die in einer Woche an ≥ 2 verschiedenen Tagen eine Kern-Aktion ausführen (`portfolio_view`, `card_detail`, `trade_history`, `watchlist_*`, `manager_search`).

**Warum diese Metrik?**

- Sie misst genau das, was das Produkt behauptet: wiederkehrenden Nutzen für Leute mit Geld im Spiel. Ein Tool, das man einmal ansieht und vergisst, ist gescheitert — egal wie viele Erstbesucher kommen.
- Sie ist der Frühindikator für alles Weitere: Zahlungsbereitschaft (Pro-Zielgruppe = Power-User), Empfehlungen, Datenwert.
- Sie ist gegen Vanity resistent: ein viraler ChatGPT-Link erzeugt Pageviews, aber keine wiederkehrenden Kern-Nutzer.
- Reine Umsatz- oder Traffic-Metriken wären in der Pre-Launch-/Validierungsphase verfrüht bzw. irreführend. **[Schluss]**

**Was würde eine Verbesserung bedeuten?** Mehr Menschen haben Sorion in ihre wöchentliche Sorare-Routine eingebaut → der Core Loop (Depot prüfen / Watchlist prüfen) trägt → Pro-Potenzial und Weiterempfehlung wachsen mit.

**Welche Events müssen dafür gemessen werden?**

1. **Stabile pseudonyme ID für eingeloggte Nutzer** (Hash der User-ID) — heute nicht vorhanden; ohne sie existiert die Metrik nicht.
2. Wochen-Salt statt Tages-Salt für anonyme Besucher (grobe Wochen-Retention, weiterhin cookiefrei) — Entscheidung dokumentieren, DSGVO-Text ggf. anpassen.
3. Whitelist-Fix: `trade_history` aufnehmen (wird heute als `pageview` verbucht **[Fakt]**), plus neue Events `watchlist_add`, `watchlist_target_set`, `sorare_link_done`, `sync_click`.
4. Kohorten-Auswertung (Signup-Woche → Aktivität Folgewochen) in `stats.html` oder als SQL-Report.

---

# 5. Strategische Ziele (6–12 Monate)

Bewusst nur vier. Nutzerwachstum ist Folge von Ziel 1+2, nicht eigenes Ziel; Monetarisierung ist als *Validierung* enthalten, nicht als Umsatzziel — beides gemäß der Lage: Pre-Launch, unbekannte Nutzung, winzige Kosten.

| # | Ziel | Warum wichtig? | Messgröße | Zielwert | Zeitraum |
|---|---|---|---|---|---|
| 1 | **Launchen und den Ist-Zustand messbar machen** — Seite offiziell live, Retention/Feature-Nutzung/Quellen sauber instrumentiert | Ohne Launch kein Feedback, ohne Messung keine Priorisierung. Beides blockiert heute jede fundierte Entscheidung. **[Schluss]** | Launch vollzogen; stabile Nutzer-ID live; 30-Tage-Baseline-Report existiert | Launch ≤ 2 Wochen; Baseline-Report ≤ 6 Wochen | 30–45 Tage |
| 2 | **Einen messbaren Wiederkehr-Loop etablieren:** Nutzer sollen einen Grund haben, ≥ 2×/Woche zurückzukommen (Depot + Watchlist mit Zielpreisen) | Retention ist die Existenzfrage des Produkts und die Vorbedingung jeder Abo-Entscheidung. Die Watchlist ist erklärte Kaufabsicht und der ehrlichste Alert-/Pro-Frühindikator. **[Schluss]** | North Star (wöchentl. wiederkehrende Kern-Nutzer); W1-Retention eingeloggter Nutzer; `watchlist_target_set` pro Nutzer | **Baseline zuerst ermitteln**; danach konkrete Ziele setzen | 90–180 Tage |
| 3 | **Zahlungsbereitschaft validieren, bevor Premium-Code entsteht** (Fake-Door /pro, Waitlist, Founding Supporter 24 €/Jahr, IDEA-001-Test) | Kostendeckung (~38 €/Monat) ist das erklärte Ziel; SorareData starb u. a. an unvalidierter Freemium-Konversion. Entscheidungsregel steht: ≥ 10 Founding-Käufe → Pro bauen, < 5 → stoppen. **[Fakt, MONETARISIERUNG]** | Founding-Käufe; Waitlist-Größe; Interest-CTR | ≥ 10 Käufe (Go) / < 5 (Stop); ≥ 30 Waitlist in 4 Wochen [Hypothese] | 60–150 Tage |
| 4 | **Vertrauens- und Datenvorsprung ausbauen bei eiserner Kostendisziplin** — Accuracy sichtbar halten, Historie wachsen lassen, Sicherheits-Reste schließen, im Budget-Rahmen bleiben | Der einzige Moat ist die eigene Historie + belegte Genauigkeit; die einzige Todesart bei 14 €/Monat Kosten ist Selbstverschulden (Leak, Datenverlust, Kostenexplosion). **[Schluss]** | BUG-008 geschlossen; sorare-proxy-FMV bereinigt; DB < 400 MB; req/min < 180; Accuracy-Anzeige je Zelle ≥ 10 Samples wachsend | alle vier erfüllt | laufend, Härtung ≤ 60 Tage |

---

# 6. Prioritäten

**P0 — unbedingt (blockiert alles andere):**
Impressum + Launch · Tracking-Fixes (Whitelist `trade_history`, stabile Nutzer-ID, Wochen-Salt-Entscheidung) · 30-Tage-Analytics-Baseline-Export.

**P1 — sehr wichtig (direkt danach):**
Watchlist mit Zielpreisen (kostenlos, voll instrumentiert) · IDEA-001-5-Minuten-Test (steuert die gesamte Premium-Architektur) · Launch-Kosmetik (OG-Tags, Favicon, Erklärtext, Mobile-Durchgang) · Sorare-API-ToS-Klärung (über den Sorare-Inside-Kontakt) **[Fakt, Memory]**.

**P2 — sinnvoll:**
Fake-Door /pro + Waitlist + Founding Supporter (nach 2–4 Wochen echter Launch-Daten) · BUG-008-Härtung · sorare-proxy auf kanonischen FMV oder dokumentierter Richtwert (Produktentscheidung Jonas, steht seit 27.07. aus) · 30d-Marktbewegung (History reicht seit ~20.08.) · Supporter-/Donation-Link.

**P3 — später (nur nach Validierung bzw. Bedarf):**
Pro v1 (Rendite-Suite je nach IDEA-001-Ausgang, Alerts, volle Historie, CSV-Export) · Stripe · Rendite auf Karten-Ebene + Essence-Einstand (IDEA-002) · neue Sorare-OAuth-App auf Jonas' Account · Supabase Pro / zweiter API-Key (nur bei messbaren Signalen).

**P4 — aktuell nicht bauen:**
Tradingbot · Daten-API/B2B · Werbung · i18n · Mobile-Apps · zweite Preisstufe · CraftLog-Ausbau · weitere Markt-Analyse-Features ohne Retention-Beleg · schnellerer Sync als Bezahlfeature.

**Die wichtigsten Entscheidungen dahinter:**

1. **Launch vor jedem Feature.** Das Produkt ist seit Wochen feature-fertig genug; der Blocker ist ein Impressum. Jede Stunde Feature-Arbeit vor dem Launch optimiert im Blindflug. **[Schluss]**
2. **Messen vor Bauen.** Drei kleine Eingriffe (Whitelist, stabile ID, Wochen-Salt) machen aus einem blinden Analytics ein entscheidungsfähiges — bevor irgendeine Roadmap-Wette platziert wird. Die Whitelist-Lücke zeigt: sogar die vorhandene Messung braucht erst einen Review.
3. **Watchlist ist das einzige größere neue Feature der nächsten zwei Monate** — weil sie gleichzeitig Core-Loop-Baustein (Retention), Messinstrument (Zielpreise = Kaufabsicht) und Premium-Vorstufe (Alerts) ist. Kein anderes Kandidaten-Feature zahlt auf drei Ziele gleichzeitig ein.
4. **Premium bleibt Validierungs-, nicht Bauprojekt** — die Entscheidungsregel aus MONETARISIERUNG.md gilt unverändert.

---

# 7. Roadmap — nächste 2 Wochen

| # | Maßnahme | Ziel | Warum jetzt? | Erfolgskriterium |
|---|---|---|---|---|
| 1 | **Impressum füllen (beide legal.html) + offizieller Launch** | Ziel 1 | Einziger Launch-Blocker, kostet einen Abend; Saisonstart-Fenster läuft **[Fakt]** | sorion.pro + craftlog.pro rechtlich sauber live; Launch-Post im Discord |
| 2 | **Tracking-Fix-Paket:** `trade_history` in die Whitelist; stabile pseudonyme ID für eingeloggte Nutzer; Salt-Rotation Tag→Woche entscheiden + umsetzen; ungenutztes `market_search` klären (senden oder streichen) | Ziel 1/2 | Jeder Tag ohne Fix produziert weitere unbrauchbare Daten; trivialer Aufwand (Function + Snippet) | Events kommen korrekt an (Live-Test); Wiederkehrer im Dashboard erstmals sichtbar; legal.html-Text geprüft |
| 3 | **30-Tage-Analytics-Export ziehen** (Besucher, Events, Quellen, Geräte) und als Baseline in `docs/` ablegen | Ziel 1 | Die Roadmap ab Woche 4 braucht diese Zahlen; auch der Monetarisierungsbericht wartet darauf **[Fakt]** | Baseline-Datei existiert; „Google/ChatGPT-Traffic"-These erstmals quantifiziert |
| 4 | **IDEA-001-Machbarkeitstest** (5 Min: `user.rewardedRankings` mit Jonas' OAuth-Token) | Ziel 3 | Steuert die gesamte Premium-Architektur; buchstäblich eine Abfrage **[Fakt, IDEAS]** | Ergebnis (Daten ja/nein + Historien-Tiefe) in IDEAS.md dokumentiert |
| 5 | **Launch-Kosmetik-Minimum:** OG-Tags + Favicon + 2-Sätze-Erklärtext für Erstbesucher; Mobile-Schnelldurchgang | Ziel 1 | Erste Links werden nach dem Launch geteilt — der erste Eindruck entscheidet über Wiederkehr | Link-Preview in Discord/X sieht korrekt aus; Marktseite + Portfolio auf Mobile bedienbar |

Bewusst NICHT in diesen zwei Wochen: Watchlist-Bau (erst Messung live), Fake-Door (erst echte Launch-Daten), jede neue Analysefunktion.

---

# 8. Roadmap — 1 bis 2 Monate

Reihenfolge = Abhängigkeitsreihenfolge:

1. **Watchlist mit Zielpreisen — kostenlos, voll instrumentiert** (`watchlist_add`, `watchlist_target_set`). Tabelle existiert; UI in Markt- und Portfolio-Seite integrieren (Stern an Spielerzeile + eigene Ansicht). Das ist der Core-Loop-Test: Kommen Watchlist-Nutzer messbar häufiger wieder?
2. **Erste Baseline-Auswertung + Zielwerte setzen:** Aus 4 Wochen Launch-Daten die „Baseline zuerst ermitteln"-Platzhalter in Abschnitt 5 durch Zahlen ersetzen (W1-Retention, Power-User-Anteil, Quellen-Split).
3. **Fake-Door „/pro" + Waitlist + Founding Supporter** (24 €/Jahr, 50 Plätze, Stripe Payment Link) über Discord ausspielen — mit vorab festgeschriebener Entscheidungsregel (≥ 10 Go / < 5 Stop). Events: `pro_page_view`, `pro_interest_click`, `waitlist_join`, `supporter_click`.
4. **Härtungs-Rest:** BUG-008 (Whitelist-Validierung + GraphQL-Variablen), sorare-proxy-Entscheidung (BUG-009), `SORARE_APIKEY` als Supabase-Secret (schnellere Portfolio-Erstladung — wichtig, sobald Promotion-Traffic kommt).
5. **30d-Marktbewegung** ergänzen (Snapshots reichen jetzt) — kleiner Aufwand, stärkt den Markt-Kern; danach aber Feature-Stopp auf der Marktseite bis Retention-Daten vorliegen.
6. **Wiedervorlage 01.09.:** `railway-rosters.toml` auf wöchentlich stellen **[Fakt, HANDOFF]**.

---

# 9. Roadmap — 3 bis 6 Monate

Meilensteine, jeweils mit Eintritts-Bedingung:

- **M1 — Retention-Urteil (Monat 3):** Baseline zeigt, ob ein Wiederkehr-Kern existiert. *Wenn W1-Retention eingeloggter Nutzer nahe null bleibt und Watchlist kaum genutzt wird:* kein neues Feature, sondern Problem-Recherche (5–10 Nutzerinterviews im Discord: Was fehlt, damit Sorion Teil der Wochenroutine wird?). Das wäre ein fundamentales Produktproblem, das vor allem anderen gelöst werden muss.
- **M2 — Pro-Entscheidung (Monat 3–4):** Founding-Regel greift. **Bei Go:** Pro v1 bauen — Inhalt je nach IDEA-001-Ausgang: mit Rendite-Suite (Depot-Ebene) oder ohne (dann Alerts + volle Historie + CSV als Kern). Alerts laufen gegen die eigene DB (0 Sorare-Calls), Rendite-Syncs pro Nutzer deckeln (API-Budget!). Stripe → `profiles.pro_until`. **Bei Stop:** weiter kostenlos wachsen, Donations als Brücke, Neubewertung nach 3 Monaten.
- **M3 — Vertrauens-Ausbau (laufend):** Accuracy-Abdeckung wächst (mehr Zellen ≥ 10 Samples); Preishistorie überschreitet 6 Monate Tiefe — ab ~Ende Oktober entsteht durch den Rollup der natürliche Free (90 Tage) / Pro (voll)-Schnitt ohne Kunstgriff **[Fakt]**.
- **M4 — Infrastruktur nur nach Signal:** Supabase Pro bzw. zweiter API-Key ausschließlich bei messbaren Grenzen (Budget-Regel 20.08.) oder zahlenden Nutzern, die es tragen.
- **Sorare-ToS-Klärung abgeschlossen** (spätestens vor der ersten Pro-Abbuchung) — Weg über den Sorare-Inside-Kontakt **[Fakt, Memory]**.

---

# 10. Roadmap — 6 bis 12 Monate

Nur gültig, wenn M1/M2 positiv ausfallen — sonst gilt: Kern verbessern statt erweitern.

- **Pro vertiefen statt verbreitern:** Rendite auf Karten-Ebene (Punkte-anteilige Zuordnung, IDEA-001-Ausbaustufe), Essence-Einstand für Crafts (IDEA-002, Datenbrücke CraftLog→Sorion), Depot-Verlauf über Zeit, Steuer-/CSV-Sicht.
- **Markt-Deep-Dives aus wachsender `market_daily`-Historie** (Saisonverläufe, Liga-Trends) — als Pro-Inhalte, sofern Pro trägt.
- **Wachstum über den belegten Kanal:** Wenn der Quellen-Split zeigt, dass Google/ChatGPT tragen → gezielt indexierbare Inhalte (z. B. saubere Spieler-Preisseiten) prüfen. Erst dann, nicht vorher. [Hypothese: der Kanal existiert — durch Baseline zu belegen]
- **Neu bewerten (frühestens hier):** zweite Preisstufe, i18n, Kooperation/Integration mit Sorare Inside (komplementäre Schwerpunkte: Inside = Lineups/Gallery, Sorion = Markt/FMV/Portfolio **[Fakt, Memory]**).
- **Q1 2027: Sorare-Gesundheitscheck wiederholen** (UKGC-Prozess 06/2027 als binäres Risiko) — vor jedem größeren Invest **[Fakt, Memory]**.
- **Weiterhin nicht:** Tradingbot (auch bei Pro-Erfolg eigenes, separates Validierungsprojekt), Daten-API, Multi-Sport.

---

# 11. Feature Backlog

## MUST BUILD

| Feature | Problem | Nutzerwert | Strategischer Beitrag | Aufwand | Priorität |
|---|---|---|---|---|---|
| Impressum + Launch | Produkt offiziell unsichtbar/abmahnbar | — (Enabler) | Blockiert alles | Minimal | P0 |
| Tracking-Fix-Paket (Whitelist, stabile ID, Wochen-Salt) | Retention & Feature-Nutzung unsichtbar/verfälscht | — (Enabler) | Grundlage jeder Entscheidung | Klein | P0 |
| Watchlist mit Zielpreisen (frei) | Kein Wiederkehr-Anlass außer eigenem Depot; Kaufabsicht unmessbar | Hoch | Retention-Loop + Premium-Frühindikator + Alert-Vorstufe | Mittel | P1 |
| IDEA-001-Test (nur der Test!) | Premium-Flaggschiff ungeklärt | — (Erkenntnis) | Steuert gesamte Pro-Architektur | Minimal | P1 |
| Launch-Kosmetik (OG/Favicon/Erklärtext/Mobile) | Erster Eindruck verspielt Erstbesucher | Mittel | Konversion Erstbesuch→Wiederkehr | Klein | P1 |

## SHOULD BUILD

| Feature | Problem | Nutzerwert | Strategischer Beitrag | Aufwand | Priorität |
|---|---|---|---|---|---|
| Fake-Door /pro + Founding Supporter | Zahlungsbereitschaft unbewiesen | Niedrig (direkt) | Go/No-Go für Pro; erster Umsatz | Klein | P2 |
| BUG-008-Härtung + sorare-proxy-Entscheidung | Query-Injection-Fläche; FMV-Doppelquelle | — (Qualität) | Vertrauen/Stabilität | Klein–mittel | P2 |
| 30d-Marktbewegung | 7d zu kurz für Trend-Einordnung | Mittel | Markt-Kern stärken | Klein | P2 |
| Supporter-/Donation-Link | Keine Zahlungsoption für Wohlgesinnte | Niedrig | Erster Zahlungs-Datenpunkt | Minimal | P2 |
| Pro v1 (nach Validierung; Umfang je IDEA-001) | Power-Usern fehlen Alerts/Historie/Rendite | Hoch (Segment) | Kostendeckung, Geschäftsmodell-Beweis | Hoch | P3 |
| SORARE_APIKEY als Supabase-Secret | Langsame Portfolio-Erstladung | Mittel | Erstbesucher-Erlebnis vor Promotion | Minimal | P2 |

## DO NOT BUILD YET

| Feature | Warum nicht | Neubewertung wenn |
|---|---|---|
| Tradingbot | Null Code, ToS-/Vertrauens-/Betriebsrisiko maximal; ein Fehltrade vernichtet das Vertrauens-Positioning | Pro etabliert + ToS geklärt + eigenes Validierungsprojekt |
| Daten-API / B2B | Kannibalisiert den einzigen Moat (Historie); Abnehmer = Konkurrenten; ToS-Risiko | Sorion selbst etabliert |
| Werbung | Cent-Erträge, bricht Cookiefrei-Versprechen der legal.html | Praktisch nie |
| i18n / Mobile-Apps | Aufwand vor Product-Market-Fit; Web-Englisch reicht der Zielgruppe | Retention belegt + Nachfrage messbar |
| Zweite Preisstufe / Credits | Komplexität ohne Gegenwert bei < 100 Zahlern | Klare Pro-Traktion |
| Weitere Markt-Analyse-Features (nach 30d-Chip) | Ohne Retention-Beleg ist unklar, ob Analyse-Tiefe überhaupt der Hebel ist | Baseline zeigt Analyse-Nutzung als Wiederkehr-Treiber |
| CraftLog-Ausbau | 6 Nutzer, bewusst simpel positioniert (Entscheidung 22.07.) | Nur auf expliziten Nutzer-Druck |
| Schnellerer Sync als Bezahlfeature | Künstliche Verknappung einer Schutzmaßnahme; frisst geteiltes API-Budget | Nie in dieser Form |

---

# 12. Messplan

**Vorhanden [Fakt]:** cookiefreies Eigen-Analytics (`analytics_events`, `track`, `stats.html`) mit pageview, manager_search, portfolio_view, card_detail, discord_join, market_search (ungenutzt), elig_toggle, scarcity_switch, signup_done, login_done; Herkunft/Land/Gerät; 400 Tage Aufbewahrung.

**Kern-KPIs (wöchentlich ansehen):**

| KPI | Quelle | Status |
|---|---|---|
| Besucher & Pageviews je Site | vorhanden | ✅ läuft |
| **Wiederkehrende Nutzer / W1-Retention (Kohorten)** | stabile ID + Wochen-Salt | 🔴 baubar erst nach Tracking-Fix |
| Aktive eingeloggte Nutzer (WAU) | stabile ID | 🔴 wie oben |
| **North Star: wöchentl. wiederkehrende Kern-Nutzer** | Kern-Events × stabile ID | 🔴 wie oben |
| Feature-Nutzung (portfolio_view, card_detail, **trade_history nach Whitelist-Fix**, manager_search, watchlist_*) | Events | 🟡 teilweise |
| Core Loop: Anteil Nutzer mit ≥ 2 aktiven Tagen/Woche; Watchlist-Nutzer vs. Nicht-Watchlist-Retention | Events + ID | 🔴 nach Watchlist |
| Funnel: Besuch → signup_done → sorare_link_done → watchlist_target_set | Events (2 neue) | 🔴 Events ergänzen |
| Conversion/Monetarisierung: pro_page_view → pro_interest_click → waitlist_join; Founding-Käufe; später Trial→Paid | Events (neu) + Stripe | 🔴 mit Fake-Door |
| Traffic-Quellen-Split (ChatGPT/Google/Discord/direkt) | referrer_host | ✅ läuft — Baseline-Auswertung fehlt |
| Kosten-Wächter: Egress, DB-MB, req/min | Supabase/Railway | ✅ Praxis etabliert |

**Event-Whitelist-Erweiterung (`track/index.ts`):** `trade_history` (Fix), `watchlist_add`, `watchlist_target_set`, `sorare_link_done`, `sync_click`, `pro_page_view`, `pro_interest_click`, `waitlist_join`, `supporter_click`, `csv_export_click`, `history_range_switch`. Regel: **jedes neue Feature shipped nur mit seinen Events** — die Trade-History-Lücke war vermeidbar.

---

# 13. Risiken und falsche Abzweigungen

1. **Weiterbauen statt launchen** — die eingespielte Bau-Routine ist selbst das Risiko: Das Projekt hat exzellente Bau-Disziplin und (bis 30.07.) gar keine Nutzungs-Disziplin. Der Launch-Blocker ist trivial und drei Wochen alt.
2. **Features aus Faszination statt aus Daten** — konkret droht das bei Markt-Analyse-Tiefe (Charts, Indizes, Deep-Dives): technisch reizvoll, Retention-Wirkung unbelegt. Beispiel-Muster aus der Projektgeschichte: drei Anläufe für den Markt-Chip, bevor die richtige Datenbasis stand.
3. **Zu früh monetarisieren** — Paywall-Reflexe vor Retention-Beleg würden den einzigen Akquisekanal (offene Marktseite/Portfolio) beschädigen. Die MONETARISIERUNG.md-Leitplanken (unantastbare Free-Liste, Validierungs-Schwellen) gelten; dieses Dokument ändert daran nichts.
4. **Zu spät Zahlungsbereitschaft testen** — das Gegenteil ist genauso falsch: Monate kostenlos wachsen ohne einen einzigen Preis-Datenpunkt. Der Founding-Test ist bewusst früh (Monat 2) angesetzt, weil er billig ist und die 6-Monats-Roadmap steuert.
5. **In Vorleistung skalieren** (zweiter Key, Supabase Pro, bezahlte Tools) — Budget-Regel 20.08. gilt: Signale zuerst.
6. **Vertrauens-Selbstbeschädigung** — unehrliche Fake-Door-Kommunikation, künstliche Free-Verschlechterung oder ein Datenleck würden das Kern-Asset (nachprüfbare Ehrlichkeit) treffen. Härtungs-Reste deshalb zeitnah schließen.
7. **Sorare-Abhängigkeit verdrängen** — API ohne SLA bleibt DAS externe Risiko (SorareData-Lektion). Defensive Architektur ist vorhanden (Caching, eigene Historie, Spiegelung); Sorare-Inside-Kanal pflegen, Q1-2027-Re-Check nicht verpassen.
8. **Der Bot als Sirene** — der „geplante Tradingbot" steht im Ökosystem-Titel und lockt als ultimatives Feature. Er ist die teuerste denkbare falsche Abzweigung der nächsten 12 Monate.

---

# 14. Die nächsten 5 Schritte

1. **Impressum füllen und launchen** (beide legal.html, Launch-Post im Discord).
Warum: Einziger Blocker zwischen „fertigem Produkt" und echten Nutzungsdaten; jede weitere Woche Pre-Launch entwertet die bereits laufende Messung.
Erfolgskriterium: sorion.pro + craftlog.pro rechtlich vollständig; Launch kommuniziert; erste Woche Post-Launch-Daten im Dashboard.

2. **Tracking reparieren und Retention messbar machen** (`trade_history` in die Whitelist, stabile pseudonyme ID für eingeloggte Nutzer, Salt-Rotation auf Woche umstellen, legal.html-Text prüfen).
Warum: Die North-Star-Metrik existiert sonst nicht; aktuell wird Trade-History-Nutzung als Pageview verbucht und jeder Wiederkehrer ist unsichtbar.
Erfolgskriterium: Live-Test zeigt korrekte Events; Dashboard weist erstmals Wiederkehrer/Kohorten aus.

3. **IDEA-001-Test durchführen** (`user.rewardedRankings` einmal mit Jonas' OAuth-Token; Ergebnis + Historien-Tiefe in IDEAS.md notieren).
Warum: Eine 5-Minuten-Abfrage entscheidet, ob das technisch exklusive Premium-Flaggschiff existiert — vor jeder weiteren Pro-Planung.
Erfolgskriterium: Dokumentiertes Ja/Nein inkl. Datenprobe; Pro-Scope in diesem Dokument entsprechend konkretisiert.

4. **30-Tage-Analytics-Baseline ziehen und auswerten** (Besucher, Events, Quellen-Split, Geräte; als Datei in `docs/` neben dieses Dokument).
Warum: Alle „Baseline zuerst ermitteln"-Zielwerte in Abschnitt 5 und die Google/ChatGPT-These hängen daran.
Erfolgskriterium: Baseline-Dokument existiert; Abschnitt 5 dieser Roadmap mit ersten Zahlen aktualisiert.

5. **Watchlist mit Zielpreisen bauen — kostenlos und voll instrumentiert** (Stern in Markt-/Portfolio-Tabelle, eigene Ansicht, Events `watchlist_add`/`watchlist_target_set`).
Warum: Der fehlende Wiederkehr-Anlass neben dem eigenen Depot; zugleich der ehrlichste Frühindikator für Alert-/Pro-Nachfrage. Tabelle existiert bereits.
Erfolgskriterium: Feature live; nach 4 Wochen belegt die Messung, ob Watchlist-Nutzer häufiger wiederkehren (Input für M1).

---

# 15. Product Lead Urteil

**Wo steht das Projekt aktuell?** Technisch deutlich weiter als produktseitig. Die Ingenieursarbeit der letzten sechs Wochen ist beeindruckend diszipliniert — Egress um Faktor 150 gesenkt, DB halbiert, Sicherheitslücken systematisch geschlossen, eine FMV-Methodik mit öffentlichem Accuracy-Beleg. Aber: Das Produkt ist nicht gelauncht, hat keine messbaren Wiederkehrer und keinen einzigen validierten Zahlungs-Datenpunkt. Sorion ist ein sehr gutes Produkt in einem Zustand, in dem niemand es offiziell benutzen darf und niemand nachweisen kann, dass es benutzt wird. Gleichzeitig ist das Marktfenster ungewöhnlich günstig: Der dominante Wettbewerber (SorareData) ist seit Mai 2025 tot, Sorares Kern-Metriken wachsen wieder, und Sorions Kostenbasis (~14 €/Monat) erlaubt beliebig langes Durchhalten.

**Der größte Hebel:** Launchen und innerhalb von 30 Tagen wissen, ob Menschen wiederkommen. Alles, was das Projekt strategisch entscheiden muss — Pro bauen oder nicht, Analyse-Tiefe oder Depot-Tiefe, wachsen oder verbessern — hängt an genau einer heute unbeantwortbaren Frage: *Kommen Nutzer von allein wieder?* Drei kleine technische Eingriffe plus ein Impressum machen diese Frage beantwortbar.

**Das größte Risiko:** Die Komfortzone des Bauens. Das Projekt hat eine hervorragende Feedback-Schleife mit sich selbst (HANDOFF, BUGS, Messungen der eigenen Infrastruktur) und noch keine mit Nutzern. Ohne bewussten Schnitt wird auch der September mit exzellent dokumentierten Infrastruktur-Verbesserungen gefüllt, während die eigentliche Produktfrage unbeantwortet bleibt. Das externe Sorare-Risiko ist real, aber nicht handelbar — das interne schon.

**Was ich als Erstes täte (90 Tage allein verantwortlich):** Woche 1: Impressum, Launch, Tracking-Fixes, IDEA-001-Test — nichts anderes. Wochen 2–4: nur zusehen und messen; Baseline ziehen; Discord als Feedback-Kanal bespielen; parallel die kleinen Härtungs-Reste schließen. Wochen 5–8: Watchlist bauen und instrumentieren, Fake-Door + Founding Supporter live stellen. Wochen 9–12: die Daten sprechen lassen — Retention-Urteil und Founding-Regel entscheiden, ob die nächsten Monate „Pro bauen", „Kern verbessern" oder „Problem neu verstehen" heißen. Keine einzige neue Analysefunktion, bevor diese Antworten vorliegen.

---

*Pflege-Regel: Dieses Dokument ist die strategische Referenz neben MONETARISIERUNG.md. Neue Feature-Ideen zuerst gegen Abschnitt 5 (Ziele) und Abschnitt 11 (Backlog-Logik) prüfen; nicht Passendes in IDEAS.md parken. Nach Launch, Baseline und Founding-Test die Platzhalter-Zielwerte durch echte Zahlen ersetzen.*
