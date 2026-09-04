# SORION — Ideen-Datenbank

> Vorgemerkte Konzepte, an denen **noch nicht gearbeitet wird**. Zweck: Gedanken
> festhalten, bevor sie verloren gehen — mit genug Kontext, dass eine spätere
> Session sie ohne Rückfragen aufgreifen kann.
> Format: ID · Idee · Warum · Was dafür nötig wäre · Offene Fragen · Aufwand
> Aktueller Stand & laufende Arbeit → [HANDOFF.md](HANDOFF.md)

---

## IDEA-001 — Sammlung als Investment, Erlöse als Rendite

**Idee (Jonas, 02.08.):** Sorare kennt im Grunde zwei Arten von Erlösen:
**Preisgeld** (Cash aus Wettbewerben) und **Essence** (die Währung fürs Craften).
Die Sammlung eines Managers ist das **Investment**, Preisgeld und Essence sind die
**Rendite**, die dieses Investment abwirft.

**Warum das stark ist:** Heute misst Sorion nur die Wertänderung der Karten
(FMV gegen Kaufpreis) und realisierte Trades. Das ist die halbe Wahrheit — eine
Karte, die im Wert stagniert, aber regelmäßig Preisgeld und Essence einbringt,
kann die bessere Investition sein als eine, die 10 % zulegt. Erst mit den Erlösen
wird aus dem Portfolio eine echte Rendite-Rechnung:

```
Gesamtrendite = (Wertänderung + Preisgeld + Essence-Wert) / eingesetztes Kapital
```

Das ist der Unterschied zwischen einem Preis-Tracker und einem Trading-Tool.

**Was dafür nötig wäre:**
- Quelle für Preisgeld je Manager finden (Sorare-API: Rewards/Payouts — noch zu prüfen)
- Quelle für erhaltene Essence je Manager
- Zeitliche Zuordnung zum eingesetzten Kapital (Rendite auf welchen Zeitraum?)

**Ausbaustufe — Zuordnung zu einzelnen Karten (Regel von Jonas, 02.08.):**
Jede Karte einer Aufstellung erzielt eine Punktzahl. Man summiert die Punkte der
Aufstellung, berechnet den prozentualen Anteil jeder Karte und verteilt die
gewonnene Essence bzw. das Preisgeld **anteilig nach Punkte-Beitrag**.

```
Anteil(Karte) = Punkte(Karte) / Summe aller Punkte der Aufstellung
Ertrag(Karte) = Anteil(Karte) × (Essence + Preisgeld dieser Aufstellung)
```

Beispiel aus der Praxis (Screenshot Jonas): Aufstellung „Klassisch / All Star",
3.198 Punkte, 4 Punkte an ~100 Essence vorbei. Bei sieben Karten mit 81/37/83/65/
68/40/43 Punkten bekäme Samuel Dahl (83) den größten Anteil, Serge-Philippe
Raux-Yao (37) den kleinsten.

**API-Recherche (02.08., im Schema geprüft + live getestet):**
Die Datenkette existiert vollständig — Jonas' Vermutung „die API gibt das nicht her"
trifft für die *Struktur* nicht zu:

| Feld | Inhalt |
|---|---|
| `user.rewardedRankings` (paginiert) | belohnte Platzierungen |
| → `so5Lineup.so5Appearances` | `score`, `captain`, `anyCard { slug }` — **genau die Einzelpunkte** |
| → `so5Rewards` | `amount { eurCents }` (Preisgeld) + `coinAmount` (Coins/Essence) |
| → `so5Fixture.slug` | Zuordnung zum Spieltag |

**ABER — der entscheidende Haken: die Daten sind an den angemeldeten Nutzer gebunden.**
Belegt durch zwei unabhängige Beobachtungen:

1. `user.rewardedRankings` läuft fehlerfrei durch, liefert ohne OAuth-Token aber
   **0 Einträge** — getestet an drei Managern: jr3hr, djkoeft und **sorare_jens**
   (letzterer hat laut Jonas in GW94 deutlich abgeräumt; auch dort 0).
2. Der Typ `So5Leaderboard` bietet ausschließlich `mySo5Lineups` und
   `mySo5Rankings` — das Präfix „my" sagt es direkt. Es gibt **kein** Feld, über
   das man Aufstellungen oder Platzierungen FREMDER Manager lesen könnte.
   Öffentlich sind nur `rewardPool` und `rewardedLineupsCount` (eine reine Zahl).

Damit ist auch der Umweg über den Spieltag versperrt: weder vom Nutzer noch vom
Leaderboard aus kommt man an fremde Aufstellungen. Anders als Sammlung, Kaufpreise
und Verkäufe, die öffentlich sind.

**Konsequenz — und die ist eher ein Vorteil:** Die Rendite-Rechnung geht **nur für
Nutzer mit verknüpftem Sorare-Konto**. Das passt doppelt:
- Die Verknüpfung existiert seit 01.08. (`link_sorare`), die Voraussetzung ist also da.
- Es ist genau die Sorte Funktion, die ein **Premium-Modell** trägt: ohne verbundenes
  Konto technisch unmöglich, und kein anderes Tool kann sie ohne dieselbe Hürde bauen.

Was dagegen **nicht** geht: fremde Manager auf ihre Rendite hin scouten. Die
Manager Search bleibt auf Sammlung, Kaufpreise und Trades beschränkt.

**Nächster Schritt (5 Minuten, kein Ausbau):** Dieselbe Abfrage einmal mit Jonas'
OAuth-Token wiederholen. Kommen Daten → umsetzbar, dann klären, wie weit die
Historie zurückreicht (Jonas' zweiter Einwand: alle vergangenen Spieltage
durchzugehen dauert). Kommen auch mit Token keine → Idee ist tot, bevor Aufwand
hineinfließt.

### 🟢 UPDATE 04.09.2026 — DER BLOCKER IST AUFGEHOBEN, Idee ist umsetzbar

Die Recherche vom 02.08. ("nur mit OAuth fuer den eigenen Nutzer, fremde
Aufstellungen unerreichbar") ist **ueberholt**. Sie hatte den falschen Einstieg
geprueft (`user.rewardedRankings`, `mySo5Rankings`). Der richtige Pfad ist:

```
so5.so5Fixture(slug:"<gw>").userFixtureResults(userSlug:"<manager>")
  -> so5LeaderboardContenders.nodes
     -> so5Lineup.so5Appearances  (Einzelpunkte, captain, substitutionState)
     -> so5Rankings.so5Rewards    (Essence-Menge, Waehrung, Preisgeld, Karten)
```

**Live bewiesen (04.09.)** an jr3hr und GW `football-28-aug-1-sep-2026`: alle acht
Aufstellungen mit Einzelpunkten und Belohnungen — **ohne OAuth-Token**, oeffentlich.
Noetig ist nur der `SORARE_APIKEY` (Query-Tiefe >7), der serverseitig in der Edge
Function `so5-results` liegt. **Damit geht die Rendite-Rechnung fuer JEDEN Manager,
nicht nur fuer verknuepfte Konten** — die Einschraenkung von damals (und die daran
geknuepfte Premium-Ueberlegung) faellt weg.

**Belohnungen sind NICHT rarity-rein (verifiziert 04.09., Hinweis Jonas):**
Sorare schuettet in einem Limited-Wettbewerb auch Rare-Essence aus (Tauschangebot
"viel Limited gegen wenig Rare"). Beleg: jr3hr, GW 47, "Bundesliga – Limited",
Lineup aus fuenf LIMITED-Karten -> Belohnung **1020 limited + 765 rare essence**.
Konsequenz fuer die Ertragsrechnung: Die Essence-Art kommt IMMER aus dem Reward
(`CardShardsReward.rarity`), niemals aus der Karten- oder Wettbewerbs-Rarity
ableiten — sonst rechnet man mit dem falschen Kurs (Faktor ~5).

**ZUORDNUNGS-EBENE ist die KARTE, nicht der Spieler (Ansage Jonas 04.09.):**
Derselbe Spieler liegt oft mehrfach im Depot (andere Rarity/Saison/Serie) und
laeuft in mehreren Aufstellungen — jede Karte ist ein eigenes Investment mit
eigenem Kaufpreis. `so5Appearances.anyCard{slug serialNumber rarityTyped
seasonYear}` liefert die Identitaet mit (63/63 Einsaetze in GW9 zugeordnet).
Beispiel aus GW9: Ibrahim Maza lief als `ibrahim-maza-2025-rare-59` (Champion)
UND `ibrahim-maza-2026-limited-117` (Bundesliga) — zwei Karten, zwei Ertraege.
Aggregation daher IMMER ueber den Karten-Slug.

**PRAEZISIERUNG der Verteilungsregel (Fund 04.09.):** Die Punktsumme darf nur ueber
die **zaehlenden** Spieler gehen — `substitutionState` STARTER und SUBBED_IN.
ON_BENCH und SUBBED_OUT liefern keine Punkte ans Lineup und duerfen deshalb auch
keinen Ertragsanteil bekommen (sonst verwaessert die Bank die Zuordnung).

**Prototyp gerechnet (GW9, Under 23 – Limited, 520 Essence auf 7 Spieler):**
Janssen (C) 154 Pkt = 29,8 % -> 155 Essence; Nusa 82 -> 83; Wilfinger 75 -> 75;
Dahl 65 -> 66; Raci 55 -> 55; Tiknaz 54 -> 54; van den Heuvel 33 -> 33.
Ueber alle acht Aufstellungen aggregiert ergibt das den Ertrag je Karte in der GW.

**ESSENCE-KURS — die letzte Luecke ist geschlossen (Jonas, 04.09.):** CraftLog
ermittelt den Essence-Wert je Rarity aus den eingetragenen Crafts:
`EV/1k = (Summe Kartenwerte / Summe eingesetzte Essence) x 1000` (Funktion
`calcScarcityAnalytics` in Craft_Log UI). **Mit jedem eingetragenen Craft wird
der Kurs genauer** — die beiden Produkte greifen also ineinander: CraftLog
liefert den Umrechnungskurs, den Sorion fuer die Rendite-Rechnung braucht.

Stand 04.09. (68 Crafts, 17.03.-01.09.): **Limited 4,63 EUR / 1k Essence**
(n=62, tragfaehig) · **Rare 22,93 EUR / 1k** (n=6 — noch duenn, mit Vorsicht
ausweisen oder Mindest-n erzwingen).

**Vollstaendige Kette einmal durchgerechnet (jr3hr, GW9):** 8 Aufstellungen ->
728 Limited- + 255 Rare-Essence -> **9,22 EUR Ertrag**, verteilt nach Punkte-
anteil: Lewis Hall 1,69 · Matanovic 1,05 · Querfeld 0,98 · Maza 0,88 · Janssen
0,72 EUR. (Lernpunkt: Hall fuehrt trotz weniger Essence als Janssen — Rare-
Essence ist ~5x wertvoller. Ohne Kurs waere die Reihenfolge falsch.)
XP-Belohnungen sind dabei NICHT bewertet (kein Marktpreis).

**Technische Huerde fuer die Integration:** `crafts` ist RLS-geschuetzt (nur
eigene Zeilen), CraftLog holt die Gesamtdaten ueber die Edge Function
`get-analytics` mit Login. Sorion braucht den Kurs ohne Login -> kleine RPC
`essence_value()` (security definer, liefert NUR die Aggregate je Rarity plus
n, keine Einzel-Crafts), taeglich guenstig zu berechnen.

**Naechste Schritte:** (1) Ertrag ueber mehrere GWs summieren und in der
GW-History/Portfolio-Karte je Spieler zeigen; (2) Essence-Wert in Euro ansetzen
(CraftLog kennt ihn, siehe IDEA-002) -> daraus die Rendite-Formel oben;
(3) Historie-Tiefe klaeren (previous-Kette, je GW ein Aufruf, gecacht).

**Aufwand:** Grundstufe (Depot-Ebene) mittel · Kartenebene hoch — und laut Jonas
zusätzlich zeitintensiv, weil alle vergangenen Aufstellungen durchlaufen werden
müssten (paginiert, ein Spieltag nach dem anderen).

---

## IDEA-002 — Essence-Wert als Einstand für gecraftete Karten

**Idee (Jonas, 02.08.):** CraftLog ermittelt bereits den **Wert von Essence**.
Diesen Wert könnte man bei gecrafteten Karten als **Kaufpreis** ansetzen, um beim
Verkauf den echten Gewinn zu ermitteln.

**Warum das konkret fehlt:** Seit dem 02.08. hat die Trade History einen Toggle für
Karten ohne Kaufbeleg (Rewards, Craft, Tausch). Diese Zeilen zeigen `P&L 0 · no cost`,
weil kein Euro-Einstand existiert. Für Rewards ist das korrekt — für **gecraftete**
Karten aber nicht: Sie haben Essence gekostet, und Essence hat einen Marktwert.
Wer eine Karte für 4.000 Essence craftet und für 12 € verkauft, hat nicht 12 €
verdient, sondern 12 € minus dem Wert der eingesetzten Essence.

**Was dafür nötig wäre:**
- Essence-Wert je Zeitpunkt aus CraftLog übernehmen (idealerweise als Zeitreihe,
  damit historische Crafts mit dem damaligen Wert bewertet werden)
- Craft-Kosten je Karte: Wie viel Essence hat dieser konkrete Craft gekostet?
  (Kosten hängen von Rarity/Tier ab — CraftLog kennt die Logik bereits)
- In `manager_trades` einen Einstand `buy_eur` aus Essence × Wert setzen, dabei
  klar kennzeichnen, dass es ein **abgeleiteter** Wert ist (nicht mit echten
  Kaufpreisen vermischen — sonst wäre die Herkunft der Zahl unklar)

**Offene Fragen:**
- Liefert die Sorare-API die eingesetzte Essence-Menge je Craft, oder muss sie aus
  Rarity/Tier rekonstruiert werden?
- Welcher Essence-Wert gilt: der zum Craft-Zeitpunkt oder der aktuelle?
  (Für Gewinnermittlung ist der Zeitpunkt des Crafts richtig.)

**Verbindung zu IDEA-001:** Essence taucht zweimal auf — als **Erlös** (man bekommt
sie) und als **Einsatz** (man craftet damit). Beide Seiten brauchen dieselbe
Bewertungsquelle; sinnvollerweise zusammen umsetzen.

**Aufwand:** Mittel — die Bewertungslogik existiert in CraftLog, es geht vor allem
um die Datenbrücke zwischen beiden Projekten.

---

## IDEA-003 — Squad-Manager (Leaderboard + Aufstellungs-Tracking + Regel-Überwachung)

**Idee (Jonas, 20.08.):** Sorion bekommt einen Squad-Manager für Sorare-Squads:

1. **Leaderboard** — Squad-Mitglieder nach **Durchschnittspunkten** ranken (über Gameweeks hinweg, nicht nur die Momentaufnahme der App).
2. **Aufstellungs-Tracking** — festhalten, **wann** welcher Manager am Spieltag welches Team aufgestellt hat.
3. **Regel-Überwachung** — Squad-interne Regeln prüfbar machen. Konkreter Fall: **Player-Cap** (ein Spieler darf von max. 4 Managern aufgestellt werden). Heute unlösbar: niemand weiß, wer als 5. aufstellt.

**API-Grundlage (recherchiert + teils live verifiziert 20.08., Details: `C:\craft-log\docs\HANDOFF.md` → „API-Wissen: Squads"):**
- Squad-Stammdaten (Name, Mitglieder mit Slug, Captain): `currentUser.squad(sport)` — auth nötig.
- **Normale** Lineups jedes Users sind ÖFFENTLICH pro Fixture abrufbar (`so5.so5Fixture(...).so5LineupsPaginated(userSlug)`) inkl. Spieler/Captain/Karte/Score.
- **Squad-Board-Lineups tauchen dort NICHT auf** (live verifiziert am Beispiel Sorare_Jens): Sie hängen an `currentUser.boards(mode: SQUAD, rarity, sport)` → `SquadStep.squadLeaderboardLineups(top: N)` (Rang + Lineup + User) und `myLineups` — **nur authentifiziert** über die `sorare-oauth`-Function (Token liegt serverseitig). Noch nicht mit echtem Token verifiziert.
- **„Wer hat schon aufgestellt?" (à la Sorare Inside) ist ÖFFENTLICH und billig** (live verifiziert 20.08.): eine Batch-Query mit einem Alias pro Mitglied — `alias: so5LineupsPaginated(userSlug: "…", first: 0) { totalCount }` auf der Fixture → ✅/❌-Liste in einem Call. Sorare Inside zeigt nur diesen Status; welches Team gestellt wurde, liefert die API öffentlich gleich mit (`nodes.so5Appearances`).
- **Kein Abgabe-Zeitstempel in der API** (`So5Lineup` hat kein `createdAt`/`submittedAt`, nur `draft`/`hidden`/`cancelledAt`) → „wann aufgestellt" geht NUR über eigenes Polling.

**Lösungsansatz Zeit-Tracking (= Jonas' Snapshot-Idee, bestätigt richtig):**
- Cron pollt im Aufstellungsfenster (z. B. alle 10–15 min) die Squad-Lineups und speichert Snapshots: Mitglied, Spieler-Slugs, Captain, first-seen/last-seen, Änderungen (Spielertausch).
- **First-seen-Reihenfolge ersetzt den fehlenden Zeitstempel** — Auflösung = Poll-Intervall.
- Rohsnapshots nach 24 h löschen (Jonas' Vorgabe); nur Aggregat behalten (wer/wann zuerst gesehen, Cap-Verstöße, GW-Punkte fürs Leaderboard).

**Player-Cap-Erkennung:**
- „**Welche** 5 Manager nutzen Spieler X" = trivial (player.slug über alle Squad-Lineups zählen).
- „**Wer war der 5.**" = nur über die First-seen-Reihenfolge aus dem Polling.
- ⚠️ **Kritische offene Frage:** Sind fremde Squad-Lineups schon **vor** dem Lock sichtbar, oder erst ab Spielbeginn (Screenshot zeigt sie live/danach)? Wenn Sorare sie bis Kickoff versteckt, ist keine Abgabe-Reihenfolge beobachtbar → Cap-Report könnte nur „diese 5 waren es" liefern, nicht „der war zuletzt". MUSS als Erstes mit echtem Token geprüft werden.

**Was dafür nötig wäre:**
- Test-Action `squad_board` in `sorare-oauth` (analog `card_pull`): `boards(mode: SQUAD)` mit Jonas' Token abfragen → klärt Datenform + Sichtbarkeits-Frage.
- Polling-Cron (Railway-Harvester oder Supabase-Cron) + 2 Tabellen: `squad_lineup_snapshots` (TTL 24 h), `squad_gw_scores` (dauerhaft, fürs Durchschnitts-Leaderboard).
- Klären: Token-Laufzeit/Refresh für unbeaufsichtigtes Polling (OAuth-Refresh-Flow in `sorare-oauth` vorhanden?).
- APIKEY mitsenden (Komplexitätslimit 500 → 30.000; Key liegt in Railway).

**Offene Fragen:**
- Sichtbarkeit fremder Squad-Lineups vor Lock (s. o. — entscheidet über den Funktionsumfang).
- Einverständnis der Squad-Mitglieder (ihre Daten erscheinen auf Sorion)? `hidden`-Flag respektieren.
- Gilt der Cap pro Gameweek oder pro Step/Board?
- Scope: eigenes Sorion-Modul oder eigenständige Seite? (Roadmap-Fokus beachten — Squad-Manager ist Neuland neben dem Markt-Kern.)

**Aufwand:** Mittel — Auth-Pfad und Cron-Infrastruktur existieren (sorare-oauth, Railway, CRON_SECRET-Muster); neu sind im Kern die zwei Tabellen, der Poller und die UI.

---

## IDEA-004 - Squad-Manager als Angebot fuer fremde Squads (verworfen fuer jetzt)

**Idee (Jonas, 22.08.):** Den Squad-Manager-Bot anderen Sorare-Squads anbieten,
eventuell gegen Gebuehr.

**Machbarkeit:** Technisch nah dran. Noetig waeren Mandantenfaehigkeit (`squad_id`
in allen Tabellen, Token/Webhook/Regelwerk je Squad - Cap-Hoehe, Frist und
Umstell-Zeit sind heute Konstanten) und ein Selbstbedienungs-Onboarding
(Captain verbindet Sorare per OAuth, legt Webhook an, traegt Discord-IDs ein).
Aufwand ~2-3 Tage. Last unkritisch: 1 Sorare-Abfrage je Squad und Poll.

**Warum jetzt NICHT:**
1. **ToS.** Geld nehmen fuer ein Produkt auf Sorares API ist genau der Fall, fuer
   den in der ROADMAP die ToS-Klaerung angesetzt ist (Weg ueber Sorare-Inside-Kontakt).
   Risiko im Zweifel der API-Key - an dem haengt auch Sorion selbst.
2. **Token-Verwahrung.** Refresh-Tokens FREMDER Sorare-Konten zu speichern ist eine
   andere Verantwortungsklasse (Leak, Widerruf, DSGVO-Loeschpflichten).
3. **Priorisierung.** ROADMAP-P0 ist Launch + Tracking-Fixes vor jedem neuen Feature;
   Sorion Pro ist selbst noch unvalidiert.
4. **Marktgroesse unklar.** Die Cap-/Claim-Regel von „Handpicked" ist speziell - viele
   Squads duerften gar keine so strengen Regeln haben, dann gibt es nichts zu automatisieren.

**Vorgemerkt als naechster konkreter Schritt (24.08.): Captain-Seite auf sorion.pro.**
Geschuetzte Ansicht mit Schaltern fuer die Sonderregeln (Position aussetzen, Spieler
ausnehmen, Cap aendern, Manager entschuldigen), dazu Cap-Uebersicht auf Knopfdruck,
Saison-Stand und Rundenauswertung. Aufwand ~1/2 Tag.

**Warum das der Bruecken-Baustein ist:** Heute setzt Jonas Sonderregeln, indem er sie
mir sagt - fuer einen fremden Squad ist das unmoeglich. Die Captain-Seite ist genau die
Selbstbedienungs-Oberflaeche, die Mandantenfaehigkeit braucht. Wer sie einmal baut, hat
den groessten Teil des Onboardings fuer fremde Squads erledigt: Sorare verbinden,
Webhook eintragen, Discord-IDs pflegen, Regeln konfigurieren. Die Backend-Seite ist
bereits vorbereitet (Tabelle `squad_overrides`, Actions `override`/`overview`/`season`).
Alternative Variante c (echter Discord-Bot mit Slash-Commands) bleibt der groessere
Umbau und waere erst danach dran - er wuerde zusaetzlich Lesezugriff auf Kanaele bringen.

**Entscheidung Jonas 24.08.: vorerst NICHT monetarisieren - Markt zu frueh.**
Squads sind ein junges Sorare-Feature. Die meisten duerften noch gar keine
geschriebenen Regeln haben - ohne Regeln gibt es nichts zu automatisieren, und der
Bot loest ein Problem, das der Markt noch nicht spuert. Zahlungsbereitschaft folgt
der Reife der Aktivitaet, nicht der Qualitaet des Werkzeugs.

**Preisueberlegung (falls es doch soweit kommt, Stand 24.08.):** ~9,99 EUR/Monat
**pro Squad** (nicht pro Kopf - geteilt ~1 EUR/Person, das verkauft der Captain ohne
Diskussion). Vergleichsgroesse ist die Arbeitszeit des Captains (20-30 min/Tag manuelle
Pflege), nicht der Preis eines Analyse-Tools. Unter 5 EUR traegt es den Support-Aufwand
nicht, ueber 15 EUR verlaesst man das Preisgefuehl fuer Hobby-Gruppenwerkzeuge.
Kostenbasis ~38 EUR/Monat inkl. Supabase Pro => **4 zahlende Squads decken alles**.
Grenzkosten je Squad nahe null (~4.400 Sorare-Abfragen/Monat gegen ein 200/min-Limit).
Wichtig waere eine **kostenlose Stufe** (Cap-Warnungen, "X hat aufgestellt"), damit der
Verteilungseffekt erhalten bleibt - 9 weitere Manager sehen den Bot taeglich.

**Was sich derweil von selbst aufbaut (Vorsprung ohne Zusatzaufwand):** Seit 21.08.
sammelt der eigene Betrieb Aufstellungs-Historie mit Zeitstempeln, Boni und
Cap-Verlaeufen - Daten, die Sorare nicht herausgibt und die rueckwirkend niemand
rekonstruieren kann. Dazu ein Regelwerk, das gegen 21 Runden verifiziert ist.

**Neubewertung wenn:** Sorare die Squad-Funktionen ausbaut, fremde Captains von sich
aus nach dem Bot fragen, oder Dritt-Tools fuer Squads auftauchen.

**Wenn wieder aufgreifen:** Erst kostenlos an 2-3 befreundete Squads geben (geht
notfalls ohne volle Mandantenfaehigkeit) und pruefen, ob sie es nach vier Wochen
behalten wollen. Erst dann ToS klaeren, und dann eher als Bestandteil von
**Sorion Pro** (Captain zahlt, Squad profitiert) statt als zweites Abo - sonst
kannibalisiert es das Hauptprodukt.

---

## IDEA-005 — Liveticker fuer Positionswechsel (erwogen 27.08.2026, offen)

**Frage Jonas:** "ob ein liveticker cool waere bei dem wir anzeigen wenn leute bei
der stage die position tauschen oder ob dafuer sowieso jeder in die Sorare App
schaut. wie viel performance wuerde uns das kosten?"

**Gemessen an Runde 25 (27.08., 83 Snapshots, 07:10-17:00 UTC), Action `rank_history`:**

| Groesse | Wert |
|---|---|
| Uebergaenge zwischen Snapshots | 82 |
| davon mit veraenderten Punkten | 14 |
| davon mit veraenderter Reihenfolge | 11 |
| davon mit veraenderter Top-3-Besetzung | 11 |
| Fuehrungswechsel | 6 |
| Poll-Laufzeit | 3,9 / 7,2 / 3,9 s |

**Kosten: praktisch null.** Die Punkte kommen mit **jedem ohnehin laufenden Poll**
mit - keine zusaetzliche Sorare-Abfrage, keine zusaetzlichen DB-Schreibvorgaenge.
Dazu kaemen nur ~11 Zeilen in `squad_notifications` und ~11 Discord-Posts je
Spieltag (Limit: 5 Posts/2 s - irrelevant).

**Wichtigster Befund: Schneller pollen bringt nichts.** Sorare aktualisiert die
Scores in **Schueben** - nur 14 von 82 Intervallen zeigten ueberhaupt eine
Aenderung, im Schnitt also alle ~45 min. Ein 2-Minuten-Ticker faende **dieselben
11 Ereignisse** bei fuenffacher Schreiblast - und genau diese Last hat laut
INC-006 die DB zweimal lahmgelegt. Der 10-Minuten-Takt bleibt richtig.

**Einschraenkung Signalqualitaet:** Die Haelfte der Wechsel fiel in die Phase, in
der die meisten Manager noch bei 0 standen (z. B. "enexxx fuehrt" um 11:00 bei
6 Managern ohne Punkte). Das sind Artefakte einer leeren Tabelle, keine Ereignisse.
**Empfehlung:** nur Top-3-Grenze und Fuehrung melden, und erst ab >= 5 Managern mit
Punkten. Das haette heute ~6 statt 17 Meldungen ergeben.

**Zur Gegenfrage "schaut nicht jeder eh in die App?":** Die reinen Zahlen stehen in
der Sorare-App. Der Mehrwert eines Tickers waere **nicht die Zahl, sondern der Push
und die Einordnung** - Abstand zum Stage-Ziel, wer gerade in den zaehlenden Top 3
steht und was die Runde fuer die Saisontabelle bedeutet (`live.projected`). Das
zeigt die App nicht.

**Status: GEBAUT am 27.08.2026.** Eigener Discord-Kanal (`DISCORD_TICKER_WEBHOOK_URL`,
Secret siehe SEC-005). Gemeldet werden **Ueberholvorgaenge im Tagesergebnis** der
laufenden Stage - eine Zeile je Aufsteiger mit den Namen der Ueberholten; wer faellt,
steht dort bereits drin. Der Zielfortschritt wird nur angehaengt, wenn der Wechsel die
wertenden Top 3 beruehrt. Erst ab 5 Managern mit Punkten (siehe Messung oben).
**Sortierung deterministisch** (Punkte, dann Slug) - sonst haetten Punktgleichstaende
Phantom-Wechsel erzeugt. Simulation auf den echten Snapshots des 27.08.: **10 Meldungen**
ueber den Spieltag. Zwei Verwerfungen auf dem Weg dorthin, beide von Jonas korrigiert:
erst nur Top-3-Grenze (zu wenig), dann komplette Tabelle je Meldung ("sieht langweilig
aus") - final nur die Einzelwechsel.

### Nachtrag 27.08. abends — Ticker auf editierte Embed-Nachricht umgestellt

Der ereignisbasierte Ticker (eine Nachricht je Ueberholvorgang) hatte zwei Maengel,
die Jonas benannt hat: Ein Manager konnte in **zwei** Gruppen vorkommen und tauchte
damit doppelt auf, und der Kanal fuellte sich mit Wiederholungen.

**Neu:** genau **eine Embed-Nachricht je Runde**, die alle 10 min per
`PATCH /webhooks/{id}/{token}/messages/{message_id}` still aktualisiert wird. Die
`message_id` kommt aus dem POST mit `?wait=true` und liegt in
`squad_ticker_messages` (Schluessel: Kanal x Step).

Inhalt: Fortschrittsbalken zum Stage-Ziel (genau einmal, ganz oben), darunter die
feste Rangliste 1-10 im Codeblock mit buendigen Spalten, Rang 1-3 als Medaille, je
Zeile das Delta zum letzten gespeicherten Stand aus `squad_score_snapshots`.
Farbe: rot < 80 %, gelb 80-99 %, gruen ab erreicht. Zahlenformat durchgehend
`1105.69`. Fussnote `Stand: HH:MM` (Berlin).

**Neu gepostet** wird nur bei: Rangwechsel in den Top 3 (erst ab 5 Managern mit
Punkten), erreichtem Stage-Ziel oder einem Punktesprung >= `TICKER_JUMP_PTS`
(Standard 40, ueber die Umgebung aenderbar). Sonst bleibt es bei der stillen
Bearbeitung. Wird die Nachricht in Discord geloescht, faellt der Ticker beim
naechsten Lauf automatisch auf einen Neupost zurueck (PATCH 404), statt zu
verstummen.

**Historie** landet nur noch in `squad_score_snapshots`, nicht mehr im Kanal.

**Ereignismeldungen (Nachtrag 27.08.):** Die editierte Tabelle zeigt den Stand,
faellt aber niemandem auf. Fuer die drei Anlaesse mit Bedeutung gibt es daher eine
eigene Nachricht im selben Kanal, direkt vor dem Neuanker der Tabelle:

- `🔀 Top 3 change — ⏫ ANDREIHAHA [75.25] 3rd · ⏬ NAMIUNK_022 [74.55] 4th`
  (alle, deren Rang sich geaendert hat und die jetzt in den Top 3 stehen oder
  vorher drin waren)
- `🎯 Target reached! 1145.20 / 1140 — 🥇 … 🥈 … 🥉 …`
- `⚠️ Back below target — 1132.10 / 1140 · 7.90 short again.`

Der letzte Fall fehlte bisher voellig: Sorare korrigiert Punkte nachtraeglich auch
nach unten - am 27.08. sank der Squad Score innerhalb eines Laufs von 1105.69 auf
1105.38. Ohne diese Meldung haette der Squad ein einmal gemeldetes "geschafft" fuer
endgueltig gehalten. Entsprechend ankert die Tabelle jetzt bei **jedem** Wechsel des
Ziel-Zustands neu (`cleared !== clearedBefore`), nicht nur beim Erreichen.

Simulation auf den echten Snapshots des 27.08.: **11 Top-3-Ereignisse** ueber den
Spieltag, alle ab ~19:45.

**Neuanker loescht die alte Tabelle (Nachtrag 27.08.):** Befund Jonas - "das
Leaderboard dass wir bearbeiten verschwindet". Zwei Probleme in einem:

1. Nach einer Ereignismeldung stand die editierte Tabelle **oberhalb** davon und
   rutschte aus dem Blick.
2. Beim Neuposten blieb die **alte** Tabelle als veralteter Zwilling im Kanal
   stehen - zwei Staende, und man muss raten, welcher gilt.

**Loesung:** Beim Neuanker wird die alte Nachricht per
`DELETE /webhooks/{id}/{token}/messages/{message_id}` entfernt, bevor die neue
gepostet wird. Geloescht wird ausschliesslich die eigene, ueber die gespeicherte
`message_id` eindeutig bestimmte Nachricht. Damit gilt dauerhaft: **genau eine
Tabelle, und sie ist immer die neueste Nachricht im Kanal.** Ereignismeldungen
bleiben als Verlauf darueber stehen. Ein 404 beim Loeschen (Nachricht war schon
weg) wird ignoriert.

**"Ziel SICHER erreicht" (27.08.):** Vorgabe Jonas - Memes posten, wenn das Ziel
sicher steht, also drei durchgespielte Aufstellungen es allein tragen.

`cleared` (Summe >= Ziel) genuegt dafuer nicht: Die Summe kann wieder fallen, weil
Sorare nachbewertet - am 27.08. sank sie innerhalb eines Laufs um 0,31. Die API
liefert keinen Spielstatus, wohl aber `lockedAt` (Anpfiff) je Spieler. **Korrigiert am selben Abend, zweimal, beide Male durch Jonas:**

1. Die Zeitschaetzung war falsch. Sorare liefert den Status direkt:
   `taskAppearances { game { statusTyped } }` mit `played` / `playing` /
   `scheduled`. Befund Jonas: "matthias ginter ist zum beispiel schon fertig ...
   im vergleich zu pedri der noch spielt" - Ginter stand auf `played`, Pedri auf
   `playing`. Das Feld war nie abgefragt worden. Die 150-Minuten-Regel haette am
   27.08. um 23:30 "sicher" gemeldet, waehrend das Barca-Spiel noch lief. Sie
   bleibt nur noch als Notnagel, falls der Status fehlt (`FINAL_AFTER_MIN`).
2. "Aufstellung komplett durch" war zu streng. Befund Jonas: "Da spieler nicht
   unter 0 punkte fallen koennen kann das ziel auch erreicht werden wenn ... nur
   3 oder 4 von 5" fertig sind. Richtig: Die Summe der **fertigen** Spieler einer
   Aufstellung ist eine **Untergrenze**, die nicht mehr unterschritten werden kann.

**Endgueltige Regel:** Je Aufstellung die Punkte der bereits fertigen Spieler
aufsummieren (= garantierte Untergrenze), die drei hoechsten Untergrenzen addieren;
erreicht diese Summe das Ziel, ist es sicher. Damit zaehlt eine Aufstellung mit 4
von 5 fertigen Spielern anteilig mit, statt gar nicht.

Gegenprobe: Die Spielerpunkte summieren sich exakt zur Aufstellungspunktzahl,
sobald alle fertig sind (mcbeast 351.75 vs. 351.74). Bei laufenden Partien hinkt der
Live-Wert der Aufstellung den Einzelwerten um wenige Punkte hinterher - fuer die
Untergrenze irrelevant, da dort nur fertige Spieler eingehen.

Wirkung am 27.08. (Ziel 1140): alte Regel 705.51 (nur 2 komplette Aufstellungen),
neue Regel **990.56** (sorare_jens 353.76 + mcbeast 351.75 + andreihaha 285.05 bei
4/5 fertig).

Meldung `🎯 TARGET HIT!` mit den drei tragenden Aufstellungen und einem Meme aus dem
**eigenen Pool `kind = 'target'`** (darf sich mit 'reminder'/'allclear' nicht
vermischen). Einmalig je Runde (`target-hit:<step>`).

**Verifiziert am 27.08. (Stage 4, Ziel 1140):** Beste drei durchgespielte waeren
maisonpanda 424.69 + andreihaha 392.77 + jr3hr 392.49 = 1209.95. Letzter Anpfiff
19:00 UTC, also sicher ab 21:30 UTC = 23:30 Berlin - genau dann feuert die Meldung.

**Gegenstueck "Ziel verfehlt" (27.08.):** Meldung `😞 Target missed.` mit
Endstand, Rueckstand und den drei besten Aufstellungen, dazu ein Meme aus dem
eigenen Pool `kind = 'missed'` (7 Stueck). **Zwei Ausloeser** (Korrektur 28.08. nach Befund Jonas "Die stage ist durch, keine
chance mehr zu gewinnen. Wo ist die benachrichtigung vom bot?"):

1. **Frueh und rechnerisch sicher:** ALLE Spiele aller Aufstellungen sind gelaufen
   (`game.statusTyped = played`) und der Squad Score liegt unter dem Ziel. Was dann
   darunter liegt, bleibt darunter - kein Punkt kommt mehr dazu.
2. **Spaet und formal:** Sorare setzt `state = FAILED`.

Anfangs haing die Meldung nur an (2). Das Flag kommt aber deutlich spaeter als das
Ergebnis: Am 28.08. stand die verlorene Stage 5 (1259.45 / 1280) laengst fest,
waehrend der Bot noch schwieg. Beide Wege teilen sich denselben Schluessel
(`target-missed:<step>`), es geht also genau eine Meldung raus - je nachdem, was
zuerst eintritt.

Die Meldung selbst liegt als Funktion `postMissed()` vor, damit beide Ausloeser
dieselbe Formulierung und denselben Meme-Pool nutzen und nicht auseinanderlaufen
koennen (die Ursache von BUG-022/023/024 war genau solche Doppelung).

**Vier getrennte Meme-Pools** (duerfen sich nie vermischen, Vorgabe Jonas 26.08.):
`reminder` (12) · `allclear` (10) · `target` (6) · `missed` (7). In allen gilt
dieselbe Regel: zufaellige Auswahl, aber nie zweimal hintereinander dasselbe.

**Erster echter Durchlauf (27.08., Runde 25, Ziel 1140):** Die 🎯-Meldung ging um
21:10 UTC (23:10 Berlin) raus, garantierte Summe **1315.02 / 1140**. Danach hat der
Poller die Runde automatisch als **R25** in `squad_step_rounds` eingetragen, und der
Saisonstand rechnet sie seitdem mit - ohne einen einzigen Handgriff. Damit ist die
gesamte Kette einmal komplett unter echten Bedingungen gelaufen.
