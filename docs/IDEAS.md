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

**Wenn wieder aufgreifen:** Erst kostenlos an 2-3 befreundete Squads geben (geht
notfalls ohne volle Mandantenfaehigkeit) und pruefen, ob sie es nach vier Wochen
behalten wollen. Erst dann ToS klaeren, und dann eher als Bestandteil von
**Sorion Pro** (Captain zahlt, Squad profitiert) statt als zweites Abo - sonst
kannibalisiert es das Hauptprodukt.
