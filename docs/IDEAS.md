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
