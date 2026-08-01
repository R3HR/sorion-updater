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

**ABER — der entscheidende Haken:** Die Abfrage läuft fehlerfrei durch, liefert
aber **ohne OAuth-Token 0 Einträge**, getestet an zwei unabhängigen Managern
(jr3hr, djkoeft). Das ist ein starker Hinweis, dass die Belohnungsdaten an den
angemeldeten Nutzer gebunden sind — anders als Sammlung, Kaufpreise und Verkäufe,
die öffentlich sind. **Nicht bewiesen**, aber wahrscheinlich.

**Konsequenz, falls sich das bestätigt:** Die Rendite-Rechnung ginge **nur für
verknüpfte Nutzer** — was gut passt, denn die Sorare-Verknüpfung existiert seit
01.08. Und es ist genau die Art Funktion, die ein Premium-Modell trägt: Sie
funktioniert nur mit verbundenem Konto und liefert etwas, das kein anderes Tool
zeigt.

**Nächster Schritt zur Klärung (5 Minuten, kein Ausbau):** Dieselbe Abfrage mit
Jonas' OAuth-Token wiederholen (er ist seit 01.08. verknüpft). Kommen Daten →
Idee ist umsetzbar, dann klären, wie weit die Historie zurückreicht. Kommen keine →
Idee ist tot, bevor Aufwand hineinfließt.

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
