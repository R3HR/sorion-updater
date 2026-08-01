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

**Ausbaustufe (Jonas: „sehr komplex, aber interessant"):** Erlöse **einzelnen
Karten** zuordnen — welche Karte hat welches Preisgeld/welche Essence verdient.
Damit ließe sich pro Karte eine echte Rendite ausweisen, nicht nur fürs Gesamtdepot.
Herausforderung: Preisgeld hängt an der **Aufstellung** (Lineup), nicht an einer
einzelnen Karte; eine faire Zuordnung braucht eine Regel (z. B. gleichmäßig auf die
aufgestellten Karten, oder gewichtet nach Score-Beitrag).

**Offene Fragen:**
- Liefert die API Rewards/Payouts pro Manager rückwirkend und öffentlich (wie
  `tokenOwner` und `soldSingleSaleTokenOffers`) oder nur mit OAuth-Token?
- Reicht die Historie weit genug zurück?

**Aufwand:** Grundstufe (Depot-Ebene) mittel · Kartenebene hoch

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
