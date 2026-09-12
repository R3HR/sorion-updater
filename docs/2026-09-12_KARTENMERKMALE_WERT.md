# Was ein einzelnes Exemplar wert macht: Level, Spezialedition, Trikotnummer (12.09.2026)

**Frage (Jonas):** Karten unterscheiden sich in Level (0 bis 10), Spezialedition und darin, ob
die Seriennummer der Trikotnummer entspricht. Alle drei erhoehen die Kartenpunkte und damit das
Gewicht in der Vereinssammlung. Sollte der FMV das beruecksichtigen?

**Antwort in einem Satz:** Nur die Spezialedition ist den Aufwand wert. Das Level ist nachweislich
wertlos fuer den Kaeufer, und der Trikotnummer-Treffer ist zu selten, um messbar zu sein.

## Datengrundlage

`TokenPrice.card` liefert zu JEDEM vergangenen Verkauf das konkrete Exemplar
(`serialNumber`, `grade`, `xp`, `specialEdition`). 160 meistgehandelte Spieler, 3.188 Verkaeufe,
davon 395 Spezialeditionen.

**Methode:** paarweiser Vergleich INNERHALB derselben Karte, gleiche Verkaufsart, hoechstens
7 Tage auseinander (9.395 Paare). So fallen Zeittrend und Verkaufsart als Stoerfaktoren heraus.
Ohne diese Kontrolle haette man sich leicht getaeuscht: In einer ersten Stichprobe hatten die
aelteren Verkaeufe zufaellig hoehere Level, und der Markt fiel gerade. Naiv ausgewertet haette
das wie ein satter Level-Bonus ausgesehen.

## 1. Level (grade): KEIN Preiseffekt

| Level-Differenz | Paare | Preisunterschied |
|---|---|---|
| +1 | 1.568 | −3,4 % |
| +2 | 250 | +0,7 % |
| +3 | 34 | +4,6 % |

Im Schnitt rund **−0,5 % je Level**, ohne erkennbare Richtung. Das ist Rauschen.

**Warum, und das ist der eigentliche Befund: Beim Transfer wird die XP halbiert.**
Gemessen an echten Karten (`xp` gegen `xpAfterTransfer`):

| XP beim Verkaeufer | nach dem Transfer | Level vorher | nachher |
|---|---|---|---|
| 678 | 339 | 2 | 1 |
| 673 | 336 | 2 | 1 |
| 600 | 300 | 2 | 1 |
| 200 | 100 | 1 | 0 |
| 100 | 50 | 0 | 0 |

Der Bonus faellt entsprechend (1,07 auf 1,06). Der Kaeufer bekommt das Level also gar nicht,
und deshalb bezahlt er es auch nicht. **Level gehoert nicht in den FMV.** Wer es doch einbaut,
baut einen Fehler ein.

## 2. Spezialedition: +6,3 %

425 Paare gleichen Levels, in denen sich nur die Edition unterscheidet: **+6,3 %**.
Das ist ein echter, gerichteter Effekt bei brauchbarer Fallzahl, und Spezialeditionen sind mit
12,4 % der Verkaeufe haeufig genug, um den Durchschnitt zu verschieben.

## 3. Trikotnummer gleich Seriennummer: nicht messbar

Pro Spieler und Rarity gibt es genau EIN passendes Exemplar. Bei Rare (typisch 100 Stueck) ist
das 1 %, bei Limited (1.000) ein Promille. In 3.188 Verkaeufen sind kaum Treffer zu erwarten,
und fuer eine belastbare Zahl braeuchte es ein Vielfaches. Das Merkmal existiert in der API
(`CardCollectionCardScoreBreakdown.shirtMatchingSerialNumber`, Trikotnummer ueber
`anyPlayer.shirtNumber`), lohnt aber erst, wenn wir Kartenmerkmale dauerhaft mitschreiben.

## Empfehlung

1. **Spezialedition einbauen**, zweiseitig: Bei der FMV-Berechnung Verkaeufe von
   Spezialeditionen um den gemessenen Aufschlag nach unten korrigieren, damit sie den Wert
   normaler Exemplare nicht heben. Und umgekehrt im Portfolio auf eine Spezialedition
   aufschlagen. Dasselbe Muster wie `FALLBACK_FACTOR` in v3.6.
2. **Level NICHT einbauen.** Der Befund ist eindeutig und hat einen klaren Grund.
3. **Trikotnummer vertagen**, bis Kartenmerkmale je Verkauf gespeichert werden.

## Der groessere Punkt

Der FMV ist heute ein Wert je (Spieler, Rarity, Eligibility), also ein Durchschnitt ueber
Exemplare. Im Portfolio sieht Jonas damit fuer SEINE konkrete Karte den Durchschnittswert.
Sobald Kartenmerkmale mitlaufen, liesse sich daraus ein Wert je Exemplar machen. Die
Spezialedition waere der erste Baustein, die Seriennummer (niedrige Nummern sind gesucht)
der naechste Kandidat fuer eine Messung.
