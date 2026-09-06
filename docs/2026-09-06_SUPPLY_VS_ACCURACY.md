# Knappheit vs. FMV-Genauigkeit (2026-09-06T20:04 UTC)

Stichprobe: meistgehandelte Spieler je Segment, 14 Tage, mindestens 5 Verkaeufe je Spieler.
bias = (Verkauf - FMV) / FMV in %. **Positiv = wir schaetzen zu niedrig.**
median_abs = Treffgenauigkeit (kleiner ist besser).


## in_season limited (60 Spieler, 1200 Verkaeufe)

| Gruppe | Bestand | Spieler | median bias | median abs |
|---|---|---|---|---|
| knappste 25 % | 111 bis 153 | 15 | 8.2 % | 13.7 % |
| 2. Viertel | 154 bis 178 | 15 | 16.5 % | 19.3 % |
| 3. Viertel | 179 bis 239 | 15 | 6.8 % | 13.0 % |
| haeufigste 25 % | 241 bis 366 | 15 | 1.6 % | 25.6 % |

Rangkorrelation Bestand vs. Bias: **-0.201** (schwach)

## in_season rare (60 Spieler, 1064 Verkaeufe)

| Gruppe | Bestand | Spieler | median bias | median abs |
|---|---|---|---|---|
| knappste 25 % | 20 bis 28 | 15 | 18.2 % | 39.1 % |
| 2. Viertel | 28 bis 31 | 15 | 26.0 % | 34.9 % |
| 3. Viertel | 31 bis 36 | 15 | 14.9 % | 33.8 % |
| haeufigste 25 % | 36 bis 91 | 15 | 20.4 % | 31.7 % |

Rangkorrelation Bestand vs. Bias: **-0.089** (kein Zusammenhang)

## classic limited (60 Spieler, 1200 Verkaeufe)

| Gruppe | Bestand | Spieler | median bias | median abs |
|---|---|---|---|---|
| knappste 25 % | 414 bis 1745 | 15 | 11.0 % | 22.5 % |
| 2. Viertel | 1773 bis 2103 | 15 | -3.0 % | 21.2 % |
| 3. Viertel | 2113 bis 2730 | 15 | 13.2 % | 22.5 % |
| haeufigste 25 % | 2731 bis 3397 | 15 | 0.1 % | 13.0 % |

Rangkorrelation Bestand vs. Bias: **-0.177** (kein Zusammenhang)

## classic rare (60 Spieler, 934 Verkaeufe)

| Gruppe | Bestand | Spieler | median bias | median abs |
|---|---|---|---|---|
| knappste 25 % | 184 bis 278 | 15 | 16.9 % | 26.0 % |
| 2. Viertel | 305 bis 387 | 15 | 10.1 % | 11.7 % |
| 3. Viertel | 389 bis 472 | 15 | 12.5 % | 13.4 % |
| haeufigste 25 % | 475 bis 599 | 15 | -4.9 % | 14.3 % |

Rangkorrelation Bestand vs. Bias: **-0.319** (schwach)

## Einordnung (Claude, 06.09.2026)

### Zur Ausgangsfrage: Knappheit als FMV-Faktor -> **nein, nicht auf dieser Basis**

Die Rangkorrelation ist in **allen vier Segmenten negativ** (-0,20 / -0,09 / -0,18 / -0,32),
also in der erwarteten Richtung: knappe Karten werden von uns eher unterschaetzt. Vier von
vier gleiche Richtung ist kein Zufall (bei Muenzwurf 1:16). **Aber die Staerke reicht nicht:**
Die Quartile zeigen kein sauberes Muster (in_season limited: 8,2 -> 16,5 -> 6,8 -> 1,6 %),
und bei in_season rare ist der Zusammenhang praktisch null. Eine Formelaenderung auf dieser
Grundlage waere Ueberanpassung an 60 Spieler je Segment.

**Empfehlung:** Bestand NICHT in die Formel aufnehmen. Stattdessen die Messung in 2 bis 4
Wochen wiederholen, wenn der Updater `card_prices.supply` flaechendeckend gefuellt hat.
Dann ueber ALLE Spieler statt 60 je Segment, und mit `deal_type` kombinierbar. Das Werkzeug
dafuer ist `tools/2026-09-06_supply-vs-accuracy.mjs`.

### Der groessere Fund: bei liquiden Karten schaetzen wir zu NIEDRIG

Der Bias ist in fast allen Gruppen **positiv**, teils deutlich (in_season rare: +15 bis +26 %).
Positiv heisst: die tatsaechlichen Verkaeufe liegen ueber unserem FMV.

**Das widerspricht der Gesamtmessung** (`accuracy_benchmark`, alle Spieler: in_season limited
bias -13,3 %, rare -11,7 %, also dort schaetzen wir zu HOCH). Der Unterschied ist die Auswahl:
Diese Stichprobe nimmt gezielt die **meistgehandelten** Spieler (mindestens 5 Verkaeufe in
14 Tagen, Top 60 nach Verkaufszahl).

**Arbeitshypothese:** Bei aktiv gehandelten Karten hinkt unser FMV der Preisbewegung
hinterher. Wir mitteln ueber vergangene Verkaeufe; steigt der Markt (Saisonstart, In-Season),
liegen wir systematisch darunter. Bei illiquiden Karten dominiert der gegenteilige Effekt
(alter, zu hoch stehengebliebener Wert). Genau diese Schwaeche raeumt uebrigens auch
sowizzy fuer den eigenen Ansatz ein ("lags behind recent price movements", siehe WETTBEWERB.md).

**Was das wert waere:** Wenn sich das bestaetigt, ist die Halbwertszeit der Gewichtung der
Hebel, nicht der Bestand: bei hoher Verkaufsdichte kuerzer gewichten (v3.4-Kandidat, stand
nach dem avg5-Vergleich ohnehin schon auf der Liste). **Vor jeder Aenderung: Backtest ueber
alle Spieler, nicht ueber 60.**

### Einschraenkungen dieser Messung

- 60 Spieler je Segment, bewusst die liquidesten. Nicht repraesentativ fuer den Gesamtmarkt.
- 14-Tage-Fenster, enthaelt den Saisonstart (steigende In-Season-Preise).
- Bestand von Sorare zum Messzeitpunkt, nicht zum Verkaufszeitpunkt (aendert sich langsam,
  aber neue Karten werden waehrend der Saison gepraegt).
- **Formel bleibt unveraendert.** Entscheidung ueber v3.4 liegt bei Jonas, nach breiterem Backtest.

### Generische Lesehilfe (aus dem Werkzeug)

- Ein **negativer** Zusammenhang hiesse: je haeufiger die Karte, desto niedriger der Bias,
  also schaetzen wir knappe Karten zu niedrig. Dann waere Bestand ein Formel-Kandidat.
- Liegt die Rangkorrelation nahe null, traegt Knappheit nichts bei, was der Preis nicht
  schon enthaelt (die Verkaeufe selbst spiegeln die Knappheit bereits).
- **Formel bleibt unveraendert, bis Jonas entscheidet.**
