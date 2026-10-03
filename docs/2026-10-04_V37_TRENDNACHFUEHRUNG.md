# FMV v3.7: Trendnachfuehrung (04.10.2026)

**Anstoss (Jonas):** "Wenn eine Karte immer wieder unter unserem FMV gehandelt wird, sollten wir
den FMV runtersetzen. So wie jetzt in der Anfangsphase der Saison: Karten haben viel Wert, fallen
aber immer weiter."

## Der Befund

Marktweit sah nichts auffaellig aus: Die taegliche Abweichung zwischen FMV und tatsaechlichen
Manager-Verkaeufen lag seit dem 24.09. zwischen -4 und +5 %, rund die Haelfte der Verkaeufe unter
dem Wert. Der Fehler zeigt sich erst **bedingt auf die Vorgeschichte** (fmv_accuracy, 21 Tage,
nur TokenOffer):

| Vorherige Verkaeufe unter FMV | Faelle | Abweichung beim naechsten Verkauf | wieder unter FMV |
|---|---|---|---|
| 0 von 3 | 32.332 | **+8,0 %** | 30 % |
| 1 von 3 | 43.659 | -1,3 % | 51 % |
| 2 von 3 | 36.539 | -4,4 % | 57 % |
| **3 von 3** | 31.388 | **-11,3 %** | **72 %** |

Der Wert hinkt also in BEIDE Richtungen hinterher. Jonas faellt die Abwaertsrichtung auf, weil zum
Saisonstart viele Karten anhaltend fallen.

## Was NICHT gemacht wurde

Der naheliegende Weg, bei "3 von 3" einfach 11 % abzuziehen, wurde geprueft und verworfen:

| Variante | abwaerts Median | abwaerts Bias | Sprunghoehe |
|---|---|---|---|
| v3.6 | ±13,4 % | -4,7 % | 5,6 % |
| fester Abschlag | ±12,7 % | **+7,5 %** | **8,9 %** |
| **Halbwertszeit / 3** | **±9,1 %** | **0,0 %** | 6,4 % |

Der feste Abschlag schiesst ueber (wir laegen dann zu niedrig) und macht den Wert unruhig.

## Was gemacht wurde (Prinzip 8)

Zeigen die letzten DREI verwendeten Verkaeufe alle in dieselbe Richtung, also alle unter oder alle
ueber dem unkorrigierten Wert, wird mit **halfLife / 3** neu gewichtet. Die Formel folgt dem Markt
dann von selbst, symmetrisch in beide Richtungen, ohne Auf- oder Abschlag.

## Ergebnis (Gegenprobe mit der fertigen lib/fmv.mjs, 600 Karten, 11.750 frische Verkaeufe)

| Teilmenge | v3.6 | v3.7 | Bias v3.6 -> v3.7 | Ziele |
|---|---|---|---|---|
| **abwaerts** (3 von 3 darunter) | ±13,1 % | **±9,0 %** | -4,4 -> **0,0 %** | 717 |
| **aufwaerts** (3 von 3 darueber) | ±19,9 % | **±17,0 %** | +4,9 -> +2,0 % | 588 |
| gesamt | ±20,6 % | ±20,2 % | -0,9 -> -0,2 % | 7.024 |

Ueber alle Ziele aendert sich wenig, denn das Signal greift bei etwa jedem siebten Verkauf. Es ist
eine gezielte Korrektur genau dort, wo Nutzer den Fehler bemerken.

**Preis:** Die Sprunghoehe steigt von 5,6 auf 6,4 %. Der Wert ist also etwas beweglicher, bleibt
aber ruhiger als jede Abschlagsvariante und als der Last-5-Schnitt des Wettbewerbs (6,1 %).

**Smoke-Test:** 9 Faelle gegen v3.6, alle bestanden (anhaltend fallend folgt nach unten, anhaltend
steigend nach oben, gemischte Lage unveraendert, unter 3 Verkaeufen kein Signal, Rueckfallebene und
Floor-Deckel unveraendert).

**Aenderungssperre:** `migrations/2026-10-04_fmv_v37_change_guard.sql`, Kante **05.10.**, ausgefuehrt.
