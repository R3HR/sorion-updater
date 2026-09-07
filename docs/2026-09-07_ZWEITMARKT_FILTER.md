# FMV nur aus Manager-Verkaeufen: Abdeckung und Genauigkeit (2026-09-07T12:32 UTC)

Datensatz: 798 Karten-Zeilen, je bis zu 20 Verkaeufe mit Verkaufsart.

## 1. Abdeckung: wie viele Karten behalten eine Basis?

Gezaehlt wird pro Karte, wie viele Verkaeufe im Bewertungsfenster liegen (In-Season 21 Tage,
Classic 90 Tage) — einmal mit allen Arten, einmal nur mit Manager-Verkaeufen.

| Segment | Karten | mit Basis (alle) | mit Basis (nur Manager) | Verlust | >=3 Sales (alle) | >=3 (nur Manager) |
|---|---|---|---|---|---|---|
| limited/classic | 198 | 184 (93 %) | 184 (93 %) | **−0 %** | 162 | 162 |
| limited/in_season | 400 | 304 (76 %) | 293 (73 %) | **−4 %** | 285 | 244 |
| rare/in_season | 200 | 165 (83 %) | 125 (63 %) | **−24 %** | 145 | 47 |
| ALLE | 798 | 653 (82 %) | 602 (75 %) | **−8 %** | 592 | 453 |

## 2. Genauigkeit gegen echte Manager-Verkaeufe (7029 Ziele)

Gemessen wird gegen Manager-Verkaeufe, denn nur die sind Marktpreise. "alle Arten" ist
der heutige Stand, "nur Manager" die Vorgabe. `ohne Schaetzung` = keine Manager-Verkaeufe
in der Historie, die Karte bekaeme gar keinen FMV mehr.

| Segment | Ziele | alle Arten | nur Manager | ohne Schaetzung |
|---|---|---|---|---|
| limited/classic | 2766 | ±22.7 % (auf gleicher Menge ±22.7 %) | ±22.8 % ❌ | 0 % |
| limited/in_season | 3504 | ±31.1 % (auf gleicher Menge ±30.5 %) | ±26.0 % ✅ | 4 % |
| rare/in_season | 759 | ±24.8 % (auf gleicher Menge ±23.7 %) | ±25.9 % ❌ | 19 % |
| ALLE | 7029 | ±26.3 % (auf gleicher Menge ±25.9 %) | ±24.4 % ✅ | 4 % |

---

## 3. Entscheidung und Umsetzung (07.09.2026)

Die Vorgabe steht fest: **nur Manager-Verkaeufe (TokenOffer) bestimmen den Wert.**
Auktion und Sofortkauf sind durch Rabattgutscheine (bis 50 %) und Zugaben (Essence,
Wheel-Tickets) verzerrt. 42 % aller Datenpunkte fallen damit weg.

Umgesetzt als **FMV v3.5** in `lib/fmv.mjs`, mit zwei Zusaetzen, die die Messung
noetig gemacht hat:

**Prinzip 5, Fensterverlaengerung** (`WINDOW_STRETCH = 3`): Liegen weniger als 3
Manager-Verkaeufe im Fenster, wird bis zum dreifachen Zeitraum zurueckgeschaut.
Es bleiben echte Marktpreise, nur aeltere; durch den Zeit-Decay wiegen sie ohnehin
wenig. Details: `2026-09-07_ZWEITMARKT_FALLBACK.md`.

**Prinzip 6, Liquiditaet aus allen Verkaufsarten**: Der Sicherheitsdeckel
(Floor x 1,5) fragt nicht nach dem Wert, sondern ob die Karte ueberhaupt lebhaft
gehandelt wird. Dafuer zaehlt JEDE Verkaufsart. Ohne das rutschten Karten allein
durch den Filter unter die Schwelle: bei rare in-season stieg die Deckelquote von
23 auf 33 %, die betroffenen Werte halbierten sich im Median und trafen schlechter
(±32,9 -> ±44,2 %). Ein Auktionspreis fliesst weiterhin in KEINEN Wert ein.

### Ergebnis (Gegencheck mit der fertigen `lib/fmv.mjs`, 7.029 Ziele)

| Segment | v3.4 | v3.5 | Bias v3.5 | Karten ohne Wert |
|---|---|---|---|---|
| limited/classic | ±22,7 % | **±22,4 %** | +0,1 % | 0 % |
| limited/in_season | ±31,1 % | **±25,2 %** | +3,3 % | 3 % |
| rare/in_season | ±24,8 % | **±22,8 %** | +2,9 % | 15 % |
| **ALLE** | ±26,3 % | **±23,8 %** | +2,1 % | 3 % |

Besser in jedem Segment. Zwei Punkte bleiben offen und sind bewusst in Kauf genommen:

- **15 % der rare/in_season-Karten bekommen keinen FMV mehr.** Fuer sie gibt es
  schlicht keine Manager-Verkaeufe. Kein Wert ist richtiger als ein von Rabatten
  gedrueckter Wert. Die UI muss das sauber anzeigen (offen).
- **Grobe Fehlschaetzungen (>100 % daneben) steigen von 7,4 auf 8,4 %.** Preis der
  duenneren Basis. Der Median sinkt dafuer deutlich.

Der Bias dreht von -0,6 auf +2,1 %: auf Manager-Verkaeufe gefiltert schaetzen wir
eher zu niedrig. Das bestaetigt die Begruendung der Vorgabe - die Sorare-Maerkte
haben den Wert bisher nach unten gezogen. Ein Restbias von +2,1 % bleibt als
Kandidat fuer eine spaetere Feinjustierung.
