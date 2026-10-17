# Bei Rare schlaegt der blosse Floor unseren FMV (11.10.2026)

**Anstoss Jonas:** "Bei Rare sieht unser FMV im Vergleich zum Floor echt schlecht aus."
Dazu sein zweiter, berechtigter Einwand: "Wieso berechnest du das nochmal, wir messen das doch
permanent?"

## Teil 1: Wir messen permanent, aber das Falsche (behoben)

Seit v3.5 rechnet der FMV NUR mit Manager-Verkaeufen. Beide Messungen zaehlten dagegen ALLE
Verkaufsarten, und `snapshot_fmv_accuracy` nahm fuer den Bias zusaetzlich den MITTELWERT.

Rare in-season, Verkaeufe seit 05.10., derselbe Zeitraum, drei Rechenwege:

| Methode | Bias |
|---|---|
| alle Arten, Mittelwert (so lief der Job) | **+33,8 %** |
| alle Arten, Median | +5,0 % |
| nur Manager, Median (Grundlage des FMV) | **-5,1 %** |

Der Mittelwert wurde von Ausreissern getragen: groesste Einzelabweichung +9.324 %, 221 Verkaeufe
ueber +300 %. Auf accuracy.html standen dadurch oeffentlich +25 bis +42 % fuer Rare in-season,
also das falsche Vorzeichen.

**Behoben** (`2026-10-11_messung_auf_managerverkaeufe.sql`): beide Funktionen filtern jetzt auf
`deal_type = 'TokenOffer'`, der Tages-Snapshot nimmt den Median. Die Tageswerte ab 08.09. wurden
aus den noch vorhandenen Rohdaten neu berechnet. Rare liegt real bei ±20 bis 24 % mit Bias -3
bis -5,5 %.

## Teil 2: Der eigentliche Befund

`accuracy_benchmark(7)`, nur Manager-Verkaeufe, FMV gegen die beiden einfachen Schaetzer:

| Segment | Schaetzer | Median-Abw | Bias | Treffer ±20 % |
|---|---|---|---|---|
| limited in-season | **fmv** | **16,2 %** | +2,1 % | **56,3 %** |
| limited in-season | floor | 17,1 % | +7,8 % | 53,8 % |
| limited classic | **fmv** | **20,0 %** | +1,6 % | **51,0 %** |
| limited classic | floor | 26,3 % | +14,3 % | 45,3 % |
| **rare in-season** | fmv | 23,6 % | -5,5 % | 44,9 % |
| **rare in-season** | **floor** | **12,0 %** | **+1,6 %** | **60,7 %** |
| rare classic | fmv | 22,0 % | -0,9 % | 46,8 % |
| rare classic | **floor** | **20,8 %** | +6,8 % | **49,5 %** |
| super_rare classic | fmv | 31,6 % | -7,8 % | 30,3 % |
| super_rare classic | **floor** | **27,5 %** | -0,8 % | **39,8 %** |

**Bei Limited gewinnt unser FMV klar. Bei Rare in-season verliert er deutlich: der blosse Floor
trifft doppelt so genau (12,0 gegen 23,6 %) und haeufiger im ±20-%-Band (60,7 gegen 44,9 %).**
Auch bei rare classic und super_rare classic liegt der Floor vorn.

`avg_sales` ist ueberall der schlechteste Schaetzer, das bleibt so.

## Warum vermutlich

Die Formel ist an Limited kalibriert: Halbwertszeit 3 Tage, Fenster 21 Tage, Deckel erst bei
Floor x 1,5. Bei Rare gibt es viel weniger Manager-Verkaeufe je Karte, die Basis ist also duenn
und alt, waehrend der Floor ein taggenaues, direktes Signal ist. Der Deckel mit Faktor 1,5 laesst
zu viel Luft nach oben, was zum negativen Bias passt (wir schaetzen zu hoch).

## Naechster Schritt (Entscheidung Jonas)

Kandidaten fuer v3.8, zu messen wie immer per Walk-Forward, getrennt je Segment:
1. **Floor-Anteil fuer Rare** (FLOOR_BLEND > 0 nur dort), seit v3.2 steht er global auf 0.
2. **Engerer Deckel fuer Rare**, etwa Floor x 1,15 statt x 1,5.
3. **Eigenes Profil fuer Rare**: laengeres Fenster, andere Halbwertszeit.
4. **Floor als Schaetzer uebernehmen**, wenn im Fenster weniger als N Manager-Verkaeufe liegen.

Wichtig dabei: Der Floor ist ein ANGEBOT, kein Verkauf. Er kann von einem einzelnen Verkaeufer
gesetzt werden und ist manipulierbar, der Backtest muss das Risiko mitbewerten (Sprunghoehe,
Ausreisser), nicht nur den Median.

---

## Umsetzung: v3.8, Floor-Anteil je Verkaufsdichte (11.10.2026)

Gemessen an echten Manager-Verkaeufen der letzten 30 Tage, direkt auf `fmv_accuracy`
(dort steht zu jedem Verkauf der FMV UND der Floor von damals):

| Segment | heute | fest 70 % Floor | **individuell** | nur Floor |
|---|---|---|---|---|
| rare in-season | 24,7 % | 16,4 % | **16,0 %** | 15,5 % |
| rare classic | 25,6 % | 22,2 % | **21,8 %** | 22,9 % |
| super_rare classic | 32,3 % | 26,2 % | **25,6 %** | 26,6 % |

**Gewaehlt: individuell je Karte**, Gewicht nach der eigenen Zahl von Manager-Verkaeufen im
Fenster: bis 1 Verkauf 80 % Floor, bis 3 65 %, bis 6 45 %, bis 12 25 %, darueber 10 %.

Warum nicht einfach der Floor, der bei in-season minimal besser trifft: Er ist ein ANGEBOT, von
einem einzelnen Verkaeufer setzbar, und hat spuerbar mehr grobe Fehler (9,9 gegen 7,2 % ueber
100 % Abweichung). Die Mischung behaelt den Verkaufsanker, trifft praktisch gleich gut und bringt
den Bias auf null (+0,1 statt -4,1 %).

**Bewusste Ausnahme zu Prinzip 2:** Bei Rare zieht der Floor auch NACH OBEN. Das widerspricht
"ein hoher Ask hebt die Verkaeuflichkeit nicht", traegt dort aber eindeutig:

| Variante | rare in-season Median | davon Faelle mit Floor UEBER dem Wert |
|---|---|---|
| beide Richtungen | **16,0 %** | **11,9 %** |
| nur nach unten | 20,0 % | 21,4 % |

Bei duenner Rare-Basis ist unser Verkaufswert oft veraltet, der Floor dagegen taggenau. Fuer
Limited bleibt Prinzip 2 unangetastet, dort ist der Floor-Anteil 0.

**Smoke-Test:** 9 Faelle, alle bestanden (limited unveraendert, rare mit duenner Basis stark
Richtung Floor, mit dichter Basis nur leicht, ohne Floor und ohne scarcity-Angabe unveraendert).

**Aenderungssperre:** `2026-10-11_fmv_v38_change_guard.sql`, Kante 12.10., ausgefuehrt.
`fmv_versions` ergaenzt.

**Nachkontrolle in 3 bis 4 Tagen:** `accuracy_benchmark(3)` je Segment. Erwartung: rare in-season
von 23,6 auf etwa 16 %, rare classic von 22,0 auf etwa 22 bei deutlich besserem Bias, und der FMV
sollte den Floor dann in allen Segmenten schlagen oder einholen.

---

## Nachkontrolle 17.10.: v3.8 war zu zaghaft, daher v3.9

**Jonas:** "Unsere Genauigkeitstabelle laedt nicht, ausserdem ist unsere Genauigkeit mittlerweile
deutlich schlechter als der Floor."

**Zur Tabelle (zwei Fehler, behoben):** `fmv_accuracy_daily` hatte RLS an, aber weder Policy noch
Leserecht, die Seite bekam 401. Und der Fallback der Seite rief `accuracy_benchmark` ohne Argument
auf, also mit 30 Tagen, und lief nach 3 Sekunden in den anon-Timeout. Jetzt: Leserecht plus Policy,
Standard auf 7 Tage begrenzt, Index auf (deal_type, created_at). Von aussen geprueft: beide
Aufrufe antworten mit 200.

**Zur Genauigkeit:** v3.8 lief korrekt (FMV/Floor bei rare in-season von 1,149 auf 1,070), aber die
Wirkung reichte nicht. Live gemessen an 2.526 Rare-Verkaeufen seit dem 13.10.:

| Segment | FMV (v3.8) | Floor |
|---|---|---|
| rare in-season | 19,9 % | **13,7 %** |
| rare classic | 23,0 % | **17,7 %** |
| limited in-season | 19,4 % | **18,3 %** |
| limited classic | **21,6 %** | 29,4 % |

**Der Grund:** 90 % aller Rare-Verkaeufe betreffen Karten mit HOECHSTENS EINEM Manager-Verkauf in
21 Tagen. Genau dort steckte trotz v3.8 noch zu viel Eigenwert drin. Ein weiterer Schritt zum
Floor senkt den Fehler in dieser Gruppe von 20,1 auf 13,1 %.

**v3.9:** Gewichte je Liquiditaetsstufe neu, geprueft je Stufe statt pauschal.
Rare und Super Rare: 0,90 bei hoechstens 1 Verkauf, dann 0,70 / 0,50 / 0,30 / 0,15.
Limited in-season neu 0,20 (bis 6 Verkaeufe) bzw. 0,10. Limited classic bleibt bei 0, dort
gewinnt der FMV deutlich.

**Preis:** Grobe Fehler ueber 100 % steigen in der duennen Gruppe von 5,3 auf etwa 8,8 %. Das ist
der Beitrag des Floors, der ein Angebot bleibt und von einem einzelnen Verkaeufer gesetzt werden
kann. Deshalb bleibt es bei 0,90 statt 1,0: der Verkaufsanker daempft Ausreisser.

Smoke-Test: 9 Faelle, alle bestanden. Aenderungssperre Kante 18.10. gesetzt.

### Out-of-Sample-Test (17.10., auf Nachfrage Jonas)

Die Gewichte oben waren zunaechst IN-SAMPLE gewaehlt: an denselben Daten bestimmt, an denen sie
bewertet wurden. Nachgeholt: Gewichte aus dem 13.-15.10., Bewertung nur auf dem 16.-17.10.
Der reine Verkaufswert wird dabei aus dem v3.8-Ergebnis zurueckgerechnet
(`sv = (fmv - w8*floor) / (1 - w8)`), damit die Kandidaten sauber neu gebildet werden koennen.

| Segment | reiner Verkaufswert | v3.8 live | v3.9 (0,90) | **v3.9 (0,97)** | nur Floor |
|---|---|---|---|---|---|
| rare in-season, Median | 42,8 % | 19,7 % | 16,0 % | **14,2 %** | 14,7 % |
| rare in-season, Treffer ±20 % | | | 56,0 % | **57,6 %** | 56,5 % |
| rare in-season, grobe Fehler | 13,5 % | 5,4 % | 7,2 % | **9,3 %** | 10,9 % |
| limited in-season, Median | 20,0 % | 20,0 % | **19,3 %** | | 20,9 % |
| limited classic, Median | 21,6 % | 21,6 % | **21,6 %** | | 24,0 % |

**Erst mit 0,97 bei den duennsten Karten schlagen wir den Floor in allen drei Massen**: Median
14,2 gegen 14,7 %, Treffer 57,6 gegen 56,5 %, grobe Fehler 9,3 gegen 10,9 %. Die verbleibenden
3 % Verkaufswert sind kein Zierrat, sie daempfen genau die Ausreisser, die den reinen Floor
teuer machen.

**Bemerkenswert:** Unser reiner Verkaufswert liegt bei rare in-season 42,8 % daneben. Das ist der
eigentliche Grund fuer das ganze Thema: Bei Karten mit hoechstens einem Manager-Verkauf in drei
Wochen hat ein Durchschnitt ueber Verkaeufe schlicht keine Grundlage mehr.

**Endstand v3.9:** rare und super_rare 0,97 / 0,80 / 0,55 / 0,30 / 0,15 je nach Zahl der
Manager-Verkaeufe; limited in-season 0,20 bzw. 0,10; limited classic 0.

**Vorbehalt:** Der Test umfasst zwei Tage und 828 Rare-Verkaeufe. Die Nachkontrolle an echten
Live-Werten folgt, sobald v3.9 zwei bis drei Tage gerechnet hat.
