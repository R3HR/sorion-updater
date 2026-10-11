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
