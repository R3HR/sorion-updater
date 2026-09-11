# v3.6: Auktionen und Sofortkaeufe als Rueckfallebene (2026-09-11T18:45 UTC)

Zielmenge: 7302 Walk-Forward-Ziele, Ziel ist immer ein Manager-Verkauf.
Median = mittlere absolute Abweichung, kleiner ist besser. Bias positiv = zu niedrig geschaetzt.


## GESAMT (n=7302)

| Variante | Median | Bias | ohne Wert | gemischt gerechnet |
|---|---|---|---|---|
| v3.3 (heute live, alles roh) | ±26.5 % | -0.6 % | 0 % | 31 % |
| v3.5 (nur Manager) | ±24.3 % | +2.3 % | 3 % | 0 % |
| A Fallback roh, wenn <1 Manager | ±24.5 % | +1.9 % | 0 % | 3 % |
| B Fallback roh, wenn <3 Manager | ±25.3 % | +0.8 % | 0 % | 13 % |
| F nur-wenn-noetig, mild 0,75/0,60 | ±24.7 % | +2.4 % | 0 % | 3 % |
| G nur-wenn-noetig, 0,85/0,70 | ±24.5 % | +2.2 % | 0 % | 3 % |
| H nur-wenn-noetig, 0,90/0,80 | ±24.5 % | +2.1 % | 0 % | 3 % |
| I nur-wenn-noetig, voll umgerechnet | ±24.8 % | +2.5 % | 0 % | 3 % |

## limited/classic (n=2827)

| Variante | Median | Bias | ohne Wert | gemischt gerechnet |
|---|---|---|---|---|
| v3.3 (heute live, alles roh) | ±22.8 % | +0.2 % | 0 % | 0 % |
| v3.5 (nur Manager) | ±22.8 % | +0.2 % | 0 % | 0 % |
| A Fallback roh, wenn <1 Manager | ±22.8 % | +0.2 % | 0 % | 0 % |
| B Fallback roh, wenn <3 Manager | ±22.8 % | +0.2 % | 0 % | 0 % |
| F nur-wenn-noetig, mild 0,75/0,60 | ±22.8 % | +0.2 % | 0 % | 0 % |
| G nur-wenn-noetig, 0,85/0,70 | ±22.8 % | +0.2 % | 0 % | 0 % |
| H nur-wenn-noetig, 0,90/0,80 | ±22.8 % | +0.2 % | 0 % | 0 % |
| I nur-wenn-noetig, voll umgerechnet | ±22.8 % | +0.2 % | 0 % | 0 % |

## limited/in_season (n=3627)

| Variante | Median | Bias | ohne Wert | gemischt gerechnet |
|---|---|---|---|---|
| v3.3 (heute live, alles roh) | ±31.0 % | -1.7 % | 0 % | 49 % |
| v3.5 (nur Manager) | ±25.7 % | +3.8 % | 4 % | 0 % |
| A Fallback roh, wenn <1 Manager | ±26.1 % | +3.0 % | 0 % | 4 % |
| B Fallback roh, wenn <3 Manager | ±27.5 % | +1.7 % | 0 % | 16 % |
| F nur-wenn-noetig, mild 0,75/0,60 | ±26.3 % | +3.7 % | 0 % | 4 % |
| G nur-wenn-noetig, 0,85/0,70 | ±26.1 % | +3.6 % | 0 % | 4 % |
| H nur-wenn-noetig, 0,90/0,80 | ±26.1 % | +3.3 % | 0 % | 4 % |
| I nur-wenn-noetig, voll umgerechnet | ±26.2 % | +3.9 % | 0 % | 4 % |

## rare/in_season (n=848)

| Variante | Median | Bias | ohne Wert | gemischt gerechnet |
|---|---|---|---|---|
| v3.3 (heute live, alles roh) | ±26.1 % | -2.9 % | 0 % | 61 % |
| v3.5 (nur Manager) | ±24.2 % | +3.1 % | 14 % | 0 % |
| A Fallback roh, wenn <1 Manager | ±24.6 % | +0.9 % | 0 % | 14 % |
| B Fallback roh, wenn <3 Manager | ±25.3 % | -0.9 % | 0 % | 41 % |
| F nur-wenn-noetig, mild 0,75/0,60 | ±25.2 % | +3.9 % | 0 % | 14 % |
| G nur-wenn-noetig, 0,85/0,70 | ±24.8 % | +2.8 % | 0 % | 14 % |
| H nur-wenn-noetig, 0,90/0,80 | ±24.3 % | +2.0 % | 0 % | 14 % |
| I nur-wenn-noetig, voll umgerechnet | ±26.2 % | +5.6 % | 0 % | 14 % |

## NUR die Faelle, in denen v3.5 gar nichts liefert (n=249)

| Variante | Median | Bias | ohne Wert | gemischt gerechnet |
|---|---|---|---|---|
| v3.3 (heute live, alles roh) | ±33.3 % | -21.6 % | 0 % | 100 % |
| A Fallback roh, wenn <1 Manager | ±33.3 % | -21.6 % | 0 % | 100 % |
| B Fallback roh, wenn <3 Manager | ±33.3 % | -21.6 % | 0 % | 100 % |
| F nur-wenn-noetig, mild 0,75/0,60 | ±33.3 % | +6.2 % | 0 % | 100 % |
| G nur-wenn-noetig, 0,85/0,70 | ±33.6 % | -5.6 % | 0 % | 100 % |
| H nur-wenn-noetig, 0,90/0,80 | ±33.3 % | -11.2 % | 0 % | 100 % |
| I nur-wenn-noetig, voll umgerechnet | ±47.4 % | +37.1 % | 0 % | 100 % |
