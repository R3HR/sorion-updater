# Nur Manager-Verkaeufe: Fensterverlaengerung statt Datenverlust (2026-09-07T12:34 UTC)

Anschluss an `2026-09-07_ZWEITMARKT_FILTER.md`. Die Vorgabe (nur TokenOffer) steht fest.
Hier geht es nur darum, wie mit Karten umzugehen ist, deren Manager-Basis zu duenn wird.
Fremde Verkaufsarten kommen in KEINER Variante zurueck, es wird nur weiter zurueckgeschaut.

Zielmenge: 7029 Walk-Forward-Ziele, Ziel ist immer ein Manager-Verkauf.
Median = mittlere absolute Abweichung, kleiner ist besser. Bias positiv = zu niedrig geschaetzt.


## limited/classic (n=2766)

| Variante | Median | Bias | ohne Schaetzung | davon Fenster gedehnt |
|---|---|---|---|---|
| A heute (alle Arten) | ±22.7 % | +0.6 % | 0 % | 0 % |
| B nur Manager | ±22.8 % | +0.7 % | 0 % | 0 % |
| C nur Manager, Fenster x2 | ±22.5 % | +0.2 % | 0 % | 6 % |
| D nur Manager, Fenster x3 | ±22.5 % | +0.1 % | 0 % | 7 % |
| E nur Manager, Fenster x5 | ±22.6 % | +0.0 % | 0 % | 7 % |

## limited/in_season (n=3504)

| Variante | Median | Bias | ohne Schaetzung | davon Fenster gedehnt |
|---|---|---|---|---|
| A heute (alle Arten) | ±31.1 % | -2.2 % | 0 % | 0 % |
| B nur Manager | ±26.0 % | +4.7 % | 4 % | 0 % |
| C nur Manager, Fenster x2 | ±25.8 % | +4.3 % | 4 % | 13 % |
| D nur Manager, Fenster x3 | ±25.7 % | +4.2 % | 3 % | 15 % |
| E nur Manager, Fenster x5 | ±25.6 % | +4.1 % | 3 % | 15 % |

Bei den 31 Zielen, die B gar nicht schaetzen koennte, trifft C auf ±29.4 %
(heutiger Stand mit allen Arten dort: ±31.4 %).

## rare/in_season (n=759)

| Variante | Median | Bias | ohne Schaetzung | davon Fenster gedehnt |
|---|---|---|---|---|
| A heute (alle Arten) | ±24.8 % | -2.6 % | 0 % | 0 % |
| B nur Manager | ±25.9 % | +9.3 % | 19 % | 0 % |
| C nur Manager, Fenster x2 | ±25.9 % | +7.7 % | 15 % | 28 % |
| D nur Manager, Fenster x3 | ±25.4 % | +6.4 % | 15 % | 33 % |
| E nur Manager, Fenster x5 | ±25.4 % | +6.0 % | 14 % | 36 % |

Bei den 27 Zielen, die B gar nicht schaetzen koennte, trifft C auf ±69.4 %
(heutiger Stand mit allen Arten dort: ±45.1 %).

## ALLE (n=7029)

| Variante | Median | Bias | ohne Schaetzung | davon Fenster gedehnt |
|---|---|---|---|---|
| A heute (alle Arten) | ±26.3 % | -0.6 % | 0 % | 0 % |
| B nur Manager | ±24.4 % | +3.2 % | 4 % | 0 % |
| C nur Manager, Fenster x2 | ±24.3 % | +2.8 % | 3 % | 12 % |
| D nur Manager, Fenster x3 | ±24.3 % | +2.6 % | 3 % | 13 % |
| E nur Manager, Fenster x5 | ±24.3 % | +2.4 % | 3 % | 14 % |

Bei den 58 Zielen, die B gar nicht schaetzen koennte, trifft C auf ±38.8 %
(heutiger Stand mit allen Arten dort: ±38.1 %).
