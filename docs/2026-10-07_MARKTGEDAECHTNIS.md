# Dauerhaftes Marktgedaechtnis (07.10.2026)

**Frage Jonas:** "Koennen wir in einem Jahr sagen, was eine Karte heute gekostet hat, und
vergleichen?"

## Lage vorher: nein, jedenfalls nicht belastbar

Zwei Putzdienste raeumten genau das weg:

| Job | Zeitplan | Wirkung |
|---|---|---|
| `snapshot_fmv_accuracy` | taeglich 05:45 | **loescht Einzelverkaeufe aelter als 90 Tage** |
| `price_history_rollup(90)` | woechentlich Mo 06:30 | behaelt aelter als 90 Tage nur EINEN FMV-Punkt je Karte und Woche |

Damit waere in einem Jahr nur noch erhalten, was wir DACHTEN (wochengenau), nicht, was eine Karte
tatsaechlich gekostet hat. Dazu kommt: Ein Jahresvergleich unseres FMV vergleicht auch unsere
eigenen Formelstaende (v3.2 bis v3.7). Echte Verkaufspreise haben dieses Problem nicht.

## Jetzt: `card_sales_weekly`

Je Karte und Kalenderwoche eine Zeile: Zahl der Manager-Verkaeufe, Median, Minimum, Maximum, dazu
als Kontext Zahl und Median ueber alle Verkaufsarten. **Wird nicht aufgeraeumt.**

- Gefuellt von `rollup_card_sales_weekly(p_days)`, idempotent (Upsert je Woche und Karte).
- Cron `card_sales_weekly_daily`, taeglich **05:15 UTC**, Fenster 4 Tage. Laeuft bewusst VOR dem
  Loeschdienst um 05:45 und bleibt klein genug fuer die gedrosselte DB (Lehre INC-005).
- Zugriff vorerst intern (RLS an, kein anon/authenticated), wie `price_history`.

**Nachgetragen:** 152.088 Zeilen, 8 Wochen zurueck bis 17.08.2026. Weiter reicht es nicht, aelter
war schon geloescht.

**Groesse: 39 MB fuer 8 Wochen, hochgerechnet rund 255 MB im Jahr** bei etwa 19.000 gehandelten
Karten je Woche. Meine erste Schaetzung von 60 bis 80 MB war zu optimistisch (sie ging von 14.500
Karten aus und unterschlug den Index). Bei 825 MB Datenbank und 8 GB im Pro-Tarif unkritisch,
aber es ist der groesste wachsende Posten nach den Rohdaten.

**Luecke, die bleibt:** Die Verkaufsart wird erst seit dem 07.09. erfasst
(`2026-09-05_accuracy_deal_type.sql`, verspaetet ausgefuehrt). Fuer die Wochen davor steht
`mgr_n = 0`, obwohl Verkaeufe stattfanden. Nur `all_n` und `all_median` sind dort brauchbar.

## `fmv_versions`

Die Formel-Schnittkanten standen bisher nur im Code von `market_move`. Jetzt als Tabelle, oeffentlich
lesbar, damit jeder Langzeitvergleich sieht, welche Version wann gerechnet hat:
v3.2 ab 22.08., v3.3 ab 26.08., v3.5 ab 12.09., v3.7 ab 05.10. (v3.4 und v3.6 haben nie einen
eigenen Snapshot erzeugt, siehe INC-009).

## Beispiel (Lamine Yamal, Limited In-Season)

| Woche | Manager-Verkaeufe | Median | Spanne | alle Arten | Median alle |
|---|---|---|---|---|---|
| 28.09. | 13 | 256,10 | 232 bis 280 | 26 | 277,52 |
| 21.09. | 11 | 248,35 | 234 bis 258 | 29 | 260,85 |
| 14.09. | 8 | 245,31 | 216 bis 275 | 31 | 270,54 |
| 17.08. | 0 (noch nicht erfasst) | | | 13 | 358,31 |

Der Rueckgang von 358 auf unter 280 ueber sieben Wochen ist genau die Bewegung, die Jonas am
Saisonstart beobachtet hat.

## Offen

- Eine RPC, damit die Seite den Jahresverlauf je Karte zeigen kann (heute nur intern lesbar).
- Ueberlegung fuer spaeter: dieselbe Verdichtung monatlich zusaetzlich, falls die Wochenzeilen nach
  Jahren zu viel werden. Bei 255 MB im Jahr ist das fruehestens in drei bis vier Jahren ein Thema.
