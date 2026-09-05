
# Verkaufsarten in Sorares tokenPrices (Messung 2026-09-05T10:27 UTC)

Stichprobe: 180 Spieler, 3598 "Sales" (je Spieler die letzten bis zu 20).

## Anteil der Verkaufsarten (nach Anzahl)

| Segment | Sales | Sofortkauf Sorare | Auktion Sorare | Manager→Manager | **Sorare gesamt** |
|---|---|---|---|---|---|
| in_season limited | 2000 | 36.5% | 42.8% | 20.6% | **79.3%** |
| in_season rare | 798 | 22.9% | 28.2% | 48.9% | **51.1%** |
| classic limited | 800 | 0.0% | 0.0% | 100.0% | **0.0%** |
| ALLE | 3598 | 25.4% | 30.0% | 44.6% | **55.4%** |

## Preisabstand innerhalb desselben Spielers (nur Spieler mit beiden Arten im Fenster)

Median von (Manager-Preis / Sorare-Preis) − 1: negativ = Zweitmarkt liegt unter Sorare.

| Segment | Spieler mit beidem | Manager vs Sofortkauf | Manager vs Auktion |
|---|---|---|---|
| in_season limited | 96 | -11.8% (n=96) | -3.3% (n=96) |
| in_season rare | 39 | -33.6% (n=38) | -9.5% (n=39) |
| classic limited | 0 | zu wenig Daten | zu wenig Daten |
| ALLE | 135 | -15.0% (n=134) | -4.2% (n=135) |

## Einordnung

**Haltung (Jonas, 05.09.): Sorion ist Verbuendeter von Sorare, nicht Gegner.** Sorare
verdient an Handel. Wer den Markt versteht, handelt mehr und sicherer. Die Zahlen hier
sind deshalb Marktkunde, keine Anklage: Sorare betreibt einen Primaermarkt (Sofortkauf,
Auktion) UND es gibt einen Zweitmarkt (Manager zu Manager). Beides in einer Preishistorie
zu sehen, ist fuer Sorare bequem, fuer Manager aber schwer zu lesen. Genau da hilft Sorion.

- **In-Season Limited: 79 % der "Sales" sind Sorares eigene Verkaeufe.** Der Zweitmarkt
  liegt im Median 12 % unter dem Sofortkauf und 3 % unter der Auktion. Wer eine In-Season-
  Limited kaufen will, sollte den Zweitmarkt pruefen, bevor er den Sofortkauf nimmt.
- **In-Season Rare: 51 % Sorare, Zweitmarkt 34 % unter Sofortkauf.** Der groesste Abstand.
- **Classic: 100 % Zweitmarkt.** Ein sauberer Markt, deshalb ist FMV dort am staerksten
  gegen den Floor (28,1 % vs 38,3 % am 05.09.).
- **Fuer den FMV:** Alle drei Arten liegen heute in einem Topf. Fuer Classic ist das egal,
  fuer In-Season bedeutet es, dass wir zu vier Fuenfteln Sorares Listenpreise schaetzen
  statt den Zweitmarkt. Optionen (Entscheidung Jonas): (a) FMV nur aus TokenOffer, (b)
  Gewichtung, (c) zwei Werte anzeigen ("Sorare price" / "market price"). Datenbasis
  sammelt ab jetzt fmv_accuracy.deal_type; Auswertung per RPC accuracy_by_deal(p_days).
- **Fuer den Knowledge-Artikel:** Ueberschrift-Richtung "In-season cards trade in two
  markets. Here is how to tell them apart." Kein "Sorare verschleiert". Nutzen fuer den
  Leser: bessere Kaufentscheidungen, mehr Handel, weniger Frust.
- Stichprobe = liquideste Spieler je Segment (sales_7d absteigend). Fuer illiquide Karten
  duerfte der Sorare-Anteil noch hoeher sein, dort verkauft oft nur Sorare.
