# Wettbewerb (Stand 06.09.2026)

Kurzüberblick über konkurrierende Sorare-Tools und was daraus für Sorion folgt.
Regel für alle Texte: **fair bleiben, nie herabsetzen.** Zahlen nennen, keine Urteile
über andere Betreiber. Wir gewinnen über Nachweise, nicht über Polemik.

---

## sowizzy.com

**Entdeckt:** 06.09.2026 über einen Reddit-Kommentar von `theabominablewonder`, der
sowizzys Preismethode erklärte.

**Funktionsumfang (breiter als Sorion):** Craft Assist (Wahrscheinlichkeiten je Slot aus
aufgedeckten Hinweisen), Gallery, Price Alerts, Lineups, Price Ratios.

**Preismethode (laut deren eigener Beschreibung):**
- gewichteter Verkaufsdurchschnitt, neuere Verkäufe zählen stärker
- wo zu wenige Verkäufe existieren (z. B. Super Rare): Blick auf andere Scarcities
  plus Multiplikator aus historischen Verhältnissen (`/price-ratios`)
- selbst eingeräumt: „lags behind recent price movements" und „has some bugs"

**Das ist exakt unser `avg_sales`-Vergleichsmaßstab.** Wir messen ihn täglich gegen
echte Verkäufe. Stand 06.09. (3-Tage-Fenster):

| Segment | Sales | Sorion FMV | Floor | gewichteter Schnitt |
|---|---|---|---|---|
| In-Season Limited | 53.952 | 21,1 % | 32,9 % | **20,2 %** |
| In-Season Rare | 7.356 | **27,9 %** | 33,9 % | 31,6 % |
| Classic Limited | 5.903 | 28,3 % | 38,3 % | **28,1 %** |
| Classic Rare | 2.050 | **26,8 %** | 24,9 % | 31,6 % |
| Classic Super Rare | 178 | **22,1 %** | 16,6 % | 60,0 % |

**Lesart, ehrlich:** Bei Limited (dem volumenstärksten Segment) liegt der einfache
gewichtete Schnitt hauchdünn vorn. Bei Rare sind wir 4 bis 5 Punkte besser, bei
Super Rare um Faktor 2,7 — und genau dort weicht sowizzy nach eigener Aussage auf
Ratios aus. Das deckt sich mit der Schwäche, die sie selbst benennen.

**Entscheidende Hürde bei ihnen:** `sowizzy.com` verlangt **Sorare-OAuth-Login**, bevor
man irgendetwas sieht.

---

## sorareterminal.com

**Entdeckt:** 06.09.2026 über einen Reddit-Post (Market-Capitalisation-Dashboard).

**Inhalt:** Marktkapitalisierung je Scarcity und Season-Typ. Methode laut Post:
`Total Supply × Kartenpreis`, wobei je Karte der **niedrigere** Wert aus letztem Verkauf
und aktuellem Floor-Angebot genommen wird (bewusst konservativ gegen Preis-Spikes).
Genannte Zahlen: 52,02 Mio. € Gesamtmarkt, davon Classic 32,79 Mio. (63 %),
In-Season 19,23 Mio. (37 %).

**Hürde:** `sorareterminal.com` leitet auf `/login` um.

**Könnten wir das auch?** Heute nicht sauber. Prüfung 06.09.: von 126.360 Zeilen in
`card_prices` haben nur **16.624 einen `available_supply`-Wert** (13 %), FMV liegt bei
46.412. Eine Marktkapitalisierung über alle Segmente wäre damit grob unvollständig
(unsere Teilrechnung ergab 5,2 Mio. € für In-Season, Classic fehlt ganz). Supply
müsste erst flächendeckend erfasst werden. Nicht kurzfristig, siehe IDEAS.

---

## Was Sorion unterscheidet

1. **Kein Login.** Beide Konkurrenten stellen eine Anmeldewand vor die Daten, sowizzy
   sogar den Sorare-OAuth. Sorion zeigt Markt, Portfolio, Gameweek-Historie und
   Genauigkeit ohne Konto. Das ist der größte Unterschied und der billigste Vorteil:
   Er kostet uns nichts und ist für neue Nutzer die höchste Hürde bei den anderen.
2. **Gemessene Genauigkeit.** Niemand sonst veröffentlicht seine Fehlerquote gegen echte
   Verkäufe. Wir messen sogar die Methode der Konkurrenz mit (`avg_sales`).
3. **Erträge je Karte.** Was eine Karte in Aufstellungen erspielt hat, rechnet nach
   aktuellem Stand niemand sonst aus.

**Wo wir hinterherliegen:** Funktionsbreite. Craft Assist, Preisalarme und Lineup-Tools
haben wir nicht. Preisalarme stehen ohnehin als Pro-Kandidat auf der Liste.

---

## Konsequenzen (Vorschlag, Entscheidung Jonas)

- **Nicht kopieren, nicht aufregen.** Zwei sichtbare Wettbewerber beweisen, dass der
  Markt existiert. Sorare Data ist an Kosten gescheitert, nicht an fehlender Nachfrage.
- **Alleinstellungsmerkmal schärfen statt Funktionen nachbauen:** „ohne Login" und
  „belegte Genauigkeit" gehören prominenter auf die Startseite.
- **Die Accuracy-Seite um den Namen der Methode erweitern:** Statt nur „avg sales"
  erklären, dass das der verbreitete Ansatz ist (gewichteter Verkaufsschnitt), damit
  Leser den Vergleich einordnen können. Ohne Namensnennung anderer Anbieter.
- **Reddit:** Wenn jemand sowizzy empfiehlt, ist das kein Angriff. Eine sachliche
  Ergänzung („hier sind die gemessenen Abweichungen beider Ansätze") wirkt stärker als
  jede Behauptung.
