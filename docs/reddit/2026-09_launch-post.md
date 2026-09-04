# Reddit-Post r/Sorare (Entwurf 04.09.2026, Zahlen vom 04.09., 3-Tage-Fenster)

Regeln fuer diesen Post (wie Discord): nichts ueber die Formel, keine
Infrastruktur, keine Kosten ausser dem, was auf der Seite steht. Jonas postet
selbst; vorher Zahlen auf accuracy.html gegen den Text pruefen (sie wandern).

---

## Titel

I built a free Sorare value tracker and publish how wrong it is, every day, against real sales

## Text

Hi r/Sorare. Solo project, six months in, free, no ads, no login. Sharing it because I think the accuracy part is genuinely useful, and I want to hear where it falls short.

**The problem it tries to solve:** the floor price is an asking price, not a traded one. Right now it sits about 19% above where in-season Limited cards actually sell. So I built an estimate (I call it FMV) from live market data and, more importantly, I measure it against every real sale, then publish the result.

**How the measurement works:** whenever a card sells, I compare the sale price to the estimate I had published *before* that sale. No hindsight. The table shows the median gap over a rolling 3-day window, so it always reflects the current model, not last month's. Same sales, same window, for the floor price.

Today's numbers (median deviation from real sale price, lower is better, ~81k sales in the last 3 days):

| Segment | Sales | FMV | Floor price |
|---|---|---|---|
| In-Season Limited | 64,460 | **23.8%** | 28.3% |
| In-Season Rare | 10,074 | **29.9%** | 33.6% |
| Classic Limited | 4,249 | **28.1%** | 35.2% |
| Classic Rare | 1,648 | 27.3% | **24.6%** |
| Classic Super Rare | 247 | 23.9% | **20.0%** |

So: clearly better than the floor where most of the volume is, and honestly worse in the two small Classic segments. I'm not going to pretend otherwise. A plain average of recent sales is about on par with FMV overall right now, which tells you how hard this actually is. The live table is here: sorion.pro/accuracy.html

**What else is on the site:**

- Market view with filters by league (grouped like the Sorare marketplace), club, nation, age, tier and value
- Portfolio for any manager (just the slug, no login): value, P&L net of Sorare's fees, filters, card detail with trend
- Gameweek history back to your first GW: every lineup, every player's score, who counted and who sat on the bench, every reward
- Earnings per card: a lineup's reward is split across the scoring cards by points contributed, so each card shows what it actually earned you (money, essence and XP kept separate, because they are not the same thing)

Try it with your own slug: sorion.pro/portfolio.html?manager=YOUR_SLUG

**What it is not:** financial advice. Every number is an estimate, and the accuracy page exists so you can judge how much to trust it instead of taking my word.

Would love feedback, especially from people who track their portfolio seriously. What's missing, what's wrong, what would you never use?

---

## Timing-Empfehlung (fuer Jonas)

- **Bester Slot: Dienstag oder Mittwoch, 15:00 bis 17:00 Uhr deutscher Zeit.**
  Grund: Reddit-Aktivitaet ist werktags am hoechsten, wenn Europa Nachmittag
  und die US-Ostkueste Vormittag hat (9 bis 11 Uhr ET). Sorare-Nutzer sitzen
  vor allem in FR/DE/UK plus USA, der Slot trifft beide.
- **Warum Di/Mi:** Gameweek-Ergebnisse kommen Montagnacht/Dienstag. Danach
  schauen Leute auf ihr Portfolio und denken ueber Kartenwerte nach. Genau die
  Stimmung, in der ein Bewertungs-Tool interessant ist.
- **Vermeiden:** Freitag/Samstag (Spieltage, Aufstellungs-Stress, niemand
  liest lange Posts) und Sonntagnachmittag (Spiele laufen).
- **Naechster passender Termin: Dienstag, 09.09.2026, ca. 15:00 Uhr.**
- Vor dem Posten: Zahlen in der Tabelle gegen accuracy.html aktualisieren
  (Datum im Text anpassen), Flair pruefen (meist "Tool" oder "Discussion"),
  Regeln des Subreddits lesen (Eigenwerbung ist dort erlaubt, wenn sie
  Substanz hat; Referral-Links sind tabu; wir haben keine).
- Nach dem Posten: die erste Stunde auf Kommentare antworten. Reddit
  rankt nach fruehem Engagement; ein Post ohne Antworten des Autors
  versinkt.
