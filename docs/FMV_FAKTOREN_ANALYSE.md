# FMV-Faktoren-Analyse (25.08.2026) — Score/Einsatz/Liga/Club vs. Verkaufspreise

> **INTERN — enthält Formel-Zutaten.** Öffentlich nur Genauigkeits-Zahlen (accuracy.html), nie Zutaten.
> Auftrag: Erklären SO5-Score (L5/L10/L40), Einsatzquote, Liga und Club die realen VERKAUFSPREISE über FMV v3.2 hinaus? Robuste Effekte → backtest-geprüfte v3.3-Terme.
> Skripte: `tools/2026-08-25_factor-data.mjs` (Datenlauf, nur lesend) · `tools/2026-08-25_fmv-v33-backtest.mjs` (offline reproduzierbar). Rohdaten: `tools/analysis-out/` (nicht committen, ~80 MB).

## Executive Summary

v3.2 liegt live bei Median 22,7 % / Bias −1,2 % (fmv_accuracy v3.2-Ära 23.–25.08., limited in-season, n=17.305) — aber der Fehler ist gerichtet: **Karten mit FMV < 2 € verkaufen sich +23 bis +75 % über FMV** (n=4.407), teure leicht darunter (−5 bis −7 %). Ursache ist der **immer aktive SELL_CAP 1,50**: Bei gedeckelten Vorhersagen beträgt der Bias im Billigsegment +35 bis +71 pp, bei ungedeckelten ~0. **Score, Einsatzquote, Liga und Club erklären nach Preisniveau-Kontrolle nichts Robustes mehr** — die scheinbaren Effekte waren Preis-Proxys. Der daraus abgeleitete v3.3-Term (Cap nur bei dünner/alter Sales-Basis) senkt im Walk-Forward über 11.917 Verkäufe den Median-Fehler 33,3 → 23,9 % und die Verzerrung +22,2 → +1,7 %.

## Datenbasis

- Stichprobe: 800 Karten (600 limited in_season/classic, 200 rare in_season), geschichtet nach Liquidität (Seed 42), je bis zu 20 Verkaufshistorien-Punkte von Sorare → 11.917 Walk-Forward-Ziele aus 774 Karten. Anonymer API-Lauf (1.624 Calls), kein Key-Verbrauch.
- Gegenprobe: fmv_accuracy, v3.2-Ära = est_at ≥ 23.08. 13:00 UTC, hours_gap < 48 (n=19.226; limited in_season 17.305).
- Score-Daten: Die API liefert (anonym) **keine Einzel-Scores mit Spieldatum**, nur heutige Aggregate (averageScore L5/L10/L40, lastFive/Ten/FortySo5Appearances). Leakage-Schutz daher: Feature-Tests primär auf Verkäufen ≤ 7 Tage bzw. auf der 2-Tage-Accuracy-Ära.

## Geprüfte Effekte (Residuum = log(Sale/FMV), Spieler-geclustert; zwei unabhängige Datensätze)

| Feature | Schicht | Effektgröße | n | p | Urteil |
|---|---|---|---|---|---|
| Preisniveau (log FMV) | alle | −11…−17 %/log-€ | 2.099 WF / 1.092 acc | <0,001 beide | aufnehmen (via Cap-Fix) |
| Cap-Bindung, Billigsegment | FMV < 2 € | Bias +35…+71 pp ggü. ungedeckelt | 611 gedeckelte frische Ziele | — | **aufnehmen → v3.3** |
| Score-Niveau L40 | alle | +3 %/10 Pkt nach Preiskontrolle | 1.092 | 0,10–0,15 | verwerfen |
| Einsatzquote (Appearances) | alle | n.s. nach Preiskontrolle | 1.092 | 0,22–0,53 | verwerfen |
| Liga (Top-5, Gruppen) | alle | −11…+3 pp; Gegenprobe p=0,92 | 17.305 | n.s. | verwerfen — Liga steckt in den Verkaufsniveaus |
| Club über Liga hinaus | alle | nur 3 Clubs mit n≥40 | — | — | verwerfen (keine Power) |
| Form L5/L40 | liquide | +2,4 %/+10 % Form | 922 | 0,059 | nicht aufgenommen (grenzwertig, nur 1 Datensatz) |
| J1/K-League/Premiership | Sonderfall | J1: Sales +221 % am 24.08. (0,42 → 2,08 €) | 519 | <0,001 | kein Formel-Term — Update-Latenz bei Regimesprung, FMV holte binnen 24 h auf |

Wichtig fürs Protokoll: Vor der Preiskontrolle sahen played_L5 (+18,5 vs. +5,3 pp) und Score-Bins (monoton +9,5 → −4,8 pp) signifikant aus — beides kollabiert mit log-Preis im Modell. Das ist das erwartete Muster „Feature ≈ Preisklasse".

## v3.3-Vorschlag: bedingter SELL_CAP

```
thinOrStale = (Sales im Decay-Fenster < 3) ODER (jüngster Sale älter als halfLife)   // 3d in-season, 14d classic
FMV = thinOrStale ? min(salesValue, floor × 1,50) : salesValue
```

Datei: `tools/2026-08-25_fmv-v33-proposal.mjs` (kompletter Ersatz für `lib/fmv.mjs`). Schutz gegen BUG-010-Absurditäten bleibt: Bei dünner/alter Basis rechnet v3.3 exakt wie v3.2.

**Walk-Forward 11.917 Ziele / 774 Karten (v3.2 → v3.3):**

| Segment | n | Median | Bias | ±20 % |
|---|---|---|---|---|
| GESAMT | 11.917 | 33,3 → 23,9 % | +22,2 → +1,7 % | 34 → 44 % |
| limited/in_season | 6.449 | 33,3 → 24,5 % | +25,3 → +2,3 % | 33 → 43 % |
| limited/classic | 2.718 | 31,4 → 22,4 % | +12,2 → +1,2 % | 36 → 47 % |
| rare/in_season | 2.750 | 35,7 → 24,1 % | +27,7 → +0,8 % | 33 → 44 % |
| … 3+ Sales (liquide) | 10.597 | 33,3 → 22,2 % | ~+24 → ~0 % | 34 → 46 % |
| … 1–2 Sales (per Design unverändert) | 1.320 | 43,4 → 43,4 % | +33 → +33 % | 28 → 28 % |
| frische Ziele ≤ 7d (geringste Floor-Unschärfe) | 3.445 | 24,3 → 20,2 % | +6,5 → −0,9 % | 44 → 50 % |
| Billigsegment < 2 € | 7.430 | 45,6 → 28,5 % | +37,4 → +4,4 % | 26 → 39 % |

## Was dagegen spricht / offene Punkte

1. **Floor = heutiger Floor** (historische Floors fehlen): Gewinne bei alten Zielen vermutlich überzeichnet; die frische Scheibe zeigt dieselbe Richtung. Live-Bestätigung nach Deploy über fmv_accuracy (Preisbin-Bias < 2 € muss von +23…+75 % Richtung 0 fallen).
2. **Nur ~2 Tage v3.2-Live-Ära**, heiße Saisonstart-Phase — nach Deploy ~1 Woche Accuracy beobachten, bevor weiter kalibriert wird.
3. **Dünne Schichten bleiben schwach** (1–2 Sales: Bias +33 %): kein belastbarer Hebel gefunden; Kaltstart ohne Sales bleibt FMV null (bewusst). Falls später gewünscht: Form-Faktor und Momentum nur mit mehr Live-Ära-Daten erneut prüfen.
4. **avg10-Vergleich:** Der simple 10er-Schnitt schlug v3.2 bei liquiden limited (21,4 vs. 22,7 %) — v3.3 (liquide 22,2 % im WF) dürfte den Abstand schließen; nach Deploy via `accuracy_benchmark(30)` gegenmessen.

## Deploy-Checkliste (macht Jonas, nicht der Bot)

1. `migrations/2026-08-25_fmv_v33_change_guard.sql`: Cut-Datum auf den echten Deploy-Tag setzen, im SQL-Editor „ohne RLS" ausführen.
2. `tools/2026-08-25_fmv-v33-proposal.mjs` → nach `lib/fmv.mjs` kopieren, pushen (Railway deployt).
3. Erwartung kommunizierbar: Werte liquider Karten steigen einmalig (kein Marktereignis); 7d-Chip bleibt ~7 Tage leer.
4. Ab Deploy: `accuracy-briefing.mjs`-CUT auf den neuen Zeitpunkt ziehen; nach ~5–7 Tagen `accuracy_benchmark(7)` prüfen (Bias < 2 € und avg10-Vergleich).
