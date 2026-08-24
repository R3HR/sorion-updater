# Handpicked Leaderboard — vollständige Spezifikation

> **Quelle:** Jonas (Captain), 24.08.2026, nach Runde 21. **Maßgeblich** für alle
> Squad-Regeln. Bei Widersprüchen zu älteren Notizen in HANDOFF.md gilt dieses Dokument.
> Umsetzung im Bot: Edge Function `squad-poll` (siehe HANDOFF.md → Squad-Manager).

---

## 1. Grundidee

Wir spielen Sorare **Set** (PvE). Pro Runde stellt jeder der 10 Manager ein Team aus
5 Karten auf und erzielt einen **Score** (Dezimalzahl, z. B. 474,10). Das Spiel selbst
wertet nur den **Squad Score** = Summe der 3 besten Team-Scores gegen ein Stage-Ziel.

Das **Handpicked Leaderboard** ist unser eigener, zusätzlicher Wettbewerb obendrauf:
Es verteilt Punkte für die Platzierung innerhalb des Squads, belohnt die 3 Stage-
Contributor und bestraft Regelverstöße. Es hat mit Sorares eigener Wertung nichts zu tun.

---

## 2. Datenmodell

### 2.1 Manager
```
Manager {
  name: String            // exakt wie in Sorare, z.B. "Sorare | MA", "Namiunk_022"
  joinedAtRound: Int      // 1 für die Gründer, 2 für McBeast und Sorare | MA
  totalPoints: Int        // Leaderboard-Punkte kumuliert
  scoreSum: Decimal       // Summe aller Runden-Scores (für Ø)
  roundsPlayed: Int       // Anzahl gewerteter Runden (inkl. 0-Runden!)
}
```

### 2.2 Runde
```
Round {
  number: Int
  date: Date              // Spieltag (nicht Auswertungstag)
  cycle: Int              // laufender Stage-Zyklus
  stageAttempted: 1..5
  squadScore: Decimal     // Summe der 3 besten Scores
  cleared: Boolean
  results: [ { manager, score: Decimal|null, lineupSubmitted: Boolean } ]
  capSuspended: "none" | "all" | "GK"   // Aussetzung durch Captain, siehe 6.6
}
```

### 2.3 Lineup (für den Cap-Check — kommt aus der Sorare-API)
```
LineupEntry {
  manager: String
  round: Int
  playerId: String        // realer Spieler, NICHT die Karte
  cardRarity: String      // Standard | Shiny | Holo | Holo Action Shot
  cardBonusPercent: Decimal  // maßgeblich ist DIESER Wert, nicht die Rarity-Bezeichnung
  position: "GK"|"DF"|"MD"|"FW"
  submittedAt: Timestamp  // Zeitpunkt der Aufstellung DIESES Spielers
}
```
`submittedAt` ist der Schlüssel für die Cap-Logik. Ohne Zeitstempel ist die Regel
nicht sauber anwendbar (siehe 6.4).

---

## 3. Stage-System (Zyklen)

### 3.1 Ziele — konstant, in jedem Zyklus identisch

| Stage | Ziel (Squad Score) | Bonus für Contributor |
|---|---|---|
| 1 | 700 | +1 |
| 2 | 980 | +2 |
| 3 | 1060 | +3 |
| 4 | 1140 | +4 |
| 5 | 1280 | +5 |

### 3.2 Ablauf
- Ein Zyklus startet immer bei **Stage 1**.
- **Pro Runde wird genau EINE Stage versucht** — auch wenn der Squad Score das Ziel
  weit überschreitet. Beispiel R1: 1023,08 clearte nur Stage 1 (700), nicht gleich Stage 2.
- `squadScore >= ziel` → **cleared**, nächste Runde versucht die nächste Stage.
- `squadScore < ziel` → **failed**, der Zyklus endet. Die nächste Runde beginnt einen
  **neuen Zyklus bei Stage 1**.
- Nach Stage 5 (gecleart) beginnt ebenfalls ein neuer Zyklus bei Stage 1.

### 3.3 Pseudocode
```
if squadScore >= TARGET[currentStage]:
    cleared = true
    awardStageBonus(top3Managers, BONUS[currentStage])
    currentStage = (currentStage == 5) ? 1 : currentStage + 1
    if currentStage == 1: cycle += 1
else:
    cleared = false
    // KEIN Bonus für niemanden
    currentStage = 1
    cycle += 1
```

**Wichtig:** Bei einer verfehlten Stage bekommt **niemand** einen Bonus — auch die
3 Besten nicht. Belegt in R10, R14, R16, R21.

---

## 4. Punktevergabe pro Runde

### 4.1 Placement Points (nach Score sortiert, absteigend)

| Platz | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 |
|---|---|---|---|---|---|---|---|---|---|---|
| Punkte | 10 | 9 | 8 | 6 | 5 | 4 | 3 | 2 | 1 | 0 |

Beachte den Sprung: Platz 3 → 4 kostet 2 Punkte, alle anderen Schritte 1 Punkt.
Das macht die Top 3 überproportional wertvoll.

### 4.2 Stage Bonus
Nur die **3 Manager, deren Scores den Squad Score gebildet haben** (= Platz 1–3 der
Runde), bekommen den Bonus der geclearten Stage. Der Bonus ist **nicht kumulativ**:
Bei Stage 3 gibt es +3, nicht +1+2+3.

### 4.3 Rundenpunkte
```
rundenPunkte = placementPoints + stageBonus - penalties
```
Der Wert kann negativ werden (z. B. Platz 10 mit Cap-Verstoß = 0 − 5 = −5).

---

## 5. Strafen

### 5.1 Kein Lineup abgegeben

| Fall | Leaderboard | Ø-Wertung |
|---|---|---|
| unentschuldigt | **−5** | zählt als **0-Runde** |
| entschuldigt (vorher angekündigt) | **0** | zählt als **0-Runde** |
| noch nicht im Squad | keine Wertung | zählt gar nicht |

**Eskalation:** Beim **2. unentschuldigten** Fehlen muss der Bot den Captain
benachrichtigen → Verwarnung. Zähler pro Manager mitführen.

### 5.2 Cap-Verstoß

| Zeitraum | Strafe |
|---|---|
| 10.08.–23.08.2026 | −2 |
| ab 24.08.2026 | **−5** |

Altfälle werden **nicht** rückwirkend angepasst. Der Bot muss die Strafe nach dem
**Spieltag** der Runde bestimmen, nicht nach dem Auswertungstag.

---

## 6. Der Player Cap — die komplizierteste Regel

### 6.1 Kern
Kein realer Spieler darf in mehr als **4 der 10 Lineups** einer Runde stehen. Gezählt
wird der reale Spieler, nicht die Karte — verschiedene Seltenheiten desselben Spielers
zählen zusammen.

### 6.2 Wertigkeit
Die 4 wertvollsten Kopien behalten ihren Platz. Wertigkeit = **angezeigter %-Bonus auf
der Karte** (inkl. Collection-Bonus), nicht die Seltenheitsstufe an sich. Reihenfolge
typischerweise Standard 0% < Shiny < Holo < Holo Action Shot, aber **immer den
tatsächlichen %-Wert vergleichen**.

Bei exakt gleichem %-Bonus: Der Manager mit der **niedrigeren Leaderboard-Platzierung**
hat Vorrang (Ausgleichsmechanik — der Schlechtere darf behalten).

### 6.3 Claim-Verfahren
Wer eine **wertvollere** Kopie hat, obwohl schon 4 Kopien stehen, darf einen Platz
beanspruchen:
- Post im Kanal **#Lineup** mit **@-Mention** des Managers, der tauschen muss
- **Deadline: 19:00 Uhr am Tag des Rundenschlusses**
- Kein Post = kein Claim. Zu später Post = kein Claim.

### 6.4 Wer zahlt — Entscheidungsbaum
```
1. Zähle Aufstellungen pro realem Spieler in dieser Runde.
2. Für jeden Spieler mit count > 4:
   a. Sortiere die Aufsteller nach submittedAt (aufsteigend).
   b. Die ersten 4 sind IMMER safe — unabhängig von ihrer Seltenheit.
   c. Jeder ab Position 5 ohne gültigen Claim bekommt EIGENE Strafe.
      → Bei 6 Kopien zahlen also ZWEI Manager (Beispiel R17).
   d. Mit gültigem Claim (rechtzeitig + höherer %-Bonus): keine Strafe für den
      Claimenden; der gementionte Manager muss tauschen. Tauscht er nicht,
      zahlt ER.
3. Fallback, falls submittedAt NICHT ermittelbar ist:
   → Strafe an den Manager mit dem NIEDRIGSTEN %-Bonus unter allen Kopien.
   → Bei Gleichstand: an den Manager mit der HÖCHSTEN Leaderboard-Platzierung.
```

**Hinweis:** Seit dem API-Tracking (ab 24.08.) sollte Schritt 3 nie mehr greifen —
die Zeitstempel liegen vor. Die Klausel bleibt nur als Notfall bestehen.

### 6.5 Keine nachträglichen Ausnahmen
Die Höhe der Stage, die Wichtigkeit des Teams für den Stage-Clear oder gute Absichten
sind **irrelevant**. Der Bot verbucht immer. (Verbindliche Captain-Entscheidung vom 19.08.)

### 6.6 Aussetzung durch den Captain
Der Captain darf den Cap **nur im Voraus** aussetzen, wenn das Slate objektiv
unspielbar ist:
- **komplett** (Präzedenzfall R16: nur 2 Spiele im Slate)
- **positionsweise** (Präzedenzfall R19: nur Torhüter ausgesetzt)

Im Post erscheint dann `ℹ️ Player cap suspended for this round` bzw.
`… for goalkeepers this round`.

---

## 7. Durchschnitte (Ø)

### 7.1 Saison-Ø
```
saisonØ = scoreSum / roundsPlayed
```
- `roundsPlayed` zählt **alle** Runden ab Squad-Eintritt, auch die mit 0 (kein Lineup).
- Manager, die später dazukamen, haben entsprechend weniger Runden (McBeast und
  Sorare | MA starten bei R2 → nach R21 haben sie 20 Runden, alle anderen 21).
- Rundung: **2 Nachkommastellen**, Dezimal**komma**.

### 7.2 Squad-Ø
```
squadØ = summe(alle squadScores) / anzahlRunden
```

### 7.3 Neues Mitglied
Ein Ersatzmanager **übernimmt den Punktestand** seines Vorgängers, sein **Ø startet
aber bei null** (scoreSum = 0, roundsPlayed = 0).

---

## 8. Tiebreak

Bei **gleicher Punktzahl** entscheidet der **höhere Ø**:
- Gesamt-Leaderboard → Saison-Ø
- Weekly → Wochen-Ø

Der Bot muss das **vor jeder Ausgabe** prüfen. Das ist die häufigste Fehlerquelle.
```
sort by (totalPoints DESC, seasonAverage DESC)
```

---

## 9. Weekly Results

- Wird **jeden Montag** gepostet, unaufgefordert.
- Periode: **Montag bis Montag**. Eine Runde gehört zu der Woche, in der sie
  **gespielt** wurde (nicht ausgewertet). Eine Runde vom Sonntag, die Montag früh
  ausgewertet wird, gehört noch zur alten Woche; eine Runde vom Montag gehört zur neuen.
- **Wochenpunkte** = Summe der Rundenpunkte (inkl. Bonus und Strafen) aller Runden
  der Periode.
- **Wochen-Ø** = Mittel der Scores dieser Runden (0-Runden zählen mit).
- Nach dem Post wird der Zähler auf null gesetzt.
- Die Anzahl Runden schwankt (bisher 4–6 pro Woche) und wird im Post ausgewiesen.

---

## 10. Discord-Ausgaben

Alle Posts sind **auf Englisch**, alle Zahlen mit **Dezimalkomma**, alle Blöcke als
Code-Block (Triple-Backticks). Trennlinie = 36× `━`. Keine Farben, keine Markierung
des 90-Punkte-Status.

### 10.1 Gesamt-Leaderboard → Kanal *Leaderboard*
```
🏆 HANDPICKED LEADERBOARD
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🥇 ParisBoemboem  147 pts  Ø 356,66  –
🥈 Sorare | MA    127 pts  Ø 349,05  –
🥉 Sorare_Jens    126 pts  Ø 342,98  –
4️⃣ andreihaha     122 pts  Ø 326,76  –
5️⃣ FFGAJ          117 pts  Ø 338,39  ▲2
6️⃣ JR3HR          114 pts  Ø 331,10  ▼1
7️⃣ MaisonPanda    109 pts  Ø 311,43  ▼1
8️⃣ McBeast        105 pts  Ø 334,89  –
9️⃣ Enexxx          76 pts  Ø 303,71  –
🔟 Namiunk_022     65 pts  Ø 298,94  –
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⭐ STAGE CLEARS
⭐         x5
⭐⭐        x4
⭐⭐⭐       x4
⭐⭐⭐⭐      x3
⭐⭐⭐⭐⭐     x1
📊 Squad Ø: 1161,94 PKT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Last Update: 24.08.2026
```

**Formatregeln:**
- Rang-Emojis in dieser Reihenfolge: `🥇 🥈 🥉 4️⃣ 5️⃣ 6️⃣ 7️⃣ 8️⃣ 9️⃣ 🔟`
- Name: linksbündig, auf **13 Zeichen** aufgefüllt, danach **2 Leerzeichen**
- Punkte: **3 Zeichen rechtsbündig**, dann ` pts`, dann 2 Leerzeichen
- `Ø ` + Wert mit 2 Nachkommastellen, dann 2 Leerzeichen
- Bewegung gegenüber der Vorrunde: `▲N`, `▼N` oder `–`
- Stage-Clears-Block: **untereinander**, kumuliert über die ganze Saison. Sterne +
  Leerzeichen so, dass die x-Zähler bündig stehen (1 Stern + 9 Leerzeichen, 2 + 8,
  3 + 7, 4 + 6, 5 + 5)
- Trennlinie **vor** „Last Update"

### 10.2 „Last Stage"-Post → Kanal *General Chat*
```
🏅 LAST STAGE
🥇 FFGAJ          474,10 PKT  +10
🥈 andreihaha     407,82 PKT  +9
🥉 Sorare_Jens    391,95 PKT  +8
❌ Stage Failed (1273,87 / 1280)
ℹ️ 6,13 points short — back to Stage 1
```

Zeilenbausteine:

| Situation | Zeile |
|---|---|
| Stage gecleart | `⭐ Stage Cleared: ⭐⭐⭐ (Stage 3 — 1254,07 / 1060)` |
| Stage verfehlt | `❌ Stage Failed (1273,87 / 1280)` |
| kein Lineup, unentschuldigt | `❌ No lineup: <Name> (-5)` |
| kein Lineup, entschuldigt | `⚪ No lineup (excused): <Name>` |
| Cap-Verstoß (eine Zeile je Verstoß) | `⚠️ Player cap violated: <Name> (-5)` |
| Kontext zum Cap-Fall | `ℹ️ Messi in 6 lineups, no claims posted` |
| Cap ausgesetzt | `ℹ️ Player cap suspended for this round` |

`+XX` hinter dem Score = die **Rundenpunkte inklusive** Bonus und Strafen.

### 10.3 Weekly Results → Kanal *General Chat*, montags
```
📆 WEEKLY RESULTS (17.08.–24.08.)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🥇 FFGAJ          +48  Ø 380,20
🥈 Sorare_Jens    +48  Ø 378,51
🥉 MaisonPanda    +40  Ø 350,69
4️⃣ ParisBoemboem  +38  Ø 368,72
5️⃣ Sorare | MA    +32  Ø 352,79
6️⃣ andreihaha     +31  Ø 355,86
7️⃣ Enexxx         +30  Ø 340,97
8️⃣ Namiunk_022    +19  Ø 302,94
9️⃣ McBeast        +16  Ø 316,63
🔟 JR3HR          +10  Ø 314,62
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Rounds this week: 6
```

---

## 11. Ablauf einer Auswertung (Reihenfolge zwingend einhalten)
```
 1. Scores aller 10 Manager einlesen.
 2. Fehlende Lineups identifizieren → entschuldigt oder nicht? (Captain fragen,
    außer vorher angekündigt) → Missed-Lineup-Zähler hochsetzen,
    bei Zähler == 2 den Captain benachrichtigen.
 3. Squad Score = Summe der 3 höchsten Scores.
 4. Aktuelle Stage aus dem Zyklus-State bestimmen → cleared / failed.
 5. Cap-Check STILL durchführen (nicht laut mitdenken, erst rechnen):
    Aufstellungen je realem Spieler zählen, Über-Cap-Fälle nach 6.4 auflösen.
    Bei fehlenden %-Boni: beim Captain nachfragen, NIEMALS aus dem
    Ergebnis-Screenshot raten (Seltenheiten sind dort nicht zuverlässig lesbar).
 6. Rundenpunkte je Manager: Placement + Bonus − Strafen.
 7. Totale, scoreSum, roundsPlayed, Wochenzähler fortschreiben.
 8. Sortieren mit Tiebreak (Punkte, dann Ø). Bewegungspfeile aus dem
    Vergleich zur Vorrunden-Platzierung.
 9. Blöcke rendern und ausgeben. Montags zusätzlich Weekly.
10. State persistieren.
```

---

## 12. Verifikationsbeispiel — Runde 21 (23.08.2026)

Der Bot sollte mit diesen Eingaben exakt die Ausgaben aus Abschnitt 10 erzeugen.

**Eingabe:** FFGAJ 474,10 · andreihaha 407,82 · Sorare_Jens 391,95 · Enexxx 370,19 ·
ParisBoemboem 352,77 · Sorare | MA 338,68 · Namiunk_022 331,39 · JR3HR 327,31 ·
MaisonPanda 275,35 · McBeast 242,71. Zyklus 5, Stage 5. Alle 10 haben aufgestellt.

**Rechnung:**
- Squad Score = 474,10 + 407,82 + 391,95 = **1273,87**
- Ziel Stage 5 = 1280 → **1273,87 < 1280 → failed**, kein Bonus, Zyklus 6 startet bei Stage 1
- Cap: Joan García 4×, Eric García 4×, Almirón 4×, Yamal 4×, Hakimi 3×, Haaland 3×,
  Neves 3× → **kein Verstoß**
- Punkte = reine Placement Points: 10 / 9 / 8 / 6 / 5 / 4 / 3 / 2 / 1 / 0
- Squad-Ø = 24400,80 / 21 = **1161,94**

---

## 13. Fallstricke, die in der Praxis Fehler verursacht haben

1. **Tiebreak vergessen.** Passiert am häufigsten. Immer nach Ø nachsortieren, auch im Weekly.
2. **Bonus bei verfehlter Stage vergeben.** Bei `failed` bekommt niemand etwas.
3. **Mehrere Stages in einer Runde clearen.** Es ist immer genau eine.
4. **Seltenheit aus dem Ergebnis-Screenshot ablesen.** Nicht zuverlässig — %-Boni beim
   Captain erfragen oder aus der API ziehen.
5. **0-Runden aus dem Ø herauslassen.** Ein nicht aufgestelltes Team zählt als 0 und
   senkt den Schnitt.
6. **Nur einen Manager bestrafen, obwohl 6 Kopien im Spiel waren.** Jeder unangekündigte
   Über-Cap-Aufsteller zahlt einzeln.
7. **Strafhöhe nach Auswertungstag statt Spieltag bestimmen.** Der Stichtag 24.08.
   bezieht sich auf den Spieltag.
8. **Cap-Zählung laut mitdenken.** Erst still durchrechnen, dann das fertige Ergebnis
   ausgeben — halbfertige Verdachtsmomente sorgen für unnötige Diskussionen.

---

## 14. Kontext, den der Bot kennen sollte

- Der Set läuft bis **07.09.2026**. Danach beginnt ein neues Set **um echtes Geld**.
- Öffentlich angekündigt: Wer bis **06.09.** keine **90 Leaderboard-Punkte** hat,
  verliert seinen Platz im Squad. Der Bot führt diesen Status **nur intern** mit —
  im Discord-Post erscheint **keine** Markierung.
- Angekündigte Regeln werden **nie rückwirkend verschärft oder gelockert**. Änderungen
  gelten ab Ankündigung, in der Regel ab dem folgenden Montag.
