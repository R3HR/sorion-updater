# SORION — Konzept: Lineup-Optimizer (IDEA-006)

> Stand: 04.09.2026 · Status: **Konzept, nicht gebaut** · Autor: Cowork-Session mit Jonas
> Frage von Jonas: „Wie kann ich aus den Daten, die wir über mein Portfolio haben, die
> bestmöglichen Aufstellungen in einer Woche erzielen? 2× die Woche soll das Portfolio
> analysiert werden … Wäre es möglich, dass für ein Portfolio wie meinem (300+ Karten)
> die bestmöglichen Aufstellungen erstellt werden?"
> Kurzantwort: **Ja.** Die Bausteine liegen zu ~70 % schon im Repo. Dieses Papier beschreibt
> Ziel, Rechenmodell, Datenmodell, Lücken, Ablauf und einen Bauplan in vier Stufen.
> Verwandt: IDEA-001 (Rendite-Rechnung, rückwärts), `so5-results`, `player-live`, ROADMAP.md.

---

## 1. Ziel

Zweimal pro Woche (Wochenend-Gameweek und Midweek-Runde) bekommt der Manager für sein
gesamtes Depot einen **Aufstellungsvorschlag je Wettbewerb**, der die **erwartete
Belohnung in Euro** über alle Wettbewerbe zusammen maximiert — nicht die erwarteten
Punkte. Jede Karte wird höchstens einmal je Gameweek eingesetzt, alle Wettbewerbsregeln
werden eingehalten, und jeder Vorschlag kommt mit Begründung (Erwartungswert, Chance auf
die Reward-Stufen, Alternativen).

Der Optimizer ist die **Vorwärts-Version der Rendite-Suite** aus IDEA-001: Dort wird
gerechnet, was eine Karte erspielt *hat*; hier, was sie diese Woche erspielen *kann*.
Beides nutzt dieselben Tabellen und denselben Essence-Kurs.

## 2. Kernidee: Erwartete Belohnung statt erwartete Punkte

Belohnungen sind **nicht linear** zu Punkten. Ein Wettbewerb zahlt in Stufen (Rang 1–3,
Top 1 %, Top 10 %, Schwellenwert für Essence …). Punkte über einer Schwelle sind wertlos,
Punkte knapp darunter ebenfalls. Deshalb gilt je Aufstellung:

```
E[Belohnung] = Σ_Stufe  P(Aufstellung landet in Stufe | Score-Verteilung) × Wert(Stufe in EUR)
```

Daraus folgen drei Effekte, die eine reine Punkte-Maximierung nicht kennt:

1. **Wettbewerbswahl ist wichtiger als Kartenwahl.** Eine mittelstarke Aufstellung in einem
   Wettbewerb mit niedriger Schwelle und dichter Reward-Verteilung ist oft mehr wert als die
   punktstärkste Aufstellung in einem umkämpften Wettbewerb.
2. **Varianz ist manchmal gut.** Liegt der Erwartungswert knapp unter einer Schwelle, ist
   eine streuende Aufstellung (unsichere Starter mit hoher Decke) besser als eine sichere.
   Liegt er knapp darüber, ist Sicherheit besser.
3. **Der Kapitän gehört dorthin, wo die Schwelle kippt**, nicht automatisch auf den
   punktbesten Spieler.

## 3. Bestandsaufnahme — was schon da ist

| Baustein | Wo | Liefert | Status |
|---|---|---|---|
| Startelf-Quote, Bank-Quote, Punkte-Prognose (Grade A–F + Score), Spielstatus, L5/L10/L40, Einsätze | Edge Function `player-live` (`supabase/functions/player-live/index.ts`) | Erwartungswert-Zutaten je Spieler; Quelle Sorare Inside via `nextClassicFixturePlayingStatusOdds`, `nextClassicFixtureProjectedGrade` | ✅ live, 10-min-Cache, **nur Wochen-Spieltag** („Classic" = Wochen-Fixture; für Midweek `nextDailyFixtureProjectedGrade` bereits abgefragt, Odds-Pendant prüfen) |
| Historische Aufstellungen mit Score, Rang, `ranking_ratio` (Perzentil), Rewards je Lineup | `so5_lineups` (Migration `2026-09-04_so5_history_store.sql`), gefüllt durch `so5-results` | **Score→Rang-Kurve je Leaderboard** und Reward-Struktur je Stufe | ✅ jr3hr: 84 GW, zurück bis 09/2025 |
| Punkte je Karte je Einsatz, Anteil, Essence/XP/Cash-Ertrag | `so5_card_earnings` | Historische Streuung je Karte (σ), realisierter Ertrag | ✅ |
| Depot mit Karten-Identität (Slug, Rarity, Saison, Serie, Kaufpreis, Level/XP) | `sorare-oauth` Action `user_cards`, `sync-portfolio` | Kandidatenmenge (300+ Karten) | ✅ |
| Essence-Kurs je Rarity in EUR | RPC `essence_value()` (aus CraftLog-Crafts, IDEA-002) | Wert von Essence-Rewards | ✅ Limited tragfähig (n=62), Rare dünn (n=6) |
| Kartenwerte je Spieler/Rarity/Eligibility | `card_prices` + `lib/fmv.mjs` | Wert von Karten-Rewards (Tier → Ø-FMV) | ✅ |
| Spieler-Meta: Alter, Nation, Liga, Club, gameplay_tier, In-Season-Eligibility | `players` / `card_prices` (backfill-player-meta, sync-club-rosters) | Regelfilter (U23, Liga, Region, In-Season) | ✅ |
| Scheduler-Muster, Discord-Ausgabe, Read-Token-Zugriff | `squad-poll` | Vorlage für Lauf + Ausgabe | ✅ (Muster, nicht wiederverwendbar 1:1) |

## 4. Lücken — was noch fehlt

| Lücke | Warum nötig | Lösung |
|---|---|---|
| **L1 — Regelwerk je Wettbewerb** | Ohne Regeln keine gültige Aufstellung | Neue Edge Function `so5-competitions` liest je offener Fixture die Leaderboards mit Regelobjekt + Prize-Pool (Sorare `so5Fixture → so5Leaderboards`; **Feldnamen verifizieren**, siehe §7) → Tabelle `so5_competitions`. Fallback: manuell gepflegte `competitions.json` für Regeln, die die API nicht sauber liefert |
| **L2 — Score→Rang-Kurve** | Übersetzt Score in Reward-Stufe | Aus `so5_lineups`: je `leaderboard` die letzten 4–6 GW → Perzentil als Funktion des Scores (monotone Interpolation). Nur eigene Lineups reichen anfangs (jr3hr: 8 Aufstellungen/GW × 84 GW); besser: Leaderboard-Stichproben anderer Manager (öffentlich, siehe §7) |
| **L3 — Reward-Bewertung in EUR** | Zielfunktion braucht eine Währung | Essence → `essence_value()`; Karten-Rewards → Ø-FMV der Tier-Rarity (Tier-Schema aus `so5_lineups.rewards`); Cash/ETH → Kurs; XP/Energy/Tickets → **0 oder Schätzwert** (Entscheidung Jonas, §12) |
| **L4 — Erwartungswert + Streuung je Karte** | Eingabe des Optimizers | Kombination aus `player-live` (Quote, Prognose) + Historie (`so5_card_earnings`, L10/L40) → `card_projections` je Lauf |
| **L5 — Midweek-Prognosen** | 2. Lauf pro Woche | `nextDailyFixture*`-Felder in `player-live` ergänzen; prüfen, ob Odds auch für Daily existieren |
| **L6 — Der Optimizer selbst** | Kernstück | §6.4 |

## 5. Datenmodell (neu)

```sql
-- Regeln und Preise je Wettbewerb je Fixture (1 Zeile je Leaderboard)
so5_competitions (
  fixture_slug     text,        -- football-28-aug-1-sep-2026
  leaderboard_slug text,        -- Sorare-Slug, enthält all_seasons = Klassisch
  display_name     text,
  rarity_rule      jsonb,       -- {main:"limited", allowed:["limited","common"], must_have:{rarity:"rare",n:1}}
  position_rule    boolean,     -- Positionspflicht (GK/DEF/MID/FWD/EXTRA)
  filters          jsonb,       -- {max_age:23, leagues:[...], regions:[...], in_season:true}
  captain_allowed  boolean,
  cap_rule         jsonb,       -- {type:"L15", limit:240} | null
  lineup_limit     int,         -- wie viele Aufstellungen je Manager erlaubt
  prize_tiers      jsonb,       -- [{from_rank:1,to_rank:1,rewards:[...]}, {top_pct:10, rewards:[...]}]
  fetched_at       timestamptz,
  primary key (fixture_slug, leaderboard_slug)
)

-- Score→Perzentil-Kurve je Leaderboard, gleitend
so5_reward_curves (
  leaderboard_slug text, window_gws int, curve jsonb,   -- [{score:280, pct:52}, {score:320, pct:25} …]
  computed_at timestamptz, primary key (leaderboard_slug)
)

-- Erwartungswerte je Karte je Lauf (flüchtig, TTL 7 Tage)
card_projections (
  run_id uuid, card_slug text, player_slug text,
  p_start numeric, p_sub numeric, p_out numeric,
  mu_start numeric, mu_sub numeric, sigma numeric,      -- Punkte
  bonus_pct numeric,                                    -- Karten-Bonus (Level/Saison/Serie)
  ev_points numeric,                                    -- p_start*mu_start + p_sub*mu_sub, × (1+bonus)
  eligible jsonb,                                       -- Liste gültiger leaderboard_slugs
  primary key (run_id, card_slug)
)

-- Ergebnis eines Laufs
lineup_proposals (
  run_id uuid, manager_slug text, fixture_slug text, leaderboard_slug text,
  cards jsonb,                 -- [{slug, position, captain}]
  ev_points numeric, ev_reward_eur numeric,
  tier_probs jsonb,            -- {"top1pct":0.08,"top10pct":0.41,"essence":0.77}
  rationale text,              -- Klartext für Discord/UI
  created_at timestamptz, primary key (run_id, leaderboard_slug)
)
```

Alle Tabellen RLS ohne Policies (Service-Role), wie im Squad-Bot. `card_projections` und
`lineup_proposals` sind je Lauf neu, alte Läufe bleiben als Backtest-Material erhalten.

## 6. Rechenmodell

### 6.1 Erwartungswert je Karte

```
p_start, p_sub, p_out   aus player-live (starterOdds / substituteOdds / nonPlayingOdds, in %)
mu_start                Prognose-Score aus player-live, ersatzweise L10-Schnitt der STARTER-Einsätze
mu_sub                  historischer Ø der SUBBED_IN-Einsätze (aus so5_card_earnings), ersatzweise 0.35 × mu_start
sigma                   Standardabweichung der letzten 10–15 zählenden Einsätze; Untergrenze 12 Punkte
bonus                   Karten-Bonus (Level-XP, Saison-Bonus, Serien-Bonus) — aus user_cards, Regeln je Saison

ev_points = (p_start × mu_start + p_sub × mu_sub) × (1 + bonus)
```

`reliabilityBasisPoints` der Prognose steuert, wie stark Prognose vs. eigene Historie gewichtet
wird (hohe Reliability → Prognose dominiert; niedrige → L10/L40 dominiert).

### 6.2 Verteilung je Aufstellung

Summe der 5 Karten (Kapitän ×1,5 gemäß Regel; nur zählende Einsätze), multipliziert mit dem Aufstellungsbonus (Multi-Club +2 %, Cap +4 %, Karten-Boni). Erste Stufe:
Normal-Approximation mit `mu = Σ ev_points`, `sigma² = Σ sigma_i² + Σ Ausfallvarianz_i`, wobei
Ausfallvarianz die Bimodalität „spielt / spielt nicht" abbildet. Zweite Stufe (Bauplan Stufe 2):
Monte Carlo mit 500–1.000 Szenarien je Gameweek — jede Karte zieht Einsatzstatus und Score,
Aufstellungen werden je Szenario bewertet. Monte Carlo erfasst Korrelationen (zwei Karten aus
demselben Spiel, Spielabsagen) besser als die Normal-Näherung.

### 6.3 Reward-Kurve und Bewertung

Aus `so5_lineups` je `leaderboard`: Paare (score, ranking_ratio) der letzten 4–6 GW → monotone
Kurve `pct(score)`. Prize-Tiers aus `so5_competitions.prize_tiers` → je Tier ein Score-Intervall.

```
P(Tier_k) = P(score ∈ Intervall_k)           -- aus 6.2
Wert(Tier_k) = Σ Rewards_k in EUR            -- Essence × Kurs(rarity aus Reward!), Karte × Ø-FMV(Tier-Rarity), Cash, XP = 0/Schätzwert
E[Reward] = Σ_k P(Tier_k) × Wert(Tier_k)
```

**Regel aus IDEA-001 gilt auch hier:** Essence-Rarity kommt IMMER aus dem Reward-Objekt
(`CardShardsReward.rarity`), nie aus der Wettbewerbs-Rarity — Limited-Wettbewerbe zahlen
auch Rare-Essence (Faktor ~5 im Wert).

### 6.4 Der Optimizer

**Problemklasse:** Zuordnungsproblem mit Nebenbedingungen (gemischt-ganzzahlige
Optimierung). Größe: ~300 Karten × ~20 Wettbewerbe × 5 Slots ≈ 30.000 Binärvariablen, davon
nach Eligibility-Filter meist < 5.000. Für HiGHS/CBC/OR-Tools Sekundenarbeit.

```
Variablen   x[c,l,s] ∈ {0,1}   Karte c in Wettbewerb l auf Slot s
            k[c,l]   ∈ {0,1}   Karte c ist Kapitän in l
            y[l]     ∈ {0,1}   Wettbewerb l wird überhaupt bespielt
Nebenbed.   Σ_l Σ_s x[c,l,s] ≤ 1                 jede Karte max. einmal je Gameweek
            Σ_c x[c,l,s] = y[l]                  jeder Slot genau besetzt, wenn bespielt
            x[c,l,s] = 0 wenn c ∉ eligible(l,s)  Rarity/Position/Alter/Liga/In-Season
            Σ_c k[c,l] = y[l] (falls Kapitän erlaubt), k[c,l] ≤ Σ_s x[c,l,s]
            Σ_c capval[c] × x[c,l,·] ≤ cap[l]     nur Cap-Wettbewerbe
            Rarity-Mix: Σ_c [rarity(c)=r] × x[c,l,·] ≥ must_have[l,r]
            Σ_l y[l] ≤ lineup_limit               falls Sorare die Zahl der Aufstellungen begrenzt
Ziel        max Σ_l E[Reward_l](x, k)
```

Weil `E[Reward_l]` über die Reward-Kurve nichtlinear ist, gibt es zwei Wege:

- **Stufe 1 (schnell, ~90 % des Nutzens): Greedy + Tauschverbesserung.** Wettbewerbe nach
  „Reward-Dichte" sortieren (bester erreichbarer E[Reward] pro gebundener Karte), je Wettbewerb
  die beste gültige 5er-Kombination per Lokalsuche, dann Karten-Tausche zwischen
  Wettbewerben, solange Σ E[Reward] steigt. Deterministisch, in JavaScript im Stack, keine
  Solver-Abhängigkeit.
- **Stufe 2 (exakt): MILP mit stückweise linearisierter Reward-Kurve** oder
  **szenariobasiert** (Monte-Carlo-Szenarien als lineare Terme). Python + OR-Tools/HiGHS als
  Railway-Job, Ergebnis in `lineup_proposals`. Nur bauen, wenn Stufe 1 messbar Wert liegen lässt
  (Backtest §9).

## 7. Regelwerk je Wettbewerb — Katalog

Diese Regeltypen müssen abgebildet werden (Beispiele aus dem Sorare-Alltag; die konkrete
Liste je Saison kommt aus der API oder der Fallback-Datei):

| Regeltyp | Beispiel | Prüfung im Optimizer |
|---|---|---|
| Rarity-Reinheit / Mix | „Limited" (5× Limited, Common erlaubt?), „Rare Pro" (min. 1 Rare), gemischt | `rarity_rule` |
| Positionen | GK, DEF, MID, FWD + EXTRA (beliebig) | Slot-Eligibility |
| Alter | Under 23 (Stichtag!) | `filters.max_age`, Alter aus `players` |
| Region/Liga | Bundesliga, Champion Europe, Champion America, Asia … | `filters.leagues/regions`, Liga aus `players` |
| Saison | In-Season vs. Klassisch (`all_seasons`) | `inSeasonEligible` je Karte |
| Kapitän | erlaubt / nicht erlaubt; Multiplikator **1,5** (verifiziert 04.09.) | `captain_allowed` |
| Cap | Summe der **L10-Ø-Bewertungen** der Startspieler ≤ Grenze (z. B. 260); bei Wochen-Wettbewerben **Bonus-Bedingung (+4 %)**, kein Ausschluss (verifiziert 04.09.) | `cap_rule` mit `hard:false/true`, L10 aus `player-live` (`LAST_TEN_PLAYED_SO5_AVERAGE_SCORE`) |
| Aufstellungslimit | mehrere Lineups je Wettbewerb (jr3hr: 2× Bundesliga-Limited in GW9) | `lineup_limit` |
| Mindest-Score für Essence | Schwelle (z. B. ≥ 250 Pkt) statt Rang | `prize_tiers` mit `min_score` |

**Gewählter Weg für Stufe 0 (Jonas 04.09.): Screenshots der Regeln je Wettbewerb → Regeldatenbank.**
Ablage und Schema in [competitions/README.md](competitions/README.md), Daten in
`competitions/rules.json`. Die JSON ist Quelle für `so5_competitions` und bleibt Fallback, auch
wenn die API die Regeln später liefert. Der API-Weg unten wird zweitrangig (Abgleich/Automatik).

**API-Quelle (zu verifizieren, Stand 04.09.):** `so5.so5Fixture(slug).so5Leaderboards { slug
displayName rarityType mainRarityType rules { … } prizePool / so5RewardsConfig { … }
lineupsCount }`. Query-Tiefe > 7 → `SORARE_APIKEY` serverseitig, gleiches Muster wie
`so5-results`. Erste Aufgabe in Stufe 0 ist ein **Schema-Introspektionslauf**, der die
tatsächlichen Feldnamen in `docs/` festhält.

**Score→Rang-Stichprobe fremder Manager:** `so5Leaderboard.so5LineupsPaginated` liefert
öffentlich Lineups mit Score und Rang. Eine Stichprobe von ~200 Lineups je Leaderboard je GW
(4 Seiten à 50) macht die Reward-Kurve unabhängig von den eigenen 8 Aufstellungen. Kosten:
~20 Leaderboards × 4 Aufrufe = 80 Aufrufe je GW — einmalig nach GW-Ende, kein Cron-Dauerlauf.

## 8. Ablauf — zwei Läufe je Woche

| Schritt | Wochen-GW (Fr–Mo) | Midweek (Di–Do) | API-Aufrufe |
|---|---|---|---|
| 1. Wettbewerbe + Regeln der offenen Fixture holen | Do früh | Mo früh | ~1–3 |
| 2. Reward-Kurven aktualisieren (nur wenn neue GW `closed`) | Di | Fr | ~80 (Stichprobe) |
| 3. Depot laden (`user_cards`), Eligibility je Karte je Wettbewerb | Do | Mo | 1–3 |
| 4. Prognosen je Karte (`player-live`-Logik, gebündelt) | Do + **Refresh Fr 08:00** (Aufstellungen werden ab Do/Fr bekannt) | Mo + Refresh Di | ~300 je Durchgang (je Spieler, nicht je Karte — Dubletten teilen) |
| 5. Optimizer rechnen, `lineup_proposals` schreiben | nach 4 | nach 4 | 0 |
| 6. Ausgabe: Discord-Post (privat) + Tabelle in Sorion-Portfolio | nach 5 | nach 5 | 0 |

**API-Budget:** Der geteilte Key hat 200 req/min, ~148 sind dauerhaft belegt (ROADMAP). Ein
Lauf braucht ~300–400 Aufrufe, über 10 Minuten gestreckt (`DELAY_MS`-Muster des Updaters)
= 30–40/min. Passt, sofern der Lauf **außerhalb des Updater-Fensters** liegt (frei: 05–15 und
21 UTC). Zwei Läufe je Woche sind kein Dauerlast-Risiko (Lehre INC-005/006: kein Cron im
Minutentakt für solche Jobs).

**Ausgabeformat je Wettbewerb (Discord/UI):**

```
🏆 Bundesliga – Limited (Cap 240)          E[Reward] 2,85 € · E[Pts] 312 · Top10%: 41 % · Essence-Schwelle: 77 %
GK  Baumann (L25, 78 % Start, 44 Pkt)  DEF Querfeld ★C (91 %, 58)  MID Maza (85 %, 61) …
Alternativen: Matanovic statt Maza (−0,12 €), sicherer (σ 28 → 22)
Warum nicht Hall hier: bringt in „Champion Europe – Rare" 1,69 € mehr
```

## 9. Bewertung — Backtest, bevor jemand dem Vorschlag folgt

`so5_lineups` enthält jr3hrs echte Aufstellungen mit realem Ertrag. Walk-Forward wie beim
FMV-Backtest (`tools/fmv-backtest.mjs`): Für GW *n* nur Daten bis GW *n−1* nutzen, Vorschlag
rechnen, mit den **tatsächlichen Spielerpunkten** der GW *n* bewerten und mit dem tatsächlich
erzielten Ertrag vergleichen. Kennzahlen: Δ Ertrag in EUR je GW, Trefferquote der Wettbewerbswahl,
Anteil Aufstellungen, bei denen der Vorschlag schlechter war. **Einschränkung:** Historische
Startelf-Quoten liegen nicht vor (flüchtig, `player-live` speichert nicht) → ab sofort je Lauf
die Prognosen in `card_projections` **behalten**, damit der Backtest ab Oktober echte Prognosen
hat. Bis dahin Näherung über tatsächliche Einsatzquote L10.

Erfolgskriterium (Vorschlag): Der Optimizer schlägt Jonas' manuelle Aufstellungen über 6 GW im
Mittel um ≥ 15 % Ertrag, ohne in mehr als einer GW schlechter zu sein.

## 10. Risiken und Grenzen

- **Prognosequalität:** Startelf-Quote und Score-Prognose kommen von Sorare Inside — der
  Optimizer ist nie besser als seine Eingaben. Gegenmittel: Reliability-Gewichtung (6.1) und
  eigene Historie als zweites Bein.
- **Verschiebung der Reward-Kurve:** Saisonstart, neue Karten, geänderte Teilnehmerzahl. Gleitendes
  Fenster 4–6 GW; Warnung im Output, wenn die Kurve auf < 3 GW basiert.
- **Regeländerungen durch Sorare** (neue Wettbewerbe, geänderte Boni, Cap-Grenzen): Regeln nie
  hart codieren, immer aus `so5_competitions` bzw. Fallback-Datei; jeder Lauf meldet unbekannte
  Regeltypen und **lässt den Wettbewerb dann aus**, statt eine ungültige Aufstellung vorzuschlagen.
- **Korrelation:** Karten aus demselben Spiel/Team fallen gemeinsam aus (Absage, Rotation). Nur
  Monte Carlo (Stufe 2) bildet das ab; Stufe 1 überschätzt die Sicherheit solcher Stapel leicht.
- **Boni-Berechnung** (Level/XP, Saison, Serie, Squad) ändert sich saisonal — eigene kleine
  Bonus-Funktion mit Versionsdatum, wie `lib/fmv.mjs`.
- **XP-/Ticket-Rewards** haben keinen Marktpreis; werden sie mit 0 bewertet, meidet der Optimizer
  Wettbewerbe, die nur XP zahlen — was Jonas vielleicht nicht will (§12).
- **Datenschutz/Rechte:** Vorschläge nur für das eigene Depot (OAuth-verknüpft). Fremde Depots sind
  öffentlich, aber ein Optimizer für Fremde ist kein Ziel (Bauchladen-Warnung ROADMAP).

## 10a. Wettbewerber: Sorare Inside Companion (Hinweis Jonas 04.09.)

Sorare Inside hat eine Browser-Erweiterung („Sorare Inside Companion", Chrome Web Store,
v0.5.3 vom 31.08.2026, ~790 Nutzer, nur mit bezahltem Sorare-Inside-Abo bzw. Beta-Zugang).
Sie legt Prognose-Overlays (projizierter Score, Siegwahrscheinlichkeit, xG/Clean-Sheet) über die
Aufstellungsseite auf sorare.com und **überträgt per Knopfdruck eine Aufstellung, die man
vorher auf sorareinside.com festgelegt hat, in den Sorare-Aufstellungsbildschirm.** Kein
Auto-Picker: Die Auswahl trifft der Manager auf der Sorare-Inside-Seite — dort lassen sich
**alle Aufstellungen einer Gameweek durchplanen**, die Erweiterung überträgt sie danach nur noch
einzeln nach Sorare (Klarstellung Jonas 04.09.). Sorare Inside ist damit Prognose + Planungs-
Oberfläche + Übertragung aus einer Hand; was fehlt, ist die Optimierung selbst.

Einordnung für dieses Konzept:

- **Gleiche Datenquelle.** Unsere Prognosen kommen ebenfalls von Sorare Inside (via Sorare-API).
  Bei der Bewertung einzelner Spieler kann Sorion also nicht besser sein — nur gleich gut.
- **Der Unterschied liegt in §2:** Sorare Inside liefert Prognosen je Spieler und lässt den
  Manager je Wettbewerb selbst wählen. Es rechnet weder die Reward-Kurve noch das ganze Depot:
  Welche Karte in welchen Wettbewerb gehört, damit die Summe der Belohnungen über alle
  Wettbewerbe maximal wird, entscheidet niemand — das ist Sorions Hebel, plus die eigene
  Ertragshistorie je Karte (`so5_card_earnings`) und der Essence-Kurs aus CraftLog.
- **Letzte Meile ist gelöst — als Muster.** Die Erweiterung zeigt, dass „Aufstellung außerhalb
  festlegen, per Klick in Sorare eintragen" technisch geht und akzeptiert wird (SorareBuddy macht
  Ähnliches). Optionen für Sorion: (a) Vorschlag als Liste, Jonas trägt von Hand ein (Stufe 1–2
  ausreichend); (b) Aufstellung direkt über die Sorare-API setzen (OAuth-Mutation für Lineups —
  Verfügbarkeit und Nutzungsbedingungen prüfen); (c) eigene kleine Erweiterung nach dem Companion-
  Muster, die `lineup_proposals` im Aufstellungsbildschirm vorbelegt. Erst nach positivem
  Backtest (§9) entscheiden.
- **Marktsignal.** Aufstellungshilfe wird bezahlt. Für MONETARISIERUNG.md: „depotweite
  Optimierung nach Ertrag" wäre klar abgrenzbar von „Prognose-Overlay + Übertragen".

## 11. Bauplan in Stufen

| Stufe | Inhalt | Aufwand | Ergebnis |
|---|---|---|---|
| **0 — Fundament** | Schema-Introspektion der Regel-Felder; Edge Function `so5-competitions` + Tabelle; Reward-Kurve aus vorhandenen `so5_lineups` (nur eigene); Reward-Bewertung in EUR (Essence-Kurs, Tier-FMV) | 1–2 Tage | Regeln + Kurven für eine Fixture in der DB, manuell prüfbar |
| **1 — Erster Vorschlag** | `card_projections` je Lauf (player-live-Logik gebündelt, Midweek-Felder ergänzt); Greedy + Tausch-Optimizer in JS; Ausgabe als Markdown/Discord; einmal von Hand gestartet | 2–3 Tage | Ein lesbarer Vorschlag für Jonas' Depot für die nächste GW |
| **2 — Messen** | Walk-Forward-Backtest gegen jr3hrs Historie; Prognosen je Lauf speichern; Kennzahlen aus §9 | 1–2 Tage | Belastbare Antwort „lohnt es sich" — **Go/No-Go für Stufe 3** |
| **3 — Betrieb** | Zwei geplante Läufe je Woche (Railway-Cron, außerhalb des Updater-Fensters), Refresh-Lauf vor Deadline, UI-Ansicht im Portfolio, Monte Carlo/MILP nur wenn Backtest Lücke zeigt | 3–5 Tage | Automatischer Vorschlag Do/Mo mit Refresh, Historie der Vorschläge |

Reihenfolge ist Absicht: **kein Automatikbetrieb, bevor der Backtest den Nutzen zeigt** (P0-Regel
der ROADMAP: Launch + Tracking vor neuen Features — der Optimizer ist ein Post-Launch-Feature,
Stufe 0–1 sind aber als Werkzeug für Jonas selbst sofort sinnvoll).

## 12. Offene Entscheidungen (Jonas)

1. **Bewertung von XP, Energy, Tickets, Craft-Clues** — 0 EUR, fester Schätzwert, oder eigener
   Zielterm („mindestens X XP je Woche")? **Stand 04.09. (Jonas): XP-Wert wird später bestimmt.**
   Bekannte Mechanik: jedes Karten-Level bringt **+1 % Punkte**, die benötigte XP je Level steigt
   **exponentiell**. Ansatz für später: Wert(XP) = Grenznutzen des nächsten Levels dieser Karte
   (+1 % × erwartete Punkte × erwartete Einsätze × Reward-Sensitivität) ÷ noch fehlende XP —
   d. h. XP ist auf niedrigen Levels viel mehr wert als auf hohen, und mehr wert auf Karten, die
   oft und in reward-nahen Aufstellungen spielen. Bis dahin: XP = 0 im Ziel, aber als Nebenausgabe
   („erwartete XP je Vorschlag") mitführen.
2. **Risikoneigung** — reiner Erwartungswert, oder Erwartungswert minus λ·Streuung
   (ruhigere Erträge)? Vorschlag: beides ausgeben, Standard = Erwartungswert.
3. **Wettbewerbe erzwingen/ausschließen** — Sperrliste bzw. Pflichtliste (z. B. Squad-Wettbewerb
   „Handpicked" hat eigene Regeln aus SQUAD_LEADERBOARD.md und Cap-Kollisionen — vermutlich
   **außen vor lassen**, weil dort das Squad-Ziel zählt, nicht der Eigenertrag).
4. **Karten sperren** — Karten, die zum Verkauf stehen oder bewusst nicht gespielt werden, per
   Flag ausnehmen.
5. **Ausgabekanal** — privater Discord-Webhook, Sorion-Portfolio-Tab, oder beides.
6. **Produktfrage** — nur eigenes Werkzeug, oder später Pro-Feature der Rendite-Suite
   (MONETARISIERUNG.md)? Beeinflusst nur Stufe 3.

## 13. Prinzipien (aus dem Ökosystem übernommen)

- Zuordnungs-Ebene ist die **Karte**, nicht der Spieler (IDEA-001, Ansage Jonas 04.09.).
- Essence-Rarity **immer aus dem Reward-Objekt**, nie abgeleitet.
- Nur `STARTER` und `SUBBED_IN` zählen zum Lineup-Score.
- Kein Cron im Minutentakt, kein Dauerlast-Job; Läufe gestreckt, außerhalb des Updater-Fensters.
- Keine Secrets im Code; `SORARE_APIKEY` nur serverseitig.
- Unbekannte Regel → Wettbewerb auslassen und melden, nie raten.
- Jede Formel (Bonus, Reward-Kurve, Erwartungswert) an **genau einer Stelle** mit Versionsdatum.
