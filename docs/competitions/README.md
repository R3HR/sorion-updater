# Wettbewerbs-Regeldatenbank (Stufe 0 des Lineup-Optimizers, IDEA-006)

> Angelegt 04.09.2026. Vorgehen (Jonas): Regeln stehen bei jedem Wettbewerb dabei
> (Sorare-App / sorareinside.com) → **screenshotten** → hier ablegen → in `rules.json`
> übertragen. Die JSON ist die Quelle für `so5_competitions` (siehe LINEUP_OPTIMIZER.md §5/§7)
> und bleibt auch dann der Fallback, wenn die Sorare-API die Regeln später liefert.

## Ablauf

1. Screenshot je Wettbewerb nach `screenshots/` legen. Dateiname:
   `<fixture-kurz>_<wettbewerb>_<rarity>.png`, z. B. `2026-09-05_bundesliga_limited.png`
   oder `2026-09-05_u23_rare.png`. Ein Screenshot je Wettbewerb, Preisstruktur mit drauf
   (ggf. zweiter Screenshot `_prizes.png`).
2. Claude überträgt die Screenshots in `rules.json` (Schema unten) und markiert Unklares mit
   `"todo"`. Jonas prüft, dann gilt der Eintrag als verifiziert (`"verified": "2026-09-05"`).
3. Ändert Sorare eine Regel: Eintrag anpassen, `verified` neu setzen, alten Stand NICHT löschen
   (Feld `history`), damit der Backtest mit den damals gültigen Regeln rechnet.

## Schema `rules.json` (ein Objekt je Wettbewerb)

```json
{
  "slug": "bundesliga-limited",              // eigener, stabiler Kurzname
  "display_name": "Bundesliga – Limited",
  "sorare_leaderboard_slug": null,           // später aus der API nachtragen
  "cadence": "weekly" | "midweek" | "both",
  "rarity_rule": { "main": "limited", "allowed": ["limited","common"], "must_have": null },
  "positions": ["GK","DEF","MID","FWD","EXTRA"],   // Slot-Reihenfolge wie im Bildschirm
  "captain": { "allowed": true, "multiplier": 1.2 },
  "filters": { "max_age": null, "leagues": ["bundesliga"], "regions": [], "in_season": false, "other": "" },
  "cap": null | { "type": "L15", "limit": 240 },
  "lineup_limit": 1,                         // erlaubte Aufstellungen je Manager
  "prize_tiers": [
    { "rank_from": 1, "rank_to": 1, "rewards": [{ "type": "card", "rarity": "rare", "tier": 2 }] },
    { "top_pct": 10, "rewards": [{ "type": "essence", "rarity": "limited", "qty": 1000 }] },
    { "min_score": 250, "rewards": [{ "type": "essence", "rarity": "limited", "qty": 300 }] }
  ],
  "bonus_rules": "",                         // Freitext: Saison-/Level-/Serienboni, falls angezeigt
  "source": "screenshots/2026-09-05_bundesliga_limited.png",
  "verified": null,
  "notes": "",
  "history": []
}
```

Reward-Typen: `card` (rarity, tier), `essence` (rarity, qty), `cash` (currency, amount),
`xp` (rarity, qty), `energy`, `ticket`, `other` (Freitext). **Essence-Rarity immer so
eintragen, wie sie im Reward steht**, nicht nach Wettbewerbs-Rarity (Limited-Wettbewerbe zahlen
auch Rare-Essence — IDEA-001).

## Schema-Ergänzungen (aus dem ersten Screenshot, 04.09.)

- `bench`: `{ "field": 1, "gk": 1 }` — Sorare erlaubt +2 Ersatzspieler (1 Feld, 1 TW). Für den
  Optimizer wichtig: Bank-Karten binden Karten, zählen aber nur bei Einwechslung (SUBBED_IN).
- `captain.multiplier`: laut Anzeige **1,5** („Kartenbewertung um 50 % erhöht"), nicht 1,2.
- `cap.hard: false` + `lineup_bonus`: Der Cap (z. B. 260) ist bei diesem Wettbewerb **keine harte
  Grenze, sondern eine Bonus-Bedingung** (+4 %); dazu Multi-Club +2 %. Der Optimizer muss den
  Bonus als Ertragsfaktor rechnen, nicht als Ausschluss. Bei echten Cap-Wettbewerben `hard: true`.
- `filters.in_season.min_cards`: „Min 4 In-Season-Karten" — Mindestanzahl, nicht Pflicht für alle.
- `season`, `captured_at`, `deadline_hint`: Kontext des Screenshots.

## Je Wettbewerb nötige Screenshots (Checkliste)

1. Regel-Popup (wie beim ersten Eintrag) ✅
2. **Preispool** („Preispool anzeigen") — ohne ihn keine Reward-Stufen
3. ~~Tooltip des Aufstellungsbonus~~ geklärt (Jonas 04.09.): **Multi-Club = max. 2 Spieler vom selben Club (+2 %)**, **Cap = Summe der L10-Ø-Bewertungen ≤ Grenze (+4 %)**. Offen: zählen Ersatzspieler mit?
4. Übersicht aller Wettbewerbe der Gameweek (Liste) — für Vollständigkeit und Rhythmus (Wochenende/Midweek)
