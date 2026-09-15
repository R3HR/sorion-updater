# Tauschboerse: Planung von null (15.09.2026)

**Status:** Prototyp, NICHT an die Website angebunden. Laeuft mit vier simulierten Managern
(User A bis D) unter `prototypes/tauschboerse/`. Anbindung an Sorion erst, wenn das Werkzeug fertig ist.

## 1. Ziel

Manager sollen sichtbar machen, **welche Karten sie haben wollen** und **welche sie abgeben wuerden**.
Andere Manager sehen das und finden so Tauschpartner. Spaeter kommt ein Trade-Builder dazu, mit dem
man einen Tausch aus mehreren Karten zusammenstellt und in Sorare als Angebot abschickt.

## 2. Grundentscheidungen (Annahmen, bitte bestaetigen)

| Frage | Entscheidung im Prototyp | Begruendung |
|---|---|---|
| Was steht auf einer Wunschliste? | **Spieler + Rarity + Eligibility** (z. B. Kobel, Limited, In-Season), nicht ein bestimmtes Exemplar | Wer "Kobel Limited" sucht, nimmt meist jede Kopie. Ein Exemplar-Wunsch waere fast nie erfuellbar. |
| Was wird zum Tausch angeboten? | **Ein konkretes Exemplar** aus dem eigenen Portfolio (mit Level und Seriennummer) | Level und Nummer 1 beeinflussen den Wert messbar (docs/2026-09-12_KARTENMERKMALE_WERT.md). |
| Wann passt ein Angebot zu einem Wunsch? | Gleicher Spieler, gleiche Rarity, gleiche Eligibility | Spaeter optional: Mindestlevel, nur Spezialedition, Hoechstpreis. |
| Zaehler "auf X Wunschlisten" | Anzahl **anderer** Manager mit passendem Wunsch, unabhaengig davon, ob die Karte angeboten ist | Zeigt Nachfrage und ist genau der Anreiz, eine Karte freizugeben. |
| Wer darf etwas anbieten? | Nur Karten aus dem eigenen, synchronisierten Portfolio | Verhindert Angebote fuer Karten, die man nicht besitzt. |
| Sichtbarkeit | Alle eingeloggten Manager sehen Wuensche und Angebote samt Managername | Sorare-Usernames und Sammlungen sind ohnehin oeffentlich. Opt-in bleibt offene Frage (Abschnitt 7). |

## 3. Nutzerfluesse (im Prototyp umgesetzt)

1. **Portfolio:** Eigene Karten mit Hinweis "Up for trade" und "♥ N wishlists".
   Klick auf eine Karte oeffnet das Detail:
   - Schalter **Offer for trade** / **Withdraw from trade**
   - **On N wishlists**, anklickbar: Liste der Manager, die sich den Spieler wuenschen. Je Manager
     sichtbar, ob er selbst etwas anbietet, das auf der eigenen Wunschliste steht (direkter Tauschkandidat).
2. **Wishlist:** Spieler suchen und hinzufuegen. Je Wunsch: wie viele Exemplare gerade angeboten
   werden, von wem, mit Level und Nummer. Entfernen per Knopf.
3. **Trade board:** Alle Karten, die andere Manager anbieten. Filter "nur meine Wunschliste",
   Rarity und Suche. Von hier aus Spieler auf die Wunschliste setzen.
4. **Matches:** Je anderem Manager: was er anbietet, das ich will, und was er will, das ich habe
   (angeboten oder noch nicht angeboten). Gegenseitige Treffer stehen oben.
5. **Build trade:** sichtbar, aber deaktiviert. Das ist Phase 2.

## 4. Datenmodell fuer die spaetere Anbindung

Der Prototyp nutzt schon dieselbe Form, damit die Uebertragung nur den Speicherort tauscht.

```
manager_cards   (existiert)  sorare_slug, card_slug, player_slug, rarity, in_season, ...
                              NEU noetig: serial_number, grade (Level), special_edition

trade_wishlist  (neu)         id, sorare_slug, player_slug, rarity, eligibility,
                              created_at, (spaeter: min_grade, max_price_eur, note)
                              unique (sorare_slug, player_slug, rarity, eligibility)

trade_listing   (neu)         id, sorare_slug, card_slug, created_at, (spaeter: note, wants_in_return)
                              unique (card_slug)
```

**Regeln, die die Datenbank durchsetzen muss (nicht der Browser):**
- Ein Angebot ist nur gueltig, solange `card_slug` im Portfolio dieses Managers liegt. Faellt die Karte
  beim Portfolio-Sync heraus (verkauft, getauscht), wird das Angebot automatisch entfernt.
- Schreiben nur fuer die eigene Zeile (RLS ueber das verknuepfte Sorare-Konto, wie `manager_identity`).
- Lesen fuer eingeloggte Nutzer. Zaehler "auf X Wunschlisten" ueber eine RPC, nicht ueber offene Tabellen.
- Level und Seriennummer muessen beim Sync mitgeschrieben werden (Level NUR im Zustand beim Sync
  vertrauenswuerdig, siehe HANDOFF API-Merkzettel).

## 5. Was der Prototyp simuliert

- Vier Manager **User A, B, C, D** mit je rund 11 Spielern (teils zwei Exemplare) aus einem echten
  Datenstand von `card_prices` (48 Karten, Limited und Rare, In-Season und Classic, Top-Ligen, FMV vom 15.09.).
- Level und Seriennummern sind zufaellig, aber stabil (fester Zufallswert).
- Startzustand mit Wuenschen und Angeboten, damit Zaehler und Matches sofort sichtbar sind.
- Umschalter "Acting as User A..D". Aenderungen bleiben im Browser gespeichert, "Reset demo" stellt den
  Startzustand wieder her.

## 6. Phase 2: Trade-Builder und Sorare

**API-Befund (Schema 07.09.2026):** Sorare kennt Direktangebote zwischen Managern:
- `prepareOffer(sendAssetIds, receiveAssetIds, receiverSlug, sendAmount, receiveAmount)` liefert eine `dealId`
- `createDirectOffer(dealId, approvals, receiverSlug, sendAssetIds, receiveAssetIds, ...)` sendet es ab

**Entscheidend:** `approvals` verlangt eine Freigabe aus der Wallet des Absenders
(`starkexLimitOrderApproval`, `solanaTokenTransferApproval`, `mangopayWalletTransferApproval`, ...).
**Sorion kann ein Angebot also nicht serverseitig im Namen eines Managers absenden**, auch nicht mit
dessen OAuth-Token. Die Signatur entsteht beim Nutzer.

Realistische Wege, in dieser Reihenfolge zu pruefen:
1. **Uebergabe an Sorare:** Sorion stellt den Tausch zusammen (beide Seiten, Karten, optional Aufzahlung
   mit FMV-Vorschlag) und oeffnet das Angebot in Sorare vorausgefuellt, falls Sorare dafuer einen Link
   anbietet. Noch nicht geprueft.
2. **Checkliste:** Gibt es keinen solchen Link, zeigt Sorion den fertigen Tausch so an, dass er in Sorare
   in unter einer Minute nachgebaut ist (Empfaenger, Karten beider Seiten mit Nummer, Betrag).
3. **Signatur im Browser des Nutzers** ueber Sorares eigenen Freigabeweg: technisch denkbar, aber nur,
   wenn Sorare das fuer Drittanbieter vorsieht. Vorher Sorares Bedingungen klaeren (Sorion ist Verbuendeter,
   nicht Umweg).

Weitere Bausteine fuer Phase 2: Fairness-Anzeige beider Seiten per FMV (inkl. Level und Nummer 1),
Benachrichtigung bei neuem Match (Discord-Bot vorhanden).

## 7. Offene Fragen an Jonas

1. **Pro-Feature oder frei?** Denkbar: Wunschliste und Angebote frei, Matches und Trade-Builder Pro.
   Eine Tauschboerse lebt von vielen Teilnehmern, eine Sperre zu Beginn bremst das Netzwerk.
2. **Opt-in fuer Sichtbarkeit?** Soll ein Manager unsichtbar Wuensche pflegen koennen?
3. **Wunsch-Details schon zum Start?** Mindestlevel, nur Spezialedition, Hoechstpreis, oder spaeter?
4. **Welche Raritaeten?** Prototyp: Limited und Rare. Super Rare und Unique mit aufnehmen?
5. **Benachrichtigungen:** Discord-Ping bei neuem Match, und wenn ja nur fuer verknuepfte Discord-Konten?
6. **Missbrauch:** Grenze fuer Anzahl Wuensche je Manager? Umgang mit Fake-Angeboten ist durch den
   Portfolio-Abgleich weitgehend geloest.

## 8. Weg vom Prototyp zu Sorion

1. Fragen aus Abschnitt 7 klaeren, Prototyp danach anpassen.
2. `manager_cards` um Seriennummer, Level und Edition erweitern (Portfolio-Sync).
3. Migration `trade_wishlist`, `trade_listing`, RPCs fuer Zaehler und Matches, RLS.
4. Aufraeumen beim Sync: Angebote fuer nicht mehr vorhandene Karten entfernen.
5. UI aus dem Prototyp in portfolio.html uebernehmen, Speicher von localStorage auf die RPCs umstellen.
6. Phase 2 getrennt planen, sobald der Weg zu Sorare (Abschnitt 6) geklaert ist.
