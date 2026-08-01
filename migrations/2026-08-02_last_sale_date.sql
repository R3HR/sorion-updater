-- Verkaufsdatum zum letzten Sale speichern (02.08.)
--
-- Lücke (von Jonas bemerkt): In card_prices stehen sale_1..sale_5 als reine PREISE.
-- Die Marktseite zeigt "LAST SALE 6,72 €", kann aber nicht sagen, ob das gestern
-- oder vor drei Wochen war — fuer ein Trading-Tool ein wichtiger Unterschied
-- (ein 3 Wochen alter Verkauf sagt etwas voellig anderes ueber Liquiditaet aus).
--
-- Die Sorare-API liefert zu jedem Verkauf ein Datum (tokenPrices.date); der Updater
-- hat es bereits im Speicher und hat es bisher weggeworfen. Ab jetzt wird es
-- mitgeschrieben.
--
-- Bewusst nur EINE Spalte statt einer eigenen Sales-Tabelle: kein Zeilenwachstum,
-- und "wann war der letzte Verkauf" ist die Frage, die in der UI zaehlt. Eine
-- vollstaendige Verkaufshistorie (fuer Charts) waere ein eigener Schritt.
--
-- Hinweis: price_history.recorded_at ist etwas ANDERES — der Tag, an dem UNSER
-- FMV diesen Wert hatte, kein Verkaufszeitpunkt.

alter table public.card_prices add column if not exists last_sale_at timestamptz;

-- Fuellt sich beim naechsten Queue-Durchlauf (~2-3 Tage fuer den Gesamtbestand).
-- Verifikation danach:
--   select player_name, sale_1, last_sale_at, now() - last_sale_at as alter
--   from card_prices where last_sale_at is not null
--   order by last_sale_at desc limit 10;
