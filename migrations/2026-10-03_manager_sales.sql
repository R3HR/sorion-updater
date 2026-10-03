-- ═══════════════════════════════════════════════════════════════════════════
-- Karten-Modal: Verkaufsliste und Schnitt passend zum FMV  (03.10.2026)
--
-- BEFUND (Jonas): Beim Modal von Lamine Yamal stehen unter "Recent Sales" fuenf
-- Sofortkaeufe um 355 EUR, waehrend der FMV 251,85 EUR zeigt. Grund: sale_1..10
-- und avg_sales kommen aus ALLEN Verkaufsarten, der FMV rechnet seit v3.6 aber
-- nur mit Manager-Verkaeufen (TokenOffer). Die Seite widerspricht sich selbst.
--
-- Loesung: die Manager-Verkaeufe zusaetzlich ablegen. Die bestehenden Spalten
-- bleiben unangetastet, damit Tabelle, Movers und Accuracy weiterlaufen.
-- ═══════════════════════════════════════════════════════════════════════════

alter table public.card_prices
  add column if not exists manager_sales    jsonb,     -- [{eur, date}], neueste zuerst
  add column if not exists avg_manager_sales numeric;  -- Schnitt derselben Liste

comment on column public.card_prices.manager_sales is
  'Letzte Verkaeufe zwischen Managern (TokenOffer), neueste zuerst. Grundlage des FMV.';

select count(*) as zeilen,
       count(*) filter (where manager_sales is not null) as schon_gefuellt
from public.card_prices;
