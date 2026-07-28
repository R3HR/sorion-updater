-- Root-Cause "Deploy Crashed (Update Rare)" (28.07.):
-- Der Updater holt seine Arbeitsportion mit
--     select ... from card_prices where scarcity = $1 order by updated_at asc limit $2
-- Fuer diese WHERE+ORDER-Kombination gab es keinen stuetzenden Index -> Postgres musste
-- ~36k Zeilen filtern und sortieren. Unter naechtlicher Parallel-Last (3 Updater + Harvester)
-- lief die Query in den statement_timeout (Fehlercode 57014). Das Script wertete den
-- Query-Fehler als fatal und rief process.exit(1) -> Railway (restartPolicyType="never")
-- meldete "Deploy Crashed". Live reproduziert am 28.07. (order-by-updated_at auf 'limited'
-- lief in 57014).
--
-- Dieser Index macht die Queue-Query zu einem simplen Index-Scan (die Zeilen liegen dann
-- bereits nach scarcity gruppiert und je scarcity nach updated_at sortiert vor) -> konstant
-- schnell, kein Timeout mehr. Hilft allen drei Scarcity-Services und dem Harvester.

create index if not exists idx_card_prices_scarcity_updated
  on public.card_prices (scarcity, updated_at);

-- Hinweis: Bei ~107k Zeilen dauert der Build nur Sekunden (kurze Schreibsperre). Falls der
-- laufende Cron dabei nicht warten soll, stattdessen als EINZELNES Statement ausfuehren:
--   create index concurrently if not exists idx_card_prices_scarcity_updated
--     on public.card_prices (scarcity, updated_at);
-- (concurrently darf NICHT in einem Transaktionsblock laufen.)

-- Verifikation danach: die vorher langsame Query sollte sofort zurueckkommen:
--   explain analyze
--   select id from card_prices where scarcity='rare' order by updated_at asc limit 120;
--   -> "Index Scan using idx_card_prices_scarcity_updated", Ausfuehrung < 50 ms.
