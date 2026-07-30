-- NACHTRAG zu 2026-07-30_market_avg_history.sql (gleicher Tag):
-- Die RPC lief live in den statement_timeout (57014) — price_history (~1,6 Mio Zeilen)
-- hat keinen Index, der (scarcity, eligibility, recorded_at) unterstuetzt; der bestehende
-- Unique-Index beginnt mit player_slug und greift hier nicht. Dieser Index haette von
-- Anfang an in die Migration gehoert.

create index if not exists idx_price_history_scarcity_elig_day
  on public.price_history (scarcity, eligibility, recorded_at);

-- Danach direkt hier im SQL Editor gegenpruefen (soll in <1 s antworten):
--   select recorded_at, round(avg(price)::numeric,4), count(*)
--   from price_history
--   where scarcity='limited' and eligibility='in_season'
--     and recorded_at >= current_date - interval '10 days'
--   group by recorded_at order by recorded_at;
