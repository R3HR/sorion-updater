-- Nachtrag (02.08.): market_overview lief in den statement_timeout (57014, 3,8 s).
--
-- Ursache: percentile_cont (Median) ueber ~100.000 Werte je Rarity erzwingt eine
-- vollstaendige Sortierung — zu teuer fuer einen Seitenaufruf. count() und avg()
-- sind dagegen billig (Streaming ueber den Index).
--
-- Loesung: Der Median wird ohnehin schon 1x taeglich in market_daily geschrieben
-- (snapshot_market_daily, vom Harvester um 04:00 UTC). Wir rechnen ihn also nicht
-- neu, sondern lesen den letzten Snapshot dazu.

create or replace function public.market_overview(p_elig text default 'in_season')
returns table (scarcity text, cards int, avg_fmv numeric, median_fmv numeric, median_as_of date)
language sql stable security definer set search_path = public as $$
  with live as (
    -- billig: nur zaehlen und mitteln, kein Sortieren
    select cp.scarcity, count(*)::int as cards, round(avg(cp.fmv)::numeric, 2) as avg_fmv
    from public.card_prices cp
    where cp.fmv is not null
      and coalesce(cp.eligibility, 'in_season') = p_elig
    group by cp.scarcity
  ),
  snap as (
    -- Median aus dem juengsten Tagessnapshot je Rarity
    select distinct on (md.scarcity) md.scarcity, md.median_fmv, md.day
    from public.market_daily md
    where md.eligibility = p_elig and md.median_fmv is not null
    order by md.scarcity, md.day desc
  )
  select l.scarcity, l.cards, l.avg_fmv, s.median_fmv, s.day
  from live l left join snap s on s.scarcity = l.scarcity
$$;

revoke execute on function public.market_overview(text) from public;
grant  execute on function public.market_overview(text) to anon, authenticated;

-- Verifikation (muss deutlich unter 1 s liegen):
--   select * from market_overview('in_season');
