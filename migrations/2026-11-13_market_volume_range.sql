-- ═══════════════════════════════════════════════════════════════════════════
-- market_volume: Zeitraum bis 800 Tage (13.11.2026, Wunsch Jonas: Monat und Jahr)
--
-- Der Streifen auf der Markt-Seite hat jetzt einen Schalter 7D / 30D / 1Y.
-- Fuer "1Y" braucht die Seite 365 Tage plus die 365 davor (Vergleich) plus den
-- Erfassungsverzug, dazu 24 Monatsbalken: rund 760 Tage. Bisher deckelte die
-- Funktion stillschweigend bei 180 Tagen, das haette die Jahreszahlen ab Mai 2027
-- unbemerkt abgeschnitten. Einzige Aenderung gegenueber 2026-11-13_market_volume.sql:
-- die Obergrenze 180 -> 800. Signatur und Rueckgabe unveraendert.
--
-- Last: die Tabelle hat rund 20 Zeilen je Tag, 800 Tage sind 16.000 Zeilen,
-- gefiltert auf eine Eligibility die Haelfte. Unkritisch.
--
-- AUSFUEHREN: npx supabase db query --remote --yes --file "<absoluter Pfad>"
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.market_volume(
  p_elig     text,
  p_scarcity text default 'all',
  p_days     int  default 56
)
returns table (day date, sales int, volume numeric, mgr_sales int, mgr_volume numeric,
               complete boolean, start_day date)
language sql
stable
security definer
set search_path = public
as $$
  with t as (select (now() at time zone 'Europe/Berlin')::date as today),
       s as (select ((min(created_at) at time zone 'Europe/Berlin')::date + 1) as start_day
             from public.market_volume_daily),
       n as (select least(greatest(coalesce(p_days, 56), 7), 800) as days)
  select m.day,
         sum(m.sales)::int,
         round(sum(m.volume), 2),
         coalesce(sum(m.sales) filter (where m.deal_type = 'TokenOffer'), 0)::int,
         round(coalesce(sum(m.volume) filter (where m.deal_type = 'TokenOffer'), 0), 2),
         (m.day >= (select start_day from s)
          and m.day < (select today from t) - case when p_elig = 'classic' then 7 else 1 end),
         (select start_day from s)
  from public.market_volume_daily m
  where m.eligibility = p_elig
    and (coalesce(p_scarcity, 'all') = 'all' or m.scarcity = p_scarcity)
    and m.day >= (select today from t) - (select days from n)
  group by m.day
  order by m.day
$$;

revoke all on function public.market_volume(text, text, int) from public, anon, authenticated;
grant execute on function public.market_volume(text, text, int) to anon, authenticated;

-- Verifikation: die neue Obergrenze steht in der Funktion
select case when pg_get_functiondef(p.oid) like '%, 800)%' then 'Obergrenze 800 aktiv' else 'FEHLER' end as pruefung
from pg_proc p join pg_namespace ns on ns.oid = p.pronamespace
where ns.nspname = 'public' and p.proname = 'market_volume';
