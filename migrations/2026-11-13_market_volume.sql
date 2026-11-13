-- ═══════════════════════════════════════════════════════════════════════════
-- Marktvolumen je Tag (13.11.2026, Wunsch Jonas: "wie viel gehandelt wird,
-- damit man erkennt, ob die Zahlen ruecklaeufig sind")
--
-- WARUM EINE EIGENE TABELLE UND NICHT fmv_accuracy:
-- fmv_accuracy nimmt je Karte und Lauf hoechstens 5 Verkaeufe auf
-- (update-scarcity.mjs, newSales.slice(0, 5)). Gemessen ueber 14 Tage: bei Limited
-- stossen 18 bis 21 % der Laeufe an diese Grenze, und diese Laeufe tragen 64 bis
-- 71 % des dort erfassten Volumens. Ein Trend darauf misst die Grenze, nicht den
-- Markt. Diese Grenze ist fuer die Genauigkeitsmessung gewollt (sonst dominieren
-- liquide Karten die Statistik) und bleibt deshalb unangetastet.
--
-- STATTDESSEN zaehlt der Updater bei jedem Lauf ALLE seit dem letzten Lauf neuen
-- Verkaeufe einer Karte (Sorare liefert je Abruf bis zu 20) und schreibt am Ende
-- des Laufs eine Summe je (Verkaufstag Berlin, Eligibility, Rarity, Verkaufsart).
-- Das sind rund 20 kleine Zeilen am Tag. Keine periodische Zusatzlast (INC-005):
-- ein Aufruf je ohnehin laufendem Updater-Lauf.
--
-- NUR VORWAERTS: Eine genaue Rueckrechnung gibt es nicht. Erster vollstaendiger
-- Tag ist der Tag NACH dem ersten erfolgreichen Schreiben (min(created_at) + 1).
--
-- VOLLSTAENDIGKEIT (gemessen 13.11. an fmv_accuracy, Verkaeufe von vor 10-30 Tagen):
--   in_season: 100 % binnen 24 h erfasst  -> Tag D vollstaendig ab Tag D+2
--   classic:   35 % nach 1 Tag, 84,5 % nach 3, 100 % nach 7 -> Tag D ab Tag D+8
-- ("binnen 24 h" heisst: ein Verkauf um 23 Uhr landet im Lauf des Folgetags, der
-- Folgetag selbst ist morgens also noch nicht sicher vollstaendig.)
-- Ohne diese Regel zeigte jede Classic-Kurve am rechten Rand einen Scheineinbruch.
--
-- AUSFUEHREN: npx supabase db query --remote --yes --file "<absoluter Pfad>"
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.market_volume_daily (
  day         date        not null,               -- Verkaufstag, Europe/Berlin
  eligibility text        not null,               -- in_season | classic
  scarcity    text        not null,               -- limited | rare | super_rare
  deal_type   text        not null,               -- TokenOffer | TokenAuction | TokenPrimaryOffer | unknown
  sales       integer     not null default 0,
  volume      numeric(14,2) not null default 0,   -- EUR
  created_at  timestamptz not null default now(), -- erstes Schreiben, bleibt stehen
  updated_at  timestamptz not null default now(),
  primary key (day, eligibility, scarcity, deal_type)
);

alter table public.market_volume_daily enable row level security;
revoke all on public.market_volume_daily from anon, authenticated;

-- ── Schreiben: nur der Updater (Service-Rolle) ─────────────────────────────
-- Erhoeht bestehende Tageswerte. Die Zeilen eines Aufrufs muessen eindeutig je
-- Schluessel sein (der Updater sammelt sie in einer Map), sonst bricht ON CONFLICT ab.
create or replace function public.add_market_volume(p_rows jsonb)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare n integer;
begin
  insert into public.market_volume_daily as m
         (day, eligibility, scarcity, deal_type, sales, volume)
  select (r->>'day')::date,
         r->>'eligibility',
         r->>'scarcity',
         coalesce(nullif(r->>'deal_type', ''), 'unknown'),
         (r->>'sales')::int,
         round((r->>'volume')::numeric, 2)
  from jsonb_array_elements(coalesce(p_rows, '[]'::jsonb)) r
  where (r->>'sales')::int > 0
  on conflict (day, eligibility, scarcity, deal_type) do update
    set sales      = m.sales + excluded.sales,
        volume     = m.volume + excluded.volume,
        updated_at = now();
  get diagnostics n = row_count;
  return n;
end
$$;

revoke all on function public.add_market_volume(jsonb) from public, anon, authenticated;
grant execute on function public.add_market_volume(jsonb) to service_role;

-- ── Lesen: fuer die Markt-Seite ────────────────────────────────────────────
-- Eine Zeile je Tag. `complete` sagt, ob der Tag schon voll erfasst ist; die Seite
-- rechnet nur mit vollstaendigen Tagen und zeigt die uebrigen gedimmt.
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
       n as (select least(greatest(coalesce(p_days, 56), 7), 180) as days)
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

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Direkt nach dem Einspielen: Tabelle leer, beide Funktionen da.
select 'tabelle' as pruefung, count(*)::text as wert from public.market_volume_daily
union all
select 'funktionen', string_agg(proname, ', ' order by proname)
from pg_proc p join pg_namespace ns on ns.oid = p.pronamespace
where ns.nspname = 'public' and proname in ('add_market_volume', 'market_volume');
