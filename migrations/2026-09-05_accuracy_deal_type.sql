-- ═══════════════════════════════════════════════════════════════════════════
-- Verkaufsart je Accuracy-Zeile  (05.09.2026)
--
-- BEFUND: Sorares tokenPrices mischen drei Arten von "Verkaeufen":
--   TokenPrimaryOffer  Sofortkauf VON SORARE (Listenpreis, kein Marktpreis)
--   TokenAuction       Auktion auf Sorares Primaermarkt
--   TokenOffer         Verkauf zwischen Managern (der eigentliche Zweitmarkt)
-- Stichprobe Kobel Limited 05.09.: 5 von 6 "Sales" waren Sorare selbst; der
-- einzige Manager-Verkauf lag 10 % unter Sorares Sofortkaufpreis. Bisher
-- warfen wir den Typ weg und bewerteten alles gleich.
--
-- WOZU DIE SPALTE: (a) Accuracy getrennt nach Verkaufsart messen (trifft FMV
-- den Zweitmarkt oder nur Sorares Listenpreis?), (b) Grundlage fuer eine
-- Formel-Entscheidung (Trennung/Gewichtung), (c) Datenbasis fuer den
-- Knowledge-Artikel zum Marktdesign. Die Formel selbst bleibt UNVERAENDERT,
-- bis Jonas auf Basis der Messung entscheidet.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Alte Zeilen behalten null.
-- ═══════════════════════════════════════════════════════════════════════════
alter table public.fmv_accuracy add column if not exists deal_type text;
create index if not exists fmv_accuracy_deal_idx on public.fmv_accuracy (deal_type, created_at);

-- Auswertung nach Verkaufsart (gleiches Fenster/gleiche Regeln wie accuracy_benchmark):
-- Median-Abweichung von FMV, Floor und Verkaufsschnitt je Segment UND Verkaufsart.
create or replace function public.accuracy_by_deal(p_days int default 3)
returns table (
  eligibility text, scarcity text, deal_type text, estimator text,
  samples int, median_abs numeric, bias numeric
)
language sql stable security definer set search_path = public as $$
  with base as (
    select eligibility, scarcity, coalesce(deal_type, 'unknown') as deal_type,
           sale_price, fmv_est, floor_est, avg_sales_est
    from public.fmv_accuracy
    where created_at >= now() - make_interval(days => p_days)
      and sale_price > 0 and (hours_gap is null or hours_gap < 48)
  ),
  long as (
    select eligibility, scarcity, deal_type, 'fmv'::text as estimator, sale_price, fmv_est as est from base where fmv_est > 0
    union all
    select eligibility, scarcity, deal_type, 'floor', sale_price, floor_est from base where floor_est > 0
    union all
    select eligibility, scarcity, deal_type, 'avg_sales', sale_price, avg_sales_est from base where avg_sales_est > 0
  )
  select l.eligibility, l.scarcity, l.deal_type, l.estimator,
         count(*)::int,
         round(percentile_cont(0.5) within group (order by abs(l.est - l.sale_price) / l.sale_price * 100)::numeric, 1),
         round(percentile_cont(0.5) within group (order by (l.est - l.sale_price) / l.sale_price * 100)::numeric, 1)
  from long l
  group by 1, 2, 3, 4
  order by 1, 2, 3, case l.estimator when 'fmv' then 1 when 'floor' then 2 else 3 end
$$;
revoke all on function public.accuracy_by_deal(int) from public;
grant execute on function public.accuracy_by_deal(int) to anon, authenticated;

-- Verifikation (direkt nach der Migration: deal_type ueberall null, fuellt sich ab dem naechsten Updater-Lauf)
select deal_type, count(*) from public.fmv_accuracy where created_at >= now() - interval '1 day' group by 1;
