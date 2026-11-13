-- ═══════════════════════════════════════════════════════════════════════════
-- Ladestand der Marktvolumen-Rueckrechnung fuer die Seite (13.11.2026)
--
-- Jonas zur ersten Grafik: halbe, schraffierte Balken ("incomplete") sagen Nutzern
-- nichts. Die Seite zeigt deshalb keine Teildaten mehr, sondern solange ein Zeitraum
-- laedt einen Fortschrittsbalken. Diese Funktion liefert dafuer nur Zaehler,
-- keine Inhalte. Nur die beiden grossen Auftraege; Tagesauftraege sind in Minuten fertig.
--
-- AUSFUEHREN: npx supabase db query --remote --yes --file "<absoluter Pfad>"
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.market_volume_progress()
returns table (job text, batches int, done int, final boolean,
               first_done timestamptz, last_done timestamptz)
language sql
stable
security definer
set search_path = public
as $$
  select j.job, j.batches, count(b.done_at)::int, j.final, min(b.done_at), max(b.done_at)
  from public.market_volume_jobs j
  left join public.market_volume_batches b on b.job = j.job
  where j.job in ('recent', 'year')
  group by j.job, j.batches, j.final
  order by j.job
$$;

revoke all on function public.market_volume_progress() from public, anon, authenticated;
grant execute on function public.market_volume_progress() to anon, authenticated;

select * from public.market_volume_progress();
