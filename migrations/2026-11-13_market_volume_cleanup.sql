-- ═══════════════════════════════════════════════════════════════════════════
-- Aufraeumen: Altlasten aus dem ersten manuellen Lauf (13.11.2026, BUG-060)
--
-- Der erste Lauf zaehlte bei neuen Zeilen (updated_at = 1970) alle 20 abgerufenen
-- Verkaeufe, bis zurueck ins Jahr 2023. Seit bae4455 zaehlt der Updater nur noch in
-- offene Tage (in_season: gestern und heute, classic: letzte 7 Tage, Stand 13.11.).
-- Geloescht werden genau die Zeilen, die nach dieser Regel nie entstanden waeren.
-- Fuer die Anzeige waren sie unsichtbar (vor Erhebungsbeginn), aber falsch.
--
-- AUSFUEHREN: npx supabase db query --remote --yes --file "<absoluter Pfad>"
-- ═══════════════════════════════════════════════════════════════════════════

delete from public.market_volume_daily
where (eligibility = 'in_season' and day < date '2026-11-12')
   or (eligibility = 'classic'   and day < date '2026-11-06');

select day, eligibility, scarcity, deal_type, sales, volume
from public.market_volume_daily order by day desc, eligibility, scarcity, deal_type;
