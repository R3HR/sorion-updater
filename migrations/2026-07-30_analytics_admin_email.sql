-- Admin-E-Mail fuer das Analytics-Dashboard korrigieren (30.07.)
-- Vorher stand hier jonasrehr@gmail.com (aus dem Session-Kontext geraten);
-- richtig ist jonas.rehr@outlook.de. Ersetzt die Fassung aus 2026-07-30_analytics.sql.
--
-- Weitere Admins spaeter: einfach in die in-Liste aufnehmen, z. B.
--   in ('jonas.rehr@outlook.de', 'zweite@adresse.de')

create or replace function public.is_analytics_admin()
returns boolean
language sql stable security definer set search_path = public as $$
  select coalesce(
    lower(coalesce(auth.jwt() -> 'user_metadata' ->> 'email', auth.jwt() ->> 'email', ''))
      in ('jonas.rehr@outlook.de'),
    false)
$$;

-- create or replace setzt die Rechte zurueck -> erneut absichern (siehe Haertung):
revoke execute on function public.is_analytics_admin() from public;
grant  execute on function public.is_analytics_admin() to anon, authenticated;

-- Verifikation:
--   select public.is_analytics_admin();   -- im SQL Editor: false (kein JWT) — korrekt
--   Dashboard (UI/stats.html) mit jonas.rehr@outlook.de einloggen -> Zahlen erscheinen.
--   Ohne Login weiterhin: rpc/analytics_pages -> 401 permission denied.
