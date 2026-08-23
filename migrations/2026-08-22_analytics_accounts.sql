-- ═══════════════════════════════════════════════════════════════════════════
-- Konto-Zahlen fuers Admin-Dashboard (stats.html)  (22.08.2026)
--
-- AUSGANGSLAGE: sorion.pro und craftlog.pro teilen sich EINE Supabase-
-- Nutzerverwaltung (auth.users), und **keine der beiden Registrierungen hat
-- bisher mitgeschrieben, von welcher Seite sie kam** — beide senden nur
-- {email, password}. Eine saubere Trennung ist rueckwirkend also nicht
-- moeglich; sie wird ab jetzt aufgebaut:
--
--   AB JETZT  : Beide Seiten senden bei der Registrierung
--               data:{ site:'sorion' | 'craftlog' } -> raw_user_meta_data->>'site'.
--               Das ist die harte Zuordnung.
--   RUECKWIRKEND: Heuristik ueber Aktivitaet —
--               profiles-Zeile  => Sorion   (nur Sorions Profilseite legt sie an)
--               crafts-Zeile    => CraftLog (nur CraftLog schreibt Crafts)
--               beides          => zaehlt bei BEIDEN (Doppelnutzer)
--               nichts davon    => "unbekannt" (registriert, nie eine Spur
--                                  hinterlassen — ehrlich als eigene Zahl
--                                  ausgewiesen statt geraten)
--
-- Die Funktion liest auth.users (deshalb security definer) und ist durch
-- is_analytics_admin() geschuetzt — genau wie die uebrigen Analytics-RPCs.
-- Ohne Admin-JWT: leeres Ergebnis, kein Datenabfluss.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.analytics_accounts(p_days int default 30)
returns table (
  scope          text,   -- 'sorion' | 'craftlog' | 'unknown' | 'total'
  accounts       int,    -- Konten gesamt in diesem Bereich
  confirmed      int,    -- E-Mail bestaetigt
  new_in_period  int,    -- neu in den letzten p_days
  active_30d     int     -- in den letzten 30 Tagen eingeloggt
)
language sql stable security definer set search_path = public, auth as $$
  with u as (
    select au.id,
           au.created_at,
           au.email_confirmed_at,
           au.last_sign_in_at,
           au.raw_user_meta_data ->> 'site'                    as site_meta,
           exists (select 1 from public.profiles p where p.user_id = au.id) as has_profile,
           exists (select 1 from public.crafts  c where c.user_id = au.id) as has_craft
    from auth.users au
    where public.is_analytics_admin()          -- Gate: sonst leere Menge
  ),
  tagged as (
    select u.*,
           -- harte Zuordnung schlaegt Heuristik
           (site_meta = 'sorion'   or (site_meta is null and has_profile)) as is_sorion,
           (site_meta = 'craftlog' or (site_meta is null and has_craft))   as is_craftlog
    from u
  ),
  buckets as (
    select 'sorion'::text   as scope, * from tagged where is_sorion
    union all
    select 'craftlog',      * from tagged where is_craftlog
    union all
    select 'unknown',       * from tagged where not is_sorion and not is_craftlog
    union all
    select 'total',         * from tagged
  )
  select b.scope,
         count(*)::int,
         count(*) filter (where b.email_confirmed_at is not null)::int,
         count(*) filter (where b.created_at      > now() - make_interval(days => p_days))::int,
         count(*) filter (where b.last_sign_in_at > now() - interval '30 days')::int
  from buckets b
  group by b.scope
  order by case b.scope when 'total' then 0 when 'sorion' then 1 when 'craftlog' then 2 else 3 end
$$;

-- Nur eingeloggte Nutzer duerfen es ueberhaupt versuchen; die Funktion selbst
-- gibt ohne Admin-JWT nichts zurueck (Gate in der Abfrage).
revoke all on function public.analytics_accounts(int) from public, anon;
grant execute on function public.analytics_accounts(int) to authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Im SQL-Editor (postgres-Rolle) greift is_analytics_admin() NICHT — dort
-- kommt bewusst nichts zurueck. Richtig testen: im Dashboard stats.html
-- eingeloggt als jonas.rehr@outlook.de.
-- Gegenprobe von aussen (anonym) muss 401 liefern:
--   curl -X POST .../rest/v1/rpc/analytics_accounts -d '{}'
