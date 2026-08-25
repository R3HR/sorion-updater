-- ═══════════════════════════════════════════════════════════════════════════
-- Wiederkehrer & Aktivitaet der Konten (stats.html)  (25.08.2026, Wunsch Jonas)
--
-- FRAGE: "Wie viele Nutzer mit Account kommen wieder, und wie aktiv sind die?"
--
-- MESSGRUNDLAGE — bewusst OHNE neues Tracking (die Analytics bleibt anonym,
-- taeglich rotierender Hash, siehe legal.html):
--   * auth.users.last_sign_in_at  -> echte Logins
--   * auth.sessions.updated_at    -> Token-Refresh laufender Sessions
--   Daraus last_seen = juengster der beiden Werte. WICHTIG: last_sign_in_at
--   allein wuerde Dauernutzer mit persistenter Session als inaktiv zeigen —
--   deshalb der Sessions-Anteil. Grenze der Methode: beendete/abgelaufene
--   Sessions verschwinden aus auth.sessions; fuer laenger zurueckliegende
--   Aktivitaet zaehlt dann nur der letzte echte Login. Fuer Trends reicht das.
--
-- DEFINITIONEN:
--   returning  = an einem SPAETEREN Kalendertag als der Registrierung noch
--                einmal gesehen (der Signup-Tag selbst zaehlt nicht als
--                Wiederkehr).
--   active_1d/7d/30d = last_seen innerhalb des Fensters (kumulativ).
--   dormant_90d      = seit ueber 90 Tagen nicht gesehen (oder nie).
--
-- Scope-Zuordnung (sorion/craftlog/unknown/total) identisch zur bestehenden
-- analytics_accounts (Meta-Feld 'site', Heuristik profiles/crafts).
-- Gate: is_analytics_admin() — ohne Admin-JWT leere Menge.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.analytics_retention()
returns table (
  scope       text,   -- 'total' | 'sorion' | 'craftlog' | 'unknown'
  accounts    int,    -- Konten gesamt
  returning_n int,    -- kamen nach dem Registrierungstag noch einmal
  active_1d   int,    -- heute gesehen
  active_7d   int,    -- in den letzten 7 Tagen gesehen
  active_30d  int,    -- in den letzten 30 Tagen gesehen
  dormant_90d int     -- seit >90 Tagen nicht gesehen (oder nie)
)
language sql stable security definer set search_path = public, auth as $$
  with u as (
    select au.id,
           au.created_at,
           au.raw_user_meta_data ->> 'site' as site_meta,
           greatest(
             coalesce(au.last_sign_in_at, au.created_at),
             coalesce((select max(s.updated_at) from auth.sessions s where s.user_id = au.id),
                      au.created_at)
           ) as last_seen,
           exists (select 1 from public.profiles p where p.user_id = au.id) as has_profile,
           exists (select 1 from public.crafts  c where c.user_id = au.id) as has_craft
    from auth.users au
    where public.is_analytics_admin()          -- Gate: sonst leere Menge
  ),
  tagged as (
    select u.*,
           (site_meta = 'sorion'   or (site_meta is null and has_profile)) as is_sorion,
           (site_meta = 'craftlog' or (site_meta is null and has_craft))   as is_craftlog
    from u
  ),
  buckets as (
    select 'sorion'::text as scope, * from tagged where is_sorion
    union all
    select 'craftlog',      * from tagged where is_craftlog
    union all
    select 'unknown',       * from tagged where not is_sorion and not is_craftlog
    union all
    select 'total',         * from tagged
  )
  select b.scope,
         count(*)::int,
         count(*) filter (where b.last_seen::date > b.created_at::date)::int,
         count(*) filter (where b.last_seen > now() - interval '1 day')::int,
         count(*) filter (where b.last_seen > now() - interval '7 days')::int,
         count(*) filter (where b.last_seen > now() - interval '30 days')::int,
         count(*) filter (where b.last_seen < now() - interval '90 days' or b.last_seen is null)::int
  from buckets b
  group by b.scope
  order by case b.scope when 'total' then 0 when 'sorion' then 1 when 'craftlog' then 2 else 3 end
$$;

revoke all on function public.analytics_retention() from public, anon;
grant execute on function public.analytics_retention() to authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Im SQL-Editor greift is_analytics_admin() NICHT (postgres-Rolle) — leere
-- Menge ist dort KORREKT. Richtig testen: stats.html, eingeloggt als Admin.
-- Anonym von aussen muss die RPC 401/leer liefern.
