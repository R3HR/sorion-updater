-- ═══════════════════════════════════════════════════════════════════════════
-- Statistik: alles in Berliner Zeit, Tag beginnt 00:00 Europe/Berlin  (06.09.2026)
--
-- BEFUND (Jonas 05.09., Launch-Tag): Die Kacheln mischten drei Zeitbasen.
--   1. analytics_events.day wurde von der track-Function als UTC-Datum gesetzt
--      (Besuche zwischen 0 und 2 Uhr Berlin zaehlten zum VORTAG). -> Function
--      seit 06.09. auf Europe/Berlin umgestellt; hier: Altdaten korrigieren.
--   2. analytics_daily/pages/events_top/sources filtern mit current_date (UTC).
--   3. analytics_accounts/retention rechnen "heute" als rollierende 24 h
--      (now() - interval), nicht als Kalendertag.
--
-- REGEL AB JETZT: Alle Auswertungs-Funktionen laufen mit set timezone = 'Europe/Berlin';
-- damit sind current_date und ::date Berliner Kalendertage. Fenster sind
-- KALENDERTAGE INKLUSIVE HEUTE: p_days = 1 -> nur heute, 30 -> heute + 29 Vortage.
-- Rollierende Intervalle gibt es in der Statistik nicht mehr.
--
-- Nicht betroffen (bewusst): fmv_accuracy (rollierende Stunden), market_daily /
-- fmv_accuracy_daily (Snapshot 05:30 UTC = 07:30 Berlin, gleiches Datum).
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS". Grants bleiben durch create or replace erhalten.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Altdaten: day aus created_at in Berliner Zeit neu setzen ─────────────
-- Betrifft nur Zeilen, deren UTC-Tag vom Berliner Tag abweicht (22:00-24:00 UTC).
update public.analytics_events
   set day = (created_at at time zone 'Europe/Berlin')::date
 where day <> (created_at at time zone 'Europe/Berlin')::date;

alter table public.analytics_events
  alter column day set default ((now() at time zone 'Europe/Berlin')::date);

-- ── 2) Tagesuebersicht ──────────────────────────────────────────────────────
create or replace function public.analytics_daily(p_site text default null, p_days int default 30)
returns table (day date, site text, visitors int, pageviews int)
language sql stable security definer
set search_path = public
set timezone = 'Europe/Berlin' as $$
  select e.day, e.site,
         count(distinct e.visitor_hash)::int as visitors,
         count(*) filter (where e.event = 'pageview')::int as pageviews
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.day >= current_date - (least(greatest(coalesce(p_days,30),1),365) - 1)
    and (p_site is null or e.site = p_site)
  group by e.day, e.site
  order by e.day desc, e.site
$$;

-- ── 3) Seiten ───────────────────────────────────────────────────────────────
create or replace function public.analytics_pages(p_site text default null, p_days int default 30)
returns table (site text, path text, pageviews int, visitors int)
language sql stable security definer
set search_path = public
set timezone = 'Europe/Berlin' as $$
  select e.site, e.path,
         count(*)::int as pageviews,
         count(distinct e.visitor_hash)::int as visitors
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.event = 'pageview'
    and e.day >= current_date - (least(greatest(coalesce(p_days,30),1),365) - 1)
    and (p_site is null or e.site = p_site)
  group by e.site, e.path
  order by pageviews desc
$$;

-- ── 4) Ereignisse ───────────────────────────────────────────────────────────
create or replace function public.analytics_events_top(p_site text default null, p_days int default 30)
returns table (site text, event text, hits int, visitors int)
language sql stable security definer
set search_path = public
set timezone = 'Europe/Berlin' as $$
  select e.site, e.event,
         count(*)::int as hits,
         count(distinct e.visitor_hash)::int as visitors
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.event <> 'pageview'
    and e.day >= current_date - (least(greatest(coalesce(p_days,30),1),365) - 1)
    and (p_site is null or e.site = p_site)
  group by e.site, e.event
  order by hits desc
$$;

-- ── 5) Herkunft ─────────────────────────────────────────────────────────────
create or replace function public.analytics_sources(p_site text default null, p_days int default 30)
returns table (referrer_host text, country text, device text, hits int)
language sql stable security definer
set search_path = public
set timezone = 'Europe/Berlin' as $$
  select coalesce(e.referrer_host,'direct') as referrer_host, e.country, e.device, count(*)::int
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.day >= current_date - (least(greatest(coalesce(p_days,30),1),365) - 1)
    and (p_site is null or e.site = p_site)
  group by 1,2,3
  order by 4 desc
  limit 100
$$;

-- ── 6) Konten: "neu" und "aktiv" als Berliner Kalendertage ──────────────────
create or replace function public.analytics_accounts(p_days int default 30)
returns table (
  scope          text,
  accounts       int,
  confirmed      int,
  new_in_period  int,    -- angelegt in den letzten p_days Kalendertagen inkl. heute
  active_30d     int     -- letzter Login in den letzten 30 Kalendertagen inkl. heute
)
language sql stable security definer
set search_path = public, auth
set timezone = 'Europe/Berlin' as $$
  with u as (
    select au.id,
           (au.created_at      at time zone 'Europe/Berlin')::date as created_day,
           (au.last_sign_in_at at time zone 'Europe/Berlin')::date as last_login_day,
           au.email_confirmed_at,
           au.raw_user_meta_data ->> 'site' as site_meta,
           exists (select 1 from public.profiles p where p.user_id = au.id) as has_profile,
           exists (select 1 from public.crafts  c where c.user_id = au.id) as has_craft
    from auth.users au
    where public.is_analytics_admin()
  ),
  tagged as (
    select u.*,
           (site_meta = 'sorion'   or (site_meta is null and has_profile)) as is_sorion,
           (site_meta = 'craftlog' or (site_meta is null and has_craft))   as is_craftlog
    from u
  ),
  buckets as (
    select 'sorion'::text as scope, * from tagged where is_sorion
    union all select 'craftlog', * from tagged where is_craftlog
    union all select 'unknown',  * from tagged where not is_sorion and not is_craftlog
    union all select 'total',    * from tagged
  )
  select b.scope,
         count(*)::int,
         count(*) filter (where b.email_confirmed_at is not null)::int,
         count(*) filter (where b.created_day    >= current_date - (least(greatest(coalesce(p_days,30),1),365) - 1))::int,
         count(*) filter (where b.last_login_day >= current_date - 29)::int
  from buckets b
  group by b.scope
  order by case b.scope when 'total' then 0 when 'sorion' then 1 when 'craftlog' then 2 else 3 end
$$;

-- ── 7) Wiederkehrer und Aktivitaet: Berliner Kalendertage ───────────────────
create or replace function public.analytics_retention()
returns table (
  scope       text,
  accounts    int,
  returning_n int,    -- an einem SPAETEREN Kalendertag als der Registrierung gesehen
  active_1d   int,    -- heute (Berliner Kalendertag) gesehen
  active_7d   int,    -- in den letzten 7 Kalendertagen inkl. heute
  active_30d  int,    -- in den letzten 30 Kalendertagen inkl. heute
  dormant_90d int     -- seit 90+ Kalendertagen nicht gesehen (oder nie)
)
language sql stable security definer
set search_path = public, auth
set timezone = 'Europe/Berlin' as $$
  with u as (
    select au.id,
           (au.created_at at time zone 'Europe/Berlin')::date as created_day,
           (greatest(
              coalesce(au.last_sign_in_at, au.created_at),
              coalesce((select max(s.updated_at) from auth.sessions s where s.user_id = au.id), au.created_at)
            ) at time zone 'Europe/Berlin')::date as last_seen_day,
           au.raw_user_meta_data ->> 'site' as site_meta,
           exists (select 1 from public.profiles p where p.user_id = au.id) as has_profile,
           exists (select 1 from public.crafts  c where c.user_id = au.id) as has_craft
    from auth.users au
    where public.is_analytics_admin()
  ),
  tagged as (
    select u.*,
           (site_meta = 'sorion'   or (site_meta is null and has_profile)) as is_sorion,
           (site_meta = 'craftlog' or (site_meta is null and has_craft))   as is_craftlog
    from u
  ),
  buckets as (
    select 'sorion'::text as scope, * from tagged where is_sorion
    union all select 'craftlog', * from tagged where is_craftlog
    union all select 'unknown',  * from tagged where not is_sorion and not is_craftlog
    union all select 'total',    * from tagged
  )
  select b.scope,
         count(*)::int,
         count(*) filter (where b.last_seen_day > b.created_day)::int,
         count(*) filter (where b.last_seen_day =  current_date)::int,
         count(*) filter (where b.last_seen_day >= current_date - 6)::int,
         count(*) filter (where b.last_seen_day >= current_date - 29)::int,
         count(*) filter (where b.last_seen_day <  current_date - 89 or b.last_seen_day is null)::int
  from buckets b
  group by b.scope
  order by case b.scope when 'total' then 0 when 'sorion' then 1 when 'craftlog' then 2 else 3 end
$$;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- (a) Berliner "heute" der Datenbank-Funktionen:
select (now() at time zone 'Europe/Berlin')::date as berlin_today, current_date as db_today_utc;
-- (b) Wie viele Alt-Zeilen wurden verschoben? (0 = nichts mehr zu tun)
select count(*) as noch_abweichend from public.analytics_events
 where day <> (created_at at time zone 'Europe/Berlin')::date;
-- (c) Tagesreihe der letzten 3 Tage (als Admin eingeloggt im SQL-Editor ggf. leer, dann ueber stats.html pruefen)
select * from public.analytics_daily('sorion', 3);
