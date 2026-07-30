-- Eigenes, DSGVO-freundliches Analytics (30.07.) — ersetzt Plausible (Abo abgelaufen).
-- Ziel: "wie viele Leute sind wo unterwegs" + Feature-Nutzung als Basis fuer eine
-- spaetere Monetarisierung. Keine Cookies, keine IP-Speicherung, kein Dritt-Dienst.
--
-- Datenschutz-Design:
--   * visitor_hash = sha256(IP + User-Agent + Tages-Salt) -> pseudonym, taeglich neu,
--     nicht ruecklesbar auf die IP, kein geraeteuebergreifendes Tracking.
--   * Es wird KEINE IP und kein User-Agent im Klartext gespeichert.
--   * Schreiben ausschliesslich serverseitig (Edge Function mit Service-Key);
--     anon/authenticated haben KEINEN Zugriff auf die Rohtabelle.

create table if not exists public.analytics_events (
  id           bigserial primary key,
  site         text not null,                    -- 'sorion' | 'craftlog'
  path         text not null,                    -- '/', '/portfolio', ...
  event        text not null default 'pageview', -- 'pageview' | 'manager_search' | ...
  referrer_host text,                            -- nur der Host, nie die volle URL
  country      text,                             -- aus CDN-Header, grob
  device       text,                             -- 'mobile' | 'desktop'
  visitor_hash text,                             -- pseudonym, taeglich rotierend
  day          date not null default current_date,
  created_at   timestamptz not null default now()
);

create index if not exists idx_analytics_day        on public.analytics_events (day);
create index if not exists idx_analytics_site_day   on public.analytics_events (site, day);
create index if not exists idx_analytics_event_day  on public.analytics_events (event, day);

-- Rohdaten sind dicht: RLS an, keine Policy fuer anon/authenticated, SELECT entziehen.
alter table public.analytics_events enable row level security;
revoke select on public.analytics_events from anon, authenticated;

-- ── Admin-Gate: die Auswertung ist NUR fuer Jonas ───────────────────────────
-- Prueft die E-Mail aus dem JWT des Aufrufers. Ohne Login (publishable Key) ist
-- auth.jwt() leer -> false. Weitere Admins: einfach in die Liste aufnehmen.
create or replace function public.is_analytics_admin()
returns boolean
language sql stable security definer set search_path = public as $$
  select coalesce(
    lower(coalesce(auth.jwt() -> 'user_metadata' ->> 'email', auth.jwt() ->> 'email', ''))
      in ('jonas.rehr@outlook.de'),
    false)
$$;
grant execute on function public.is_analytics_admin() to anon, authenticated;

-- ── Auswertungs-RPCs (nur Aggregate, nur fuer Admin) ────────────────────────
-- Uebersicht: Besucher/Aufrufe je Tag und Site
create or replace function public.analytics_daily(p_site text default null, p_days int default 30)
returns table (day date, site text, visitors int, pageviews int)
language sql stable security definer set search_path = public as $$
  select e.day, e.site,
         count(distinct e.visitor_hash)::int as visitors,
         count(*) filter (where e.event = 'pageview')::int as pageviews
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.day >= current_date - least(greatest(coalesce(p_days,30),1),365)
    and (p_site is null or e.site = p_site)
  group by e.day, e.site
  order by e.day desc, e.site
$$;

-- Wo sind die Leute? Aufrufe je Seite
create or replace function public.analytics_pages(p_site text default null, p_days int default 30)
returns table (site text, path text, pageviews int, visitors int)
language sql stable security definer set search_path = public as $$
  select e.site, e.path,
         count(*)::int as pageviews,
         count(distinct e.visitor_hash)::int as visitors
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.event = 'pageview'
    and e.day >= current_date - least(greatest(coalesce(p_days,30),1),365)
    and (p_site is null or e.site = p_site)
  group by e.site, e.path
  order by pageviews desc
$$;

-- Feature-Nutzung (die Zahlen, mit denen man spaeter ueber Geld redet)
create or replace function public.analytics_events_top(p_site text default null, p_days int default 30)
returns table (site text, event text, hits int, visitors int)
language sql stable security definer set search_path = public as $$
  select e.site, e.event,
         count(*)::int as hits,
         count(distinct e.visitor_hash)::int as visitors
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.event <> 'pageview'
    and e.day >= current_date - least(greatest(coalesce(p_days,30),1),365)
    and (p_site is null or e.site = p_site)
  group by e.site, e.event
  order by hits desc
$$;

-- Herkunft + Geraete
create or replace function public.analytics_sources(p_site text default null, p_days int default 30)
returns table (referrer_host text, country text, device text, hits int)
language sql stable security definer set search_path = public as $$
  select coalesce(e.referrer_host,'direct') as referrer_host, e.country, e.device, count(*)::int
  from public.analytics_events e
  where public.is_analytics_admin()
    and e.day >= current_date - least(greatest(coalesce(p_days,30),1),365)
    and (p_site is null or e.site = p_site)
  group by 1,2,3
  order by 4 desc
  limit 100
$$;

-- Die Auswertung ist PRIVAT: nur eingeloggte Admins (siehe is_analytics_admin()).
-- anon (publishable Key) bekommt kein Execute-Recht — zusaetzlich greift der Gate im WHERE.
revoke execute on function public.analytics_daily(text,int)      from anon;
revoke execute on function public.analytics_pages(text,int)      from anon;
revoke execute on function public.analytics_events_top(text,int) from anon;
revoke execute on function public.analytics_sources(text,int)    from anon;
grant  execute on function public.analytics_daily(text,int)      to authenticated;
grant  execute on function public.analytics_pages(text,int)      to authenticated;
grant  execute on function public.analytics_events_top(text,int) to authenticated;
grant  execute on function public.analytics_sources(text,int)    to authenticated;

-- Aufbewahrung: alles aelter als 400 Tage loeschen (Datenminimierung).
create or replace function public.analytics_prune()
returns int language plpgsql security definer set search_path = public as $$
declare n int;
begin
  delete from public.analytics_events where day < current_date - 400;
  get diagnostics n = row_count;
  return n;
end $$;

-- Verifikation nach dem Einspielen:
--   select count(*) from analytics_events;                          -- 0, bis Traffic kommt
--   select * from analytics_pages('sorion', 30);                    -- leer, aber muss laufen
--   -- Rohtabelle muss von aussen dicht sein (mit publishable Key):
--   -- curl .../rest/v1/analytics_events?select=id  =>  permission denied
--   -- Auswertung ohne Login muss ebenfalls scheitern:
--   -- curl -X POST .../rest/v1/rpc/analytics_pages -d '{}'  =>  permission denied for function
--   select public.is_analytics_admin();   -- im SQL Editor: false (kein JWT) — das ist korrekt
