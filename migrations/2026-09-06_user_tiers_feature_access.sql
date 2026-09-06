-- ═══════════════════════════════════════════════════════════════════════════
-- Unterstuetzer-Stufen + Feature-Freischaltung  (06.09.2026, Vorgabe Jonas)
--
-- ZIEL: Die Cash-Schwelle auf der Leaderboards-Seite wird ein Pro-Feature.
-- Nicht-Pro sehen dort nur einen unscharfen Platzhalter.
--
-- WICHTIG — warum das serverseitig sitzt: Ein CSS-Weichzeichner verdeckt nur
-- die Anzeige. Solange die Zahl im JSON steht, liest sie jeder mit F12 oder
-- per direktem REST-Aufruf. Deshalb (a) wird der direkte Tabellenzugriff auf
-- reward_thresholds entzogen und (b) liefert die neue RPC die Cash-Spalten
-- NUR an berechtigte Konten; alle anderen bekommen dort NULL.
--
-- AUFBAU (Vorgabe Jonas: "Nutzer-Tabelle mit Schaltern, je ein Schalter pro
-- Unterstuetzer-Stufe"):
--   user_tiers      — je Konto ein Schalter pro Stufe (supporter/pro/vip),
--                     dazu valid_until: ein gekuendigtes Abo laeuft aus,
--                     statt ewig freigeschaltet zu bleiben. NULL = unbefristet.
--   feature_access  — welche Stufe schaltet welches Feature frei. Damit laesst
--                     sich ein Feature spaeter OHNE Deploy verschieben.
--   has_feature()   — die EINZIGE Stelle, die "darf dieser Nutzer das?"
--                     beantwortet. (Lehre BUG-022/023/024: dieselbe Regel an
--                     mehreren Stellen implementiert = garantierte Divergenz.)
--
-- Stufen-Rangfolge: vip > pro > supporter. Wer VIP hat, hat alles darunter —
-- der Schalter muss also nicht dreifach gesetzt werden.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Nutzer-Tabelle mit Schaltern ───────────────────────────────────────
create table if not exists public.user_tiers (
  user_id     uuid primary key references auth.users(id) on delete cascade,
  supporter   boolean     not null default false,   -- Ko-fi "Supporter"      0,50 EUR
  pro         boolean     not null default false,   -- Ko-fi "Pro-Supporter"  5,00 EUR
  vip         boolean     not null default false,   -- Ko-fi "Sorion VIP"    25,00 EUR
  valid_until date,                                 -- NULL = unbefristet
  source      text        not null default 'manual',-- manual | kofi | ...
  note        text,
  updated_at  timestamptz not null default now()
);

alter table public.user_tiers enable row level security;
-- Jeder darf NUR die eigene Zeile lesen (fuer die Anzeige "du bist Pro").
drop policy if exists user_tiers_read_own on public.user_tiers;
create policy user_tiers_read_own on public.user_tiers
  for select to authenticated using (auth.uid() = user_id);
-- Schreiben kann NIEMAND ueber die API: keine insert/update/delete-Policy.
-- Gesetzt wird nur per Service-Key oder set_user_tier() (unten).
revoke all on public.user_tiers from anon;
grant select on public.user_tiers to authenticated;

-- ── 2) Feature -> benoetigte Stufe ────────────────────────────────────────
create table if not exists public.feature_access (
  feature_key text primary key,
  min_tier    text        not null default 'pro',   -- free | supporter | pro | vip
  label       text,
  updated_at  timestamptz not null default now()
);
insert into public.feature_access (feature_key, min_tier, label) values
  ('leaderboard_cash', 'pro', 'Leaderboards: Cash-Schwelle (Punkte fuer Geld)')
on conflict (feature_key) do nothing;

alter table public.feature_access enable row level security;
drop policy if exists feature_access_read on public.feature_access;
create policy feature_access_read on public.feature_access
  for select to anon, authenticated using (true);
grant select on public.feature_access to anon, authenticated;

-- ── 3) Rangfolge + Berechtigung an EINER Stelle ───────────────────────────
create or replace function public.tier_rank(p_tier text)
returns int language sql immutable as $fn$
  select case lower(coalesce(p_tier, ''))
           when 'free' then 0
           when '' then 0
           when 'supporter' then 1
           when 'pro' then 2
           when 'vip' then 3
           else 99 end          -- unbekannte Stufe = nie erfuellbar
$fn$;

-- Hoechste aktive Stufe des aufrufenden Kontos (0 = keine / abgelaufen / anonym)
create or replace function public.my_tier_rank()
returns int language sql stable security definer set search_path = public, auth as $fn$
  select coalesce((
    select greatest(case when t.vip then 3 else 0 end,
                    case when t.pro then 2 else 0 end,
                    case when t.supporter then 1 else 0 end)
    from public.user_tiers t
    where t.user_id = auth.uid()
      and (t.valid_until is null or t.valid_until >= current_date)
  ), 0)
$fn$;

-- Darf der Aufrufer dieses Feature sehen? Unbekanntes Feature = NEIN
-- (bewusst "fail closed": ein Tippfehler verschenkt kein Bezahl-Feature).
create or replace function public.has_feature(p_feature text)
returns boolean language sql stable security definer set search_path = public as $fn$
  select coalesce((
    select public.my_tier_rank() >= public.tier_rank(f.min_tier)
    from public.feature_access f where f.feature_key = p_feature
  ), false)
$fn$;
revoke execute on function public.has_feature(text) from public;
grant execute on function public.has_feature(text) to anon, authenticated;
revoke execute on function public.my_tier_rank() from public;
grant execute on function public.my_tier_rank() to anon, authenticated;

-- ── 4) Leaderboard-Daten mit Cash-Gate ────────────────────────────────────
create or replace function public.leaderboard_thresholds()
returns table (
  fixture_slug text, fixture_name text, start_date date,
  competition text, rarity text, lineups int, top_score numeric,
  cash_rank int, cash_score numeric,
  essence_rank int, essence_score numeric,
  cash_locked boolean
)
language sql stable security definer set search_path = public as $fn$
  with g as (select public.has_feature('leaderboard_cash') as ok)
  select r.fixture_slug, r.fixture_name, r.start_date,
         r.competition, r.rarity, r.lineups, r.top_score,
         r.cash_rank,                       -- bezahlte Raenge bleiben frei
         case when g.ok then r.cash_score end,   -- NUR die Punkte-Schwelle ist Pro
         r.essence_rank, r.essence_score,
         not g.ok
  from public.reward_thresholds r cross join g
  order by r.start_date
$fn$;
revoke execute on function public.leaderboard_thresholds() from public;
grant execute on function public.leaderboard_thresholds() to anon, authenticated;

-- Direkter Tabellenzugriff wird entzogen — sonst waere das Gate wertlos.
drop policy if exists reward_thresholds_read on public.reward_thresholds;
revoke select on public.reward_thresholds from anon, authenticated;

-- ── 5) Schalter setzen (fuer Jonas) ───────────────────────────────────────
-- Aufruf im SQL-Editor:  select set_user_tier('mail@example.com', 'pro');
--   ausschalten:         select set_user_tier('mail@example.com', 'pro', false);
--   befristet:           select set_user_tier('mail@example.com', 'pro', true, '2026-12-31');
create or replace function public.set_user_tier(
  p_email       text,
  p_tier        text,
  p_on          boolean default true,
  p_valid_until date default null
)
returns text language plpgsql security definer set search_path = public, auth as $fn$
declare uid uuid; res text;
begin
  -- Erlaubt: direkter SQL-Zugriff (kein JWT) ODER ein Analytics-Admin.
  if current_setting('request.jwt.claims', true) is not null
     and not public.is_analytics_admin() then
    raise exception 'not allowed';
  end if;
  if lower(p_tier) not in ('supporter', 'pro', 'vip') then
    raise exception 'unbekannte Stufe: % (supporter|pro|vip)', p_tier;
  end if;

  select id into uid from auth.users where lower(email) = lower(p_email);
  if uid is null then return 'kein Konto mit ' || p_email; end if;

  insert into public.user_tiers (user_id) values (uid) on conflict (user_id) do nothing;
  execute format('update public.user_tiers set %I = $1, valid_until = $2, updated_at = now() where user_id = $3',
                 lower(p_tier)) using p_on, p_valid_until, uid;

  res := p_email || ': ' || p_tier || ' = ' || p_on
         || coalesce(' bis ' || p_valid_until::text, ' (unbefristet)');
  return res;
end $fn$;
revoke execute on function public.set_user_tier(text, text, boolean, date) from public, anon;
grant execute on function public.set_user_tier(text, text, boolean, date) to authenticated;

-- ── 6) Selbst-Schalter fuer den Betreiber (Wunsch Jonas 06.09.) ───────────
-- Zum Testen der Bezahl-Features im eigenen Profil: an/aus, ohne SQL-Editor.
-- ZWEI Sperren: (a) nur ein Analytics-Admin darf die Funktion ueberhaupt
-- ausfuehren, (b) sie fasst ausschliesslich die Zeile von auth.uid() an —
-- selbst bei einem Fehler in (a) koennte niemand fremde Konten freischalten.
create or replace function public.set_my_tier(p_tier text, p_on boolean default true)
returns text language plpgsql security definer set search_path = public, auth as $fn$
declare uid uuid := auth.uid();
begin
  if uid is null then raise exception 'nicht eingeloggt'; end if;
  if not public.is_analytics_admin() then raise exception 'not allowed'; end if;
  if lower(p_tier) not in ('supporter', 'pro', 'vip') then
    raise exception 'unbekannte Stufe: % (supporter|pro|vip)', p_tier;
  end if;
  insert into public.user_tiers (user_id, source) values (uid, 'self-test')
    on conflict (user_id) do nothing;
  execute format('update public.user_tiers set %I = $1, updated_at = now() where user_id = $2',
                 lower(p_tier)) using p_on, uid;
  return lower(p_tier) || ' = ' || p_on;
end $fn$;
revoke execute on function public.set_my_tier(text, boolean) from public, anon;
grant execute on function public.set_my_tier(text, boolean) to authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- 1) Anonym darf die Tabelle NICHT mehr lesen (muss leer/401 geben):
--    curl ".../rest/v1/reward_thresholds?select=cash_score&limit=1" -H "apikey: <anon>"
-- 2) Die RPC liefert Zeilen, aber cash_score = null und cash_locked = true:
select competition, rarity, cash_score, cash_locked, essence_score
from public.leaderboard_thresholds() limit 5;
-- 3) Freischalten (deine Konto-Mail einsetzen) und Punkt 2 erneut pruefen:
--    select set_user_tier('jonas.rehr@outlook.de', 'pro');
select feature_key, min_tier from public.feature_access;
