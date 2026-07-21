-- SORION Migration 2026-07-22 — User-Profile + Watchlist (Trading-Accounts)
-- Im Supabase Dashboard → SQL Editor ausführen.
-- Danach unter Authentication → URL Configuration: Site URL = https://sorion.pro
-- und https://sorion.pro/profile.html als Redirect URL eintragen!

-- 1) Profile (1:1 zu auth.users)
create table if not exists profiles (
  user_id      uuid primary key references auth.users(id) on delete cascade,
  display_name text,
  sorare_slug  text,
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now()
);

alter table profiles enable row level security;

drop policy if exists "profiles_select_own" on profiles;
create policy "profiles_select_own" on profiles for select using (auth.uid() = user_id);
drop policy if exists "profiles_insert_own" on profiles;
create policy "profiles_insert_own" on profiles for insert with check (auth.uid() = user_id);
drop policy if exists "profiles_update_own" on profiles;
create policy "profiles_update_own" on profiles for update using (auth.uid() = user_id);
drop policy if exists "profiles_delete_own" on profiles;
create policy "profiles_delete_own" on profiles for delete using (auth.uid() = user_id);

-- 2) Watchlist mit Zielpreisen (Basis für Stufe 2/3: Badges + Notifications)
create table if not exists watchlist (
  id           bigint generated always as identity primary key,
  user_id      uuid not null references auth.users(id) on delete cascade,
  player_slug  text not null,
  scarcity     text not null default 'limited',
  eligibility  text not null default 'in_season',
  target_price numeric,               -- Zielpreis in EUR
  direction    text not null default 'below',  -- 'below' = kaufen wenn drunter, 'above' = verkaufen wenn drüber
  note         text,
  created_at   timestamptz not null default now(),
  unique (user_id, player_slug, scarcity, eligibility)
);

alter table watchlist enable row level security;

drop policy if exists "watchlist_select_own" on watchlist;
create policy "watchlist_select_own" on watchlist for select using (auth.uid() = user_id);
drop policy if exists "watchlist_insert_own" on watchlist;
create policy "watchlist_insert_own" on watchlist for insert with check (auth.uid() = user_id);
drop policy if exists "watchlist_update_own" on watchlist;
create policy "watchlist_update_own" on watchlist for update using (auth.uid() = user_id);
drop policy if exists "watchlist_delete_own" on watchlist;
create policy "watchlist_delete_own" on watchlist for delete using (auth.uid() = user_id);

-- Kontrolle
select 'profiles' as tbl, count(*) from profiles
union all
select 'watchlist', count(*) from watchlist;
