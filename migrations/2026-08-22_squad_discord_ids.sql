-- Zuordnung Sorare-Slug -> Discord-User-ID, damit der Bot Manager direkt
-- @mentionen kann (`<@123...>`). Ohne ID kann Discord niemanden anpingen —
-- der Nickname allein erzeugt nur Text, keine Benachrichtigung.
create table if not exists public.squad_discord_users (
  sorare_slug text primary key,
  discord_id text not null,
  updated_at timestamptz not null default now()
);
alter table public.squad_discord_users enable row level security;
