-- Meme-Pool fuer die taegliche Claim-Erinnerung (Jonas 25.08.).
-- Der Bot haengt an jede Erinnerung ein zufaellig gewaehltes aktives Meme an.
create table if not exists public.squad_memes (
  id bigint generated always as identity primary key,
  url text not null,
  note text,
  active boolean not null default true,
  added_at timestamptz not null default now()
);
alter table public.squad_memes enable row level security;
