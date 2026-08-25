-- Merken, wann ein Meme zuletzt gezogen wurde, damit sich keines direkt
-- wiederholt (Vorgabe Jonas 25.08.).
alter table public.squad_memes add column if not exists last_used_at timestamptz;
