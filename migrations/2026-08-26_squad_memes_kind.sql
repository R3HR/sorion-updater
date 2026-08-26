-- Getrennte Meme-Sammlungen (Vorgabe Jonas 26.08.): Die Memes fuer die
-- Handlungs-Erinnerung duerfen sich nicht mit denen der Entwarnung mischen.
--   'reminder' = Handlungsbedarf (Konflikte, fehlende Aufstellungen)
--   'allclear' = alles in Ordnung
alter table public.squad_memes add column if not exists kind text not null default 'reminder';
update public.squad_memes set kind = 'reminder' where kind is null;
