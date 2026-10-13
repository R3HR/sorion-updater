-- Fix BUG-014 — Identität hing am ÄNDERBAREN Sorare-Slug
-- Im Supabase Dashboard → SQL Editor ausführen.
--
-- Problem: `sorare_users` wurde nur über `sorare_slug` gefunden. Benennt sich ein
-- Nutzer bei Sorare um (Jonas: JR3HR → R3HR), findet der Login nichts, legt ein
-- ZWEITES Konto an — und alle bisherigen Crafts hängen am alten user_id.
-- Fix: zusätzlich die stabile Sorare-User-ID speichern und primär danach suchen.

-- 1) Spalte für die stabile Sorare-User-ID
alter table sorare_users
  add column if not exists sorare_user_id text;

create unique index if not exists sorare_users_sorare_user_id_key
  on sorare_users (sorare_user_id)
  where sorare_user_id is not null;

-- 2) Kontrolle: bestehende Zeilen ansehen
--    (sorare_user_id wird beim nächsten Login je Nutzer automatisch nachgetragen)
select supabase_user_id, sorare_slug, sorare_nickname, sorare_user_id
from sorare_users
order by sorare_slug;
