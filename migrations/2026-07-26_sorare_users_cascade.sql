-- Fix BUG-011 (Befund 1.7) — sorare_users blockiert die Kontolöschung
-- Im Supabase Dashboard → SQL Editor ausführen.
--
-- Problem: sorare_users.supabase_user_id -> auth.users(id) stand auf ON DELETE NO ACTION.
-- Dadurch schlug delete-account (DSGVO Art. 17) für jeden Sorare-Login-Nutzer mit
-- Foreign-Key-Verletzung fehl, und personenbezogene Daten (slug/nickname) blieben liegen.
-- Fix: auf ON DELETE CASCADE umstellen — konsistent mit profiles/watchlist.

alter table sorare_users
  drop constraint sorare_users_supabase_user_id_fkey;

alter table sorare_users
  add constraint sorare_users_supabase_user_id_fkey
    foreign key (supabase_user_id) references auth.users(id) on delete cascade;

-- Kontrolle: sollte jetzt CASCADE zeigen
select
  con.conname as constraint_name,
  case con.confdeltype when 'c' then 'CASCADE' else con.confdeltype::text end as on_delete
from pg_constraint con
join pg_class rel on rel.oid = con.conrelid
where rel.relname = 'sorare_users' and con.contype = 'f';
