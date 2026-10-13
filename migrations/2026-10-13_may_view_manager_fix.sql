-- SEC-006 Nachbesserung — may_view_manager erkannte Sorion-Nutzer nicht
-- Im Supabase Dashboard → SQL Editor ausführen.
--
-- Fehler: Die Funktion prüfte die Eigentümerschaft NUR gegen `sorare_users`.
-- Das ist die Zuordnung von CraftLog. Sorion (portfolio.html) ermittelt den
-- eigenen Manager aber über `profiles.sorare_slug`. Wer nur dort steht - oder
-- wessen Slug in den beiden Tabellen auseinanderläuft (z. B. nach einer
-- Sorare-Umbenennung, BUG-014) - galt als Fremder und bekam leere Ergebnisse.
--
-- Fix: beide Zuordnungen akzeptieren, Vergleich case-insensitiv
-- (portfolio.html speichert den Slug klein: String(slug).toLowerCase()).

create or replace function public.may_view_manager(p_slug text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select auth.uid() is not null
     and (
       -- eigener Manager laut CraftLog-Zuordnung
       exists (select 1 from sorare_users su
                where su.supabase_user_id = auth.uid()
                  and lower(su.sorare_slug) = lower(p_slug))
       -- oder laut Sorion-Zuordnung
       or exists (select 1 from profiles p
                   where p.user_id = auth.uid()
                     and lower(p.sorare_slug) = lower(p_slug))
       -- oder Pro/VIP/Creator → fremde Manager erlaubt
       or public.is_pro(auth.uid())
     );
$$;

grant execute on function public.may_view_manager(text) to anon, authenticated;

-- Kontrolle: zeigt, unter welchen Slugs du jeweils gefuehrt wirst.
-- Weichen sie voneinander oder von manager_sync ab, sollten sie angeglichen werden.
select 'profiles'     as quelle, p.user_id::text as id, p.sorare_slug from profiles p where p.sorare_slug is not null
union all
select 'sorare_users', su.supabase_user_id::text, su.sorare_slug from sorare_users su
order by sorare_slug;
