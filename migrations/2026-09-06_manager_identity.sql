-- ═══════════════════════════════════════════════════════════════════════════
-- Manager-Identitaet: Umbenennungen bei Sorare ueberleben  (06.09.2026, BUG-039)
--
-- BEFUND: Jonas benannte sich bei Sorare von JR3HR in R3HR um. Sorare behaelt den
-- alten Slug "jr3hr" als Alias, der neue kanonische Slug lautet aber
-- "r3hr-7625d620-89dc-4c09-86b1-d1b3a7f21c50" (Nickname + UUID, 41 Zeichen).
-- Folgen: (1) die Sorare-Neuverifizierung schrieb den UUID-Slug ins Profil,
-- (2) sync-portfolio lehnte ihn ab (Regex max. 40 Zeichen), (3) "r3hr" als
-- Eingabe kennt Sorare nicht. Jeder umbenannte Manager verliert so den Zugang.
--
-- MODELL: Jeder Manager hat EINEN Primaer-Slug = manager_sync.sorare_slug, unter dem
-- ALLE Daten liegen (manager_cards, manager_trades, so5_lineups, so5_card_earnings,
-- squad_*). Dazu kommen Sorares feste Nutzer-ID, der aktuelle kanonische Slug und
-- der Nickname. Eingaben (alter Slug, neuer Slug, Nickname) werden per
-- resolve_manager() auf den Primaer-Slug aufgeloest. Zeilen werden NIE umbenannt.
-- Sorare bietet keine Suche per ID an; die ID dient dem Abgleich innerhalb unserer Daten.
--
-- AUSFUEHREN: per CLI (npx supabase db query --linked --file ...).
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Identitaetsfelder ───────────────────────────────────────────────────
alter table public.manager_sync
  add column if not exists sorare_user_id text,   -- Sorare "User:<uuid>", stabil bei Umbenennung
  add column if not exists current_slug   text,   -- Sorares aktueller kanonischer Slug
  add column if not exists nickname       text;   -- Anzeigename bei Sorare (z. B. "R3HR")
create index if not exists idx_manager_sync_user_id  on public.manager_sync (sorare_user_id);
create index if not exists idx_manager_sync_current  on public.manager_sync (current_slug);
create index if not exists idx_manager_sync_nickname on public.manager_sync (lower(nickname));

alter table public.profiles
  add column if not exists sorare_user_id text;    -- gesetzt bei der Sorare-Verifizierung

-- ── 2) Aufloesung: Eingabe -> Primaer-Slug (oder null) ───────────────────────
-- Reihenfolge: exakter Primaer-Slug, aktueller Slug, eindeutiger Nickname.
-- Oeffentlich aufrufbar (nur Slug-Zuordnung, keine sensiblen Daten).
create or replace function public.resolve_manager(p_input text)
returns text
language sql stable security definer set search_path = public as $$
  with i as (select lower(trim(coalesce(p_input, ''))) as v)
  select s.sorare_slug
  from public.manager_sync s, i
  where i.v <> ''
    and (   s.sorare_slug = i.v
         or s.current_slug = i.v
         or (lower(s.nickname) = i.v
             and (select count(*) from public.manager_sync s2 where lower(s2.nickname) = i.v) = 1))
  order by (s.sorare_slug = i.v) desc, (s.current_slug = i.v) desc
  limit 1
$$;
revoke all on function public.resolve_manager(text) from public;
grant execute on function public.resolve_manager(text) to anon, authenticated;

-- ── 3) Datenfix fuer den ausloesenden Fall ───────────────────────────────────
-- Identitaet von jr3hr eintragen (Sorare-Antwort vom 06.09.), damit die Aufloesung
-- "r3hr" / "r3hr-7625..." -> "jr3hr" sofort greift, ohne auf den naechsten Sync zu warten.
update public.manager_sync
   set sorare_user_id = 'User:433001e3-3d84-4c45-88a6-3b290fb12e40',
       current_slug   = 'r3hr-7625d620-89dc-4c09-86b1-d1b3a7f21c50',
       nickname       = 'R3HR'
 where sorare_slug = 'jr3hr';

-- Profil zurueck auf den Primaer-Slug (die Neuverifizierung hatte den UUID-Slug geschrieben)
update public.profiles
   set sorare_slug = 'jr3hr',
       sorare_user_id = 'User:433001e3-3d84-4c45-88a6-3b290fb12e40'
 where user_id = '06bf8075-3823-48b0-90c1-0052a9915e66'
   and sorare_slug = 'r3hr-7625d620-89dc-4c09-86b1-d1b3a7f21c50';

-- Haengengebliebene Claim-Zeile eines nie synchronisierten Slugs (Nebenfehler BUG-036-Fix)
delete from public.manager_sync where sorare_slug = 'r3hr' and card_count is null;

-- ── Verifikation ───────────────────────────────────────────────────────────
select public.resolve_manager('r3hr')  as via_nickname,
       public.resolve_manager('R3HR')  as via_nickname_gross,
       public.resolve_manager('r3hr-7625d620-89dc-4c09-86b1-d1b3a7f21c50') as via_current_slug,
       public.resolve_manager('jr3hr') as via_primary,
       public.resolve_manager('gibt-es-nicht') as unbekannt;
select sorare_slug, sorare_verified, sorare_user_id from public.profiles where user_id = '06bf8075-3823-48b0-90c1-0052a9915e66';
