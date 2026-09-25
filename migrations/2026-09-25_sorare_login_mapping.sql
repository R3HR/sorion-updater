-- ═══════════════════════════════════════════════════════════════════════════
-- Anmeldung nur mit Sorare: Zuordnung umbenennungsfest machen (25.09.2026)
--
-- Die Edge Function sorare-oauth (action "login") sucht das Konto primaer ueber
-- die STABILE Sorare-User-ID und nur ersatzweise ueber den Slug. Die Spalte
-- fehlte in dieser Datenbank, die Funktion faellt deshalb still auf den Slug
-- zurueck. Nach einer Umbenennung (BUG-014, jr3hr -> R3HR) legt der Slug-Weg ein
-- ZWEITES Konto an und die alten Daten sind verwaist. Deshalb jetzt nachziehen.
-- ═══════════════════════════════════════════════════════════════════════════

alter table public.sorare_users add column if not exists sorare_user_id text;

-- Eine Sorare-Identitaet darf hoechstens einem Sorion-Konto gehoeren.
create unique index if not exists sorare_users_sorare_user_id_key
  on public.sorare_users (sorare_user_id) where sorare_user_id is not null;

-- Nachschlagen beim Login
create index if not exists sorare_users_slug_idx on public.sorare_users (sorare_slug);

-- Kein anon/authenticated-Zugriff: die Tabelle ist reine Server-Zuordnung.
revoke all on public.sorare_users from anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
select count(*) as zeilen,
       count(*) filter (where sorare_user_id is not null) as mit_stabiler_id
from public.sorare_users;
