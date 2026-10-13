-- SEC-006 Schritt 2a — RPCs anlegen (ungefaehrlich, nimmt nichts weg)
-- Im Supabase Dashboard → SQL Editor ausführen.
-- Reihenfolge: (1) dieses Skript, (2) portfolio.html ausrollen + testen,
--              (3) 2026-10-13_manager_lockdown.sql
--
-- Vorher: manager_cards/-trades/-sync hatten SELECT-Policies `USING (true)`.
-- Das Frontend grenzte nur per `?sorare_slug=eq.<slug>` in der URL ein — das ist
-- keine Autorisierung. Jeder konnte alle 109.000 Zeilen am Stück abziehen und die
-- geplante Pro-Funktion "Manager suchen" gratis nutzen.
--
-- Nachher: kein direkter Tabellenzugriff mehr. Zugriff nur über RPCs, die
-- pro Manager-Slug antworten und selbst entscheiden:
--   • eigener Slug            -> immer erlaubt (auch ohne Pro)
--   • fremder Slug            -> nur mit Pro/VIP/Creator
--   • nicht eingeloggt        -> nichts
-- Hinweis: Kauf-/Verkaufspreise stammen aus der ÖFFENTLICHEN Sorare-API
-- (tokenOwner.amounts). Es geht hier nicht um Geheimhaltung, sondern um den
-- Schutz des aufgebauten Datensatzes und um die Monetarisierung.

-- ── 1) Pro-Prüfung als wiederverwendbare Funktion ─────────────────────────
create or replace function public.is_pro(p_uid uuid)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (
    select 1
    from user_tiers t
    where t.user_id = p_uid
      and (t.pro or t.vip or t.creator)
      and (t.valid_until is null or t.valid_until >= current_date)
  );
$$;

-- Darf der aktuelle Aufrufer diesen Manager sehen?
create or replace function public.may_view_manager(p_slug text)
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select auth.uid() is not null
     and (
       exists (select 1 from sorare_users su
                where su.supabase_user_id = auth.uid()
                  and su.sorare_slug = p_slug)
       or public.is_pro(auth.uid())
     );
$$;

-- ── 2) RPCs je Tabelle (setof <tabelle> = Spalten bleiben automatisch gleich)
create or replace function public.manager_cards_for(p_slug text)
returns setof manager_cards
language sql stable security definer set search_path = public as $$
  select m.* from manager_cards m
  where m.sorare_slug = p_slug and public.may_view_manager(p_slug);
$$;

create or replace function public.manager_trades_for(p_slug text)
returns setof manager_trades
language sql stable security definer set search_path = public as $$
  select m.* from manager_trades m
  where m.sorare_slug = p_slug and public.may_view_manager(p_slug);
$$;

create or replace function public.manager_sync_for(p_slug text)
returns setof manager_sync
language sql stable security definer set search_path = public as $$
  select m.* from manager_sync m
  where m.sorare_slug = p_slug and public.may_view_manager(p_slug);
$$;

-- ── 3) Nur die RPCs freigeben ─────────────────────────────────────────────
grant execute on function public.manager_cards_for(text)  to anon, authenticated;
grant execute on function public.manager_trades_for(text) to anon, authenticated;
grant execute on function public.manager_sync_for(text)   to anon, authenticated;
grant execute on function public.may_view_manager(text)   to anon, authenticated;
grant execute on function public.is_pro(uuid)             to anon, authenticated;

-- Dieser Teil ist UNGEFAEHRLICH: er legt nur Funktionen an, nimmt nichts weg.
-- Das alte Frontend laeuft unveraendert weiter.
-- Danach: portfolio.html ausrollen, pruefen, DANN Teil 2 ausfuehren.
