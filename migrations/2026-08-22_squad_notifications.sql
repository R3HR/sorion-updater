-- Squad-Manager: Discord-Benachrichtigungen (Dedup) + Position je Aufstellung
-- (Position noetig fuer Tages-Ausnahmen wie "TW heute frei").

alter table public.squad_lineup_log add column if not exists position text;

-- Jede Benachrichtigung genau einmal: event_key ist der fachliche Schluessel
-- (z. B. viol:<step>:<spieler>:<manager-set>), squad-poll postet nur, wenn der
-- Insert hier gelingt. RLS ohne Policies = nur Service-Role.
create table if not exists public.squad_notifications (
  event_key text primary key,
  sent_at timestamptz not null default now(),
  payload jsonb
);
alter table public.squad_notifications enable row level security;
