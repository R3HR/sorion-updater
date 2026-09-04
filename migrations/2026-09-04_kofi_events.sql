-- ═══════════════════════════════════════════════════════════════════════════
-- Ko-fi-Ereignisse (Spenden, Mitgliedschaften)  (04.09.2026)
--
-- WOZU: Der Discord-Service (services/discord) legt hier jedes verarbeitete
-- Ko-fi-Ereignis ab. Das ist (a) die Dedup-Grundlage — Ko-fi wiederholt
-- Webhooks bei ausbleibender Antwort — und (b) die Historie fuer alles, was
-- Jonas spaeter will: Milestones ("50. Supporter"), Monatsstatistik,
-- Rollen-Abgleich, Zaehler in der Nachricht.
--
-- DATENSCHUTZ: KEINE E-Mail-Adresse. Der Anzeigename nur, wenn der Spender
-- ihn bei Ko-fi oeffentlich gesetzt hat (is_public). Nur der Service-Key
-- schreibt und liest — die Tabelle ist NICHT oeffentlich.
--
-- SCHREIBLAST (Lehre INC-005/006): ein Insert je Spende. Vernachlaessigbar.
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════
create table if not exists public.kofi_events (
  message_id          text        primary key,        -- Ko-fi message_id (Dedup)
  tx_id               text,
  occurred_at         timestamptz not null,
  type                text        not null,           -- Tip | Subscription | Commission | Shop Order
  kind                text        not null,           -- donation | sub_new | sub_renew
  amount              numeric     not null,
  currency            text        not null default 'EUR',
  from_name           text,                           -- null, wenn nicht oeffentlich
  is_public           boolean     not null default true,
  message             text,
  tier_key            text,                           -- supporter | pro | vip | member
  tier_name           text,
  discord_message_id  text,                           -- fuer spaetere Korrekturen
  created_at          timestamptz not null default now()
);
create index if not exists idx_kofi_events_time on public.kofi_events (occurred_at desc);
create index if not exists idx_kofi_events_kind on public.kofi_events (kind, occurred_at desc);

-- Zugriff: nur Service-Rolle. Kein anon, kein authenticated.
alter table public.kofi_events enable row level security;
revoke all on public.kofi_events from anon, authenticated;

-- Zahlen fuer den Nachrichten-Footer und spaetere Statistik.
-- supporters_total = verschiedene Namen + anonyme Einzelereignisse;
-- month_total_eur  = Summe des laufenden Kalendermonats (nur EUR).
create or replace function public.kofi_stats()
returns table (supporters_total int, month_total_eur numeric)
language sql stable security definer set search_path = public as $$
  select
    (select count(distinct coalesce(from_name, message_id))::int from public.kofi_events),
    (select coalesce(round(sum(amount), 2), 0) from public.kofi_events
      where currency = 'EUR' and occurred_at >= date_trunc('month', now()))
$$;
revoke all on function public.kofi_stats() from public;
-- absichtlich KEIN grant an anon/authenticated: nur der Service ruft sie auf.

-- Verifikation
select count(*) as kofi_events from public.kofi_events;
select * from public.kofi_stats();
