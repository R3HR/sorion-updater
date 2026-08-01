-- Gespeicherte Portfolios + Trades je Sorare-Manager (01.08.)
--
-- Ziel: API-Verbrauch senken. Heute kostet JEDER Portfolio-Aufruf 6–16 Sorare-Anfragen.
-- Kuenftig kommt die Anzeige aus dieser DB (0 Anfragen); geholt wird nur beim Sync.
--
-- Schutzmechanik: Die Sperre haengt am MANAGER-SLUG, nicht am Betrachter. Ein Manager
-- wird hoechstens 1x pro TTL von Sorare geholt, egal wie viele Leute ihn ansehen —
-- damit ist das Budget auch gegen anonyme Aufrufe geschuetzt.
--
-- Datenschutz: Sorare-Sammlungen und Kaufpreise (tokenOwner) sind OEFFENTLICH — wir
-- spiegeln also nur oeffentliche Daten, keine Geheimnisse unserer Nutzer. Die Zuordnung
-- Konto -> Sorare-Slug liegt weiterhin privat in `profiles` (RLS auf auth.uid()).

-- ── 1) Sync-Status je Manager (das Herz der Sperre) ─────────────────────────
create table if not exists public.manager_sync (
  sorare_slug text primary key,
  synced_at   timestamptz not null default now(),
  card_count  int,
  trade_count int,
  last_error  text
);

-- ── 2) Karten eines Managers ───────────────────────────────────────────────
create table if not exists public.manager_cards (
  sorare_slug text not null,
  card_slug   text not null,
  player_slug text,
  player_name text,
  picture_url text,
  rarity      text,
  in_season   boolean,
  buy_eur     numeric,          -- Kaufpreis aus tokenOwner (oeffentlich)
  buy_date    timestamptz,
  buy_type    text,             -- SINGLE_SALE_OFFER | SHARDS | REWARD | ...
  primary key (sorare_slug, card_slug)
);
create index if not exists idx_manager_cards_slug on public.manager_cards (sorare_slug);

-- ── 3) Realisierte Trades eines Managers ───────────────────────────────────
create table if not exists public.manager_trades (
  sorare_slug text not null,
  card_slug   text not null,
  sold_at     timestamptz not null,
  sell_eur    numeric,
  buy_eur     numeric,          -- letzter Kauf VOR dem Verkauf (kann null sein: Reward/Craft)
  buy_at      timestamptz,
  buy_via     text,             -- 'offer' | 'auction'
  rarity      text,
  primary key (sorare_slug, card_slug, sold_at)
);
create index if not exists idx_manager_trades_slug on public.manager_trades (sorare_slug);

-- ── 4) Lesen ist oeffentlich (gespiegelte oeffentliche Daten), Schreiben nur
--       serverseitig ueber die Edge Function mit Service-Key ────────────────
alter table public.manager_sync   enable row level security;
alter table public.manager_cards  enable row level security;
alter table public.manager_trades enable row level security;

drop policy if exists manager_sync_read   on public.manager_sync;
drop policy if exists manager_cards_read  on public.manager_cards;
drop policy if exists manager_trades_read on public.manager_trades;
create policy manager_sync_read   on public.manager_sync   for select to anon, authenticated using (true);
create policy manager_cards_read  on public.manager_cards  for select to anon, authenticated using (true);
create policy manager_trades_read on public.manager_trades for select to anon, authenticated using (true);

grant select on public.manager_sync, public.manager_cards, public.manager_trades to anon, authenticated;
-- Kein insert/update/delete fuer anon/authenticated: nur der Service-Key (Function) schreibt.

-- ── 5) Konto-Loeschung: gespiegeltes Portfolio mitloeschen ──────────────────
-- Kein FK moeglich (Schluessel ist der Sorare-Slug, nicht die user_id), daher als
-- Funktion, die `delete-account` zusaetzlich aufruft.
create or replace function public.purge_manager_data(p_slug text)
returns void language sql security definer set search_path = public as $$
  delete from public.manager_trades where sorare_slug = p_slug;
  delete from public.manager_cards  where sorare_slug = p_slug;
  delete from public.manager_sync   where sorare_slug = p_slug;
$$;
revoke execute on function public.purge_manager_data(text) from public, anon, authenticated;

-- Verifikation:
--   select * from manager_sync order by synced_at desc limit 5;
--   -- Schreibversuch von aussen muss scheitern:
--   -- curl -X POST .../rest/v1/manager_cards -d '{"sorare_slug":"x","card_slug":"y"}'  => 401/403
