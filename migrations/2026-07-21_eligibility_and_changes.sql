-- SORION Migration 2026-07-21 — InSeason/Classic + Prozent-Spalten
-- Ersetzt die NICHT ausgeführte Migration 2026-07-06 (alles hier enthalten).
-- Im Supabase Dashboard → SQL Editor als Ganzes ausführen.

-- 1) Neue Spalten
alter table card_prices  add column if not exists change_24h numeric;
alter table card_prices  add column if not exists change_7d  numeric;
alter table card_prices  add column if not exists eligibility text not null default 'in_season';
alter table price_history add column if not exists eligibility text not null default 'in_season';

-- 2) Alte Unique-Constraints/-Indizes entfernen (Namen dynamisch — wir kennen sie nicht)
do $$
declare c record;
begin
  -- Unique-CONSTRAINTS (keine Primary Keys)
  for c in
    select conname, conrelid::regclass::text as tbl
    from pg_constraint
    where contype = 'u'
      and conrelid in ('card_prices'::regclass, 'price_history'::regclass)
  loop
    execute format('alter table %s drop constraint %I', c.tbl, c.conname);
  end loop;
  -- Unique-INDIZES ohne Constraint dahinter
  for c in
    select i.indexrelid::regclass::text as idx
    from pg_index i
    join pg_class t on t.oid = i.indrelid
    where t.relname in ('card_prices', 'price_history')
      and i.indisunique and not i.indisprimary
      and not exists (select 1 from pg_constraint pc where pc.conindid = i.indexrelid)
  loop
    execute format('drop index %s', c.idx);
  end loop;
end $$;

-- 3) Neue Unique-Constraints inkl. eligibility
alter table card_prices
  add constraint card_prices_slug_scarcity_elig_key unique (player_slug, scarcity, eligibility);
alter table price_history
  add constraint price_history_slug_scarcity_date_elig_key unique (player_slug, scarcity, recorded_at, eligibility);

-- 4) Classic-Zeilen für alle vorhandenen Spieler anlegen
--    (updated_at = epoch → der Railway-Updater nimmt sie sofort in die Queue)
insert into card_prices (player_slug, player_name, scarcity, picture_url, team_name, league_name, tier, eligibility, updated_at)
select player_slug, player_name, scarcity, picture_url, team_name, league_name, tier, 'classic', 'epoch'::timestamptz
from card_prices
where eligibility = 'in_season'
on conflict do nothing;

-- Kontrolle: sollte ~gleiche Zahl in_season wie classic zeigen
select eligibility, count(*) from card_prices group by eligibility;
