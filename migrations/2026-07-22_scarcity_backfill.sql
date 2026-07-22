-- SORION Migration 2026-07-22 (3) — Scarcity-Vervollständigung
-- Regel: Sorare mintet jeden Spieler in allen Seltenheiten. Harvester/Import
-- legten bisher nur die gesehene Scarcity an (z. B. reno-munz ohne limited).
-- Dieses Backfill ergänzt für jeden bekannten Spieler alle fehlenden
-- (scarcity × eligibility)-Kombinationen mit updated_at=epoch (→ Update-Queue).
-- Im Supabase Dashboard → SQL Editor ausführen.

with players as (
  select distinct on (player_slug)
         player_slug, player_name, picture_url, team_name, league_name
  from card_prices
  order by player_slug, updated_at desc
)
insert into card_prices (player_slug, player_name, picture_url, team_name, league_name, scarcity, eligibility, updated_at)
select p.player_slug, p.player_name, p.picture_url, p.team_name, p.league_name,
       s.scarcity, e.eligibility, 'epoch'::timestamptz
from players p
cross join (values ('limited'), ('rare'), ('super_rare')) as s(scarcity)
cross join (values ('in_season'), ('classic')) as e(eligibility)
on conflict (player_slug, scarcity, eligibility) do nothing;

-- Kontrolle: jede Scarcity sollte jetzt gleich viele Spieler haben
select scarcity, eligibility, count(*) from card_prices group by 1, 2 order by 1, 2;
