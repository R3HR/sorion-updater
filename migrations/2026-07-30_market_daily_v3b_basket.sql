-- NACHTRAG v3b (30.07.) — behebt eine Methoden-Naht in der Auffüllung aus v3.
--
-- Befund nach v3: Der heutige Snapshot (aus card_prices, nur Zeilen mit aktuellem FMV)
-- zählte 6.217 Spieler, die aufgefüllten Vortage 7.680 — verschiedene Grundgesamtheiten.
-- Ein Prozentwert über zwei unterschiedliche Körbe misst teils die Korb-Änderung, nicht
-- den Markt (derselbe Fehlertyp wie bei den rotierenden Stichproben).
--
-- v3b: Die aufgefüllten Tage verwenden EXAKT den Korb der heutigen Bewertung — die
-- Spieler, die aktuell einen FMV haben. Für jeden dieser Spieler wird pro Tag der letzte
-- bekannte Preis <= Tag genommen. Damit ist die Reihe same-basket und direkt mit dem
-- angezeigten Avg vergleichbar. Ab morgen liefert der Harvester ohnehin reine
-- card_prices-Snapshots (identische Methode).

-- Aufgefüllte Tage verwerfen (heutigen echten Snapshot behalten)
delete from public.market_daily where day < current_date;

-- Same-basket-Auffüllung: Korb = Spieler mit aktuellem FMV in card_prices
insert into public.market_daily (day, scarcity, eligibility, avg_fmv, n)
select d.day,
       cp.scarcity,
       coalesce(cp.eligibility, 'in_season') as elig,
       round(avg(x.price)::numeric, 4),
       count(*)::int
from (select generate_series(current_date - 14, current_date - 1, interval '1 day')::date as day) d
join public.card_prices cp on cp.fmv is not null
cross join lateral (
  select ph.price
  from public.price_history ph
  where ph.scarcity    = cp.scarcity
    and ph.eligibility = coalesce(cp.eligibility, 'in_season')
    and ph.player_slug = cp.player_slug
    and ph.recorded_at <= d.day
    and ph.recorded_at >= d.day - 45
  order by ph.recorded_at desc
  limit 1
) x
group by 1, 2, 3
on conflict (day, scarcity, eligibility) do update
  set avg_fmv = excluded.avg_fmv, n = excluded.n;

-- Verifikation: n sollte über die Tage nun ähnlich sein (kein Sprung zum heutigen Tag):
--   select day, avg_fmv, n from market_daily
--   where scarcity='limited' and eligibility='in_season' order by day desc limit 12;
-- Frühe Tage dürfen kleineres n haben (Spieler ohne Historie damals) — market_move
-- lehnt Vergleiche mit >25 % Abweichung automatisch ab.
