-- Filter nach Land/Liga und Position (30.07.)
--
-- 1) position: gab es bisher gar nicht. Sorare liefert sie am anyPlayer als
--    `anyPositions` (Array, i. d. R. ein Wert): Goalkeeper | Defender | Midfielder | Forward.
-- 2) league_country: Land der LIGA (aus activeClub.domesticLeague.country.code),
--    ausdruecklich NICHT die Nationalitaet des Spielers — die gibt es ebenfalls und
--    waere etwas anderes (Beispiel: Heung-min Son = Nationalitaet 'kr', Liga 'us';
--    Jude Bellingham = 'gb-eng', Liga 'es'). Der Name macht die Unterscheidung explizit.
--    Codes sind ISO-2 ('de','es','us') plus die britischen Sonderfaelle 'gb-eng','gb-sct'.
--
-- Beide Spalten fuellt der Updater ab sofort bei jedem Durchlauf mit; ein voller Sweep
-- dauert ~2-3 Tage, danach sind sie flaechendeckend gesetzt. Die UI kommt mit dem
-- Uebergangszustand zurecht (Filter erscheinen erst, wenn genug Daten da sind).

alter table public.card_prices add column if not exists position       text;
alter table public.card_prices add column if not exists league_country text;

-- Liga-Namen vereinheitlichen: In der DB stehen teils Sponsorennamen aus der Drittquelle
-- (sorarehoops), der Updater schreibt kuenftig die Sorare-Bezeichnung. Ohne Angleichung
-- erschiene dieselbe Liga doppelt im Filter. Nur die beiden per API VERIFIZIERTEN
-- Zuordnungen — alle uebrigen gleichen sich beim naechsten Queue-Durchlauf von selbst an.
update public.card_prices set league_name = 'Eredivisie'       where league_name = 'VriendenLoterij Eredivisie';
update public.card_prices set league_name = 'Primera División' where league_name = 'LALIGA EA Sports';

-- Verifikation:
--   select league_country, league_name, count(*) from card_prices
--     where league_name is not null group by 1,2 order by 1,3 desc;
--   select position, count(*) from card_prices group by 1;   -- fuellt sich ueber ~2-3 Tage
