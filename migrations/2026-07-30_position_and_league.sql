-- Filter nach Liga und Position (30.07.)
--
-- Position gab es bisher gar nicht in der DB. Sorare liefert sie am anyPlayer als
-- `anyPositions` (Array, i. d. R. ein Wert): Goalkeeper | Defender | Midfielder | Forward.
-- Der Updater schreibt sie ab sofort bei jedem Durchlauf mit — ein voller Sweep dauert
-- ~2-3 Tage, danach ist die Spalte flaechendeckend gefuellt.

alter table public.card_prices add column if not exists position text;

-- Liga-Namen vereinheitlichen: In der DB stehen teils Sponsorennamen aus der Drittquelle
-- (sorarehoops), der Updater schreibt kuenftig die Sorare-Bezeichnung. Ohne Angleichung
-- erschiene dieselbe Liga doppelt im Filter. Nur die beiden per API VERIFIZIERTEN
-- Zuordnungen — alle uebrigen Namen stimmen bereits ueberein bzw. gleichen sich beim
-- naechsten Queue-Durchlauf von selbst an.
update public.card_prices set league_name = 'Eredivisie'       where league_name = 'VriendenLoterij Eredivisie';
update public.card_prices set league_name = 'Primera División' where league_name = 'LALIGA EA Sports';

-- Verifikation:
--   select league_name, count(*) from card_prices where league_name is not null
--     group by 1 order by 2 desc;
--   select position, count(*) from card_prices group by 1;   -- fuellt sich ueber ~2-3 Tage
