-- Spieltags-Zustand mitfuehren (08.09.2026)
--
-- WARUM: Sorare setzt einen gerade beendeten Spieltag zuerst auf "computed" und erst
-- spaeter auf "closed". Die Punktzahlen stehen bereits im Zustand computed fest
-- (geprueft an Game Week 11: alle Rangstufen mit Score belegt, 3.745 USD Preistopf),
-- das Leaderboard ist also schon auswertbar. Bisher nahm der Sync nur "closed" und
-- haette einen Dienstag- oder Freitagabend-Lauf komplett leer laufen lassen.
--
-- WOFUER die Spalte: Ein im Zustand computed erfasster Spieltag darf NICHT als fertig
-- gelten. Sorare korrigiert Scores gelegentlich nach. Der naechste Lauf holt jede Zeile
-- erneut, bis der Spieltag closed ist, und friert sie erst dann ein.

alter table public.reward_thresholds
  add column if not exists fixture_state text;

comment on column public.reward_thresholds.fixture_state is
  'aasmState des Spieltags bei der letzten Synchronisation: computed = vorlaeufig, wird erneut geholt; closed = endgueltig.';

-- Bestand: alles bisher Erfasste stammt aus closed-Spieltagen.
update public.reward_thresholds set fixture_state = 'closed' where fixture_state is null;

create index if not exists reward_thresholds_state_idx
  on public.reward_thresholds (fixture_state) where fixture_state <> 'closed';

-- Verifikation
select fixture_state, count(*) as zeilen, count(distinct fixture_slug) as spieltage
from public.reward_thresholds group by fixture_state;
