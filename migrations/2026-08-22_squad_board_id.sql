-- Board-Zugehoerigkeit je Step. Ohne sie laufen die Stage-Nummern beim naechsten
-- Board von 1..10 weiter statt zweimal 1..5 - der Stage-Bonus waere dann falsch.
alter table public.squad_step_scores add column if not exists board_id text;
