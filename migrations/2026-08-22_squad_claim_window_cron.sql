-- Zusaetzlicher 5-Minuten-Takt rund um die Claim-Frist (19:00 Berlin).
-- Die Function verwirft Aufrufe ausserhalb 17:30-19:15 Berlin sofort, daher
-- kostet der haeufige Cron ausserhalb des Fensters praktisch nichts.
select cron.schedule(
  'squad-poll-claimwindow',
  '*/5 * * * *',
  $$
  select net.http_post(
    url := 'https://jxhdlcpdupmkpsoytzes.supabase.co/functions/v1/squad-poll',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'X-Cron-Secret', '__SQUAD_CRON_SECRET__'
    ),
    body := '{"action":"poll","window":"claim"}'::jsonb
  );
  $$
);
