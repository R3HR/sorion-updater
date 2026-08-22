-- Squad-Poller nach INC-005 wieder einschalten.
-- Bewusst */15 statt */10: nach dem Disk-IO-Totalausfall vom 22.08. gilt
-- "so wenig Dauerlast wie moeglich". 15 Min reichen fuer die
-- Aufstellungs-Reihenfolge (Player-Cap) voellig aus.
-- Der Cache-Waermer 'warm_market_aggregates' bleibt ABGESCHALTET (Treiber
-- des Ausfalls) — Kaltstarts faengt seit 20.08. der Client-Wachhund ab.
select cron.schedule(
  'squad-poll-15min',
  '*/15 * * * *',
  $$
  select net.http_post(
    url := 'https://jxhdlcpdupmkpsoytzes.supabase.co/functions/v1/squad-poll',
    headers := jsonb_build_object(
      'Content-Type', 'application/json',
      'X-Cron-Secret', '__SQUAD_CRON_SECRET__'
    ),
    body := '{"action":"poll"}'::jsonb
  );
  $$
);
