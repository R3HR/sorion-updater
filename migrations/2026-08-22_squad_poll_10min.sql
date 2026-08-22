-- Poll-Takt auf 10 Minuten (Entscheidung Jonas; nach INC-005 zwischenzeitlich
-- auf 15 gesetzt). Der Poller ist NICHT die Ursache des Ausfalls gewesen —
-- das war der Cache-Waermer mit 6 Vollaggregaten ueber card_prices alle 10 Min.
-- squad-poll macht pro Lauf 1 Sorare-Abfrage + wenige kleine Schreibvorgaenge
-- auf einer Mini-Tabelle; 144 statt 96 Laeufe/Tag sind dafuer unkritisch.
select cron.unschedule('squad-poll-15min');
select cron.schedule(
  'squad-poll-10min',
  '*/10 * * * *',
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
