-- ═══════════════════════════════════════════════════════════════════════════
-- price_history-Diaet v2  (20.08.2026) — ERSETZT 2026-08-02_price_history_diet.sql
-- (die alte Fassung wurde nie eingespielt; diese hier ergaenzt Index-Drops und
-- den wiederkehrenden Aufraeum-Job)
--
-- AUSGANGSLAGE (Groessen-/Index-Report Jonas, 20.08.):
--   price_history gesamt 346 MB = 127 MB Daten + 219 MB Indizes, DB bei 515/500 MB.
--   Bis zum 02.08. schrieb der Updater TAEGLICH eine Zeile pro Karte, auch bei
--   unveraendertem Preis — fast alles Wiederholungen.
--
-- WAS PASSIERT (alles verlustfrei fuer die Aussagekraft):
--  1. Duplikate raus: Zeilen, deren Preis identisch mit dem Vorgaenger derselben
--     Karte ist. Ein fehlender Tag BEDEUTET "unveraendert" — genau so liest
--     unser Code (Carry-Forward in calcChanges/player_history) die Tabelle
--     bereits heute.
--  2. Drei tote/redundante Indizes (Report: Benutzungen 0 / 25 / Praefix):
--       price_history_pkey (37 MB, 0 Scans — niemand fragt nach id; die
--         id-SPALTE bleibt, nur der Index/Constraint faellt),
--       price_history_player_slug_scarcity_idx (17 MB, Praefix des grossen
--         Unique-Keys — der uebernimmt alle 25 bisherigen Nutzungen),
--       card_prices_player_slug_idx (2,3 MB, Praefix von slug_scarcity_elig_key).
--  3. Aufraeum-Job fuer die Zukunft (Stufe 2 der Kompression): aelter als
--     90 Tage -> nur noch der letzte Punkt je Kalenderwoche. Greift erst ab
--     ~Ende Oktober (aelteste Daten sind vom 21.07.), dann automatisch
--     woechentlich via pg_cron. Kein Feature schaut weiter als 90 Tage zurueck
--     (Sparklines 30d, Prozente 45d) — die Kurvenform bleibt erhalten.
--
-- WICHTIG: Das abschliessende VACUUM FULL steht NICHT in dieser Datei — es darf
-- nicht in einer Transaktion laufen. Es kommt als eigener zweiter Schritt
-- (siehe unten) und gibt den Platz erst wirklich frei.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 0) Vorher-Zahlen (nur Info) ────────────────────────────────────────────
select count(*) as zeilen_vorher from public.price_history;

-- ── 1) Duplikate loeschen ──────────────────────────────────────────────────
delete from public.price_history
where id in (
  select id from (
    select id, price,
           lag(price) over (partition by player_slug, scarcity, eligibility
                            order by recorded_at) as vorheriger_preis
    from public.price_history
  ) t
  where vorheriger_preis is not null and price = vorheriger_preis
);

select count(*) as zeilen_nachher from public.price_history;

-- ── 2) Tote/redundante Indizes ─────────────────────────────────────────────
alter table public.price_history drop constraint if exists price_history_pkey;
drop index if exists public.price_history_player_slug_scarcity_idx;
drop index if exists public.card_prices_player_slug_idx;

-- ── 3) Wochen-Vergroeberung als wiederkehrender Job ────────────────────────
create or replace function public.price_history_rollup(p_keep_days int default 90)
returns int
language plpgsql security definer set search_path = public as $$
declare n int;
begin
  -- Aelter als p_keep_days: je Karte und Kalenderwoche nur den JUENGSTEN
  -- Punkt behalten. Loescht nie den letzten Stand einer Karte (rn=1 bleibt).
  delete from public.price_history ph
  using (
    select id,
           row_number() over (
             partition by player_slug, scarcity, eligibility,
                          date_trunc('week', recorded_at::timestamp)
             order by recorded_at desc, id desc) as rn
    from public.price_history
    where recorded_at < current_date - p_keep_days
  ) t
  where ph.id = t.id and t.rn > 1;
  get diagnostics n = row_count;
  return n;
end $$;
revoke all on function public.price_history_rollup(int) from public;

-- Woechentlich montags 06:30 UTC (ausserhalb aller Cron-Fenster; Roster 07:00
-- ist der naechste Nachbar). Fallback-sicher: fehlt pg_cron, faellt NUR dieser
-- Block weg (Hinweis in den Messages), der Rest der Migration bleibt bestehen.
do $$
begin
  create extension if not exists pg_cron;
  perform cron.schedule('price_history_rollup_weekly', '30 6 * * 1',
                        'select public.price_history_rollup(90)');
exception when others then
  raise notice 'pg_cron nicht verfuegbar (%): Rollup-Job bitte melden, dann haengen wir ihn an den Harvester', sqlerrm;
end $$;

-- ═══ SCHRITT 2 — NACH dieser Datei als EIGENE Abfrage ausfuehren: ═══════════
--
--   vacuum full public.price_history;
--
-- Packt Tabelle + verbleibende Indizes neu (auch der 149-MB-Key schrumpft
-- proportional zu den geloeschten Zeilen). Sperrt die Tabelle 1-2 Minuten —
-- am besten AUSSERHALB der Updater-Fenster (frei: 05-15 und 21 Uhr UTC).
