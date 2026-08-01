-- price_history entschlacken (02.08.)
--
-- PROBLEM: Der Updater schrieb pro bewerteter Karte JEDEN TAG eine Zeile — bei
-- ~104.000 Karten also rund 100.000 Zeilen taeglich, ~3 Mio pro Monat. Die
-- allermeisten davon wiederholen denselben Preis (illiquide Karten bewegen sich
-- tagelang nicht). Stand 27.07.: 1.589.335 Zeilen.
--
-- LOESUNG: Nur noch Aenderungen speichern. Der Updater macht das ab sofort
-- (update-scarcity.mjs, Deploy 02.08.); diese Migration raeumt die Altlast auf.
--
-- WARUM DAS NICHTS KAPUTT MACHT: Alle Auswertungen lesen den letzten bekannten
-- Wert VOR einem Stichtag (Carry-Forward) — `calcChanges()` im Updater ebenso wie
-- `player_history()` fuer die Sparklines. Entfernt man einen Punkt, der denselben
-- Preis hat wie sein Vorgaenger, bleibt dieses Ergebnis fuer JEDEN Stichtag
-- identisch: der Vorgaenger liefert exakt denselben Wert. Es geht also keine
-- Information verloren, nur Wiederholung.
--
-- ACHTUNG: Dieser Schritt LOESCHT Zeilen (im Gegensatz zur Index-Bereinigung).
-- Deshalb: erst zaehlen, Zahl ansehen, dann loeschen.

-- ── SCHRITT 1: Erst nur ZAEHLEN (nichts wird veraendert) ────────────────────
-- Wie viele Zeilen sind reine Wiederholungen des Vortagswerts?
select count(*) as ueberfluessige_zeilen
from (
  select price,
         lag(price) over (partition by player_slug, scarcity, eligibility
                          order by recorded_at) as vorheriger_preis
  from public.price_history
) t
where vorheriger_preis is not null and price = vorheriger_preis;

-- Zum Vergleich: Gesamtzahl
select count(*) as zeilen_gesamt from public.price_history;

-- ── SCHRITT 2: Loeschen (erst ausfuehren, wenn die Zahlen oben plausibel sind)
-- Behalten wird jeweils der ERSTE Punkt einer Preisstufe — also jeder echte
-- Preiswechsel. Geloescht werden nur Punkte, die exakt dem Vorgaenger entsprechen.
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

-- ── SCHRITT 3: Platz freigeben ──────────────────────────────────────────────
-- Hier IST vacuum full sinnvoll (anders als bei der Index-Bereinigung): es wurden
-- echte Zeilen geloescht, deren Platz sonst belegt bliebe. Die Tabelle ist dabei
-- ~1 Minute exklusiv gesperrt -> ausserhalb der Updater-Fenster ausfuehren
-- (Cron laeuft 22-23, 0-4 und 16-20 UTC; also z. B. mittags).
vacuum full public.price_history;
analyze public.price_history;

-- ── SCHRITT 4: Kontrolle ────────────────────────────────────────────────────
select count(*) as zeilen_danach from public.price_history;
select pg_size_pretty(pg_total_relation_size('public.price_history')) as groesse_danach;

-- Stichprobe: ein liquider Spieler sollte weiterhin eine sinnvolle Kurve haben
-- select recorded_at, price from price_history
-- where player_slug = 'ivan-perisic' and scarcity = 'limited' and eligibility = 'classic'
-- order by recorded_at;

-- ── Optional fuer spaeter: Aufbewahrungsgrenze ──────────────────────────────
-- Mit dem Aenderungs-Schreiben waechst die Tabelle nur noch langsam. Falls doch
-- noetig, kann sie zusaetzlich begrenzt werden (Sparklines brauchen 30-90 Tage):
-- create or replace function public.prune_price_history(p_days int default 400)
-- returns int language plpgsql security definer set search_path = public as $$
-- declare n int;
-- begin
--   delete from public.price_history where recorded_at < current_date - p_days;
--   get diagnostics n = row_count; return n;
-- end $$;
