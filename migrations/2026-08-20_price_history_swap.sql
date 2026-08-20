-- ═══════════════════════════════════════════════════════════════════════════
-- price_history-Diaet v3: UMKOPIEREN statt Loeschen  (20.08.2026)
-- ERSETZT Schritt 1+2 der v2-Diaet (deren grosses DELETE ist im SQL-Editor
-- am Zeitlimit gestorben — Befund: 1.171.962 von 1.691.974 Zeilen sind noch
-- Duplikate. Der Editor fuehrt Anweisungen EINZELN aus, die Index-Drops
-- danach liefen deshalb trotzdem.)
--
-- STRATEGIE: Bei 69 % Muell ist Kopieren billiger als Loeschen. Die ~520k
-- guten Zeilen wandern in eine frische Tabelle (ohne Indizes -> schneller
-- Bulk-Insert), dann Tausch. Das ersetzt auch das VACUUM FULL — die neue
-- Tabelle ist von Geburt an kompakt. Erwartung: price_history ~296 -> ~90 MB.
--
-- SICHERES ZEITFENSTER: price_history wird NUR vom Preis-Updater beschrieben
-- (Cron 22-23, 0-4, 16-20 UTC). Zwischen 05 und 15 Uhr UTC ausfuehren.
--
-- Nach dem Tausch wird der Lockdown (BUG-012!) EXAKT neu angewendet — eine
-- neue Tabelle bekaeme sonst die Default-Rechte und waere oeffentlich lesbar.
-- ═══════════════════════════════════════════════════════════════════════════

-- ── 1) Frische Tabelle: gleiche Spalten/Defaults/Identity, KEINE Indizes ───
create table public.price_history_neu
  (like public.price_history including defaults including identity including constraints);

-- ── 2) Nur die Behalter kopieren: erster Punkt + jede echte Aenderung ──────
insert into public.price_history_neu overriding system value
select ph.* from public.price_history ph
where ph.id in (
  select id from (
    select id, price,
           lag(price) over (partition by player_slug, scarcity, eligibility
                            order by recorded_at) as prev
    from public.price_history
  ) t
  where prev is null or price is distinct from prev
);

-- Identity-Zaehler hinter die kopierten IDs setzen (Updater fuegt ohne id ein)
select setval(pg_get_serial_sequence('public.price_history_neu', 'id'),
              (select coalesce(max(id), 1) from public.price_history_neu));

-- ── 3) Tausch ──────────────────────────────────────────────────────────────
drop table public.price_history;
alter table public.price_history_neu rename to price_history;

-- ── 4) Indizes neu (nur die zwei, die der Report als lebendig auswies) ─────
-- Unique-Key = Konfliktziel des Updater-Upserts (onConflict:
-- 'player_slug,scarcity,recorded_at,eligibility' in update-scarcity.mjs:280)
alter table public.price_history
  add constraint price_history_slug_scarcity_date_elig_key
  unique (player_slug, scarcity, recorded_at, eligibility);
create index price_history_recorded_at_idx on public.price_history (recorded_at);
-- BEWUSST KEIN Primaerschluessel: der alte pkey hatte 0 Benutzungen (37 MB tot).
-- Die id-Spalte + Identity bleiben fuer den woechentlichen Rollup-Job erhalten.

-- ── 5) Lockdown exakt wie 2026-07-27_price_history_lockdown_FIX.sql ────────
alter table public.price_history enable row level security;
alter table public.price_history force  row level security;
revoke all on public.price_history from anon, authenticated;
-- (service_role behaelt seine Default-Rechte — der Updater schreibt weiter)

-- ── Verifikation ───────────────────────────────────────────────────────────
select count(*)                                                        as zeilen,
       pg_size_pretty(pg_total_relation_size('public.price_history'))  as gesamt_neu,
       (select count(*) from (
          select price, lag(price) over (partition by player_slug, scarcity, eligibility
                                         order by recorded_at) as prev
          from public.price_history) t
        where prev is not null and price = prev)                       as verbliebene_duplikate;
-- Erwartung: ~520k Zeilen, unter 100 MB, 0 Duplikate.
-- Danach von aussen pruefen (macht Claude): anon-Zugriff -> 401,
-- RPC player_history liefert weiter, Updater-Upsert im naechsten Fenster.
