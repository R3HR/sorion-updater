-- ═══════════════════════════════════════════════════════════════════════════
-- Kleiner Antwort-Cache fuer Edge Functions  (05.09.2026, Launch-Tag)
--
-- WARUM: Der Head-to-Head auf accuracy.html (RPC accuracy_benchmark, ~80k
-- Zeilen mit Perzentilen, ~1 s) lief am Launch-Tag zweimal ins 3-s-Limit,
-- weil mehrere Besucher die Seite gleichzeitig oeffneten und JEDER die RPC
-- ausloeste. Ein Speicher-Cache in der Edge Function hilft nicht: Supabase
-- gibt jeder Anfrage eine frische Instanz (gemessen: 7 von 7 Aufrufen Miss).
--
-- LOESUNG: Die Function liest hier zuerst. Frisch (< 10 min) -> sofort
-- antworten. Veraltet -> die alte Antwort SOFORT liefern und im Hintergrund
-- neu rechnen (stale-while-revalidate). Nur der allererste Aufruf ueberhaupt
-- wartet auf die Datenbank. Aus "eine RPC je Besucher" wird "eine RPC je
-- 10 Minuten", egal wie viele Besucher.
--
-- Generisch gehalten (key/body), damit weitere Functions ihn nutzen koennen
-- (z. B. die Fixture-Liste von so5-results).
--
-- ZUGRIFF: nur Service-Rolle. Kein anon, kein authenticated.
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════
create table if not exists public.edge_cache (
  key         text        primary key,
  body        jsonb       not null,
  updated_at  timestamptz not null default now()
);
alter table public.edge_cache enable row level security;
revoke all on public.edge_cache from anon, authenticated;

-- Verifikation: leer bis zum ersten Function-Aufruf, dann eine Zeile je Schluessel
select key, updated_at, pg_column_size(body) as bytes from public.edge_cache;
