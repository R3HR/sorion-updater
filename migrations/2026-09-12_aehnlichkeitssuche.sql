-- ═══════════════════════════════════════════════════════════════════════════
-- Aehnlichkeitssuche fuer das Suchfeld  (12.09.2026, Wunsch Jonas)
--
-- ANLASS: "Aleix Garcia" war nicht auffindbar, weil Sorare ihn "Aleix García"
-- mit Akzent auf dem i fuehrt. Die Suche lief nur ueber player_name mit ilike.
--
-- ZWEI STUFEN:
--   1. Im Frontend geloest: zusaetzlich ueber `player_slug` suchen. Sorares Slugs
--      sind immer akzentfrei (aleix-garcia-serrano, alexander-nubel), damit ist
--      jede Akzentfrage erledigt, ohne eine Zeile SQL.
--   2. Diese Funktion: faengt TIPPFEHLER und Namensvarianten ab ("alex garcia",
--      "belingham", "halland"). Sie laeuft nur, wenn die normale Suche NICHTS
--      findet, und liefert dann Vorschlaege.
--
-- SCHWELLE 0,20, empirisch bestimmt (Aehnlichkeit zum echten Slug):
--   alex-garcia -> aleix-garcia-serrano  0,435     halland  -> erling-haaland  0,278
--   belingham   -> jude-bellingham       0,529     nuebel   -> alexander-nubel 0,211
--   zufallstext -> aleix-garcia-serrano  0,000
-- Tiefer als 0,20 waere Rauschen; die Treffer sind ohnehin nach Guete sortiert
-- und auf wenige begrenzt.
-- ═══════════════════════════════════════════════════════════════════════════

-- pg_trgm ist bereits installiert (geprueft 12.09.).
-- Index fuer die Trigramm-Suche: ohne ihn muesste jede Anfrage 126.690 Zeilen lesen.
create index if not exists card_prices_slug_trgm
  on public.card_prices using gin (player_slug gin_trgm_ops);

create or replace function public.search_players_similar(
  p_query       text,
  p_eligibility text default 'in_season',
  p_limit       int  default 8
)
returns table (player_slug text, player_name text, team_name text, aehnlichkeit real)
language sql
stable
security definer
set search_path = public
-- Der %-Operator nutzt den GIN-Index; seine Schwelle kommt aus dieser Einstellung.
set pg_trgm.similarity_threshold = '0.2'
as $$
  with q as (
    select nullif(regexp_replace(lower(trim(coalesce(p_query, ''))), '[^a-z0-9]+', '-', 'g'), '') as needle
  )
  select cp.player_slug,
         max(cp.player_name)                                     as player_name,
         max(cp.team_name)                                       as team_name,
         max(similarity(cp.player_slug, (select needle from q))) as aehnlichkeit
  from public.card_prices cp, q
  where q.needle is not null
    and length(q.needle) >= 3                     -- unter 3 Zeichen ist alles aehnlich
    and cp.eligibility = coalesce(p_eligibility, 'in_season')
    and (cp.fmv is not null or cp.floor_price is not null or cp.sale_1 is not null)
    and cp.player_slug % q.needle
  group by cp.player_slug
  order by aehnlichkeit desc, player_name
  limit greatest(1, least(coalesce(p_limit, 8), 20))
$$;

revoke all on function public.search_players_similar(text, text, int) from public;
grant execute on function public.search_players_similar(text, text, int) to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
select 'alex garcia' as eingabe, player_name, round(aehnlichkeit::numeric, 3) as guete
from public.search_players_similar('alex garcia', 'in_season', 3);
