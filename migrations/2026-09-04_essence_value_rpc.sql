-- ═══════════════════════════════════════════════════════════════════════════
-- Essence-Kurs oeffentlich lesbar machen  (04.09.2026)
--
-- ZWECK: Sorion zeigt je Spieler die erwirtschafteten Belohnungen. Geld, XP und
-- Essence bleiben dabei GETRENNT (Ansage Jonas) — zusaetzlich soll neben der
-- Essence-Menge ein SCHAETZWERT in Euro stehen. Den Kurs kennt CraftLog:
--   EUR je 1.000 Essence = (Summe Kartenwerte / Summe eingesetzte Essence) * 1000
-- je Rarity, ueber alle eingetragenen Crafts. Mit jedem Craft wird er genauer.
--
-- WARUM EINE RPC: `crafts` ist RLS-geschuetzt (jeder sieht nur die eigenen
-- Zeilen); CraftLog holt die Gesamtsicht ueber eine eingeloggte Edge Function.
-- Sorion braucht den Kurs OHNE Login. Diese Funktion gibt deshalb ausschliesslich
-- die AGGREGATE heraus (Kurs + Stichprobengroesse je Rarity) — niemals einzelne
-- Crafts, keine Namen, keine user_id. Damit bleibt die Tabelle dicht.
--
-- samples wird mitgeliefert, damit die Oberflaeche einen duennen Kurs als
-- unsicher kennzeichnen kann (Stand 04.09.: limited n=62 tragfaehig,
-- rare n=6 duenn). Unter 3 Crafts wird gar kein Kurs ausgewiesen — das waere
-- Rauschen, keine Schaetzung.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.essence_value()
returns table (
  scarcity   text,     -- 'limited' | 'rare' | 'super_rare'
  eur_per_1k numeric,  -- Wert von 1.000 Essence in Euro
  samples    int       -- Anzahl Crafts dahinter (Verlaesslichkeit)
)
language sql stable security definer set search_path = public as $$
  select coalesce(c.scarcity, 'limited')::text,
         round((sum(c.value) / nullif(sum(coalesce(c.essence, 1000)), 0) * 1000)::numeric, 2),
         count(*)::int
  from public.crafts c
  where c.value > 0
  group by 1
  having count(*) >= 3
  order by 1
$$;

revoke all on function public.essence_value() from public;
grant execute on function public.essence_value() to anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Erwartet (Stand 04.09.): limited ~4,63 (n=62), rare ~22,93 (n=6).
select * from public.essence_value();
