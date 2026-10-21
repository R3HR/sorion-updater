-- ═══════════════════════════════════════════════════════════════════════════
-- Portfolios wieder fuer jeden aufrufbar, nur die Geldzahlen geschuetzt
-- (21.10.2026, Vorgabe Jonas)
--
-- Der Lockdown vom 13.10. hat jedes fremde Portfolio komplett gesperrt. Gewollt
-- war der Schutz vor Massenabzug und die Monetarisierung, nicht eine leere Seite
-- fuer jeden Gast. Neue Aufteilung:
--
--   SAMMLUNG (welche Karten)  -> oeffentlich. Steht ohnehin auf sorare.com.
--   KAUFPREISE (buy_eur/date/type) -> nur eigener Manager oder Pro.
--   HANDELSHISTORIE (manager_trades) -> unveraendert gesperrt, das ist die
--     vollstaendige Gewinn- und Verlustrechnung.
--
-- WICHTIG (Grundregel Pro-Features): Ein Blur im Frontend ist KEIN Schutz. Die
-- Kaufpreise werden hier serverseitig auf NULL gesetzt, sie verlassen den Server
-- also gar nicht erst. Der Depotwert selbst ist aus der oeffentlichen Sammlung und
-- den oeffentlichen Preisen ohnehin ableitbar; er wird im Frontend nur verdeckt,
-- das ist Produktentscheidung, kein Geheimnisschutz.
--
-- Der Bulk-Abzug bleibt zu: Zugriff gibt es nur je Manager ueber diese RPC,
-- die Tabellen selbst bleiben fuer anon und authenticated gesperrt.
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.manager_cards_for(p_slug text)
returns setof manager_cards
language sql
stable
security definer
set search_path to 'public'
as $function$
  select m.sorare_slug, m.card_slug, m.player_slug, m.player_name, m.picture_url,
         m.rarity, m.in_season,
         case when public.may_view_manager(p_slug) then m.buy_eur  end,
         case when public.may_view_manager(p_slug) then m.buy_date end,
         case when public.may_view_manager(p_slug) then m.buy_type end
  from manager_cards m
  where m.sorare_slug = p_slug;
$function$;

-- Sync-Stand (wann zuletzt geholt, wie viele Karten) ist keine Geldinformation.
create or replace function public.manager_sync_for(p_slug text)
returns setof manager_sync
language sql
stable
security definer
set search_path to 'public'
as $function$
  select m.* from manager_sync m where m.sorare_slug = p_slug;
$function$;

-- Damit das Frontend weiss, ob es die Geldkacheln verdecken muss. Gibt nur
-- Auskunft ueber die RECHTE DES AUFRUFERS, keine fremden Daten.
grant execute on function public.may_view_manager(text) to anon, authenticated;

-- manager_trades_for bleibt wie es ist: gesperrt ohne Zugang.

-- ── Kontrolle ──────────────────────────────────────────────────────────────
select count(*) as karten_sichtbar,
       count(buy_eur) as mit_kaufpreis
from public.manager_cards_for('jr3hr');
