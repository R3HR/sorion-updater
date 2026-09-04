-- ═══════════════════════════════════════════════════════════════════════════
-- Accuracy-Haupttabelle: Messfenster 30 -> 3 Tage rollierend  (27.08.2026)
--
-- ENTSCHEIDUNG Jonas ("go", vor dem Reddit-Publish): Die Haupttabelle der
-- Accuracy-Seite soll dasselbe 3-Tage-Fenster nutzen wie das FMV-vs-Floor-
-- Duell darunter. Grund: Das 30-Tage-Fenster schleppte v3.1/v3.2-Vorhersagen
-- bis ~25.09. mit — die Seite haette zwei scheinbar widerspruechliche Zahlen
-- gezeigt (Tabelle ~27 %, Duell ~24 %). Mit 3 Tagen misst ALLES auf der Seite
-- immer das AKTUELLE Modell; nach jeder Formelaenderung ist die Anzeige binnen
-- 3 Tagen sauber. Stichprobe bleibt gross (~40k Verkaeufe je 3 Tage).
--
-- Historische fmv_accuracy_daily-Zeilen bleiben unangetastet (interner
-- Verlauf); ab heute tragen die Tageszeilen das 3d-Fenster. Putzdienst
-- (Rohdaten > 90 Tage) unveraendert.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create or replace function public.snapshot_fmv_accuracy(p_day date default current_date)
returns int
language plpgsql security definer set search_path = public as $$
declare n int;
begin
  insert into public.fmv_accuracy_daily (day, eligibility, scarcity, samples, median_abs_delta, bias)
  select p_day, eligibility, scarcity,
         count(*)::int,
         round((percentile_cont(0.5) within group (order by abs(delta_pct)))::numeric, 1),
         round(avg(delta_pct)::numeric, 1)
  from public.fmv_accuracy
  where created_at > now() - interval '3 days' and hours_gap < 48
  group by eligibility, scarcity
  on conflict (day, eligibility, scarcity) do update
    set samples          = excluded.samples,
        median_abs_delta = excluded.median_abs_delta,
        bias             = excluded.bias;
  get diagnostics n = row_count;

  -- Putzdienst unveraendert: Rohdaten > 90 Tage raus (Backtest-Reserve)
  delete from public.fmv_accuracy where created_at < now() - interval '90 days';
  return n;
end $$;
revoke all on function public.snapshot_fmv_accuracy(date) from public;

-- Heutigen Snapshot sofort neu schreiben, damit die Seite direkt umspringt
select public.snapshot_fmv_accuracy(current_date);

-- ── Verifikation ───────────────────────────────────────────────────────────
-- Erwartung: samples je Zeile deutlich kleiner als zuvor (3d statt 30d),
-- median_abs_delta bei limited/in_season im Bereich ~24 statt ~27.
select * from public.fmv_accuracy_daily where day = current_date
order by scarcity, eligibility;
