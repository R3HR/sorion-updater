# SORION

Sorare-Marktpreis-Tracker (FMV) für Fußball-Karten. Node.js-Scripts auf Railway + Supabase (DB & Edge Functions) + statische UI.

**PFLICHT vor jeder Arbeit — in dieser Reihenfolge lesen:**

1. [docs/HANDOFF.md](docs/HANDOFF.md) — aktueller Stand, TODOs, Architektur-Wissen, Regeln
2. [docs/BUGS.md](docs/BUGS.md) — Bug-Archiv (Symptom/Ursache/Fix/Status)
3. [docs/INCIDENTS.md](docs/INCIDENTS.md) — Fatal Errors, Crashes, Sicherheitslücken

**Nach jeder Arbeit:** die betroffenen Dateien aktualisieren (Status, neue Erkenntnisse, neue Bugs).

## Eiserne Regeln

- FMV-Berechnung existiert genau einmal: `lib/fmv.mjs`. Nie duplizieren.
- Keine Secrets in Code oder Git — nur `process.env` / `Deno.env.get`.
- Push auf `main` löst Railway-Deploys aus. Nichts Halbfertiges pushen.
- Edge Functions liegen außerhalb dieses Repos: `C:\craft-log\supabase\functions\` (Deploy: `supabase functions deploy <name>`).

- Vorgemerkte Konzepte (nicht in Arbeit): [docs/IDEAS.md](docs/IDEAS.md)
