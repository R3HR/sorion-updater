@echo off
REM SORION Statistik lokal starten (05.09.2026).
REM Warum nicht Doppelklick auf stats.html: Als file:// speichert Chrome keine
REM Passwoerter und der lokale Speicher ist unzuverlaessig -> staendig neu anmelden.
REM Ueber http://localhost funktionieren Passwort-Manager und die Sitzung bleibt.
REM Fenster offen lassen, solange die Statistik gebraucht wird; Schliessen beendet den Server.
cd /d "%~dp0"
start "" "http://localhost:8123/stats.html"
python -m http.server 8123
