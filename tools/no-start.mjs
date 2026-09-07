// Auffangnetz: Dieses Repo hat KEINEN sinnvollen Standard-Startbefehl.
// Jeder Railway-Dienst bringt seinen eigenen mit — ueber die Config-as-code-
// Datei im Repo-Root (railway-<dienst>.toml), die startCommand UND cronSchedule
// festlegt. Faellt Railway auf "npm start" zurueck, ist der Pfad nicht gesetzt.
//
// Vorher stand hier "node update.mjs" — eine Datei, die es seit dem Umbau vom
// 28.07. nicht mehr gibt. Der Dienst stuerzte dann mit MODULE_NOT_FOUND ab,
// was die eigentliche Ursache verschleierte (07.09. beim Anlegen von "Rewards").
console.error([
  '',
  'Kein Startbefehl gesetzt.',
  '',
  'Dieser Dienst laeuft ueber eine Config-as-code-Datei. In Railway:',
  '  Settings -> Config-as-code -> Pfad eintragen, z. B. /railway-rewards.toml',
  '  danach Redeploy.',
  '',
  'Vorhandene Konfigurationen im Repo-Root:',
  '  railway-limited.toml, railway-rare.toml, railway-sr.toml,',
  '  railway-rosters.toml, railway-harvest.toml, railway-rewards.toml',
  '',
].join('\n'));
process.exit(1);
