// Auffangnetz: Dieses Repo hat KEINEN sinnvollen Standard-Startbefehl.
// Jeder Railway-Dienst bringt seinen eigenen mit. Faellt Railway auf "npm start"
// zurueck, fehlt am Dienst der Startbefehl.
//
// Vorher stand hier "node update.mjs", eine Datei, die es seit dem Umbau vom
// 28.07. nicht mehr gibt (MODULE_NOT_FOUND verschleierte die Ursache, 07.09.).
// Bis 19.11. verwies diese Meldung auf Config-as-code-Dateien. Die hat Railway
// abgekuendigt (INC-008): neue Dienste bekommen Startbefehl und Zeitplan direkt
// in den Diensteinstellungen. Beim Anlegen von "Market History" (19.11.) fuehrte
// der alte Hinweis in die Irre.
console.error([
  '',
  'Kein Startbefehl gesetzt.',
  '',
  'In Railway am Dienst: Settings -> Deploy -> Custom Start Command eintragen',
  '(und bei Zeitplan-Diensten Cron Schedule), speichern, danach Redeploy.',
  '',
  'Startbefehle der bestehenden Dienste (siehe docs/HANDOFF.md):',
  '  Updater Limited   node update-scarcity.mjs limited',
  '  Update Rare       node update-scarcity.mjs rare',
  '  Updater SR        node update-scarcity.mjs super_rare',
  '  Market History    node tools/market-volume-history.mjs',
  '  Rewards           node tools/sync-reward-thresholds.mjs && node tools/sync-lineup-costs.mjs && node tools/sync-essence-earnings.mjs',
  '',
].join('\n'));
process.exit(1);
