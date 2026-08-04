// Baut die Funktionsuebersicht mit eingebetteten Original-Schriften von sorion.pro
import { readFileSync, writeFileSync } from 'fs';

const FONTS = 'C:/craft-log/sorion-ui/fonts/';
const b64 = f => readFileSync(FONTS + f).toString('base64');
const orbitron = b64('Orbitron-700-latin.woff2');
const mono     = b64('ShareTechMono-400-latin.woff2');
const exo      = b64('Exo2-400-latin.woff2');
const exo6     = b64('Exo2-600-latin.woff2');

// ── Inhalt: Was kann sorion.pro heute? ─────────────────────────────────────
const sections = [
  {
    id: 'markt', label: 'Marktseite', hint: 'sorion.pro',
    lead: 'Der Preisüberblick über den gesamten Sorare-Markt. Öffentlich, ohne Anmeldung.',
    items: [
      ['Markttabelle', 'Alle bewerteten Karten, 50 pro Seite, jede Spalte sortierbar: FMV, Floor, letzter Verkauf, Ø Verkäufe, Liquidität, 24h- und 7-Tage-Veränderung.', 'live'],
      ['Eigene FMV-Formel', 'Zeitgewichteter Marktwert (v3.1): jüngere Verkäufe zählen mehr, der Floor zieht nur nach unten, ohne Verkäufe gibt es bewusst keinen Wert statt einer Fantasiezahl.', 'live'],
      ['Verkaufsdatum', 'Unter dem letzten Verkauf steht sein Alter — frisch grün, alt gedämpft. Ein drei Wochen alter Preis sagt etwas anderes als einer von gestern.', 'live'],
      ['Filter', 'Spielersuche, Verein (mit Vorschlagsliste), Land → Liga als aufklappbares Menü, Position GK/DF/MD/FW, Seltenheit und In-Season/Classic — beliebig kombinierbar.', 'live'],
      ['Liga-Ranking', 'Alle Ligen nach Gesamtwert, gruppiert nach Land und Liga (Deutschland und Österreich heißen beide „Bundesliga"). Eigener Seltenheits-Umschalter, Klick filtert die Tabelle.', 'live'],
      ['Kennzahlen oben', 'Erfasste Karten, Ø FMV für Limited und Rare mit echter 7-Tage-Marktbewegung aus Vollmarkt-Snapshots, Stand der letzten Aktualisierung.', 'live'],
      ['Treffsicherheit', 'Wie weit lagen unsere Schätzungen von den tatsächlichen Verkäufen entfernt — je Seltenheit, Median der letzten 30 Tage.', 'live'],
      ['Top &amp; Flop Movers', 'Die fünf größten Gewinner und Verlierer der letzten sieben Tage.', 'live'],
      ['Manager Search', 'Kurzbilanz eines beliebigen Managers als Vorschau, bevor man sein volles Portfolio öffnet.', 'live'],
    ],
  },
  {
    id: 'portfolio', label: 'Portfolio', hint: 'sorion.pro/portfolio',
    lead: 'Die Sammlung eines Managers als Investment gelesen — mit Einstand, Wertentwicklung und realisierten Trades.',
    items: [
      ['Sammlung ansehen', 'Jeder Manager per Name aufrufbar, ohne Anmeldung. Karten mit Bild, aktuellem Wert und Gewinn/Verlust seit Kauf.', 'live'],
      ['Kaufpreise', 'Sorare legt Kaufpreis, -datum und Herkunft offen — gekauft, ersteigert, gecraftet oder als Belohnung erhalten.', 'live'],
      ['Netto oder brutto', 'Ein Umschalter rechnet die 5 % Sorare-Marktgebühr heraus. Standard ist netto: das, was beim Verkauf wirklich ankommt.', 'live'],
      ['Karten-Detail', 'Klick auf eine Karte: Einstand, Verkaufsrechnung mit Erlös und Break-even-Preis, Marktdaten und der Kursverlauf der letzten 30 Tage.', 'live'],
      ['Trade History', 'Abgeschlossene Trades mit Kauf, Verkauf brutto und netto, Haltedauer und Gewinn — dazu Einsatz, realisierter Gewinn, gezahlte Gebühren und Trefferquote.', 'live'],
      ['Karten ohne Einstand', 'Verkäufe aus Belohnungen, Craft oder Tausch lassen sich zuschalten. Sie verzerren die Rendite nicht, ihr Erlös wird separat ausgewiesen.', 'live'],
      ['Filter und Sortierung', 'Nach Seltenheit, Saison-Zugehörigkeit, Herkunft und Gewinnern/Verlierern; sortierbar nach Gewinn, Wert, Kaufpreis, Datum oder Trend.', 'live'],
      ['Synchronisieren', 'Ein Knopf holt die Sammlung frisch von Sorare, mit Zeitstempel. Dazwischen kommt alles aus unserer Datenbank — das hält die Seite schnell und die Abfragen niedrig.', 'live'],
    ],
  },
  {
    id: 'konto', label: 'Konto', hint: 'sorion.pro/profile',
    lead: 'Freiwillig — die Marktseite und fremde Portfolios funktionieren ohne Anmeldung.',
    items: [
      ['Registrierung und Login', 'Mit Bestätigungsmail, Passwort-Zurücksetzung und E-Mail-Wechsel.', 'live'],
      ['Sorare-Konto verbinden', 'Anmeldung über Sorare als Nachweis, dass dir der Managername gehört. Kein zweites Konto: der Nachweis landet in deinem bestehenden Profil.', 'live'],
      ['Daten mitnehmen', 'Vollständiger Export als JSON und Konto-Löschung mit einem Klick — inklusive gespeichertem Portfolio.', 'live'],
      ['Merkliste mit Zielpreisen', 'Spieler beobachten und einen Wunschpreis hinterlegen. Datenbank steht, Oberfläche fehlt noch.', 'geplant'],
      ['Preis-Benachrichtigungen', 'Melden, wenn ein beobachteter Spieler den Zielpreis erreicht.', 'geplant'],
    ],
  },
  {
    id: 'motor', label: 'Im Hintergrund', hint: 'läuft ohne Zutun',
    lead: 'Was Sorion tut, während niemand hinsieht — die Datengrundlage für alles oben.',
    items: [
      ['Preis-Aktualisierung', 'Drei Dienste arbeiten die Karten nach Alter ab und holen Verkäufe und Angebote von Sorare. Neu angelegte Karten werden bevorzugt.', 'live'],
      ['Marktbeobachtung', 'Täglich werden Auktionen und Angebote durchsucht, um Spieler zu finden, die wir noch nicht kennen.', 'live'],
      ['Kader-Abgleich', 'Täglich alle Sorare-Vereine durchgehen und fehlende Spieler anlegen — auch Reservisten, die nie gehandelt werden und deshalb sonst fehlen.', 'live'],
      ['Vollmarkt-Snapshot', 'Einmal täglich der Durchschnitt des gesamten Markts. Grundlage für ehrliche 7-Tage-Bewegungen statt schwankender Stichproben.', 'live'],
      ['Genauigkeits-Messung', 'Jeder neue Verkauf wird gegen unsere vorherige Schätzung protokolliert. So wird die Formel überprüfbar statt behauptet.', 'live'],
      ['Besucherzählung', 'Selbst betrieben, ohne Cookies und ohne Dritt-Dienst; IP-Adressen werden nicht gespeichert.', 'live'],
    ],
  },
];

const openWork = [
  ['Impressum', 'Pflichtangaben stehen noch als Platzhalter — vor jeder Werbung zu füllen.', 'blocker'],
  ['Marktseite verschlanken', 'Die Seite lädt derzeit alle Karten in den Browser (~15 MB). Serverseitiges Filtern bringt das auf etwa 50 KB.', 'arbeit'],
  ['Datenbank entlasten', 'Kursverlauf wird bereits nur noch bei echten Preisänderungen geschrieben; die Altlast wird noch aufgeräumt.', 'arbeit'],
];

const chip = s => ({
  live:    ['live', 'ok'],
  geplant: ['geplant', 'plan'],
  blocker: ['offen', 'stop'],
  arbeit:  ['in Arbeit', 'work'],
}[s]);

const rows = items => items.map(([name, desc, st]) => {
  const [txt, cls] = chip(st);
  return `        <li class="row">
          <div class="row-main">
            <h3>${name}</h3>
            <p>${desc}</p>
          </div>
          <span class="chip ${cls}">${txt}</span>
        </li>`;
}).join('\n');

const html = `<style>
  @font-face { font-family:'Orbitron'; font-weight:700; font-display:swap;
    src:url(data:font/woff2;base64,${orbitron}) format('woff2'); }
  @font-face { font-family:'ShareTechMono'; font-weight:400; font-display:swap;
    src:url(data:font/woff2;base64,${mono}) format('woff2'); }
  @font-face { font-family:'Exo2'; font-weight:400; font-display:swap;
    src:url(data:font/woff2;base64,${exo}) format('woff2'); }
  @font-face { font-family:'Exo2'; font-weight:600; font-display:swap;
    src:url(data:font/woff2;base64,${exo6}) format('woff2'); }

  /* Bewusst nur ein Erscheinungsbild: Sorion ist ein dunkles Terminal-Produkt.
     Die Tokens sind die des Originals (sorion-ui). */
  :root{
    --bg:#0d0d1a; --panel:#13131f; --panel2:#1a1a2e; --line:#2a2a45; --line2:#3a3a60;
    --accent:#b060ff; --accent-dim:#8040cc;
    --text:#e8e8ff; --muted:#9090c0; --faint:#5f5f85;
    --ok:#00f5a0; --work:#ffb830; --stop:#ff2d78; --plan:#7090ff;
    --mono:'ShareTechMono',ui-monospace,Menlo,Consolas,monospace;
    --body:'Exo2',system-ui,sans-serif;
    --display:'Orbitron',var(--mono);
  }
  *{box-sizing:border-box;}
  body{margin:0;background:var(--bg);color:var(--text);font-family:var(--body);
       line-height:1.65;-webkit-font-smoothing:antialiased;}
  /* Scanlines wie im Produkt */
  body::before{content:'';position:fixed;inset:0;pointer-events:none;z-index:0;
    background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(176,96,255,.015) 2px,rgba(176,96,255,.015) 4px);}
  .wrap{position:relative;z-index:1;max-width:1000px;margin:0 auto;padding:56px 22px 90px;}

  header{border-bottom:1px solid var(--line);padding-bottom:26px;margin-bottom:34px;}
  .brand{font-family:var(--display);font-size:clamp(26px,5vw,38px);letter-spacing:.05em;margin:0;}
  .brand i{color:var(--accent);font-style:normal;animation:blink 1.2s steps(2) infinite;}
  @keyframes blink{50%{opacity:0}}
  .kicker{font-family:var(--mono);font-size:11px;letter-spacing:.24em;text-transform:uppercase;
          color:var(--accent);margin:0 0 10px;}
  .sub{color:var(--muted);max-width:62ch;margin:12px 0 0;font-size:15px;}

  .tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px;margin:30px 0 44px;}
  .tile{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:15px 17px;
        position:relative;overflow:hidden;}
  .tile::before{content:'';position:absolute;inset:0 auto 0 0;width:3px;
                background:linear-gradient(180deg,var(--accent),var(--accent-dim));}
  .tile b{display:block;font-family:var(--display);font-size:26px;font-weight:700;
          font-variant-numeric:tabular-nums;}
  .tile span{font-family:var(--mono);font-size:9px;letter-spacing:.2em;text-transform:uppercase;
             color:var(--accent);opacity:.9;}
  .tile em{display:block;font-family:var(--mono);font-size:10px;color:var(--faint);font-style:normal;margin-top:3px;}

  section{margin-bottom:30px;}
  .sec-head{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;
            border-bottom:1px solid var(--line);padding-bottom:9px;margin-bottom:4px;}
  .sec-head h2{font-family:var(--display);font-size:15px;font-weight:700;letter-spacing:.09em;
               text-transform:uppercase;margin:0;}
  .sec-head .where{font-family:var(--mono);font-size:11px;color:var(--faint);margin-left:auto;}
  .sec-lead{color:var(--muted);font-size:14px;margin:10px 0 14px;max-width:64ch;}

  ul{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:1px;
     background:var(--line);border:1px solid var(--line);border-radius:8px;overflow:hidden;}
  .row{background:var(--panel);display:flex;gap:16px;align-items:flex-start;padding:14px 17px;
       transition:background .13s;}
  .row:hover{background:var(--panel2);}
  .row-main{flex:1;min-width:0;}
  .row h3{font-family:var(--body);font-weight:600;font-size:14.5px;margin:0 0 3px;color:var(--text);}
  .row p{margin:0;font-size:13.5px;color:var(--muted);max-width:70ch;}
  .chip{flex:none;font-family:var(--mono);font-size:9.5px;letter-spacing:.14em;text-transform:uppercase;
        padding:4px 9px;border-radius:4px;border:1px solid;white-space:nowrap;margin-top:2px;}
  .chip.ok  {color:var(--ok);  border-color:rgba(0,245,160,.4); background:rgba(0,245,160,.09);}
  .chip.plan{color:var(--plan);border-color:rgba(112,144,255,.4);background:rgba(112,144,255,.09);}
  .chip.work{color:var(--work);border-color:rgba(255,184,48,.4); background:rgba(255,184,48,.09);}
  .chip.stop{color:var(--stop);border-color:rgba(255,45,120,.4); background:rgba(255,45,120,.09);}

  .note{border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:6px;
        background:var(--panel);padding:15px 18px;margin-top:34px;}
  .note h2{font-family:var(--display);font-size:13px;letter-spacing:.09em;text-transform:uppercase;margin:0 0 8px;}
  .note p{margin:0;color:var(--muted);font-size:13.5px;max-width:70ch;}

  footer{margin-top:44px;padding-top:18px;border-top:1px solid var(--line);
         font-family:var(--mono);font-size:10px;letter-spacing:.1em;color:var(--faint);
         display:flex;justify-content:space-between;gap:14px;flex-wrap:wrap;}

  @media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important;}}
</style>

<div class="wrap">
  <header>
    <p class="kicker">Funktionsübersicht · Stand 2. August 2026</p>
    <h1 class="brand">SORION<i>_</i></h1>
    <p class="sub">Marktdaten und Portfolio-Analyse für Sorare. Was die Seite heute kann,
       was im Hintergrund läuft und was noch offen ist.</p>
  </header>

  <div class="tiles">
    <div class="tile"><span>// Bewertete Karten</span><b>104.755</b><em>3 Seltenheiten × 2 Saisons</em></div>
    <div class="tile"><span>// Vereine</span><b>476</b><em>in 42 Ligen filterbar</em></div>
    <div class="tile"><span>// Fertige Funktionen</span><b>27</b><em>über 4 Bereiche</em></div>
    <div class="tile"><span>// Offen</span><b>5</b><em>2 geplant, 3 in Arbeit</em></div>
  </div>

${sections.map(s => `  <section>
    <div class="sec-head">
      <h2>${s.label}</h2>
      <span class="where">${s.hint}</span>
    </div>
    <p class="sec-lead">${s.lead}</p>
    <ul>
${rows(s.items)}
    </ul>
  </section>`).join('\n\n')}

  <section>
    <div class="sec-head">
      <h2>Vor der Werbung zu erledigen</h2>
      <span class="where">nicht sichtbar für Besucher</span>
    </div>
    <p class="sec-lead">Punkte, die den Betrieb oder den Start betreffen — keine Funktionen.</p>
    <ul>
${rows(openWork)}
    </ul>
  </section>

  <div class="note">
    <h2>Wie die Preise entstehen</h2>
    <p>Der FMV ist eine eigene Schätzung aus den letzten Verkäufen, gewichtet nach Alter,
       begrenzt durch das günstigste aktive Angebot. Gibt es keine Verkäufe, zeigt Sorion
       bewusst keinen Wert — ein Angebot allein ist kein Marktpreis. Wie gut die Schätzungen
       treffen, steht auf der Marktseite selbst.</p>
  </div>

  <footer>
    <span>sorion.pro · nicht mit Sorare verbunden</span>
    <span>Alle Werte unverbindlich, keine Anlageberatung</span>
  </footer>
</div>
`;

writeFileSync('C:/Users/Jonas/AppData/Local/Temp/claude/C--Users-Jonas-Programme-MtG-LTeach/1bdc7f41-3b98-47b9-9261-a5cf94fb36ef/scratchpad/sorion-funktionen.html', html);
console.log('gebaut:', (html.length / 1024).toFixed(0), 'KB');
