// Technischer Bauplan Sorion — mit eingebetteten Original-Schriften
import { readFileSync, writeFileSync } from 'fs';
const FONTS = 'C:/craft-log/sorion-ui/fonts/';
const b64 = f => readFileSync(FONTS + f).toString('base64');
const orbitron = b64('Orbitron-700-latin.woff2');
const mono     = b64('ShareTechMono-400-latin.woff2');
const exo      = b64('Exo2-400-latin.woff2');
const exo6     = b64('Exo2-600-latin.woff2');

const html = `<style>
  @font-face{font-family:'Orbitron';font-weight:700;font-display:swap;src:url(data:font/woff2;base64,${orbitron}) format('woff2');}
  @font-face{font-family:'ShareTechMono';font-weight:400;font-display:swap;src:url(data:font/woff2;base64,${mono}) format('woff2');}
  @font-face{font-family:'Exo2';font-weight:400;font-display:swap;src:url(data:font/woff2;base64,${exo}) format('woff2');}
  @font-face{font-family:'Exo2';font-weight:600;font-display:swap;src:url(data:font/woff2;base64,${exo6}) format('woff2');}

  :root{
    --bg:#0d0d1a;--panel:#13131f;--panel2:#1a1a2e;--line:#2a2a45;--line2:#3a3a60;
    --accent:#b060ff;--teal:#12a89c;--text:#e8e8ff;--muted:#9090c0;--faint:#5f5f85;
    --ok:#00f5a0;--warn:#ffb830;--stop:#ff2d78;
    --mono:'ShareTechMono',ui-monospace,Menlo,Consolas,monospace;
    --body:'Exo2',system-ui,sans-serif;--display:'Orbitron',var(--mono);
  }
  *{box-sizing:border-box;}
  body{margin:0;background:var(--bg);color:var(--text);font-family:var(--body);line-height:1.7;
       -webkit-font-smoothing:antialiased;}
  .wrap{max-width:940px;margin:0 auto;padding:52px 22px 90px;}

  header{border-bottom:1px solid var(--line);padding-bottom:24px;margin-bottom:8px;}
  .kicker{font-family:var(--mono);font-size:11px;letter-spacing:.24em;text-transform:uppercase;color:var(--accent);margin:0 0 10px;}
  h1{font-family:var(--display);font-size:clamp(24px,4.6vw,34px);letter-spacing:.04em;margin:0;}
  h1 i{color:var(--accent);font-style:normal;}
  .lede{color:var(--muted);max-width:64ch;margin:14px 0 0;}

  h2{font-family:var(--display);font-size:15px;letter-spacing:.09em;text-transform:uppercase;
     margin:46px 0 4px;padding-bottom:8px;border-bottom:1px solid var(--line);}
  h3{font-family:var(--body);font-weight:600;font-size:15.5px;margin:26px 0 6px;color:var(--text);}
  p{margin:10px 0;color:var(--muted);max-width:72ch;}
  p strong,li strong{color:var(--text);font-weight:600;}
  ul{color:var(--muted);max-width:72ch;padding-left:20px;}
  li{margin:5px 0;}
  code{font-family:var(--mono);font-size:.9em;background:var(--panel2);border:1px solid var(--line);
       border-radius:4px;padding:1px 5px;color:#d8c8ff;}

  .fig{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px;margin:18px 0;
       overflow-x:auto;}
  .fig .mermaid{min-width:520px;}
  .cap{font-family:var(--mono);font-size:10px;letter-spacing:.14em;text-transform:uppercase;
       color:var(--faint);margin-top:10px;}

  .tbl{width:100%;border-collapse:collapse;margin:14px 0;font-size:13.5px;}
  .tbl th{font-family:var(--mono);font-size:10px;letter-spacing:.16em;text-transform:uppercase;
          color:#c99aff;text-align:left;padding:9px 11px;border-bottom:1px solid var(--line);}
  .tbl td{padding:9px 11px;border-bottom:1px solid rgba(42,42,69,.55);color:var(--muted);vertical-align:top;}
  .tbl td:first-child{color:var(--text);white-space:nowrap;}
  .tbl code{white-space:nowrap;}
  .scroll{overflow-x:auto;}

  .note{border:1px solid var(--line);border-left:3px solid var(--accent);border-radius:6px;
        background:var(--panel);padding:14px 17px;margin:18px 0;}
  .note p{margin:0;max-width:70ch;}
  .note.warn{border-left-color:var(--warn);}

  .steps{counter-reset:s;list-style:none;padding:0;margin:14px 0;}
  .steps li{counter-increment:s;position:relative;padding-left:34px;margin:12px 0;color:var(--muted);max-width:72ch;}
  .steps li::before{content:counter(s);position:absolute;left:0;top:2px;width:22px;height:22px;
    border:1px solid var(--line2);border-radius:4px;display:grid;place-items:center;
    font-family:var(--mono);font-size:10px;color:var(--accent);}

  footer{margin-top:52px;padding-top:18px;border-top:1px solid var(--line);
         font-family:var(--mono);font-size:10px;letter-spacing:.1em;color:var(--faint);}
</style>

<div class="wrap">
<header>
  <p class="kicker">Technischer Bauplan · Stand 2. August 2026</p>
  <h1>SORION<i>_</i></h1>
  <p class="lede">Wie ein Preis von Sorare bis auf den Bildschirm kommt — Bausteine, Datenfluss,
     Bewertungsformel und die Regeln, die das System zusammenhalten.</p>
</header>

<h2>1 · Die Bausteine</h2>
<p>Sorion besteht aus vier Teilen, die nichts voneinander wissen müssen. Sie kommunizieren
   ausschließlich über die Datenbank.</p>

<div class="fig">
<pre class="mermaid">
flowchart LR
  SA["Sorare GraphQL API"]
  subgraph RW["Railway · Hintergrundjobs"]
    U1["3× Preis-Updater"]
    HV["Harvester"]
    RS["Kader-Abgleich"]
  end
  subgraph SB["Supabase"]
    DB[("PostgreSQL<br/>+ Row Level Security")]
    EF["Edge Functions"]
    AU["Auth"]
  end
  subgraph GP["GitHub Pages"]
    UI["sorion.pro<br/>statisches HTML"]
  end
  BR(["Browser"])

  SA -->|GraphQL| U1 & HV & RS
  U1 & HV & RS -->|Service-Key| DB
  BR -->|lesen, publishable Key| DB
  BR --> UI
  BR -->|schreiben/geschützt| EF
  EF -->|Service-Key| DB
  EF -->|OAuth, Sync| SA
  AU --- DB
</pre>
<p class="cap">Abb. 1 — Der Browser liest direkt aus der Datenbank; schreiben darf nur der Server.</p>
</div>

<div class="scroll"><table class="tbl">
<thead><tr><th>Baustein</th><th>Aufgabe</th><th>Warum dort</th></tr></thead>
<tbody>
<tr><td>Railway</td><td>Dauerläufer: Preise holen, Spieler finden, Kader abgleichen</td><td>Braucht Zeitpläne und lange Laufzeiten — für Edge Functions ungeeignet</td></tr>
<tr><td>Supabase</td><td>Datenbank, Anmeldung, kurze Serverfunktionen</td><td>PostgREST macht Tabellen direkt lesbar, ohne eigenes Backend</td></tr>
<tr><td>GitHub Pages</td><td>Auslieferung der Oberfläche</td><td>Rein statisch: kein Server, keine Kosten, kein Angriffspunkt</td></tr>
<tr><td>Browser</td><td>Filtern, Rechnen, Darstellen</td><td>Was der Nutzer sieht, entsteht bei ihm — spart Serverlast</td></tr>
</tbody></table></div>

<h2>2 · Der Kreislauf: wie ein Preis entsteht</h2>
<p>Der Preis-Updater ist das Herz. Er läuft alle fünf Minuten in den aktiven Fenstern, dreimal
   parallel — je einmal für Limited, Rare und Super Rare.</p>

<div class="fig">
<pre class="mermaid">
sequenceDiagram
  autonumber
  participant U as Preis-Updater
  participant D as card_prices
  participant S as Sorare API
  participant H as price_history
  participant A as fmv_accuracy

  U->>D: 120 Zeilen holen<br/>(älteste updated_at zuerst)
  loop je Zeile
    U->>S: 1 Anfrage: Verkäufe + Angebot<br/>+ Verein/Liga/Position/Bild
    S-->>U: bis zu 20 Verkäufe, Floor
    U->>H: Verlauf laden (45 Tage)
    Note over U: FMV berechnen<br/>(lib/fmv.mjs)
    U->>A: neue Verkäufe gegen<br/>frühere Schätzung protokollieren
    U->>H: nur schreiben, wenn<br/>Preis sich geändert hat
    U->>D: FMV, Floor, Verkäufe,<br/>24h/7d, Metadaten
  end
</pre>
<p class="cap">Abb. 2 — Eine Sorare-Anfrage pro Karte. Die Warteschlange sorgt dafür, dass alles drankommt.</p>
</div>

<h3>Die Warteschlange</h3>
<p>Es gibt keine Liste, was als Nächstes dran ist — die Sortierung <em>ist</em> die Warteschlange:
   <code>ORDER BY updated_at ASC LIMIT 120</code>. Wer am längsten nicht aktualisiert wurde, kommt zuerst.
   Neu angelegte Karten bekommen <code>updated_at = 1970-01-01</code> und werden dadurch sofort vorgezogen.
   Ein voller Durchlauf über alle ~119.000 Zeilen dauert zwei bis drei Tage.</p>

<div class="note warn"><p><strong>Fallstrick, der uns eingeholt hat:</strong> Diese Abfrage
   braucht den Index <code>(scarcity, updated_at)</code>. Ohne ihn sortiert Postgres bei jedem Lauf
   36.000 Zeilen und läuft unter nächtlicher Last in den Timeout — der Job bricht ab und Railway
   meldet „Deploy Crashed". Genau das passierte jede Nacht, bis der Index gesetzt war.</p></div>

<h2>3 · Die Bewertungsformel</h2>
<p>Der FMV ist der eigentliche Kern — alles andere ist Infrastruktur. Er lebt an genau einer Stelle:
   <code>lib/fmv.mjs</code>. Die Regeln entstanden aus Fehlern, die vorher sichtbare Falschpreise
   erzeugt haben.</p>

<div class="scroll"><table class="tbl">
<thead><tr><th>Regel</th><th>Wirkung</th><th>Warum</th></tr></thead>
<tbody>
<tr><td>Zeit-Gewichtung</td><td>Halbwertszeit 3 Tage (In-Season) bzw. 14 Tage (Classic); Fenster 21 bzw. 90 Tage</td><td>Ein Verkauf von gestern sagt mehr als einer von vor drei Wochen</td></tr>
<tr><td>Ausreißer kappen</td><td>Ab 5 Verkäufen werden höchster und niedrigster verworfen</td><td>Ein Ausrutscher soll den Wert nicht bestimmen</td></tr>
<tr><td>Keine Verkäufe → kein Wert</td><td>FMV bleibt leer statt geschätzt</td><td><em>Ein Angebot ist kein Preis.</em> Vorher stand eine Karte mit 731 € an der Spitze, weil jemand sie so eingestellt hatte</td></tr>
<tr><td>Floor nur nach unten</td><td>Liegt das günstigste Angebot über dem Verkaufswert, wird es ignoriert</td><td>Man verkauft, indem man unterbietet — nicht indem man das teuerste Angebot kopiert</td></tr>
<tr><td>Verkaufbarkeits-Deckel</td><td>Mischung 35 % Floor, gedeckelt bei Floor × 1,05</td><td>Zum angezeigten FMV soll man die Karte tatsächlich los werden</td></tr>
</tbody></table></div>

<div class="note"><p><strong>Selbstkontrolle:</strong> Jeder neue Verkauf wird gegen die <em>vorherige</em>
   Schätzung protokolliert (<code>fmv_accuracy</code>) — nie gegen die aktuelle, sonst würde man sich
   selbst benoten. Die Marktseite zeigt daraus die mittlere Abweichung je Seltenheit.</p></div>

<h2>4 · Das Datenmodell</h2>
<p>Zwei Zeilen pro Spieler und Seltenheit — eine für In-Season, eine für Classic. Diese Trennung
   macht den Saisonwechsel zum Selbstläufer: Eine Karte wird bewertet nach der Zeile, die zu ihrer
   Saison-Zugehörigkeit passt.</p>

<div class="scroll"><table class="tbl">
<thead><tr><th>Tabelle</th><th>Schlüssel</th><th>Inhalt</th></tr></thead>
<tbody>
<tr><td><code>card_prices</code></td><td>Spieler × Seltenheit × Saison</td><td>Der aktuelle Stand: FMV, Floor, letzte Verkäufe, Verein, Liga, Position. ~119.000 Zeilen</td></tr>
<tr><td><code>price_history</code></td><td>+ Tag</td><td>Kursverlauf. Seit 02.08. nur noch bei echten Preisänderungen — vorher 100.000 Zeilen täglich</td></tr>
<tr><td><code>market_daily</code></td><td>Tag × Seltenheit × Saison</td><td>Vollmarkt-Schnappschuss, 6 Zeilen pro Tag. Grundlage der 7-Tage-Bewegung</td></tr>
<tr><td><code>manager_cards</code></td><td>Manager × Karte</td><td>Gespiegelte Sammlung inkl. Kaufpreis</td></tr>
<tr><td><code>manager_trades</code></td><td>Manager × Karte × Verkauf</td><td>Abgeschlossene Trades mit Ein- und Verkauf</td></tr>
<tr><td><code>manager_sync</code></td><td>Manager</td><td>Wann zuletzt geholt — steuert die Sperre</td></tr>
<tr><td><code>fmv_accuracy</code></td><td>fortlaufend</td><td>Schätzung gegen tatsächlichen Verkauf</td></tr>
<tr><td><code>profiles</code></td><td>Nutzer</td><td>Konto, verknüpfter Managername</td></tr>
</tbody></table></div>

<div class="note warn"><p><strong>Wichtige Unterscheidung:</strong> <code>price_history</code> ist ein
   <em>Änderungs-Log unserer Schätzung</em>, kein Marktabbild. Eine Zeile entsteht nur, wenn der Updater
   diese Karte berührt — ein voller Durchlauf dauert Tage. Tagesdurchschnitte daraus wären rotierende
   Stichproben. Für Marktaggregate ist <code>market_daily</code> zuständig, das aus dem Vollbestand rechnet.</p></div>

<h2>5 · Der Portfolio-Weg</h2>
<p>Portfolios kommen nicht mehr live von Sorare, sondern aus einer Spiegelung. Der Grund ist Verbrauch:
   Vorher kostete jeder Aufruf 6–16 Sorare-Anfragen, bei jedem Neuladen erneut.</p>

<div class="fig">
<pre class="mermaid">
flowchart TD
  A["Besucher öffnet Portfolio"] --> B{"Schon gespiegelt?"}
  B -->|ja| C["Aus manager_cards lesen<br/>0 Sorare-Anfragen"]
  B -->|nein| D["sync-portfolio"]
  D --> E{"Zuletzt geholt<br/>vor &lt; 24 h?"}
  E -->|ja| C
  E -->|nein| F["Sammlung + Trades<br/>von Sorare holen"]
  F --> G["manager_* neu schreiben"]
  G --> C
  H["Eigener Sync-Knopf"] --> I{"Eingeloggt und<br/>eigener Slug?"}
  I -->|nein| J["403"]
  I -->|ja| K{"Letzter Sync<br/>&lt; 10 min?"}
  K -->|ja| L["abgelehnt"]
  K -->|nein| F
</pre>
<p class="cap">Abb. 3 — Die Sperre hängt am Manager, nicht am Betrachter.</p>
</div>

<p>Das ist der entscheidende Kniff: Weil die Sperre am <strong>Managernamen</strong> hängt, wird ein
   Manager höchstens einmal pro Tag geholt — <em>egal wie viele Leute ihn ansehen</em>. Damit ist das
   Sorare-Kontingent auch gegen anonyme Aufrufe geschützt, ohne dass wir Besucher aussperren müssen.</p>

<h2>6 · Wer darf was</h2>
<p>Es gibt drei Schlüssel mit klar getrennten Rechten. Wer sie verwechselt, baut eine Lücke.</p>

<div class="scroll"><table class="tbl">
<thead><tr><th>Schlüssel</th><th>Wo</th><th>Darf</th></tr></thead>
<tbody>
<tr><td>publishable</td><td>im Quelltext der Seite, öffentlich</td><td>Marktdaten und gespiegelte Portfolios <em>lesen</em> — sonst nichts</td></tr>
<tr><td>service_role</td><td>nur Railway und Edge Functions</td><td>alles; umgeht Row Level Security</td></tr>
<tr><td>Nutzer-Token</td><td>Browser nach Anmeldung</td><td>ausschließlich die eigenen Zeilen (<code>auth.uid()</code>)</td></tr>
</tbody></table></div>

<div class="note warn"><p><strong>Zwei Lektionen, die Geld oder Daten gekostet hätten:</strong>
   <code>verify_jwt = true</code> an einer Edge Function ist <em>kein</em> Nutzer-Schutz — das Gateway
   akzeptiert auch den öffentlichen Schlüssel. Und <code>revoke ... from anon</code> reicht bei Funktionen
   nicht: Postgres vergibt <code>EXECUTE</code> zusätzlich an <code>PUBLIC</code>. Beides führte zu
   Auswertungen, die ohne Login aufrufbar waren.</p></div>

<h2>7 · Zeitplan</h2>
<div class="scroll"><table class="tbl">
<thead><tr><th>Job</th><th>Wann</th><th>Tut</th></tr></thead>
<tbody>
<tr><td>Preis-Updater ×3</td><td>alle 5 Min, 22–04 und 16–20 UTC</td><td>je 120 Karten aktualisieren</td></tr>
<tr><td>Kader-Abgleich</td><td>täglich 04:00 UTC</td><td>alle Sorare-Vereine durchgehen, fehlende Spieler anlegen</td></tr>
<tr><td>Harvester</td><td>täglich 05:30 UTC</td><td>Auktionen und Angebote nach unbekannten Spielern durchsuchen, danach Vollmarkt-Schnappschuss</td></tr>
</tbody></table></div>
<p>Die Fenster sind bewusst gesetzt: Sie umgehen die europäische Hauptspielzeit und halten den
   Verbrauch beim geteilten Sorare-Kontingent unter der Grenze.</p>

<h2>8 · Wie Änderungen live gehen</h2>
<ol class="steps">
  <li><strong>Oberfläche:</strong> Datei in <code>Sorion_pro/UI/</code> ändern, nach <code>sorion-ui/</code>
      kopieren, beide Repos pushen. GitHub Pages liefert nach etwa zehn Minuten aus.</li>
  <li><strong>Hintergrundjobs:</strong> Push nach <code>main</code> — Railway baut und deployt selbst.</li>
  <li><strong>Serverfunktionen:</strong> <code>npx supabase functions deploy &lt;name&gt;</code>.</li>
  <li><strong>Datenbank:</strong> SQL-Datei in <code>migrations/</code> ablegen und von Hand im SQL-Editor
      ausführen. Bewusst nicht automatisch — eine falsche Migration trifft alle Daten auf einmal.</li>
</ol>

<div class="note"><p><strong>Deshalb prüft der Code, was existiert:</strong> Weil Migrationen von Hand
   laufen, fragt der Updater beim Start ab, welche Spalten schon da sind, und lässt fehlende Felder weg.
   So bricht nichts, wenn Code und Datenbank kurzzeitig auseinanderlaufen.</p></div>

<h2>9 · Woran das System heute noch trägt</h2>
<ul>
  <li><strong>Marktseite lädt alles in den Browser</strong> — etwa 15 MB und 199 Anfragen pro Besuch.
      Serverseitiges Filtern würde daraus rund 50 KB machen. Die Aggregat-Funktionen dafür stehen bereits.</li>
  <li><strong>Datenbank am Limit des freien Tarifs</strong> — die Kursverlauf-Diät greift seit dem 02.08.,
      die Altlast wird noch aufgeräumt.</li>
  <li><strong>Ein geteiltes Sorare-Kontingent</strong> für Hintergrundjobs und Nutzeranfragen. Durch die
      Spiegelung ist der nutzerseitige Anteil inzwischen klein.</li>
</ul>

<footer>Sorion · technischer Bauplan · nicht mit Sorare verbunden</footer>
</div>
`;

writeFileSync('C:/Users/Jonas/AppData/Local/Temp/claude/C--Users-Jonas-Programme-MtG-LTeach/1bdc7f41-3b98-47b9-9261-a5cf94fb36ef/scratchpad/sorion-bauplan.html', html);
console.log('gebaut:', (html.length / 1024).toFixed(0), 'KB');
