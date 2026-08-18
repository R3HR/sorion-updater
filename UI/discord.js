/* SORION — Discord-Einladung, rechte Bildschirmkante, scrollt mit.
 *
 * Bewusst EINE gemeinsame Datei statt vier Kopien: Der Einladungslink steht
 * damit an genau einer Stelle (INVITE unten). Eingebunden per
 *   <script src="discord.js" defer></script>
 * vor </body> auf index/portfolio/profile/legal.
 *
 * Ebenen: z-index 50 — ueber der Steuerleiste (40) und dem Liga-Dropdown
 * (30/31), aber UNTER den Modals (100), damit ein offenes Karten-Detail nicht
 * von der Fahne durchstochen wird.
 *
 * Warum die Beschreibung nur beim Ueberfahren erscheint (gemessen 18.08. im
 * Browser): Der Seiteninhalt ist auf 1600 px begrenzt und fuellt darunter die
 * volle Breite. Bei 1440 px endet die rechte Spalte (Liga-Ranking) bei 1398 px,
 * die Fahne beginnt bei 1396 — dauerhaft ausgeklappt haette sie 212 px davon
 * verdeckt. Genug Platz gaebe es erst ab ~2100 px Fensterbreite, also praktisch
 * nie. Die Lasche selbst ist nur 34 px breit und ueberlappt damit nichts.
 *
 * Ergebnis: Die Lasche ist IMMER sichtbar (Marke + Schriftzug + Puls), der
 * Beschreibungsteil erscheint beim Ueberfahren und verschwindet wieder. Kein
 * Zustand, nichts zum Wegklicken, nichts dauerhaft Verdecktes.
 */
(function () {
  var INVITE  = 'https://discord.gg/mncXeJJKbn';
  var BLURPLE = '#5865F2';   // Discord-Markenfarbe — nur als FLAECHE (Knopf, Rahmen)
  // Als SCHRIFT auf dunklem Grund faellt die Markenfarbe durch: gemessen 3,70:1
  // gegen #1a1a2e, gefordert sind 4,5:1 fuer kleine Schrift. Diese Aufhellung
  // liest sich noch klar als Discord-Blau und kommt auf 5,31:1.
  var BLURPLE_TXT = '#7B85F7';

  if (document.getElementById('dc-rail')) return;   // doppelte Einbindung abfangen

  var css = document.createElement('style');
  css.textContent = [
    '#dc-rail{position:fixed;right:0;top:50%;transform:translateY(-50%);z-index:50;',
    '  display:flex;align-items:stretch;font-family:"Share Tech Mono",monospace;',
    '  filter:drop-shadow(0 6px 24px rgba(0,0,0,.55))}',

    /* Lasche: immer sichtbar, klappt auf und zu */
    '#dc-tab{width:34px;flex:0 0 34px;border:1px solid ' + BLURPLE + ';border-right:none;',
    '  border-radius:8px 0 0 8px;background:linear-gradient(180deg,#1a1a2e,#13131f);',
    '  color:' + BLURPLE_TXT + ';cursor:pointer;display:flex;flex-direction:column;',
    '  align-items:center;justify-content:center;gap:9px;padding:14px 0;',
    '  transition:background .15s,color .15s}',
    '#dc-tab:hover,#dc-tab:focus-visible{background:' + BLURPLE + ';color:#fff;outline:none}',
    '#dc-tab .dc-vert{writing-mode:vertical-rl;transform:rotate(180deg);font-size:10px;',
    '  letter-spacing:.22em;text-transform:uppercase}',
    '#dc-tab svg{width:17px;height:17px;fill:currentColor;flex:none}',

    /* Aufgeklappter Bereich */
    '#dc-body{width:0;overflow:hidden;transition:width .22s ease;',
    '  background:linear-gradient(180deg,#1a1a2e,#13131f);',
    '  border-top:1px solid ' + BLURPLE + ';border-bottom:1px solid ' + BLURPLE + '}',
    '#dc-rail:hover #dc-body,#dc-rail:focus-within #dc-body{width:212px}',
    '#dc-inner{width:212px;padding:16px 16px 15px}',
    '#dc-inner .dc-h{display:flex;align-items:center;gap:8px;color:#fff;font-size:12px;',
    '  letter-spacing:.06em;margin-bottom:8px}',
    '#dc-inner .dc-h svg{width:19px;height:19px;fill:' + BLURPLE_TXT + ';flex:none}',
    '#dc-inner p{color:#9090c0;font-size:11px;line-height:1.55;margin:0 0 13px}',
    '#dc-cta{display:flex;align-items:center;justify-content:center;gap:7px;',
    '  background:' + BLURPLE + ';color:#fff;text-decoration:none;border-radius:6px;',
    '  padding:9px 10px;font-size:11px;letter-spacing:.1em;text-transform:uppercase;',
    '  transition:filter .15s}',
    '#dc-cta:hover{filter:brightness(1.15)}',

    /* Sanfter Puls, damit die eingeklappte Lasche auffaellt.
       box-shadow statt transform — transform wuerde einen eigenen
       Stapelkontext aufmachen (dieselbe Falle wie beim Liga-Dropdown). */
    '@keyframes dc-pulse{0%,100%{box-shadow:0 0 0 0 rgba(88,101,242,0)}',
    '  50%{box-shadow:0 0 0 5px rgba(88,101,242,.18)}}',
    '#dc-tab{animation:dc-pulse 3.4s ease-in-out infinite}',
    '#dc-rail:hover #dc-tab{animation:none}',
    '@media (prefers-reduced-motion:reduce){',
    '  #dc-tab{animation:none}#dc-body{transition:none}}',

    /* Handy: runder Knopf unten rechts statt Randleiste */
    '@media (max-width:720px){',
    '  #dc-rail{top:auto;bottom:18px;right:16px;transform:none}',
    '  #dc-body{display:none}',   /* nicht nur width:0 — sonst behaelt der Block */
    /* seine natuerliche HOEHE (~170 px), die Fahne wird so hoch, und der Knopf
       haengt an deren Oberkante statt unten rechts. Im Test sass er dadurch
       139 px zu hoch. */
    '  #dc-tab{width:50px;flex-basis:50px;height:50px;border-radius:50%;padding:0;',
    '    border:1px solid ' + BLURPLE + ';background:' + BLURPLE + ';color:#fff}',
    '  #dc-tab .dc-vert{display:none}',
    '  #dc-tab svg{width:24px;height:24px}}',
  ].join('');
  document.head.appendChild(css);

  // Offizielle Discord-Bildmarke, verlinkt auf den eigenen Server
  var MARK = '<svg viewBox="0 0 24 24" aria-hidden="true"><path d="M20.317 4.3698a19.7913 19.7913 0 0 0-4.8851-1.5152.0741.0741 0 0 0-.0785.0371c-.211.3753-.4447.8648-.6083 1.2495-1.8447-.2762-3.68-.2762-5.4868 0-.1636-.3933-.4058-.8742-.6177-1.2495a.077.077 0 0 0-.0785-.037 19.7363 19.7363 0 0 0-4.8852 1.515.0699.0699 0 0 0-.0321.0277C.5334 9.0458-.319 13.5799.0992 18.0578a.0824.0824 0 0 0 .0312.0561c2.0528 1.5076 4.0413 2.4228 5.9929 3.0294a.0777.0777 0 0 0 .0842-.0276c.4616-.6304.8731-1.2952 1.226-1.9942a.076.076 0 0 0-.0416-.1057c-.6528-.2476-1.2743-.5495-1.8722-.8923a.077.077 0 0 1-.0076-.1277c.1258-.0943.2517-.1923.3718-.2914a.0743.0743 0 0 1 .0776-.0105c3.9278 1.7933 8.18 1.7933 12.0614 0a.0739.0739 0 0 1 .0785.0095c.1202.099.246.1981.3728.2924a.077.077 0 0 1-.0066.1276 12.2986 12.2986 0 0 1-1.873.8914.0766.0766 0 0 0-.0407.1067c.3604.698.7719 1.3628 1.225 1.9932a.076.076 0 0 0 .0842.0286c1.961-.6067 3.9495-1.5219 6.0023-3.0294a.077.077 0 0 0 .0313-.0552c.5004-5.177-.8382-9.6739-3.5485-13.6604a.061.061 0 0 0-.0312-.0286zM8.02 15.3312c-1.1825 0-2.1569-1.0857-2.1569-2.419 0-1.3332.9555-2.4189 2.157-2.4189 1.2108 0 2.1757 1.0952 2.1568 2.419 0 1.3332-.9555 2.4189-2.1569 2.4189zm7.9748 0c-1.1825 0-2.1569-1.0857-2.1569-2.419 0-1.3332.9554-2.4189 2.1569-2.4189 1.2108 0 2.1757 1.0952 2.1568 2.419 0 1.3332-.946 2.4189-2.1568 2.4189z"/></svg>';

  var rail = document.createElement('aside');
  rail.id = 'dc-rail';
  rail.setAttribute('aria-label', 'Sorion on Discord');
  rail.innerHTML =
      '<a id="dc-tab" href="' + INVITE + '" target="_blank" rel="noopener noreferrer"'
    + ' title="Join Sorion on Discord">' + MARK + '<span class="dc-vert">Discord</span></a>'
    + '<div id="dc-body"><div id="dc-inner">'
    + '<div class="dc-h">' + MARK + 'Sorion on Discord</div>'
    + '<p>Price questions, feature requests, and what the data is actually saying'
    + ' — straight from other managers.</p>'
    + '<a id="dc-cta" href="' + INVITE + '" target="_blank" rel="noopener noreferrer">Join server</a>'
    + '</div></div>';
  document.body.appendChild(rail);

  // Beides sind echte Links auf denselben Server — kein Zustand, kein Schalter.
  // Der Klick wird nur fuer die Reichweitenmessung mitgezaehlt (Event steht in
  // der Whitelist von supabase/functions/track, sonst antwortet der Beacon 400).
  rail.querySelectorAll('#dc-tab, #dc-cta').forEach(function (a) {
    a.addEventListener('click', function () {
      if (window.track) { try { track('discord_join'); } catch (e) {} }
    });
  });

})();
