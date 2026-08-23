/* SORION — Barrierefreiheit (a11y.js)
 *
 * EINE Seite fuer alle statt einer separaten "barrierefreien Version":
 * zwei Seiten wuerden auseinanderdriften, und Menschen mit Einschraenkungen
 * bekaemen eine Abstellgleis-Kopie. Stattdessen (Entscheidung 22.08.):
 *
 *  A) Grundlagen fuer ALLE Besucher, ohne Schalter:
 *     - sichtbarer Tastaturfokus (gelber Ring) auf allen bedienbaren Elementen
 *     - "Skip to content"-Link fuer Tastatur/Screenreader
 *     - prefers-reduced-motion wird seitenweit respektiert
 *     - fehlende Beschriftungen (Schliessen, Sync, Sortier-Spalten) per ARIA
 *     - gedaempfte Schrift seitenweit auf >= 4,5:1 (Token --dim, siehe index.html)
 *
 *  B) Schalter "Aa" (unten links, auf allen Seiten, gemerkt in localStorage):
 *     hoher Kontrast (Weiss auf Schwarz, kraeftigere Raender, hellere Akzente),
 *     unterstrichene Links, ~15 % groessere Darstellung. Schaltet sich von
 *     selbst ein, wenn das Betriebssystem "mehr Kontrast" wuenscht
 *     (prefers-contrast: more) und noch keine eigene Wahl gespeichert ist.
 *
 * Eingebunden per  <script src="a11y.js" defer></script>  vor </body>.
 * Grenzen (ehrlich): Das ist WCAG-Grundversorgung — Kontrast, Fokus, Tastatur,
 * Labels, Bewegung. Eine vollstaendige Screenreader-Fuehrung durch die
 * Markttabelle (Live-Regionen, Sortier-Ansagen) ist ein eigener Schritt.
 */
(function () {
  var KEY = 'sorion_a11y';
  var root = document.documentElement;

  // ── A) Grundlagen fuer alle ───────────────────────────────────────────────
  var base = document.createElement('style');
  base.textContent = [
    /* Tastaturfokus: Gelb (#ffe600) hat auf allen Flaechen der Seite >10:1 */
    'a:focus-visible,button:focus-visible,input:focus-visible,select:focus-visible,',
    '[tabindex]:focus-visible,th[onclick]:focus-visible{outline:3px solid #ffe600!important;',
    '  outline-offset:2px!important;border-radius:4px}',
    /* Skip-Link: unsichtbar, bis er per Tab fokussiert wird */
    '.a11y-skip{position:fixed;left:12px;top:-60px;z-index:200;background:#ffe600;color:#0d0d1a;',
    '  padding:10px 14px;border-radius:6px;font:700 13px/1 "Exo 2",sans-serif;text-decoration:none;',
    '  transition:top .15s}',
    '.a11y-skip:focus{top:12px;outline:3px solid #0d0d1a}',
    /* Bewegung aus, wenn das System es wuenscht */
    '@media (prefers-reduced-motion:reduce){*,*::before,*::after{animation-duration:.01ms!important;',
    '  animation-iteration-count:1!important;transition-duration:.01ms!important;scroll-behavior:auto!important}}',

    /* ── B) Kontrast-Modus (html.a11y) ───────────────────────────────────── */
    'html.a11y{--bg:#000;--bg2:#000;--surface:#0a0a12;--surface2:#12121c;--surface3:#1a1a26;',
    '  --border:#6a6a94;--border2:#8a8ab4;--text:#fff;--text2:#dcdcf2;--dim:#c8c8e0;--muted:#c8c8e0;',
    '  --purple:#d4a8ff;--purple2:#b880ff;--up:#6dffb8;--down:#ff7a9e;--limited:#ffd070;',
    '  --rare:#ff8a8a;--sr:#a0b8ff}',
    'html.a11y body{background:#000!important;zoom:1.15}',
    '@media (max-width:720px){html.a11y body{zoom:1.08}}',
    'html.a11y a{text-decoration:underline;text-underline-offset:3px}',
    'html.a11y .hero-stat,html.a11y table,html.a11y .table-wrap,html.a11y .modal,html.a11y .p-stat,',
    'html.a11y .card,html.a11y input,html.a11y button{border-color:#8a8ab4!important}',
    'html.a11y thead th{color:#fff!important}',
    'html.a11y .hero-stat::before{background:var(--purple)!important}',
    /* Placeholder-Text war die duennste Stelle der Seite */
    'html.a11y ::placeholder{color:#b8b8d8!important;opacity:1}',

    /* ── Schalter ────────────────────────────────────────────────────────── */
    '#a11y-toggle{position:fixed;left:14px;bottom:14px;z-index:50;min-width:44px;height:44px;',
    '  padding:0 14px;border-radius:22px;border:1px solid var(--border2,#3a3a60);',
    '  background:var(--surface2,#1a1a2e);color:var(--text,#e8e8ff);cursor:pointer;',
    '  font:700 15px/1 "Exo 2",sans-serif;display:inline-flex;align-items:center;gap:8px;',
    '  box-shadow:0 6px 20px rgba(0,0,0,.45)}',
    '#a11y-toggle:hover{border-color:var(--purple,#b060ff)}',
    '#a11y-toggle[aria-pressed="true"]{background:#ffe600;color:#0d0d1a;border-color:#ffe600}',
    '#a11y-toggle .lbl{font:11px/1 "Share Tech Mono",monospace;letter-spacing:.08em;text-transform:uppercase}',
    '@media (max-width:720px){#a11y-toggle .lbl{display:none}#a11y-toggle{padding:0;width:44px;justify-content:center}}',
    '.a11y-sr{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}',
  ].join('');
  document.head.appendChild(base);

  // Skip-Link + Hauptinhalt markieren
  var main = document.querySelector('main') || document.querySelector('.wrap') || document.body.children[1];
  if (main && !main.id) main.id = 'main-content';
  if (main && !main.hasAttribute('tabindex')) main.setAttribute('tabindex', '-1');
  if (main && main.tagName !== 'MAIN' && !main.getAttribute('role')) main.setAttribute('role', 'main');
  var skip = document.createElement('a');
  skip.className = 'a11y-skip'; skip.href = '#' + (main ? main.id : '');
  skip.textContent = document.documentElement.lang === 'de' ? 'Zum Inhalt springen' : 'Skip to content';
  document.body.insertBefore(skip, document.body.firstChild);

  // Fehlende Beschriftungen nachruesten (nur wo keine sind)
  function label(sel, text) {
    document.querySelectorAll(sel).forEach(function (el) {
      if (!el.getAttribute('aria-label') && !el.getAttribute('title')) el.setAttribute('aria-label', text);
    });
  }
  label('.modal-close, .pm-close', 'Close');
  label('#btn-sync', 'Sync portfolio');
  label('#lg-btn', 'Filter by league');
  document.querySelectorAll('th[onclick]').forEach(function (th) {
    if (!th.getAttribute('role')) { th.setAttribute('role', 'button'); th.setAttribute('tabindex', '0');
      th.setAttribute('aria-label', 'Sort by ' + th.textContent.replace(/[↕↓↑]/g, '').trim());
      th.addEventListener('keydown', function (e) { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); th.click(); } }); }
  });

  // ── B) Schalter ───────────────────────────────────────────────────────────
  var saved = null;
  try { saved = localStorage.getItem(KEY); } catch (e) {}
  var on = saved === null
    ? (window.matchMedia && window.matchMedia('(prefers-contrast: more)').matches)
    : saved === '1';

  var btn = document.createElement('button');
  btn.id = 'a11y-toggle'; btn.type = 'button';
  btn.innerHTML = '<span aria-hidden="true">Aa</span><span class="lbl" aria-hidden="true">Contrast</span>'
    + '<span class="a11y-sr">High contrast and larger text</span>';
  btn.title = 'High contrast & larger text';
  function apply(state, remember) {
    root.classList.toggle('a11y', state);
    btn.setAttribute('aria-pressed', state ? 'true' : 'false');
    if (remember) { try { localStorage.setItem(KEY, state ? '1' : '0'); } catch (e) {} }
  }
  apply(on, false);
  btn.addEventListener('click', function () {
    apply(!root.classList.contains('a11y'), true);
    if (window.track) { try { track('a11y_toggle'); } catch (e) {} }
  });
  document.body.appendChild(btn);
})();
