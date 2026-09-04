// SORION — Unterstuetzungs-Panel mit eingebettetem Ko-fi.
//
// ENTSCHEIDUNG Jonas (04.09.): Der Besucher soll AUF DER SEITE bleiben, das
// wirkt professioneller als ein Absprung zu ko-fi.com. Deshalb wird Ko-fi
// eingebettet — aber ERST BEIM KLICK auf "Support". Wer nicht spendet, baut
// keine Verbindung zu Ko-fi auf; das haelt die Seite schnell und deckt sich
// mit Ziffer 5c der Datenschutzerklaerung (Einwilligung durch aktives
// Oeffnen). Ein dauerhaft eingebetteter Iframe wuerde bei JEDEM Aufruf laden.
//
// Einbinden: <div id="support-panel"></div> plus <script src="support.js" defer>.
// Ohne den Container passiert nichts ausser dem Footer-Link.
(function () {
  const KOFI_PAGE  = 'https://ko-fi.com/sorionpro';
  const KOFI_EMBED = 'https://ko-fi.com/sorionpro/?hidefeed=true&widget=true&embed=true&preview=true';

  const css = `
    .sup-box { background: var(--surface, #13131f); border: 1px solid var(--border, #2a2a45);
      border-left: 3px solid #ff5f5f; border-radius: 10px; padding: 18px 20px; margin: 22px 0; }
    .sup-top { display: flex; gap: 18px; align-items: center; flex-wrap: wrap; }
    .sup-txt { flex: 1; min-width: 240px; }
    .sup-head { font-family: 'Orbitron', monospace; font-size: 15px;
      color: var(--text, #e8e8ff); margin-bottom: 6px; }
    .sup-sub { font-family: 'Share Tech Mono', monospace; font-size: 12.5px;
      color: var(--text2, #9090c0); line-height: 1.6; }
    .sup-btn { display: inline-flex; align-items: center; gap: 8px; cursor: pointer;
      font-family: 'Share Tech Mono', monospace; font-size: 13px; letter-spacing: .08em;
      color: #fff; background: #ff5f5f; border: none; border-radius: 8px; padding: 12px 20px;
      white-space: nowrap; transition: transform .15s, box-shadow .15s; }
    .sup-btn:hover { transform: translateY(-1px); box-shadow: 0 6px 20px rgba(255,95,95,.35); }
    .sup-embed { margin-top: 16px; border-top: 1px solid var(--border, #2a2a45); padding-top: 16px; }
    .sup-embed iframe { width: 100%; height: 680px; border: none; border-radius: 8px;
      background: #f9f9f9; display: block; }
    .sup-note { font-family: 'Share Tech Mono', monospace; font-size: 10.5px;
      color: var(--text2, #9090c0); opacity: .8; margin-top: 8px; }
    .sup-note a { color: var(--purple, #b060ff); }
    /* Navi-Knopf: auffaelliger als ein Textlink, aber leiser als die
       Profil-Schaltflaeche daneben (die bleibt der Hauptweg). */
    /* Hoehere Spezifitaet als die generischen Button-Regeln der Seiten,
       sonst erbt der Knopf deren 10px und wirkt nicht prominent. */
    .nav-links .nav-support, .nav-support {
      font-family: 'Share Tech Mono', monospace; font-size: 12px;
      letter-spacing: .1em; text-transform: uppercase; cursor: pointer;
      color: #ff7a7a; background: rgba(255,95,95,.10); border: 1px solid rgba(255,95,95,.55);
      border-radius: 6px; padding: 10px 16px; margin: 0; transition: all .15s; white-space: nowrap; }
    .nav-links .nav-support:hover, .nav-support:hover {
      background: #ff5f5f; color: #fff; border-color: #ff5f5f;
      box-shadow: 0 0 18px rgba(255,95,95,.35); transform: translateY(-1px); }
    /* Modal: Ko-fi oeffnet sich UEBER der Seite, der Besucher bleibt hier */
    .sup-back { position: fixed; inset: 0; background: rgba(5,5,12,.85);
      backdrop-filter: blur(4px); z-index: 90; display: flex; align-items: center;
      justify-content: center; padding: 20px; }
    .sup-modal { background: var(--surface, #13131f); border: 1px solid var(--border2, #3a3a5a);
      border-radius: 12px; width: 100%; max-width: 460px; max-height: 92vh; overflow-y: auto;
      box-shadow: 0 0 60px rgba(255,95,95,.18); }
    .sup-mhead { display: flex; align-items: center; gap: 12px; padding: 16px 18px;
      border-bottom: 1px solid var(--border, #2a2a45); }
    .sup-mtitle { font-family: 'Orbitron', monospace; font-size: 14px; color: var(--text, #e8e8ff); }
    .sup-close { margin-left: auto; background: none; border: none; cursor: pointer;
      color: var(--text2, #9090c0); font-size: 22px; line-height: 1; padding: 4px 8px; }
    .sup-close:hover { color: var(--text, #e8e8ff); }
    .sup-mbody { padding: 14px 18px 18px; }
    .sup-mbody .sup-sub { margin-bottom: 12px; }
    @media (max-width: 560px) {
      .sup-btn { width: 100%; justify-content: center; }
      .sup-embed iframe { height: 620px; }
      .nav-links .nav-support, .nav-support { font-size: 11px; padding: 9px 12px; }
    }`;

  function open(box) {
    if (box.dataset.open) return;
    box.dataset.open = '1';
    try { if (window.track) window.track('support_click'); } catch (e) {}
    const wrap = document.createElement('div');
    wrap.className = 'sup-embed';
    // Der Iframe entsteht erst jetzt — vorher gab es keinen Kontakt zu Ko-fi.
    wrap.innerHTML = `<iframe src="${KOFI_EMBED}" title="Support SORION on Ko-fi"
        loading="lazy" referrerpolicy="no-referrer-when-downgrade"></iframe>
      <div class="sup-note">Payment is handled by Ko-fi, we never see your payment details.
        Trouble with the frame? <a href="${KOFI_PAGE}" target="_blank" rel="noopener noreferrer">Open Ko-fi directly →</a></div>`;
    box.appendChild(wrap);
    const btn = box.querySelector('.sup-btn');
    if (btn) btn.textContent = '☕ Thank you!';
    wrap.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }

  function styles() {
    if (document.getElementById('sup-css')) return;
    const st = document.createElement('style');
    st.id = 'sup-css';
    st.textContent = css;
    document.head.appendChild(st);
  }

  function panel() {
    const host = document.getElementById('support-panel');
    if (!host || host.dataset.done) return;
    host.dataset.done = '1';

    const box = document.createElement('div');
    box.className = 'sup-box';
    box.innerHTML = `
      <div class="sup-top">
        <div class="sup-txt">
          <div class="sup-head">☕ Support SORION</div>
          <div class="sup-sub">No ads, no paywall, no tracking. Servers and domains cost
            about €38 a month. If SORION saves you time, you can chip in.</div>
        </div>
        <button class="sup-btn" type="button">Buy me a coffee →</button>
      </div>`;
    box.querySelector('.sup-btn').addEventListener('click', () => open(box));
    host.appendChild(box);
  }

  // Footer-Link auf jeder Seite mit Footer (fuehrt zu Ko-fi, neues Tab)
  function footerLink() {
    for (const f of document.querySelectorAll('footer, .footer')) {
      if (f.dataset.sup) continue;
      f.dataset.sup = '1';
      // In die vorhandene Link-Zeile einreihen (letzter <span>), sonst steht
      // der Link als eigene Zeile unter dem Footer.
      const target = f.querySelector('span:last-of-type') || f;
      const a = document.createElement('a');
      a.href = KOFI_PAGE; a.target = '_blank'; a.rel = 'noopener noreferrer';
      a.textContent = 'Support';
      a.style.cssText = 'color:var(--purple,#b060ff);text-decoration:none';
      a.addEventListener('click', () => { try { if (window.track) window.track('support_click'); } catch (e) {} });
      target.appendChild(document.createTextNode(' · '));
      target.appendChild(a);
    }
  }

  // Vom Navi-Knopf aufgerufen: Ko-fi in einem Fenster UEBER der Seite. Der
  // Iframe entsteht auch hier erst jetzt, nicht beim Laden der Seite.
  window.openSupport = function () {
    if (document.querySelector('.sup-back')) return;
    try { if (window.track) window.track('support_click'); } catch (e) {}
    const back = document.createElement('div');
    back.className = 'sup-back';
    back.innerHTML = `
      <div class="sup-modal" role="dialog" aria-modal="true" aria-label="Support SORION">
        <div class="sup-mhead">
          <span class="sup-mtitle">☕ Support SORION</span>
          <button class="sup-close" type="button" aria-label="Close">×</button>
        </div>
        <div class="sup-mbody">
          <div class="sup-sub">No ads, no paywall, no tracking. Servers and domains cost
            about €38 a month. If SORION saves you time, you can chip in.</div>
          <iframe src="${KOFI_EMBED}" title="Support SORION on Ko-fi" loading="lazy"
            referrerpolicy="no-referrer-when-downgrade"
            style="width:100%;height:640px;border:none;border-radius:8px;background:#f9f9f9;display:block"></iframe>
          <div class="sup-note">Payment is handled by Ko-fi, we never see your payment details.
            Trouble with the frame? <a href="${KOFI_PAGE}" target="_blank" rel="noopener noreferrer">Open Ko-fi directly →</a></div>
        </div>
      </div>`;
    const close = () => { back.remove(); document.removeEventListener('keydown', onKey); };
    const onKey = e => { if (e.key === 'Escape') close(); };
    back.addEventListener('click', e => { if (e.target === back) close(); });
    back.querySelector('.sup-close').addEventListener('click', close);
    document.addEventListener('keydown', onKey);
    document.body.appendChild(back);
  };

  const run = () => { styles(); panel(); footerLink(); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', run);
  else run();
})();
