// SORION — Unterstuetzung (Ko-fi).
//
// Aufteilung nach der Panne vom 04.09.: Alles, was AUSSEHEN betrifft, steht
// direkt in den Seiten (Navi-Knopf) bzw. hier als Stil fuer den statischen
// Panel-Kasten. Ein spaet geladenes Skript darf ueber das Aussehen eines
// sichtbaren Knopfes nicht entscheiden — sonst zeigt der Browser den nackten
// Standardknopf, bis das Skript da ist.
//
// Der offizielle Ko-fi-Button steht statisch im Panel-Markup der Seiten.
// Dieses Skript liefert zusaetzlich:
//   * Stile fuer den Panel-Kasten (.sup-box)
//   * ein Modal fuer den Navi-Knopf (openSupport), damit der Besucher auf der
//     Seite bleibt statt in einen neuen Tab zu springen
//   * einen dezenten Footer-Link auf jeder Seite
(function () {
  const KOFI_PAGE  = 'https://ko-fi.com/sorionpro';
  const KOFI_EMBED = 'https://ko-fi.com/sorionpro/?hidefeed=true&widget=true&embed=true&preview=true';

  const css = `
    .sup-box { background: var(--surface, #13131f); border: 1px solid var(--border, #2a2a45);
      border-left: 3px solid #b060ff; border-radius: 10px; padding: 18px 20px; margin: 22px 0; }
    .sup-top { display: flex; gap: 18px; align-items: center; flex-wrap: wrap; }
    .sup-txt { flex: 1; min-width: 240px; }
    .sup-head { font-family: 'Orbitron', monospace; font-size: 15px;
      color: var(--text, #e8e8ff); margin-bottom: 6px; }
    .sup-sub { font-family: 'Share Tech Mono', monospace; font-size: 12.5px;
      color: var(--text2, #9090c0); line-height: 1.6; }
    .sup-kofi { flex-shrink: 0; }
    .sup-kofi img { display: block; }
    .sup-note { font-family: 'Share Tech Mono', monospace; font-size: 10.5px;
      color: var(--text2, #9090c0); opacity: .8; margin-top: 10px; }
    .sup-note a { color: var(--purple, #b060ff); }
    /* Modal fuer den Navi-Knopf: Ko-fi oeffnet UEBER der Seite */
    .sup-back { position: fixed; inset: 0; background: rgba(5,5,12,.85);
      backdrop-filter: blur(4px); z-index: 90; display: flex; align-items: center;
      justify-content: center; padding: 20px; }
    .sup-modal { background: var(--surface, #13131f); border: 1px solid var(--border2, #3a3a5a);
      border-radius: 12px; width: 100%; max-width: 460px; max-height: 92vh; overflow-y: auto;
      box-shadow: 0 0 60px rgba(176,96,255,.22); }
    .sup-mhead { display: flex; align-items: center; gap: 12px; padding: 16px 18px;
      border-bottom: 1px solid var(--border, #2a2a45); }
    .sup-mtitle { font-family: 'Orbitron', monospace; font-size: 14px; color: var(--text, #e8e8ff); }
    .sup-close { margin-left: auto; background: none; border: none; cursor: pointer;
      color: var(--text2, #9090c0); font-size: 22px; line-height: 1; padding: 4px 8px; }
    .sup-close:hover { color: var(--text, #e8e8ff); }
    .sup-mbody { padding: 14px 18px 18px; }
    .sup-mbody .sup-sub { margin-bottom: 12px; }
    @media (max-width: 560px) { .sup-kofi { width: 100%; } }`;

  function styles() {
    if (document.getElementById('sup-css')) return;
    const st = document.createElement('style');
    st.id = 'sup-css';
    st.textContent = css;
    document.head.appendChild(st);
  }

  // Navi-Knopf: Ko-fi in einem Fenster ueber der Seite (Besucher bleibt hier).
  // Der Iframe entsteht erst beim Oeffnen und verschwindet beim Schliessen.
  window.openSupport = function () {
    if (document.querySelector('.sup-back')) return;
    try { if (window.track) window.track('support_click'); } catch (e) {}
    styles();
    const back = document.createElement('div');
    back.className = 'sup-back';
    back.innerHTML = `
      <div class="sup-modal" role="dialog" aria-modal="true" aria-label="Support SORION">
        <div class="sup-mhead">
          <span class="sup-mtitle">☕ Support SORION</span>
          <button class="sup-close" type="button" aria-label="Close">×</button>
        </div>
        <div class="sup-mbody">
          <div class="sup-sub">No ads, no paywall, no tracking. One person has been
            building SORION since March, several hours a day, and it runs on about €38
            a month for servers and domains. If it saves you time, you can chip in.</div>
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

  function footerLink() {
    for (const f of document.querySelectorAll('footer, .footer')) {
      if (f.dataset.sup) continue;
      f.dataset.sup = '1';
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

  const run = () => { styles(); footerLink(); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', run);
  else run();
})();
