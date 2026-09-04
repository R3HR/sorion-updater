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
    @media (max-width: 560px) {
      .sup-btn { width: 100%; justify-content: center; }
      .sup-embed iframe { height: 620px; }
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

  function panel() {
    const host = document.getElementById('support-panel');
    if (!host || host.dataset.done) return;
    host.dataset.done = '1';
    const st = document.createElement('style');
    st.textContent = css;
    document.head.appendChild(st);

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

  const run = () => { panel(); footerLink(); };
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', run);
  else run();
})();
