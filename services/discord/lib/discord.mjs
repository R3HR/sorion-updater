// Discord-Webhook: Payload senden, bei 429/5xx kurz warten und erneut.
const HOOK = process.env.DISCORD_DONATIONS_WEBHOOK ?? '';

export async function postToDiscord(payload, { attempts = 3 } = {}) {
  if (!HOOK) throw new Error('DISCORD_DONATIONS_WEBHOOK nicht gesetzt');
  let lastErr;
  for (let i = 1; i <= attempts; i++) {
    const res = await fetch(HOOK + '?wait=true', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (res.ok || res.status === 204) {
      try { return (await res.json())?.id ?? null; } catch { return null; }
    }
    const text = await res.text().catch(() => '');
    lastErr = new Error(`Discord ${res.status}: ${text.slice(0, 200)}`);
    if (res.status === 429) {
      const retryAfter = Number(res.headers.get('retry-after') ?? 2);
      await sleep(Math.min(retryAfter, 10) * 1000);
    } else if (res.status >= 500) {
      await sleep(1000 * i);
    } else {
      break;   // 4xx ausser 429: Wiederholen bringt nichts
    }
  }
  throw lastErr;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
