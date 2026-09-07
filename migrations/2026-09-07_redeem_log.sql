-- ═══════════════════════════════════════════════════════════════════════════
-- Gutschein-Codes: Mehrfach-Einloesung durch dieselbe Person verhindern
-- (07.09.2026) — Voraussetzung dafuer, einen Code oeffentlich zu verteilen.
--
-- ANLASS (Jonas): Ein Pro-Code fuer 30 Tage soll im Discord hinterlegt werden,
-- weil das Publikum fuer zahlende Nutzer noch fehlt. Ein oeffentlicher Code
-- mit `max_uses` allein reicht dafuer nicht: Bisher konnte EIN Konto denselben
-- Code mehrfach einloesen und die Laufzeit dabei jedes Mal verlaengern
-- (redeem_code haengt die Monate an die bestehende Laufzeit an). Aus 30 Tagen
-- waeren so beliebig viele geworden.
--
-- LOESUNG: Ein Protokoll je (Code, Konto). Zweiter Versuch desselben Kontos
-- wird abgewiesen, ohne den Zaehler zu verbrauchen.
--
-- AUSFUEHREN: SQL-Editor, "ohne RLS".
-- ═══════════════════════════════════════════════════════════════════════════

create table if not exists public.redeem_log (
  code_hash   text        not null,
  user_id     uuid        not null references auth.users(id) on delete cascade,
  redeemed_at timestamptz not null default now(),
  primary key (code_hash, user_id)
);
alter table public.redeem_log enable row level security;
revoke all on public.redeem_log from anon, authenticated;   -- nur Service-Rolle

-- redeem_code neu: gleiche Signatur, zusaetzlich die Einmal-je-Konto-Regel.
create or replace function public.redeem_code(p_code text)
returns jsonb language plpgsql security definer
set search_path = public, extensions, auth as $fn$
declare
  uid  uuid := auth.uid();
  h    text;
  c    public.redeem_codes%rowtype;
  alt  date;
  bis  date;
begin
  if uid is null then raise exception 'nicht eingeloggt'; end if;
  h := encode(digest(upper(trim(coalesce(p_code, ''))), 'sha256'), 'hex');

  -- (a) Creator-Schluessel: getarnt als gewoehnlicher Code, kein Protokoll
  if exists (select 1 from public.app_secrets s
             where s.name = 'creator_key' and s.secret_hash = h) then
    insert into public.user_tiers (user_id, source, creator) values (uid, 'key', true)
      on conflict (user_id) do update set creator = true, updated_at = now();
    return jsonb_build_object('ok', true, 'kind', 'creator');
  end if;

  -- (b) Dieses Konto hat diesen Code schon eingeloest
  if exists (select 1 from public.redeem_log l where l.code_hash = h and l.user_id = uid) then
    return jsonb_build_object('ok', false, 'reason', 'already');
  end if;

  -- (c) Gueltiger, noch nicht ausgeschoepfter Code?
  select * into c from public.redeem_codes
   where code_hash = h
     and (expires_at is null or expires_at >= current_date)
     and used_count < max_uses;
  if not found then
    return jsonb_build_object('ok', false);
  end if;

  select valid_until into alt from public.user_tiers where user_id = uid;
  if c.months is null then
    bis := null;
  else
    bis := (greatest(current_date, coalesce(alt, current_date))
            + (c.months || ' months')::interval)::date;
  end if;

  insert into public.user_tiers (user_id, source) values (uid, 'code')
    on conflict (user_id) do nothing;
  execute format('update public.user_tiers set %I = true, valid_until = $1, source = ''code'','
              || ' updated_at = now() where user_id = $2', c.tier)
    using bis, uid;
  update public.redeem_codes set used_count = used_count + 1 where code_hash = h;
  insert into public.redeem_log (code_hash, user_id) values (h, uid)
    on conflict do nothing;

  return jsonb_build_object('ok', true, 'kind', 'tier', 'tier', c.tier, 'valid_until', bis);
end $fn$;
revoke execute on function public.redeem_code(text) from public, anon;
grant execute on function public.redeem_code(text) to authenticated;

-- ── Uebersicht: wird der Code genutzt? ────────────────────────────────────
-- Der Code selbst steht nirgends im Klartext (nur Hash) — deshalb ist die
-- Notiz beim Anlegen die Wiedererkennung. Aufruf nur im SQL-Editor.
create or replace function public.redeem_stats()
returns table (
  note text, tier text, months int, max_uses int, used_count int,
  konten bigint, expires_at date, letzte_einloesung timestamptz
)
language sql stable security definer set search_path = public as $fn$
  select c.note, c.tier, c.months, c.max_uses, c.used_count,
         count(l.user_id), c.expires_at, max(l.redeemed_at)
  from public.redeem_codes c
  left join public.redeem_log l on l.code_hash = c.code_hash
  group by c.code_hash, c.note, c.tier, c.months, c.max_uses, c.used_count, c.expires_at
  order by c.created_at desc
$fn$;
revoke execute on function public.redeem_stats() from public, anon, authenticated;

-- ── Verifikation ───────────────────────────────────────────────────────────
select * from public.redeem_stats();
