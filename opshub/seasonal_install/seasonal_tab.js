// ─── SEASONAL EMAIL TAB (backend: seasonal_email.py) ─────────────────────────
// Everything here is prefixed "se" so it can't clash with the other tabs.
let seConfig = null, seEnv = null, seSource = 'starburst', seSkip = true, sePreviewed = false, seLastProfile = null;

const seEsc = (s) => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const seFmt = (n) => Number(n || 0).toLocaleString('en-US');
const seSeason = (e) => e === 'WINTER_ALERT' ? 'winter' : e === 'SUMMER_ALERT' ? 'summer' : e;

async function seCall(path, opts = {}) {
  const r = await fetch(path, { credentials: 'same-origin', ...opts });
  const body = await r.json().catch(() => ({}));
  if (!r.ok || body.status === 'error') throw new Error(body.message || `Request failed (${r.status})`);
  return body;
}

function seMsg(kind, text) {
  const colors = { bad: 'var(--danger)', ok: 'var(--success)', warn: 'var(--warn)' };
  document.getElementById('se-msg').innerHTML = text
    ? `<div style="font-size:12px;color:${colors[kind]};font-family:var(--mono);line-height:1.6;">${seEsc(text)}</div>` : '';
}

async function seLoad() {
  try {
    if (!seConfig) {
      seConfig = await seCall('/api/seasonal/config');
      document.getElementById('se-btn-create').textContent = seConfig.modes.github ? 'Create PR' : 'Create Files';
      seStarburstHint();
    }
  } catch (e) { seMsg('bad', `Couldn’t load Seasonal Email settings: ${e.message}`); }
  seLoadRuns();
}

function seSelectEnv(code) {
  seEnv = code;
  document.querySelectorAll('#se-env-selector .env-btn').forEach(b => b.classList.toggle('active', b.dataset.env === code));
  const dt = document.getElementById('se-dt-result'); if (dt) dt.textContent = '';
  seStarburstHint();
  seInvalidate();
}

async function seCheckDevtools() {
  const out = document.getElementById('se-dt-result');
  if (!seEnv) { out.textContent = 'Choose an environment first.'; return; }
  out.style.color = 'var(--muted)'; out.textContent = `Looking up ${seEnv}…`;
  try {
    const d = await seCall(`/api/seasonal/devtools?env=${encodeURIComponent(seEnv)}`);
    if (d.error) { out.style.color = 'var(--danger)'; out.textContent = d.error; return; }
    if (!d.ips.length) { out.style.color = 'var(--warn)'; out.textContent = `No running ${d.tag} server in ${d.env} (${d.region}).`; return; }
    out.style.color = d.ips.length === 1 ? 'var(--success)' : 'var(--warn)';
    out.textContent = `${d.env}: ${d.ips.join(', ')} (${d.tag})` + (d.ips.length > 1 ? ' — more than one, you’ll pick one when triggering' : '');
  } catch (e) { out.style.color = 'var(--danger)'; out.textContent = e.message; }
}

function seStarburstHint() {
  const hint = document.getElementById('se-sb-hint');
  const shared = seConfig && seConfig.starburst_shared_login;
  if (seEnv && shared && shared[seEnv] === false)
    hint.innerHTML = `No shared Starburst account is set up for ${seEsc(seEnv)}, so enter your own Starburst login. It’s used for this lookup only and never saved.`;
  else
    hint.textContent = 'Same as HER Revocation: used for this lookup only and never saved. Leave blank to use OpsHub’s shared Starburst account.';
}

function seSelectSource(src) {
  seSource = src;
  document.querySelectorAll('#se-src-selector .env-btn').forEach(b => b.classList.toggle('active', b.dataset.src === src));
  document.getElementById('se-src-starburst').style.display = src === 'starburst' ? 'block' : 'none';
  document.getElementById('se-src-upload').style.display = src === 'upload' ? 'block' : 'none';
  seInvalidate();
}

function seSetSkip(on) {
  seSkip = on;
  const t = document.getElementById('se-skip-toggle');
  t.classList.toggle('on', on); t.setAttribute('aria-checked', on ? 'true' : 'false');
  document.getElementById('se-skip-ack-row').style.display = on ? 'flex' : 'none';
  document.getElementById('se-skip-ack').checked = false;
}
function seToggleSkip() { seSetSkip(!seSkip); seInvalidate(); }

function sePilotChanged() {
  const id = document.getElementById('se-pilot').value.trim();
  const hint = document.getElementById('se-pilot-hint');
  const p = seConfig && seConfig.pilots.find(x => String(x.pilot_id) === id);
  if (p && seLastProfile !== id) {
    if (p.env) seSelectEnv(p.env);
    if (p.default_send_time_ist) document.getElementById('se-time').value = p.default_send_time_ist;
    const f = p.default_filters || {};
    document.getElementById('se-f-email').checked = !!f.exclude_no_email;
    document.getElementById('se-f-notif').checked = !!f.exclude_notifications_disabled;
    document.getElementById('se-f-test').checked = !!f.exclude_test_users;
    seSetSkip(p.skip_subscription_check_default !== false);
  }
  seLastProfile = p ? id : null;
  if (!id) hint.textContent = '';
  else if (!/^\d{4,7}$/.test(id)) hint.textContent = 'Pilot IDs are numbers, for example 10106.';
  else if (p) {
    const todo = (p.to_confirm || []).length ? ` To confirm: ${p.to_confirm.join('; ')}.` : '';
    hint.textContent = `${p.name}${p.env ? ' · usually ' + p.env : ''}.${p.notes ? ' ' + p.notes : ''}${todo}`;
  } else hint.textContent = 'No saved defaults for this pilot. Choose the environment above.';
  seInvalidate();
}

// ── Live user count (no button needed) ─────────────────────────
let seCountTimer = null, seCountSeq = 0, seCountKey = null;
const seCountCache = {};

function seCountBox(html) { document.getElementById('se-count').innerHTML = html; }

function seScheduleCount() {
  clearTimeout(seCountTimer);
  seCountTimer = setTimeout(seAutoCount, 700);
}

async function seAutoCount() {
  if (seSource === 'upload') return seCountFile();
  const id = document.getElementById('se-pilot').value.trim();
  if (!/^\d{4,7}$/.test(id) || !seEnv) {
    seCountKey = null;
    seCountBox('Enter a pilot ID and choose the environment to see how many users will get the email.');
    return;
  }
  const user = document.getElementById('se-sb-user').value.trim();
  const pass = document.getElementById('se-sb-pass').value;
  if (!!user !== !!pass) { seCountBox('Enter both your Starburst login and password, or leave both blank.'); return; }
  const f = ['se-f-email', 'se-f-notif', 'se-f-test'].map(x => document.getElementById(x).checked ? 1 : 0).join('');
  const key = `${seEnv}|${id}|${f}|${user}`;
  if (key === seCountKey) return;                       // same inputs as the count already shown
  seCountKey = key;
  if (seCountCache[key]) return seShowCount(seCountCache[key]);
  const seq = ++seCountSeq;
  seCountBox('<span style="color:var(--warn);">Counting users in Starburst…</span> this can take up to a minute for large pilots.');
  const fd = new FormData();
  fd.set('pilot_id', id); fd.set('env', seEnv);
  fd.set('exclude_no_email', document.getElementById('se-f-email').checked);
  fd.set('exclude_notifications_disabled', document.getElementById('se-f-notif').checked);
  fd.set('exclude_test_users', document.getElementById('se-f-test').checked);
  if (user) { fd.set('sb_user', user); fd.set('sb_password', pass); }
  try {
    const d = await seCall('/api/seasonal/count', { method: 'POST', body: fd });
    if (seq !== seCountSeq) return;                     // inputs changed meanwhile; a newer count is coming
    seCountCache[key] = d;
    seShowCount(d);
  } catch (e) {
    if (seq !== seCountSeq) return;
    seCountKey = null;
    seCountBox(`<span style="color:var(--danger);">${seEsc(e.message)}</span>`);
  }
}

function seShowCount(d) {
  const before = d.enabled_count !== d.user_count ? ` <span style="color:var(--muted);">of ${seFmt(d.enabled_count)} enabled</span>` : '';
  seCountBox(`<span style="font-size:18px;font-weight:600;color:var(--success);">${seFmt(d.user_count)}</span> users${before}
    <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-top:2px;">${seEsc(d.pilot_name)} · ${seEsc(d.env)} · ${seEsc(d.catalog)}.public.user_meta_data</div>
    ${d.user_count === 0 ? '<div style="color:var(--danger);margin-top:4px;">No enabled users for this pilot here. Check the pilot ID and environment.</div>' : ''}`);
}

function seCountFile() {
  const file = document.getElementById('se-file').files[0];
  if (!file) { seCountBox('Choose a file to see how many users it has.'); return; }
  const reader = new FileReader();
  reader.onload = (ev) => {
    const re = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    const seen = new Set(); let invalid = 0, dupes = 0;
    ev.target.result.split(/\r?\n/).forEach((line, i) => {
      const v = line.split(',')[0].trim();
      if (!v || (i === 0 && v.toLowerCase() === 'uuid')) return;
      if (!re.test(v)) { invalid++; return; }
      if (seen.has(v)) { dupes++; return; }
      seen.add(v);
    });
    const notes = [];
    if (dupes) notes.push(`${seFmt(dupes)} duplicate${dupes > 1 ? 's' : ''} will be removed`);
    if (invalid) notes.push(`${seFmt(invalid)} line${invalid > 1 ? 's aren’t' : ' isn’t'} a UUID and will be skipped`);
    seCountBox(`<span style="font-size:18px;font-weight:600;color:var(--success);">${seFmt(seen.size)}</span> users in ${seEsc(file.name)}
      ${notes.length ? `<div style="color:var(--warn);margin-top:4px;">${seEsc(notes.join(' · '))}</div>` : ''}
      ${seen.size === 0 ? '<div style="color:var(--danger);margin-top:4px;">No UUIDs found. Put one UUID per line.</div>' : ''}`);
  };
  reader.readAsText(file);
}

function seInvalidate() {
  seScheduleCount();
  sePreviewed = false;
  document.getElementById('se-btn-create').disabled = true;
  document.getElementById('se-summary').innerHTML = 'Fill in the campaign and click Check Details.';
  seMsg();
}

function seForm() {
  const fd = new FormData();
  fd.set('ticket', document.getElementById('se-ticket').value.trim());
  fd.set('pilot_id', document.getElementById('se-pilot').value.trim());
  fd.set('env', seEnv || '');
  fd.set('event_type', document.getElementById('se-season').value);
  fd.set('send_date', document.getElementById('se-date').value);
  fd.set('send_time', document.getElementById('se-time').value);
  fd.set('user_source', seSource);
  fd.set('exclude_no_email', document.getElementById('se-f-email').checked);
  fd.set('exclude_notifications_disabled', document.getElementById('se-f-notif').checked);
  fd.set('exclude_test_users', document.getElementById('se-f-test').checked);
  fd.set('skip_subscription_check', seSkip);
  fd.set('skip_ack', document.getElementById('se-skip-ack').checked);
  if (seSource === 'starburst') {
    fd.set('sb_user', document.getElementById('se-sb-user').value.trim());
    fd.set('sb_password', document.getElementById('se-sb-pass').value);
  }
  const file = document.getElementById('se-file').files[0];
  if (seSource === 'upload' && file) fd.set('user_file', file);
  return fd;
}

async function sePreview() {
  const btn = document.getElementById('se-btn-check');
  btn.disabled = true; btn.textContent = 'Checking…';
  seMsg('warn', seSource === 'starburst'
    ? 'Counting users in Starburst… this can take up to a minute for large pilots.'
    : 'Reading the uploaded list…');
  try {
    const d = await seCall('/api/seasonal/preview', { method: 'POST', body: seForm() });
    const row = (k, v) => `<div style="display:flex;justify-content:space-between;gap:12px;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.05);"><span style="color:var(--muted);">${k}</span><span style="font-family:var(--mono);text-align:right;word-break:break-all;">${seEsc(v)}</span></div>`;
    const warns = [...(d.ticket.warnings || [])];
    if (d.duplicates) warns.push(`${seFmt(d.duplicates)} duplicate UUIDs will be removed.`);
    if (d.invalid_count) warns.push(`${seFmt(d.invalid_count)} lines aren’t UUIDs and will be skipped (e.g. ${d.invalid_sample.join(', ')}).`);
    document.getElementById('se-summary').innerHTML =
      `<div style="font-size:15px;line-height:1.5;color:inherit;margin-bottom:10px;">Send the <b>${seEsc(seSeason(document.getElementById('se-season').value))}</b> email to <b>${seFmt(d.user_count)}</b> ${seEsc(d.pilot_name)} users at <b>${seEsc(d.when.ist)}</b>.</div>` +
      row('Environment', `${d.env} (${d.script_env})`) +
      row('UTC / US East', `${d.when.utc} / ${d.when.us_east}`) +
      row('scheduledTime', d.when.epoch) +
      (d.enabled_count !== undefined ? row('Enabled users', `${seFmt(d.enabled_count)} before filters`) : '') +
      row('Folder', d.path) +
      row('Script', d.script_name) +
      row('User file' + (d.csv_files.length > 1 ? 's' : ''), d.csv_files.join(', ')) +
      row('Jenkins job', d.jenkins_job) +
      row('Ticket', d.ticket.summary ? `${document.getElementById('se-ticket').value.toUpperCase()}: ${d.ticket.summary}` : d.ticket.status) +
      warns.map(w => `<div style="margin-top:8px;font-size:11.5px;color:var(--warn);">⚠ ${seEsc(w)}</div>`).join('');
    sePreviewed = d.user_count > 0;
    document.getElementById('se-btn-create').disabled = !sePreviewed;
    seResult(d);
  } catch (e) { seMsg('bad', e.message); }
  finally { btn.disabled = false; btn.textContent = 'Check Details'; }
}

// Result right under the buttons, so the count is visible without scrolling to Summary.
function seResult(d) {
  const filtered = d.enabled_count !== undefined && d.enabled_count !== d.user_count
    ? ` <span style="color:var(--muted);">(${seFmt(d.enabled_count)} enabled before filters)</span>` : '';
  document.getElementById('se-msg').innerHTML =
    `<div style="margin-top:4px;padding:12px 14px;border:1px solid rgba(62,207,142,0.35);background:rgba(62,207,142,0.07);border-radius:8px;line-height:1.6;">
       <div style="font-size:20px;font-weight:600;color:var(--success);">${seFmt(d.user_count)} users</div>
       <div style="font-size:12.5px;">${seEsc(d.pilot_name)} · ${seEsc(seSeason(document.getElementById('se-season').value))} · ${seEsc(d.env)} · ${seEsc(d.when.ist)}${filtered}</div>
       <div style="font-size:11.5px;color:var(--muted);font-family:var(--mono);margin-top:4px;">${seEsc(d.path)}${seEsc(d.script_name)} · ${seEsc(d.csv_files.join(', '))} · ${seEsc(d.jenkins_job)}</div>
       ${d.user_count === 0 ? '<div style="font-size:12px;color:var(--danger);margin-top:4px;">No users match. Check the pilot ID, environment and filters.</div>' : ''}
     </div>`;
}

async function seCreate() {
  if (!sePreviewed) return;
  const btn = document.getElementById('se-btn-create');
  const label = btn.textContent;
  btn.disabled = true; btn.textContent = 'Creating…';
  try {
    const { run } = await seCall('/api/seasonal/runs', { method: 'POST', body: seForm() });
    seMsg('ok', seConfig.modes.github
      ? `PR opened for ${run.script_name}. Merge it, then trigger the run on the right.`
      : `Files ready for ${run.script_name}. Download them from the run on the right, raise the Utils PR, then mark it merged.`);
    sePreviewed = false;
    seLoadRuns();
  } catch (e) { seMsg('bad', e.message); btn.disabled = false; }
  finally { btn.textContent = label; }
}

const SE_STEPS = [['PR_OPEN', 'PR'], ['READY', 'Merged'], ['QUEUED', 'Triggered'], ['RUNNING', 'Running'], ['SUCCESS', 'Scheduled']];
function seTrack(state) {
  const s = state === 'MANUAL' ? 'QUEUED' : state;
  const idx = SE_STEPS.findIndex(x => x[0] === s);
  const failed = ['FAILED', 'ERROR', 'CANCELLED'].includes(state);
  return `<div style="display:flex;flex-wrap:wrap;gap:4px;margin:8px 0;">` + SE_STEPS.map(([k, label], i) => {
    const done = s === 'SUCCESS' || (idx >= 0 && i < idx), now = i === idx && s !== 'SUCCESS';
    const style = done ? 'background:rgba(62,207,142,0.12);color:var(--success);border-color:rgba(62,207,142,0.3);'
               : now ? 'color:var(--accent2);border-color:var(--accent2);font-weight:600;' : 'color:var(--muted);';
    return `<span style="font-size:10.5px;font-family:var(--mono);padding:2px 8px;border:1px solid rgba(255,255,255,0.1);border-radius:4px;${style}">${label}</span>`;
  }).join('') + (failed ? `<span style="font-size:10.5px;font-family:var(--mono);padding:2px 8px;border:1px solid rgba(255,90,90,0.4);border-radius:4px;color:var(--danger);font-weight:600;">${state}</span>` : '') + `</div>`;
}

function seRunHTML(r) {
  const id = seEsc(r.run_id);
  const link = (url, text) => url && !url.startsWith('file:') ? `<a href="${seEsc(url)}" target="_blank" rel="noopener" style="color:var(--accent2);">${seEsc(text)}</a>` : '';
  let actions = '';
  if (r.state === 'PR_OPEN' && r.files_zip) {
    actions = `<div style="margin-top:6px;">
      <a href="/api/seasonal/runs/${encodeURIComponent(r.run_id)}/files" style="color:var(--accent2);">Download ${seEsc(r.script_name)} + user file</a>
      <div style="display:flex;gap:6px;margin-top:8px;flex-wrap:wrap;">
        <input class="form-input" data-pr placeholder="PR link (optional)" style="flex:1;min-width:160px;font-family:var(--mono);font-size:11.5px;">
        <button class="btn btn-outline btn-sm" data-act="merged">Mark PR Merged</button></div></div>`;
  } else if (r.state === 'READY') {
    actions = `<div style="display:flex;gap:6px;margin-top:6px;flex-wrap:wrap;">
      <input class="form-input" data-ip placeholder="DevTools IP (blank = auto)" style="flex:1;min-width:160px;font-family:var(--mono);font-size:11.5px;">
      <button class="btn se-btn-primary btn-sm" data-act="trigger">${seConfig && seConfig.modes.jenkins ? 'Trigger Jenkins' : 'Get Jenkins Parameters'}</button></div>`;
  } else if (r.state === 'MANUAL' && r.manual_params) {
    const p = r.manual_params;
    actions = `<div style="margin-top:6px;">Open <a href="${seEsc(p.job_url)}" target="_blank" rel="noopener" style="color:var(--accent2);">${seEsc(r.jenkins_job)}</a> → Build with Parameters:
      <div style="font-family:var(--mono);font-size:11.5px;margin:6px 0;line-height:1.9;">` +
      ['FOLDER', 'SCRIPT', 'API_TOKEN', 'IP', 'JIRA_REQUEST_ID'].map(k =>
        `<div><span style="color:var(--muted);display:inline-block;width:130px;">${k}</span>${seEsc(p[k])}${k === 'API_TOKEN' ? '' : ` <button class="btn btn-outline btn-sm" data-copy="${seEsc(p[k])}" style="padding:0 6px;font-size:10px;">Copy</button>`}</div>`).join('') +
      `</div><div style="display:flex;gap:6px;flex-wrap:wrap;">
        <input class="form-input" data-build placeholder="Jenkins build link" style="flex:1;min-width:160px;font-family:var(--mono);font-size:11.5px;">
        <button class="btn se-btn-primary btn-sm" data-act="mark-ok">Mark Scheduled</button>
        <button class="btn btn-outline btn-sm" data-act="mark-fail">Mark Failed</button></div></div>`;
  }
  return `<div class="se-run" data-id="${id}" style="padding:12px 0;border-top:1px solid rgba(255,255,255,0.06);color:inherit;">
    <div style="display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;">
      <div><b>${seEsc(r.pilot_name)}</b> · ${seEsc(seSeason(r.event_type))} · ${seFmt(r.user_count)} users · ${seEsc(r.env)}</div>
      <div style="font-family:var(--mono);font-size:11px;color:var(--muted);">${seEsc(r.ticket)}</div>
    </div>
    <div style="font-size:11px;color:var(--muted);font-family:var(--mono);margin-top:2px;">${seEsc(r.send_date)} ${seEsc(r.send_time)} IST · ${seEsc(r.folder)}/${seEsc(r.script_name)} · ${seEsc(r.created_by)}</div>
    ${seTrack(r.state)}
    ${r.error ? `<div style="font-size:11.5px;color:var(--danger);">${seEsc(r.error)}</div>` : ''}
    ${actions}
    <div style="display:flex;gap:10px;align-items:center;margin-top:8px;font-size:11.5px;flex-wrap:wrap;">
      ${link(r.pr_url, r.pr_number ? `PR #${r.pr_number}` : 'PR')}
      ${link(r.jenkins_build_url, r.jenkins_build_number ? `Build #${r.jenkins_build_number}` : 'Build')}
      ${r.target_ip ? `<span style="color:var(--muted);font-family:var(--mono);">${seEsc(r.target_ip)}</span>` : ''}
      ${['PR_OPEN', 'QUEUED', 'RUNNING'].includes(r.state) && !r.files_zip ? `<button class="btn btn-outline btn-sm" data-act="refresh">Check Status</button>` : ''}
      ${r.jenkins_build_url && seConfig && seConfig.modes.jenkins ? `<button class="btn btn-outline btn-sm" data-act="console">Console</button>` : ''}
      <button class="btn btn-outline btn-sm" data-act="audit" aria-expanded="false">Audit</button>
    </div>
    <div data-audit style="display:none;margin-top:8px;">${seAuditHTML(r)}</div>
    <div data-out style="font-size:11.5px;color:var(--danger);margin-top:6px;"></div>
  </div>`;
}

// Who did what and when, for one run (from the run's own history on the server).
function seAuditHTML(r) {
  const when = (iso) => new Date(iso).toLocaleString('en-GB', { day: '2-digit', month: 'short', year: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit' });
  const facts = [
    ['Run ID', r.run_id], ['Created', `${when(r.created_at)} by ${r.created_by}`],
    r.triggered_at ? ['Triggered', `${when(r.triggered_at)} by ${r.triggered_by}`] : null,
    r.finished_at ? ['Finished', when(r.finished_at)] : null,
    ['Pilot', `${r.pilot_name} (${r.pilot_id})`], ['Environment', r.env], ['Event', r.event_type],
    ['Send time', `${r.send_date} ${r.send_time} IST (epoch ${r.epoch})`],
    ['Users', `${seFmt(r.user_count)} from ${r.user_source === 'starburst' ? 'Starburst' : 'uploaded list'}`],
    ['Skip subscription check', r.skip_subscription_check ? 'on' : 'off'],
    ['Files', `${seConfig ? seConfig.scripts_dir : 'prod-one-time-scripts'}/${r.folder}/${r.script_name} + ${r.csv_files.join(', ')}`],
    r.target_ip ? ['DevTools server', `${r.target_ip} (${r.target_tag})`] : null,
    r.jenkins_build_url ? ['Jenkins build', r.jenkins_build_url] : null,
    ['Ticket', r.ticket],
  ].filter(Boolean);
  const steps = (r.history || []).slice().reverse().map(h =>
    `<div style="display:flex;gap:10px;padding:3px 0;"><span style="color:var(--muted);white-space:nowrap;">${seEsc(when(h.at))}</span><span>${seEsc(h.msg)}</span></div>`).join('');
  return `<div style="border:1px solid rgba(255,255,255,0.08);border-radius:8px;padding:10px 12px;font-size:11.5px;">
    <div style="font-family:var(--mono);line-height:1.8;">${facts.map(([k, v]) =>
      `<div><span style="color:var(--muted);display:inline-block;min-width:170px;">${seEsc(k)}</span>${seEsc(v)}</div>`).join('')}</div>
    <div style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.06);">
      <div style="color:var(--muted);text-transform:uppercase;letter-spacing:.04em;font-size:10.5px;margin-bottom:4px;">History (newest first)</div>
      ${steps || '<span style="color:var(--muted);">No history recorded.</span>'}
    </div></div>`;
}

const seOpenAudits = new Set();   // keep audits open across the 30-second refresh

function seUpdateStats(runs) {
  document.getElementById('se-stat-runs').textContent = runs.length;
  if (!runs.length) return;
  const r = runs[0];
  document.getElementById('se-stat-lastrun').textContent = new Date(r.created_at).toLocaleDateString('en-GB', { day: '2-digit', month: 'short' });
  document.getElementById('se-stat-lastrun-sub').textContent = `${r.pilot_name} · ${seSeason(r.event_type)}`;
  const labels = { PR_OPEN: 'PR Open', READY: 'Merged', QUEUED: 'Triggered', MANUAL: 'In Jenkins', RUNNING: 'Running', SUCCESS: 'Scheduled', FAILED: 'Failed', ERROR: 'Error', CANCELLED: 'Cancelled' };
  const st = document.getElementById('se-stat-status');
  st.textContent = labels[r.state] || r.state;
  st.style.color = r.state === 'SUCCESS' ? 'var(--success)' : ['FAILED', 'ERROR'].includes(r.state) ? 'var(--danger)' : '';
  document.getElementById('se-stat-status-sub').textContent = `${r.send_date} ${r.send_time} IST`;
}

async function seLoadRuns() {
  const box = document.getElementById('se-runs');
  try {
    const { runs } = await seCall('/api/seasonal/runs');
    seUpdateStats(runs);
    box.innerHTML = runs.length ? runs.map(seRunHTML).join('') : 'No runs yet.';
    seOpenAudits.forEach(id => {
      const el = box.querySelector(`.se-run[data-id="${CSS.escape(id)}"]`);
      if (!el) return;
      el.querySelector('[data-audit]').style.display = 'block';
      const b = el.querySelector('button[data-act="audit"]');
      b.textContent = 'Hide Audit'; b.setAttribute('aria-expanded', 'true');
    });
  } catch (e) { box.innerHTML = `<span style="color:var(--danger);">${seEsc(e.message)}</span>`; }
}

document.addEventListener('click', async (ev) => {
  const copy = ev.target.closest('#se-runs button[data-copy]');
  if (copy) { navigator.clipboard?.writeText(copy.dataset.copy); copy.textContent = 'Copied'; setTimeout(() => { copy.textContent = 'Copy'; }, 1500); return; }
  const btn = ev.target.closest('#se-runs button[data-act]');
  if (!btn) return;
  const el = btn.closest('.se-run'), id = encodeURIComponent(el.dataset.id), out = el.querySelector('[data-out]');
  const json = (body) => ({ method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const val = (sel) => (el.querySelector(sel)?.value || '').trim() || null;
  if (btn.dataset.act === 'audit') {
    const box = el.querySelector('[data-audit]'), open = box.style.display === 'none';
    box.style.display = open ? 'block' : 'none';
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    btn.textContent = open ? 'Hide Audit' : 'Audit';
    open ? seOpenAudits.add(el.dataset.id) : seOpenAudits.delete(el.dataset.id);
    return;
  }
  btn.disabled = true; out.textContent = '';
  try {
    const act = btn.dataset.act;
    if (act === 'refresh') await seCall(`/api/seasonal/runs/${id}/refresh`, { method: 'POST' });
    if (act === 'merged') await seCall(`/api/seasonal/runs/${id}/pr-merged`, json({ pr_url: val('[data-pr]') }));
    if (act === 'mark-ok' || act === 'mark-fail')
      await seCall(`/api/seasonal/runs/${id}/mark`, json({ result: act === 'mark-ok' ? 'SUCCESS' : 'FAILED', build_url: val('[data-build]') }));
    if (act === 'trigger') {
      const typed = val('[data-ip]');
      const t = await seCall(`/api/seasonal/runs/${id}/target`);
      if (t.error && !typed) throw new Error(t.error);
      const ip = typed || t.ips[0] || 'none found';
      if (seConfig.modes.jenkins && !confirm(`Run ${t.job} on DevTools server ${ip} (${t.tag})?\n\nThis schedules real customer emails.`)) return;
      await seCall(`/api/seasonal/runs/${id}/trigger`, json({ ip_override: typed }));
    }
    if (act === 'console') {
      const c = await seCall(`/api/seasonal/runs/${id}/console`);
      document.getElementById('se-console').textContent = c.console || 'No output yet.';
      return;
    }
    await seLoadRuns();
  } catch (e) { out.textContent = e.message; }
  finally { btn.disabled = false; }
});

// keyboard access for the env-btn style choices and the toggle
document.addEventListener('keydown', (ev) => {
  if ((ev.key === 'Enter' || ev.key === ' ') && ev.target.closest('#page-seasonal [role=button], #page-seasonal [role=switch]')) {
    ev.preventDefault(); ev.target.click();
  }
});

setInterval(() => { if (document.getElementById('page-seasonal')?.classList.contains('active')) seLoadRuns(); }, 30000);
