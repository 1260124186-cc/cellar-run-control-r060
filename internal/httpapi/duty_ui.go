package httpapi

import (
	"net/http"
)

// dutyRoomPage is a self-contained local page. It has no external scripts,
// styles, fonts, or network calls beyond the service's own JSON API, which
// matches the on-machine-only notification scope.
const dutyRoomPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cellar Duty Room - Outbox</title>
<style>
  :root {
    --bg:#1b1d1a; --panel:#252824; --ink:#e9e7df; --muted:#a8a79d;
    --line:#3a3e38; --accent:#9db17c; --warn:#d08a5f; --danger:#c97064; --ok:#7fae87;
  }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--ink);
         font:14px/1.45 -apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; }
  header { padding:14px 20px; border-bottom:1px solid var(--line);
           display:flex; flex-wrap:wrap; gap:12px; align-items:center; }
  header h1 { font-size:16px; margin:0; letter-spacing:.04em; }
  header .hint { color:var(--muted); font-size:12px; }
  main { padding:18px 20px 60px; max-width:1180px; margin:0 auto; }
  .bar { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:14px; align-items:end; }
  label { display:flex; flex-direction:column; gap:4px; font-size:12px; color:var(--muted); }
  input, select, button, textarea {
    background:var(--panel); color:var(--ink); border:1px solid var(--line);
    border-radius:6px; padding:7px 9px; font:inherit;
  }
  button { cursor:pointer; }
  button.primary { background:var(--accent); color:#1b1d1a; border-color:var(--accent); font-weight:600; }
  button.warn { border-color:var(--warn); color:var(--warn); }
  button.danger { border-color:var(--danger); color:var(--danger); }
  button:disabled { opacity:.45; cursor:not-allowed; }
  table { width:100%; border-collapse:collapse; background:var(--panel);
          border:1px solid var(--line); border-radius:8px; overflow:hidden; }
  th, td { text-align:left; padding:9px 10px; border-bottom:1px solid var(--line);
           vertical-align:top; font-size:13px; }
  th { color:var(--muted); font-weight:600; font-size:11px; text-transform:uppercase;
       letter-spacing:.05em; }
  tr:last-child td { border-bottom:none; }
  .pill { display:inline-block; padding:1px 8px; border-radius:999px; font-size:11px;
          border:1px solid var(--line); }
  .pill.pending { color:var(--warn); border-color:var(--warn); }
  .pill.claimed { color:#c9b26a; border-color:#c9b26a; }
  .pill.handled { color:var(--ok); border-color:var(--ok); }
  .subject { font-weight:600; }
  .detail { color:var(--muted); margin-top:3px; }
  .meta { color:var(--muted); font-size:12px; margin-top:5px; }
  .actions { display:flex; gap:6px; flex-wrap:wrap; }
  .tokenbox { margin-top:8px; }
  .tokenbox input { width:100%; font-family:ui-monospace,monospace; font-size:12px; }
  .empty { padding:26px; text-align:center; color:var(--muted); }
  #status { margin-left:auto; font-size:12px; color:var(--muted); }
  code { font-family:ui-monospace,monospace; }
  .toast { position:fixed; right:18px; bottom:18px; background:var(--panel);
           border:1px solid var(--line); padding:10px 14px; border-radius:8px;
           max-width:380px; opacity:0; transform:translateY(6px); transition:.2s; }
  .toast.show { opacity:1; transform:none; }
  .toast.error { border-color:var(--danger); }
</style>
</head>
<body>
<header>
  <h1>CELLAR DUTY ROOM · LOCAL OUTBOX</h1>
  <span class="hint">on-machine only · no SMS or external push</span>
  <div id="status"></div>
</header>
<main>
  <div class="bar">
    <label>Duty terminal
      <input id="terminal" placeholder="e.g. operator-desk-1" size="22">
    </label>
    <label>State
      <select id="filterState">
        <option value="">all</option>
        <option value="pending" selected>pending</option>
        <option value="claimed">claimed</option>
        <option value="handled">handled</option>
      </select>
    </label>
    <label>Recipient
      <select id="filterRecipient">
        <option value="">all</option>
        <option value="duty-planner">duty-planner</option>
        <option value="duty-operator">duty-operator</option>
        <option value="duty-observer">duty-observer</option>
      </select>
    </label>
    <label>Claim queue
      <select id="claimRecipient">
        <option value="">any recipient</option>
        <option value="duty-planner">duty-planner</option>
        <option value="duty-operator">duty-operator</option>
        <option value="duty-observer">duty-observer</option>
      </select>
    </label>
    <button class="primary" id="claimNext">Claim next</button>
    <button id="refresh">Refresh</button>
    <label style="flex-direction:row;align-items:center;gap:6px">
      <input type="checkbox" id="auto" checked> auto 5s
    </label>
  </div>
  <table>
    <thead>
      <tr>
        <th style="width:60px">#</th>
        <th style="width:150px">Recipient / type</th>
        <th>Notification</th>
        <th style="width:170px">Associated run</th>
        <th style="width:120px">State</th>
        <th style="width:230px">Actions</th>
      </tr>
    </thead>
    <tbody id="rows"></tbody>
  </table>
</main>
<div class="toast" id="toast"></div>

<script>
const $ = (id) => document.getElementById(id);
const tokens = new Map();

function restoreTerminal() {
  const saved = localStorage.getItem('cellar.terminal');
  if (saved) $('terminal').value = saved;
}
function terminal() {
  const value = $('terminal').value.trim();
  if (value) localStorage.setItem('cellar.terminal', value);
  return value;
}
function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (c) => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function toast(message, isError) {
  const node = $('toast');
  node.textContent = message;
  node.className = 'toast show' + (isError ? ' error' : '');
  clearTimeout(node.timer);
  node.timer = setTimeout(() => node.className = 'toast', 3500);
}
async function api(method, path, payload) {
  const options = { method, headers: {} };
  if (payload) {
    options.headers['Content-Type'] = 'application/json';
    options.body = JSON.stringify(payload);
  }
  const response = await fetch(path, options);
  const text = await response.text();
  const data = text ? JSON.parse(text) : {};
  if (!response.ok) {
    const detail = data.error ? data.error.code + ': ' + data.error.detail : response.statusText;
    throw new Error(detail);
  }
  return data;
}
async function refresh() {
  const params = new URLSearchParams();
  const state = $('filterState').value;
  const recipient = $('filterRecipient').value;
  if (state) params.set('state', state);
  if (recipient) params.set('recipient', recipient);
  $('status').textContent = 'updated ' + new Date().toLocaleTimeString();
  try {
    const data = await api('GET', '/v1/outbox/messages?' + params.toString());
    render(data.messages || []);
  } catch (err) {
    toast('cannot load messages: ' + err.message, true);
  }
}
function render(messages) {
  const rows = $('rows');
  if (!messages.length) {
    rows.innerHTML = '<tr><td colspan="6" class="empty">No messages match this view.</td></tr>';
    return;
  }
  rows.innerHTML = messages.map((m) => {
    const held = tokens.get(m.id);
    let actions;
    if (m.state === 'handled') {
      actions = '<span class="meta">handled ' + esc((m.handled_at||'').replace('T',' ').replace(/(\.\d+)?Z.*$/,'')) + '</span>';
    } else if (m.state === 'claimed' && !held) {
      actions = '<span class="meta">held by <code>' + esc(m.claimed_by) + '</code></span>'
        + '<div class="actions"><button class="warn" onclick="releaseOld(\''+m.id+'\')">Reclaim after lease</button></div>'
        + '<div class="meta">lease ends ' + esc((m.lease_expires_at||'').replace('T',' ').replace(/(\.\d+)?Z.*$/,'')) + '</div>';
    } else {
      actions = '<div class="actions">'
        + (m.state === 'pending'
            ? '<button class="primary" onclick="claimOne(\''+m.id+'\')">Claim</button>'
            : '')
        + (held
            ? '<button class="primary" onclick="completeOne(\''+m.id+'\')">Complete</button>'
              + '<button class="warn" onclick="releaseOne(\''+m.id+'\')">Release</button>'
            : '')
        + '</div>'
        + (held ? '<div class="tokenbox"><input readonly value="claim token: '+esc(held)+'"></div>'
                : '<div class="meta">attempts ' + m.attempts + '</div>');
    }
    return '<tr>'
      + '<td>' + m.sequence + '</td>'
      + '<td><div><strong>' + esc(m.recipient) + '</strong></div>'
         + '<div class="meta">' + esc(m.type) + '</div></td>'
      + '<td><div class="subject">' + esc(m.subject) + '</div>'
         + '<div class="detail">' + esc(m.detail) + '</div></td>'
      + '<td>' + (m.run_id ? '<code>'+esc(m.run_id)+'</code>' : '<span class="meta">-</span>') + '</td>'
      + '<td><span class="pill '+m.state+'">'+m.state+'</span></td>'
      + '<td>' + actions + '</td>'
      + '</tr>';
  }).join('');
}
async function claimNext() {
  const who = terminal();
  if (!who) return toast('set a duty terminal identifier first', true);
  try {
    const recipient = $('claimRecipient').value;
    const m = await api('POST', '/v1/outbox/claims', {terminal_id: who, recipient: recipient});
    tokens.set(m.id, m.claim_token);
    toast('claimed #' + m.sequence + ' — only this terminal can complete it');
    refresh();
  } catch (err) { toast(err.message, true); }
}
async function claimOne(id) {
  const who = terminal();
  if (!who) return toast('set a duty terminal identifier first', true);
  try {
    const m = await api('POST', '/v1/outbox/messages/'+id+'/claim', {terminal_id: who});
    tokens.set(id, m.claim_token);
    toast('claimed message ' + id);
    refresh();
  } catch (err) { toast(err.message, true); }
}
async function completeOne(id) {
  const token = tokens.get(id);
  try {
    await api('POST', '/v1/outbox/messages/'+id+'/complete',
      {terminal_id: terminal(), claim_token: token});
    tokens.delete(id);
    toast('message completed');
    refresh();
  } catch (err) { toast(err.message, true); }
}
async function releaseOne(id) {
  const token = tokens.get(id);
  try {
    await api('POST', '/v1/outbox/messages/'+id+'/release',
      {terminal_id: terminal(), claim_token: token});
    tokens.delete(id);
    toast('message released back to pending');
    refresh();
  } catch (err) { toast(err.message, true); }
}
async function releaseOld(id) {
  // Only succeeds once the lease has expired; the server replaces the stale
  // token, so the previous terminal cannot complete it anymore.
  await claimOne(id);
}
document.getElementById('claimNext').addEventListener('click', claimNext);
document.getElementById('refresh').addEventListener('click', refresh);
document.getElementById('filterState').addEventListener('change', refresh);
document.getElementById('filterRecipient').addEventListener('change', refresh);
let timer = setInterval(() => { if ($('auto').checked) refresh(); }, 5000);
restoreTerminal();
refresh();
</script>
</body>
</html>`

func registerDutyRoom(mux *http.ServeMux) {
	mux.HandleFunc("GET /duty", func(response http.ResponseWriter, request *http.Request) {
		http.Redirect(response, request, "/duty/", http.StatusSeeOther)
	})
	mux.HandleFunc("GET /duty/", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "text/html; charset=utf-8")
		response.Header().Set("Cache-Control", "no-store")
		_, _ = response.Write([]byte(dutyRoomPage))
	})
}
