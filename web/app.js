// EOD Market Data -- browser page. Talks to eod_web.py; every /api call carries the per-start token.
"use strict";

const TOKEN = document.querySelector('meta[name="eod-token"]').content;
const $ = (id) => document.getElementById(id);

let S = null;              // the form (same keys as gui_settings.json)
let INFO = null;           // static info from /api/state
let matches = [];          // [[symbol, name, exchange, label], ...] aligned with #matches options
let loaded = 0;            // tickers loaded for the checked markets
let running = false, refreshing = false, cancelling = false, stopped = false;
let logSeq = 0, msgSeq = 0, tickersVersion = 0;
let searchSerial = 0;

// ------------------------------------------------------------------------------------------------ api --
async function api(path, body) {
  const opts = { headers: { "X-Token": TOKEN } };
  if (body !== undefined) {
    opts.method = "POST";
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const r = await fetch(path, opts);
  const js = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(js.error || `${r.status} ${r.statusText}`);
  return js;
}

// ------------------------------------------------------------------------------------------- messages --
const msgQueue = [];
let msgOpen = false;

function showMessage(kind, title, message, buttons) {
  return new Promise((resolve) => {
    msgQueue.push({ kind, title, message, buttons: buttons || [["OK", true]], resolve });
    pumpMessages();
  });
}

function pumpMessages() {
  if (msgOpen || !msgQueue.length) return;
  const m = msgQueue.shift();
  msgOpen = true;
  const dlg = $("msg-dialog");
  dlg.dataset.kind = m.kind === "info" && /complete|loaded/i.test(m.title) ? "success" : m.kind;
  $("msg-title").textContent = m.title;
  $("msg-text").textContent = m.message;
  const box = $("msg-buttons");
  box.replaceChildren();
  m.buttons.forEach(([label, value], i) => {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = label;
    b.className = i === m.buttons.length - 1 ? (m.kind === "error" ? "red" : "blue") : "subtle";
    b.onclick = () => { dlg.close(); finish(value); };
    box.append(b);
  });
  const finish = (value) => { msgOpen = false; m.resolve(value); setTimeout(pumpMessages, 0); };
  dlg.oncancel = (e) => { e.preventDefault(); dlg.close(); finish(m.buttons.length > 1 ? false : true); };
  dlg.showModal();
  box.lastChild.focus();
}

const showResult = (r) => (r && r.title ? showMessage(r.kind, r.title, r.message) : Promise.resolve());

// ------------------------------------------------------------------------------------------- settings --
let saveTimer = null;
function saveSoon() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => api("/api/settings", S).catch(() => {}), 700);
}

function checkedMarkets() {
  return INFO.exchanges.filter((ex) => S.markets.includes(ex));
}

// ------------------------------------------------------------------------------------------------ UI --
function buildStatic() {
  const mk = $("markets");
  for (const ex of INFO.exchanges) {
    const lab = document.createElement("label");
    lab.className = "chip";
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = ex;
    cb.onchange = () => {
      S.markets = INFO.exchanges.filter((e) => mk.querySelector(`input[value="${e}"]`).checked);
      saveSoon();
      doSearch();
    };
    lab.append(cb, ex);
    mk.append(lab);
  }
  const pr = $("presets");
  for (const [label, start] of INFO.presets) {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = label;
    b.dataset.preset = label;
    b.onclick = () => {
      S.start = start; S.end = INFO.today; S.date_preset = label;
      $("start").value = start; $("end").value = INFO.today;
      showPreset();
      saveSoon();
    };
    pr.append(b);
  }
  const lay = $("layouts");
  for (const [text, val] of INFO.layouts) {
    const lab = document.createElement("label");
    const rb = document.createElement("input");
    rb.type = "radio";
    rb.name = "layout";
    rb.value = val;
    rb.onchange = () => { S.layout = val; saveSoon(); };
    const span = document.createElement("span");
    span.textContent = text;
    lab.append(rb, span);
    lay.append(lab);
  }
  $("config-hint").textContent = `Default folders come from ${INFO.config_file} (if present).`;
}

function applySettingsToForm() {
  for (const cb of $("markets").querySelectorAll("input")) cb.checked = S.markets.includes(cb.value);
  for (const k of ["all", "metastock_ascii", "bhavcopy", "bse_bhavcopy", "bse_adjust", "us_source_alpaca",
                   "nse_source_official"]) $(k).checked = !!S[k];
  $("start").value = S.start;
  $("end").value = S.end;
  showPreset();
  for (const rb of document.querySelectorAll('input[name="layout"]')) rb.checked = rb.value === S.layout;
  for (const rb of document.querySelectorAll('input[name="interval"]')) rb.checked = rb.value === S.interval;
  showOutputField();
  renderSelected();
  updateEnabled();
}

// the active quick range is highlighted; dates picked by hand clear it
function showPreset() {
  for (const b of $("presets").querySelectorAll("button")) {
    const on = b.dataset.preset === S.date_preset;
    b.classList.toggle("active", on);
    b.setAttribute("aria-pressed", on ? "true" : "false");
  }
}

function outputKey() { return S.interval === "hourly" ? "output_hourly" : "output"; }
function showOutputField() { $("output").value = S[outputKey()] || ""; }

function updateEnabled() {
  const hourly = S.interval === "hourly";
  $("interval-note").textContent = hourly ? INFO.hourly_note : "";
  for (const rb of document.querySelectorAll('input[name="layout"]')) rb.disabled = hourly || running;
  for (const id of ["bhavcopy", "us_source_alpaca", "nse_source_official"]) $(id).disabled = hourly || running;
  const tickerOff = running || S.all;
  for (const id of ["search", "search-btn", "matches", "selected", "add", "remove", "clear"]) $(id).disabled = tickerOff;
  $("run").disabled = running;
  $("run-label").textContent = running ? "Downloading..." : "Download";
  const pill = $("run-pill");
  pill.className = "pill " + (running ? "running" : refreshing ? "busy" : "idle");
  $("run-pill-text").textContent = running ? (cancelling ? "Cancelling" : "Downloading") :
    refreshing ? "Refreshing tickers" : "Ready";
  $("picker").classList.toggle("off", tickerOff);
  $("cancel").disabled = !running || cancelling;
  $("cancel").textContent = cancelling ? "Cancelling..." : "Cancel";
  $("refresh").disabled = running || refreshing;
  $("refresh").textContent = refreshing ? "Refreshing..." : "Refresh ticker list";
}

function updateTickerStatus() {
  const parts = [loaded ? `${loaded.toLocaleString()} tickers loaded`
                        : "No tickers loaded yet -- click 'Refresh ticker list' to search"];
  const n = Object.keys(S.selected).length;
  if (n) parts.push(`${n} selected`);
  $("ticker-status").textContent = parts.join(" | ");
}

async function doSearch() {
  const serial = ++searchSerial;
  const q = new URLSearchParams({ q: $("search").value, markets: checkedMarkets().join(",") });
  try {
    const r = await api(`/api/search?${q}`);
    if (serial !== searchSerial) return;          // a newer keystroke already asked again
    matches = r.matches;
    loaded = r.loaded;
    const sel = $("matches");
    sel.replaceChildren(...matches.map((m, i) => new Option(m[3], String(i))));
    updateTickerStatus();
  } catch (e) { /* server gone: the poll loop reports it */ }
}

function formatEntry(sym, name, ex) {
  const shown = ex ? `${ex}.${sym}` : sym;
  name = (name || "").trim();
  return name ? `${shown} — ${name}` : shown;
}

function renderSelected() {
  const sel = $("selected");
  sel.replaceChildren(...Object.entries(S.selected).map(([sym, [name, ex]]) => {
    const o = new Option(formatEntry(sym, name, ex));
    o.dataset.symbol = sym;
    return o;
  }));
  updateTickerStatus();
}

function selectTicker(sym, name, ex) { S.selected[sym] = [name || "", ex || null]; }

function addFromMatches(indices) {
  if (!indices.length) return;
  for (const i of indices) selectTicker(...matches[i]);
  renderSelected();
  saveSoon();
}

async function onSearchEnter() {
  const typed = $("search").value.trim().toUpperCase();
  if (!typed) return;
  await doSearch();
  const exact = matches.find((m) => m[0] === typed);
  if (exact) selectTicker(...exact);
  else if (matches.length === 1) selectTicker(...matches[0]);
  else if (matches.length) {
    await showMessage("info", "Multiple matches",
      `${matches.length} matches for '${typed}' -- select the one(s) you want from the list on the left and ` +
      "click Add ▸ (or double-click one).");
    return;
  } else selectTicker(typed, "", null);   // not in the loaded lists (e.g. a brand-new IPO), exchange unknown
  $("search").value = "";
  await doSearch();
  renderSelected();
  saveSoon();
}

async function refreshTickers() {
  const r = await api("/api/refresh-tickers", { markets: checkedMarkets() });
  if (!r.ok) return showResult(r);
  refreshing = true;
  updateEnabled();
}

// ------------------------------------------------------------------------------------------- folders --
let dirState = { path: "", selected: null };

async function listDir(path) {
  const r = await api(`/api/listdir?${new URLSearchParams({ path })}`);
  dirState = { path: r.path, parent: r.parent, selected: null };
  $("dir-path").value = r.path;
  $("dir-up").disabled = !r.path;
  const roots = $("dir-roots");
  roots.replaceChildren(...[...r.roots, r.home].map((p) => {
    const b = document.createElement("button");
    b.type = "button";
    b.textContent = p === r.home ? "Home" : p;
    b.onclick = () => listDir(p);
    return b;
  }));
  const ul = $("dir-list");
  ul.replaceChildren(...r.dirs.map((d) => {
    const li = document.createElement("li");
    const full = r.path ? joinPath(r.path, d) : d;
    li.textContent = "📁 " + d;
    li.onclick = () => {
      ul.querySelectorAll("li.sel").forEach((x) => x.classList.remove("sel"));
      li.classList.add("sel");
      dirState.selected = full;
      $("dir-path").value = full;
    };
    li.ondblclick = () => listDir(full);
    return li;
  }));
}

function joinPath(base, name) {
  const sep = base.includes("\\") || /^[A-Za-z]:/.test(base) ? "\\" : "/";
  return base.endsWith(sep) ? base + name : base + sep + name;
}

async function browse() {
  const dlg = $("dir-dialog");
  const current = $("output").value.trim();
  await listDir(current);
  if (current) $("dir-path").value = current;    // keep a not-yet-existing folder as typed
  dlg.showModal();
}

// ------------------------------------------------------------------------------------------------ run --
async function startRun() {
  S[outputKey()] = $("output").value.trim();
  S.start = $("start").value;
  S.end = $("end").value;
  const r = await api("/api/run", S);
  if (!r.ok) return showResult(r);
  running = true;
  cancelling = false;
  $("progress").value = 0;
  $("status").textContent = "Starting...";
  updateEnabled();
}

async function cancelRun() {
  cancelling = true;
  updateEnabled();
  await api("/api/cancel", {});
}

// ----------------------------------------------------------------------------------------------- poll --
const LOG_MAX_LINES = 4000;

function logClass(line) {
  if (/\[(ERROR|CRITICAL)\]/.test(line)) return "l-err";
  if (/\[WARNING\]/.test(line)) return "l-warn";
  if (/(^|] )---/.test(line)) return "l-run";
  if (/\b(All done|Done --|Done\.)/.test(line)) return "l-done";
  return "";
}

function appendLog(lines) {
  if (!lines.length) return;
  const box = $("log");
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
  const frag = document.createDocumentFragment();
  for (const line of lines) {
    const div = document.createElement("div");
    div.textContent = line;
    const cls = logClass(line);
    if (cls) div.className = cls;
    frag.append(div);
  }
  box.append(frag);
  while (box.childElementCount > LOG_MAX_LINES) box.firstElementChild.remove();   // keep the page light
  if (atBottom) box.scrollTop = box.scrollHeight;
}

function applyPoll(p) {
  appendLog(p.log);
  logSeq = p.log_seq;
  const wasRunning = running;
  running = p.running;
  refreshing = p.refreshing;
  if (!running) cancelling = false;
  $("status").textContent = p.status;
  const pg = p.progress;
  $("progress").max = Math.max(pg.total, 1);
  $("progress").value = running || wasRunning ? pg.done : $("progress").value;
  updateEnabled();
  if (p.tickers_version !== tickersVersion) {
    tickersVersion = p.tickers_version;
    doSearch();
  }
  for (const m of p.messages) if (m.id > msgSeq) showMessage(m.kind, m.title, m.message);
  msgSeq = p.msg_seq;
}

async function pollLoop() {
  if (stopped) return;
  try {
    applyPoll(await api(`/api/poll?log=${logSeq}&msg=${msgSeq}`));
    setTimeout(pollLoop, running || refreshing ? 500 : 1500);
  } catch (e) {
    $("status").textContent = "Lost contact with the app -- is its console window still open? Retrying...";
    setTimeout(pollLoop, 3000);
  }
}

// ------------------------------------------------------------------------------------------- wiring --
function wire() {
  for (const k of ["metastock_ascii", "bhavcopy", "bse_bhavcopy", "bse_adjust", "us_source_alpaca",
                   "nse_source_official"]) $(k).onchange = () => { S[k] = $(k).checked; saveSoon(); };
  $("all").onchange = () => { S.all = $("all").checked; updateEnabled(); doSearch(); saveSoon(); };
  for (const rb of document.querySelectorAll('input[name="interval"]')) {
    rb.onchange = () => {
      S[outputKey()] = $("output").value.trim() || S[outputKey()];   // remember the field for the old interval
      S.interval = rb.value;
      showOutputField();
      updateEnabled();
      saveSoon();
    };
  }
  $("start").onchange = () => { S.start = $("start").value; S.date_preset = ""; showPreset(); saveSoon(); };
  $("end").onchange = () => { S.end = $("end").value; S.date_preset = ""; showPreset(); saveSoon(); };
  $("output").oninput = () => { S[outputKey()] = $("output").value.trim(); saveSoon(); };

  let searchTimer = null;
  $("search").oninput = () => { clearTimeout(searchTimer); searchTimer = setTimeout(doSearch, 120); };
  $("search").onkeydown = (e) => { if (e.key === "Enter") { e.preventDefault(); onSearchEnter(); } };
  $("search-btn").onclick = () => {
    if (!loaded) {
      if (!checkedMarkets().length) {
        return showMessage("warning", "No market selected",
          "Please select at least one market (NASDAQ, NYSE, BSE, NSE) first, then search again.");
      }
      return refreshTickers();
    }
    doSearch();
  };
  $("add").onclick = () => addFromMatches([...$("matches").selectedOptions].map((o) => +o.value));
  $("matches").ondblclick = (e) => { if (e.target.tagName === "OPTION") addFromMatches([+e.target.value]); };
  const removeOptions = (opts) => {
    for (const o of opts) delete S.selected[o.dataset.symbol];
    renderSelected();
    saveSoon();
  };
  $("remove").onclick = () => removeOptions([...$("selected").selectedOptions]);
  $("selected").ondblclick = (e) => { if (e.target.tagName === "OPTION") removeOptions([e.target]); };
  $("clear").onclick = () => { S.selected = {}; renderSelected(); saveSoon(); };

  $("refresh").onclick = refreshTickers;
  $("load-file").onclick = () => {
    if (checkedMarkets().length !== 1) {
      return showMessage("warning", "Pick one market first",
        "Check exactly one market checkbox above (the one this file's tickers belong to -- e.g. BSE), " +
        "then click 'Load tickers from file...' again.");
    }
    $("file-input").value = "";
    $("file-input").click();
  };
  $("file-input").onchange = async () => {
    const f = $("file-input").files[0];
    if (!f) return;
    const buf = new Uint8Array(await f.arrayBuffer());
    let bin = "";
    for (let i = 0; i < buf.length; i += 0x8000) bin += String.fromCharCode(...buf.subarray(i, i + 0x8000));
    const r = await api("/api/load-tickers", { exchange: checkedMarkets()[0], filename: f.name, data: btoa(bin) });
    if (r.ok) await doSearch();
    showResult(r);
  };

  $("browse").onclick = browse;
  $("dir-go").onclick = () => listDir($("dir-path").value.trim());
  $("dir-path").onkeydown = (e) => { if (e.key === "Enter") { e.preventDefault(); listDir($("dir-path").value.trim()); } };
  $("dir-up").onclick = () => listDir(dirState.parent || "");
  $("dir-cancel").onclick = () => $("dir-dialog").close();
  $("dir-ok").onclick = () => {
    const chosen = $("dir-path").value.trim();
    if (chosen) {
      $("output").value = chosen;
      S[outputKey()] = chosen;
      saveSoon();
    }
    $("dir-dialog").close();
  };
  $("open-folder").onclick = async () => showResult(await api("/api/open-folder", { path: $("output").value }));

  $("run").onclick = startRun;
  $("cancel").onclick = cancelRun;
  $("quit").onclick = async () => {
    if (running) {
      const ok = await showMessage("warning", "Download in progress",
        "A download is still running. Quit anyway?\n\n(Whatever's already been downloaded stays cached, so you " +
        "can resume later by running again with the same settings.)", [["No", false], ["Yes", true]]);
      if (!ok) return;
    }
    clearTimeout(saveTimer);
    await api("/api/settings", S).catch(() => {});
    await api("/api/shutdown", {}).catch(() => {});
    stopped = true;
    document.body.innerHTML = '<main><fieldset><legend>Stopped</legend>' +
      "<p>The app has stopped. You can close this tab. Start EOD_Market_Data again to come back.</p></fieldset></main>";
  };
  window.addEventListener("beforeunload", (e) => {
    if (running && !stopped) { e.preventDefault(); e.returnValue = ""; }
  });
}

// ----------------------------------------------------------------------------------------------- start --
(async function init() {
  try {
    INFO = await api("/api/state");
  } catch (e) {
    document.body.textContent = "Couldn't reach the app: " + e.message + " -- reload the page, or start EOD_Market_Data again.";
    return;
  }
  S = INFO.settings;
  buildStatic();
  wire();
  applySettingsToForm();
  applyPoll({ ...INFO, messages: [] });     // log so far, running state; old dialogs aren't replayed
  msgSeq = INFO.msg_seq;
  tickersVersion = INFO.tickers_version;
  await doSearch();
  pollLoop();
})();
