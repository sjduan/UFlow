const DEFAULT_API_BASE = "http://127.0.0.1:18082";
const DEFAULT_MODEL_ID = "phasee04-monitor";

const state = {
  apiBase: DEFAULT_API_BASE,
  modelId: DEFAULT_MODEL_ID,
  pollMs: 2000,
  timer: null,
  lastOkAt: null,
};

const $ = (id) => document.getElementById(id);

function initialValue(name, fallback) {
  const url = new URL(window.location.href);
  const fromQuery = url.searchParams.get(name);
  if (fromQuery) {
    return fromQuery;
  }
  return window.localStorage.getItem(`uflow-monitor-${name}`) || fallback;
}

function formatBytes(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) {
    return "0 B";
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = n;
  let idx = 0;
  while (size >= 1024 && idx < units.length - 1) {
    size /= 1024;
    idx += 1;
  }
  return `${size >= 10 || idx === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[idx]}`;
}

function setText(id, value) {
  const node = $(id);
  if (node) {
    node.textContent = value;
  }
}

function setStatus(kind, label) {
  const pill = $("status-pill");
  pill.textContent = label;
  pill.classList.remove("online", "offline");
  if (kind) {
    pill.classList.add(kind);
  }
}

async function fetchJson(path) {
  const url = `${state.apiBase.replace(/\/$/, "")}${path}`;
  const response = await fetch(url, { cache: "no-store" });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || payload.detail || `HTTP ${response.status}`);
  }
  return payload;
}

async function postJson(path, body) {
  const url = `${state.apiBase.replace(/\/$/, "")}${path}`;
  const response = await fetch(url, {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || payload.response?.detail || payload.detail || `HTTP ${response.status}`);
  }
  return payload;
}

function renderConfig(configPayload) {
  const config = configPayload.config || {};
  if (config.api_base && $("api-base") !== document.activeElement) {
    $("api-base").value = state.apiBase;
  }
  if (config.default_model_id && $("model-id") !== document.activeElement) {
    $("model-id").value = state.modelId || config.default_model_id;
  }
  if (config.poll_interval_ms) {
    state.pollMs = Number(config.poll_interval_ms);
  }
  setText("tunnel-path", config.tunnel_path || "local -> host1 -> host2 container");
  setText("daemon-socket", config.daemon_socket || "-");
}

function renderHealth(payload) {
  setText("monitor-state", payload.monitor_api || "unknown");
  setText("daemon-state", payload.daemon_reachable ? "online" : "offline");
  setText("hbm-state", payload.hbm_available ? "available" : "unavailable");
  setText("health-detail", JSON.stringify(payload, null, 2));
}

function renderStats(payload) {
  const stats = payload.stats || {};
  setText("ddr-objects", String(stats.ddr_objects || 0));
  setText("ddr-requested", formatBytes(stats.ddr_requested_bytes));
  setText("ddr-committed", formatBytes(stats.ddr_committed_bytes));
  setText("ddr-safe", formatBytes(stats.ddr_safe_allocatable_bytes));
  setText("cgroup-current", formatBytes(stats.ddr_cgroup_current_bytes));
  if (stats.hbm_available === false) {
    setText("hbm-free", "unavailable");
  } else {
    setText("hbm-free", formatBytes(stats.hbm_free_bytes));
  }
}

function renderTrace(payload) {
  const trace = payload.trace || {};
  const enabled = trace.trace_enabled === true || trace.trace_enabled === "true";
  setText("trace-state", enabled ? `enabled:${trace.trace_mode || "unknown"}` : "disabled");
  setText("trace-events", `${trace.trace_buffered_events || 0} / drop ${trace.trace_dropped_events || 0}`);
}

function objectRow(object) {
  const path = object.ddr_path || "";
  return `
    <tr>
      <td>${object.object_id}</td>
      <td>${escapeHtml(object.name || "-")}</td>
      <td>${escapeHtml(object.role || "-")}</td>
      <td>${escapeHtml(object.placement || "-")}</td>
      <td>${escapeHtml(object.target || "-")}</td>
      <td>${formatBytes(object.requested_bytes)}</td>
      <td>${escapeHtml(object.state || "-")}</td>
      <td class="path-cell" title="${escapeHtml(path)}">${escapeHtml(path || "-")}</td>
    </tr>
  `;
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderObjects(payload) {
  const objects = payload.objects || [];
  setText("object-count", `${objects.length} objects`);
  const body = $("objects-body");
  if (!objects.length) {
    body.innerHTML = '<tr><td colspan="8" class="empty">No data yet</td></tr>';
    return;
  }
  body.innerHTML = objects.map(objectRow).join("");
}

function commandTemplate(name) {
  if (name === "create-ddr") {
    return {
      op: "CreateDataObject",
      model_id: state.modelId || DEFAULT_MODEL_ID,
      name: "ui.demo.ddr.1m",
      role: "user",
      hint: "mandatory:ddr",
      target: "host:0",
      nbytes: 1048576,
      shape: "1048576",
      dtype: "uint8",
    };
  }
  if (name === "release-object") {
    return {
      op: "ReleaseDataObject",
      object_id: 100,
    };
  }
  if (name === "shutdown") {
    return {
      op: "ShutdownDaemon",
    };
  }
  if (name === "start-trace") {
    return {
      op: "StartTrace",
      run_id: `ui-hot-${Date.now()}`,
      mode: "hot",
      categories: "control,transfer,channel,chunk,acl,backend",
      max_events: 200000,
      include_args: 1,
    };
  }
  if (name === "stop-trace") {
    return {
      op: "StopTrace",
      flush: 1,
    };
  }
  return {
    op: "GetStats",
  };
}

function setCommandInput(command) {
  $("command-input").value = JSON.stringify(command, null, 2);
}

function renderCommandResult(payload) {
  setText("command-result", JSON.stringify(payload, null, 2));
}

async function sendCommand() {
  const button = $("send-command");
  button.disabled = true;
  try {
    const raw = $("command-input").value.trim();
    if (!raw) {
      throw new Error("Command JSON is empty");
    }
    const command = JSON.parse(raw);
    const payload = await postJson("/v1/command", command);
    renderCommandResult(payload);
    await poll();
  } catch (error) {
    renderCommandResult({
      ok: false,
      error: error.message,
      api_base: state.apiBase,
    });
  } finally {
    button.disabled = false;
  }
}

async function poll() {
  try {
    const [config, health, stats, trace, objects] = await Promise.all([
      fetchJson("/v1/config"),
      fetchJson("/healthz"),
      fetchJson("/v1/stats"),
      fetchJson("/v1/trace/status"),
      fetchJson(`/v1/objects?model_id=${encodeURIComponent(state.modelId)}`),
    ]);
    renderConfig(config);
    renderHealth(health);
    renderStats(stats);
    renderTrace(trace);
    renderObjects(objects);
    state.lastOkAt = new Date();
    setText("last-updated", state.lastOkAt.toLocaleTimeString());
    setStatus("online", "online");
  } catch (error) {
    setStatus("offline", "offline");
    setText("daemon-state", "offline");
    const message = {
      ok: false,
      error: error.message,
      last_ok_at: state.lastOkAt ? state.lastOkAt.toISOString() : null,
      api_base: state.apiBase,
    };
    setText("health-detail", JSON.stringify(message, null, 2));
  } finally {
    window.clearTimeout(state.timer);
    state.timer = window.setTimeout(poll, state.pollMs);
  }
}

function applyConfig() {
  state.apiBase = $("api-base").value.trim() || DEFAULT_API_BASE;
  state.modelId = $("model-id").value.trim() || DEFAULT_MODEL_ID;
  window.localStorage.setItem("uflow-monitor-api", state.apiBase);
  window.localStorage.setItem("uflow-monitor-model_id", state.modelId);
  window.clearTimeout(state.timer);
  poll();
}

function init() {
  state.apiBase = initialValue("api", DEFAULT_API_BASE);
  state.modelId = initialValue("model_id", DEFAULT_MODEL_ID);
  $("api-base").value = state.apiBase;
  $("model-id").value = state.modelId;
  $("apply-config").addEventListener("click", applyConfig);
  setCommandInput(commandTemplate("get-stats"));
  $("send-command").addEventListener("click", sendCommand);
  document.querySelectorAll("[data-template]").forEach((button) => {
    button.addEventListener("click", () => {
      setCommandInput(commandTemplate(button.getAttribute("data-template")));
    });
  });
  poll();
}

init();
