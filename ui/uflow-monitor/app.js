const DEFAULT_API_BASE = "http://127.0.0.1:18082";
const DEFAULT_MODEL_ID = "phasee04-monitor";
const TRACE_ARTIFACTS = [
  "trace_events.json",
  "trace_summary.json",
  "trace_summary.csv",
  "transfer_summary.csv",
  "summary.md",
];

const state = {
  apiBase: DEFAULT_API_BASE,
  modelId: DEFAULT_MODEL_ID,
  pollMs: 2000,
  timer: null,
  lastOkAt: null,
  consecutiveFailures: 0,
  transfers: [],
  nextBeforeEventId: null,
  hasMore: false,
  selectedEventId: null,
  filters: {
    status: "",
    path: "",
    engine: "",
    direction: "",
    limit: 50,
  },
};

const $ = (id) => document.getElementById(id);

function initialValue(name, fallback) {
  const url = new URL(window.location.href);
  return url.searchParams.get(name) || window.localStorage.getItem(`uflow-monitor-${name}`) || fallback;
}

function isMissing(value) {
  return value === null || value === undefined || value === "";
}

function rawValue(value) {
  return isMissing(value) ? "N/A" : String(value);
}

function numberValue(value) {
  if (isMissing(value)) {
    return null;
  }
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function formatBytes(value) {
  const number = numberValue(value);
  if (number === null) {
    return "N/A";
  }
  if (number === 0) {
    return "0 B";
  }
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let size = Math.abs(number);
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  const sign = number < 0 ? "-" : "";
  return `${sign}${size >= 10 || index === 0 ? size.toFixed(0) : size.toFixed(2)} ${units[index]}`;
}

function formatMicros(value) {
  const number = numberValue(value);
  if (number === null) {
    return "N/A";
  }
  if (Math.abs(number) >= 1000) {
    return `${(number / 1000).toFixed(2)} ms`;
  }
  return `${number.toFixed(number >= 100 ? 1 : 3)} µs`;
}

function formatBandwidth(value) {
  const number = numberValue(value);
  return number === null ? "N/A" : `${number.toFixed(3)} GiB/s`;
}

function formatPercent(value) {
  const number = numberValue(value);
  return number === null ? "N/A" : `${(number * 100).toFixed(1)}%`;
}

function setText(id, value, raw = null) {
  const node = $(id);
  if (!node) {
    return;
  }
  node.textContent = value;
  if (raw !== null && !isMissing(raw)) {
    node.title = String(raw);
  } else {
    node.removeAttribute("title");
  }
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function setConnectionState(kind, label) {
  const pill = $("status-pill");
  pill.textContent = label;
  pill.classList.remove("online", "stale", "offline");
  pill.classList.add(kind);
}

function setBanner(message) {
  const banner = $("error-banner");
  if (!message) {
    banner.classList.add("hidden");
    banner.textContent = "";
    return;
  }
  banner.textContent = message;
  banner.classList.remove("hidden");
}

async function fetchJson(path) {
  const url = `${state.apiBase.replace(/\/$/, "")}${path}`;
  const response = await fetch(url, { cache: "no-store" });
  let payload;
  try {
    payload = await response.json();
  } catch (_error) {
    throw new Error(`${path}: response is not JSON (HTTP ${response.status})`);
  }
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || payload.detail || `${path}: HTTP ${response.status}`);
  }
  return payload;
}

async function postJson(path, body) {
  const url = `${state.apiBase.replace(/\/$/, "")}${path}`;
  const response = await fetch(url, {
    method: "POST",
    cache: "no-store",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    throw new Error(payload.error || payload.response?.detail || payload.detail || `${path}: HTTP ${response.status}`);
  }
  return payload;
}

function renderConfig(payload) {
  const config = payload.config || {};
  if ($("api-base") !== document.activeElement) {
    $("api-base").value = state.apiBase;
  }
  if ($("model-id") !== document.activeElement) {
    $("model-id").value = state.modelId || config.default_model_id || DEFAULT_MODEL_ID;
  }
  const pollMs = numberValue(config.poll_interval_ms);
  if (pollMs !== null && pollMs >= 500) {
    state.pollMs = pollMs;
  }
  setText("tunnel-path", config.tunnel_path || "local → ssh serving-b → openeuler-2403-DS");
  setText("daemon-socket", rawValue(config.daemon_socket));
}

function renderHealth(payload) {
  setText("monitor-state", rawValue(payload.monitor_api));
  setText("daemon-state", payload.daemon_reachable ? "online" : "offline");
  setText("hbm-state", payload.hbm_available === true ? "available" : payload.hbm_available === false ? "unavailable" : "N/A");
  setText("health-detail", JSON.stringify(payload, null, 2));
}

function renderStats(payload) {
  const stats = payload.stats || {};
  setText("device-id", rawValue(stats.device_id));
  setText("hbm-objects", rawValue(stats.hbm_objects));
  setText("hbm-used", formatBytes(stats.hbm_actual_bytes), stats.hbm_actual_bytes);
  setText("hbm-free", stats.hbm_available === false ? "unavailable" : formatBytes(stats.hbm_free_bytes), stats.hbm_free_bytes);
  setText("hbm-total", stats.hbm_available === false ? "unavailable" : formatBytes(stats.hbm_total_bytes), stats.hbm_total_bytes);

  setText("ddr-mode", rawValue(stats.ddr_backend_mode));
  setText("ddr-objects", rawValue(stats.ddr_objects));
  setText("ddr-requested", formatBytes(stats.ddr_requested_bytes), stats.ddr_requested_bytes);
  setText("ddr-committed", formatBytes(stats.ddr_committed_bytes), stats.ddr_committed_bytes);
  setText("ddr-safe", formatBytes(stats.ddr_safe_allocatable_bytes), stats.ddr_safe_allocatable_bytes);
  setText("cgroup-current", formatBytes(stats.ddr_cgroup_current_bytes), stats.ddr_cgroup_current_bytes);
  setText("ddr-root", rawValue(stats.ddr_root));

  setText("ssd-mode", rawValue(stats.ssd_io_mode));
  setText("ssd-objects", rawValue(stats.ssd_objects));
  setText("ssd-committed", formatBytes(stats.ssd_committed_bytes), stats.ssd_committed_bytes);
  setText("ssd-free", formatBytes(stats.ssd_fs_free_bytes), stats.ssd_fs_free_bytes);
  setText("ssd-read", `${formatBytes(stats.ssd_read_bytes)} / ${rawValue(stats.ssd_read_ops)} ops`, stats.ssd_read_bytes);
  setText("ssd-write", `${formatBytes(stats.ssd_write_bytes)} / ${rawValue(stats.ssd_write_ops)} ops`, stats.ssd_write_bytes);
  setText("ssd-root", rawValue(stats.ssd_root));

  setText("executor-workers", rawValue(stats.direct_transfer_workers));
  setText("executor-queue", rawValue(stats.direct_transfer_queue_depth));
  setText("executor-completed", rawValue(stats.direct_transfer_completed_jobs));
  setText("executor-wait", formatMicros(stats.direct_transfer_queue_wait_us_total), stats.direct_transfer_queue_wait_us_total);
  setText("event-plan-count", `${rawValue(stats.transfer_events)} events · ${rawValue(stats.transfer_plans)} plans`);

  setText("h2d-lanes", rawValue(stats.direct_h2d_lane_count ?? stats.h2d_lane_count));
  setText("h2d-busy", `${rawValue(stats.direct_h2d_busy_lanes ?? stats.h2d_busy_lanes)} busy`);
  setText("d2h-lanes", rawValue(stats.direct_d2h_lane_count ?? stats.d2h_lane_count));
  setText("d2h-busy", `${rawValue(stats.direct_d2h_busy_lanes ?? stats.d2h_busy_lanes)} busy`);
  setText("lane-waits", rawValue(stats.direct_lane_wait_count ?? stats.lane_wait_count));
  setText("lane-wait-us", formatMicros(stats.direct_lane_wait_us_total ?? stats.budget_wait_us));
  setText("event-reuse", rawValue(stats.direct_event_reuse_count));
  setText("stream-create", `${rawValue(stats.direct_stream_create_count)} streams`);
  setText("pinned-pool", `${formatBytes(stats.pinned_used_bytes)} / ${formatBytes(stats.pinned_total_bytes)}`);
}

function renderObjects(payload) {
  const objects = Array.isArray(payload.objects) ? payload.objects : [];
  setText("object-count", `${objects.length} objects`);
  const body = $("objects-body");
  if (!objects.length) {
    body.innerHTML = '<tr><td colspan="8" class="empty">No objects in this namespace</td></tr>';
    return;
  }
  body.innerHTML = objects.map((object) => {
    const path = object.backing_path || object.ddr_path || "";
    return `<tr>
      <td class="mono">${escapeHtml(rawValue(object.object_id))}</td>
      <td>${escapeHtml(rawValue(object.name))}</td>
      <td>${escapeHtml(rawValue(object.role))}</td>
      <td>${escapeHtml(rawValue(object.placement))}</td>
      <td class="mono">${escapeHtml(rawValue(object.target))}</td>
      <td title="${escapeHtml(rawValue(object.requested_bytes))}">${escapeHtml(formatBytes(object.requested_bytes))}</td>
      <td>${escapeHtml(rawValue(object.state))}</td>
      <td class="path-cell" title="${escapeHtml(path)}">${escapeHtml(path || "N/A")}</td>
    </tr>`;
  }).join("");
}

function renderSummary(payload) {
  const summary = payload.summary || {};
  const groups = Array.isArray(summary.groups) ? summary.groups : [];
  setText("summary-meta", `latest ${rawValue(summary.window_limit)} · generated ${summary.generated_at ? new Date(summary.generated_at * 1000).toLocaleTimeString() : "N/A"}`);
  setText("summary-sample", rawValue(summary.sample_size));
  setText("summary-fallback", `${rawValue(summary.fallback_count)} · ${formatPercent(summary.fallback_rate)}`);
  setText("summary-ssd-read", formatBytes(summary.ssd_io?.read_bytes), summary.ssd_io?.read_bytes);
  setText("summary-ssd-write", formatBytes(summary.ssd_io?.write_bytes), summary.ssd_io?.write_bytes);
  const body = $("summary-body");
  if (!groups.length) {
    body.innerHTML = '<tr><td colspan="7" class="empty">No completed transfer data in this window</td></tr>';
    return;
  }
  body.innerHTML = groups.map((group) => `<tr>
    <td class="mono">${escapeHtml(rawValue(group.actual_path))}</td>
    <td class="mono">${escapeHtml(rawValue(group.actual_engine))}</td>
    <td>${escapeHtml(directionLabel(group.direction))}</td>
    <td><span class="badge ${escapeHtml(String(group.status || ""))}">${escapeHtml(rawValue(group.status))}</span></td>
    <td>${escapeHtml(rawValue(group.count))}</td>
    <td>${escapeHtml(formatMicros(group.p50_latency_us))} / ${escapeHtml(formatMicros(group.p95_latency_us))}</td>
    <td>${escapeHtml(formatBandwidth(group.p50_bandwidth_gib_s))} / ${escapeHtml(formatBandwidth(group.p95_bandwidth_gib_s))}</td>
  </tr>`).join("");
}

function renderSummaryUnavailable(error) {
  setText("summary-meta", "Unavailable");
  for (const id of ["summary-sample", "summary-fallback", "summary-ssd-read", "summary-ssd-write"]) {
    setText(id, "N/A");
  }
  $("summary-body").innerHTML = `<tr><td colspan="7" class="empty">${escapeHtml(error.message || String(error))}</td></tr>`;
}

function directionLabel(direction) {
  const labels = {
    h2d: "host → device",
    d2h: "device → host",
    ssd_to_ddr: "SSD → DDR",
    ddr_to_ssd: "DDR → SSD",
  };
  return labels[direction] || rawValue(direction);
}

function mediaLabel(transfer) {
  const source = String(transfer.src_medium || "?").toUpperCase();
  const destination = String(transfer.dst_medium || "?").toUpperCase();
  return `${source} → ${destination}`;
}

function actualPathLabel(transfer) {
  if (transfer.direct_kind === "logical" && String(transfer.actual_path || "").includes("direct")) {
    return "SSD↔HBM logical direct";
  }
  return rawValue(transfer.actual_path);
}

function transferDecision(transfer) {
  const planned = transfer.planned_path;
  const actual = transfer.actual_path;
  if (transfer.fallback_used === true) {
    return { label: "runtime fallback", className: "fallback", detail: transfer.fallback_reason || "reason unavailable" };
  }
  if (!isMissing(planned) && !isMissing(actual) && planned !== actual) {
    return { label: "planner/metadata mismatch", className: "mismatch", detail: `${planned} → ${actual}` };
  }
  if (transfer.direct_kind === "logical" && String(actual || "").includes("direct")) {
    return { label: "logical direct", className: "logical", detail: transfer.direct_candidate || "logical candidate" };
  }
  return { label: "as planned", className: "complete", detail: "" };
}

function visibleTransfers() {
  if (!state.filters.direction) {
    return state.transfers;
  }
  return state.transfers.filter((transfer) => transfer.direction === state.filters.direction);
}

function renderTransfers() {
  const transfers = visibleTransfers();
  const body = $("transfers-body");
  setText("transfer-meta", `${transfers.length} shown · ${state.transfers.length} fetched`);
  $("load-older").disabled = !state.hasMore;
  if (!transfers.length) {
    body.innerHTML = '<tr><td colspan="10" class="empty">No transfers match this bounded window and filter</td></tr>';
    return;
  }
  body.innerHTML = transfers.map((transfer) => {
    const decision = transferDecision(transfer);
    const status = String(transfer.status || "unknown");
    const rowClasses = [status, decision.className].filter(Boolean).join(" ");
    return `<tr class="${escapeHtml(rowClasses)}" data-event-id="${escapeHtml(rawValue(transfer.event_id))}">
      <td class="mono">${escapeHtml(rawValue(transfer.event_id))}</td>
      <td><span class="badge ${escapeHtml(status)}">${escapeHtml(status)}</span></td>
      <td>${escapeHtml(directionLabel(transfer.direction))}</td>
      <td>${escapeHtml(mediaLabel(transfer))}</td>
      <td class="mono">${escapeHtml(rawValue(transfer.requested_mode))}</td>
      <td><div class="path-stack"><span>${escapeHtml(rawValue(transfer.planned_path))}</span><strong>${escapeHtml(actualPathLabel(transfer))}</strong></div></td>
      <td class="engine-cell">${escapeHtml(rawValue(transfer.actual_engine))}</td>
      <td title="${escapeHtml(rawValue(transfer.nbytes))}">${escapeHtml(formatBytes(transfer.nbytes))}</td>
      <td title="${escapeHtml(rawValue(transfer.actual_latency_us))}">${escapeHtml(formatMicros(transfer.actual_latency_us))}</td>
      <td title="${escapeHtml(decision.detail)}"><span class="badge ${escapeHtml(decision.className)}">${escapeHtml(decision.label)}</span></td>
    </tr>`;
  }).join("");
  body.querySelectorAll("tr[data-event-id]").forEach((row) => {
    row.addEventListener("click", () => showTransferDetail(Number(row.dataset.eventId)));
  });
}

function renderTransfersUnavailable(error) {
  state.transfers = [];
  state.hasMore = false;
  state.nextBeforeEventId = null;
  setText("transfer-meta", "Unavailable");
  $("load-older").disabled = true;
  $("transfers-body").innerHTML = `<tr><td colspan="10" class="empty">${escapeHtml(error.message || String(error))}</td></tr>`;
}

function detailRows(transfer, fields) {
  return fields.map(([label, key, formatter]) => {
    const value = transfer[key];
    const shown = formatter ? formatter(value) : rawValue(value);
    return `<dt>${escapeHtml(label)}</dt><dd title="${escapeHtml(rawValue(value))}">${escapeHtml(shown)}</dd>`;
  }).join("");
}

function detailGroup(title, transfer, fields) {
  return `<section class="detail-group"><h4>${escapeHtml(title)}</h4><dl>${detailRows(transfer, fields)}</dl></section>`;
}

function showTransferDetail(eventId) {
  const transfer = state.transfers.find((item) => Number(item.event_id) === Number(eventId));
  if (!transfer) {
    return;
  }
  state.selectedEventId = eventId;
  const decision = transferDecision(transfer);
  setText("detail-title", `Event ${eventId} · ${mediaLabel(transfer)} · ${decision.label}`);
  $("detail-content").innerHTML = `<div class="detail-grid">
    ${detailGroup("Context", transfer, [
      ["plan", "plan_id"], ["operation", "operation"], ["mode", "requested_mode"], ["bytes", "nbytes", formatBytes],
      ["planned path", "planned_path"], ["planned engine", "planned_engine"], ["actual path", "actual_path"], ["actual engine", "actual_engine"],
    ])}
    ${detailGroup("Outcome", transfer, [
      ["status", "status"], ["latency", "actual_latency_us", formatMicros], ["bandwidth", "actual_bandwidth_gib_s", formatBandwidth],
      ["fallback", "fallback_used"], ["fallback reason", "fallback_reason"], ["error", "error"],
    ])}
    ${detailGroup("Queue / channel / ACL", transfer, [
      ["queue wait", "channel_queue_wait_us", formatMicros], ["worker", "channel_worker_execute_us", formatMicros],
      ["lane", "channel_lane_id"], ["lane wait", "channel_lane_wait_us", formatMicros], ["CPU copy", "channel_cpu_copy_us", formatMicros],
      ["ACL submit", "channel_acl_submit_us", formatMicros], ["ACL wait", "channel_acl_wait_us", formatMicros], ["wall", "channel_wall_us", formatMicros],
    ])}
    ${detailGroup("SSD / relay / direct", transfer, [
      ["SSD bytes", "ssd_io_bytes", formatBytes], ["SSD submit", "ssd_io_submit_us", formatMicros], ["SSD wait", "ssd_io_wait_us", formatMicros],
      ["relay stages", "relay_stage_count"], ["DDR↔HBM", "relay_ddr_hbm_us", formatMicros], ["relay total", "relay_total_us", formatMicros],
      ["candidate", "direct_candidate"], ["direct kind", "direct_kind"], ["setup", "direct_setup_us", formatMicros], ["register", "direct_register_us", formatMicros],
      ["ACL", "direct_acl_us", formatMicros], ["direct total", "direct_total_us", formatMicros],
    ])}
  </div>`;
  $("transfer-detail").classList.remove("hidden");
}

function transferQuery(beforeEventId = null) {
  const params = new URLSearchParams();
  params.set("limit", String(state.filters.limit));
  if (state.filters.status) params.set("status", state.filters.status);
  if (state.filters.path) params.set("path", state.filters.path);
  if (state.filters.engine) params.set("engine", state.filters.engine);
  if (beforeEventId) params.set("before_event_id", String(beforeEventId));
  return `/v1/transfers?${params.toString()}`;
}

async function refreshTransfers({ append = false } = {}) {
  const payload = await fetchJson(transferQuery(append ? state.nextBeforeEventId : null));
  const fetched = Array.isArray(payload.transfers) ? payload.transfers : [];
  state.transfers = append ? [...state.transfers, ...fetched] : fetched;
  state.hasMore = payload.has_more === true;
  state.nextBeforeEventId = payload.next_before_event_id || null;
  renderTransfers();
}

function renderTrace(payload) {
  const trace = payload.trace || {};
  const enabled = trace.trace_enabled === true || trace.trace_enabled === "true";
  const badge = $("trace-state");
  badge.textContent = enabled ? `enabled · ${trace.trace_mode || "unknown"}` : "disabled";
  badge.classList.toggle("enabled", enabled);
  setText("trace-run", rawValue(trace.trace_run_id));
  setText("trace-buffered", rawValue(trace.trace_buffered_events));
  setText("trace-dropped", rawValue(trace.trace_dropped_events));
  setText("trace-flushes", rawValue(trace.trace_flush_count));

  const runId = trace.trace_run_id;
  const flushCount = numberValue(trace.trace_flush_count) || 0;
  const lastFlushDir = trace.trace_last_flush_dir;
  const error = trace.trace_last_flush_error;
  if (error) {
    setText("trace-message", `Last flush failed: ${error}`);
  } else if (flushCount > 0 && runId && lastFlushDir) {
    setText("trace-message", `Last artifact: ${lastFlushDir}`);
  } else {
    setText("trace-message", "No trace artifact has been flushed.");
  }
  const links = $("artifact-links");
  if (!(flushCount > 0 && runId && lastFlushDir)) {
    links.innerHTML = "";
    return;
  }
  links.innerHTML = TRACE_ARTIFACTS.map((filename) => {
    const href = `${state.apiBase.replace(/\/$/, "")}/v1/trace/artifact?run_id=${encodeURIComponent(runId)}&file=${encodeURIComponent(filename)}`;
    return `<a href="${escapeHtml(href)}" target="_blank" rel="noopener">${escapeHtml(filename)}</a>`;
  }).join("");
}

async function runTraceAction(action) {
  const buttons = [$("trace-start"), $("trace-flush"), $("trace-stop")];
  buttons.forEach((button) => { button.disabled = true; });
  try {
    let payload;
    if (action === "start") {
      payload = await postJson("/v1/trace/start", {
        run_id: `monitor-v2-${Date.now()}`,
        mode: "hot",
        categories: "control,transfer,channel,chunk,acl,backend",
        max_events: 200000,
        include_args: 1,
      });
    } else if (action === "flush") {
      payload = await postJson("/v1/trace/flush", {});
    } else {
      payload = await postJson("/v1/trace/stop", { flush: 1 });
    }
    setText("trace-message", JSON.stringify(payload.response || payload, null, 2));
    const trace = await fetchJson("/v1/trace/status");
    renderTrace(trace);
  } catch (error) {
    setText("trace-message", `Trace ${action} failed: ${error.message}`);
  } finally {
    buttons.forEach((button) => { button.disabled = false; });
  }
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
  if (name === "release-object") return { op: "ReleaseDataObject", object_id: 100 };
  if (name === "shutdown") return { op: "ShutdownDaemon" };
  return { op: "GetStats" };
}

function setCommandInput(command) {
  $("command-input").value = JSON.stringify(command, null, 2);
}

async function sendCommand() {
  const button = $("send-command");
  button.disabled = true;
  try {
    const command = JSON.parse($("command-input").value.trim());
    const payload = await postJson("/v1/command", command);
    setText("command-result", JSON.stringify(payload, null, 2));
    await poll();
  } catch (error) {
    setText("command-result", JSON.stringify({ ok: false, error: error.message }, null, 2));
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

    const [transfersResult, summaryResult] = await Promise.allSettled([
      refreshTransfers(),
      fetchJson("/v1/transfers/summary?limit=200"),
    ]);
    if (transfersResult.status === "rejected") {
      renderTransfersUnavailable(transfersResult.reason);
    }
    if (summaryResult.status === "fulfilled") {
      renderSummary(summaryResult.value);
    } else {
      renderSummaryUnavailable(summaryResult.reason);
    }

    state.lastOkAt = new Date();
    state.consecutiveFailures = 0;
    setText("last-updated", `Updated ${state.lastOkAt.toLocaleTimeString()}`);
    setText("failure-count", "0 failures");
    setConnectionState("online", "online");
    setBanner("");
  } catch (error) {
    state.consecutiveFailures += 1;
    setText("failure-count", `${state.consecutiveFailures} failure${state.consecutiveFailures === 1 ? "" : "s"}`);
    setText("daemon-state", "offline");
    const lastOk = state.lastOkAt ? state.lastOkAt.toLocaleTimeString() : "never";
    setText("last-updated", `Last successful refresh: ${lastOk}`);
    setConnectionState(state.lastOkAt ? "stale" : "offline", state.lastOkAt ? "stale" : "offline");
    setBanner(`Monitor refresh failed: ${error.message}. Last successful refresh: ${lastOk}. Previously loaded data is preserved.`);
    setText("health-detail", JSON.stringify({ ok: false, error: error.message, last_ok_at: state.lastOkAt?.toISOString() || null }, null, 2));
  } finally {
    window.clearTimeout(state.timer);
    state.timer = window.setTimeout(poll, state.pollMs);
  }
}

function readFilters() {
  state.filters = {
    status: $("filter-status").value,
    path: $("filter-path").value.trim(),
    engine: $("filter-engine").value.trim(),
    direction: $("filter-direction").value,
    limit: Number($("filter-limit").value),
  };
  state.nextBeforeEventId = null;
  refreshTransfers().catch(renderTransfersUnavailable);
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
  $("apply-filters").addEventListener("click", readFilters);
  $("refresh-transfers").addEventListener("click", () => refreshTransfers().catch(renderTransfersUnavailable));
  $("load-older").addEventListener("click", () => refreshTransfers({ append: true }).catch(renderTransfersUnavailable));
  $("close-detail").addEventListener("click", () => $("transfer-detail").classList.add("hidden"));
  $("trace-start").addEventListener("click", () => runTraceAction("start"));
  $("trace-flush").addEventListener("click", () => runTraceAction("flush"));
  $("trace-stop").addEventListener("click", () => runTraceAction("stop"));
  setCommandInput(commandTemplate("get-stats"));
  $("send-command").addEventListener("click", sendCommand);
  document.querySelectorAll("[data-template]").forEach((button) => {
    button.addEventListener("click", () => setCommandInput(commandTemplate(button.dataset.template)));
  });
  poll();
}

init();
