"use strict";

const $ = (id) => document.getElementById(id);
const recordPath = "/tables/{table}/partition-keys/{partition}/records/{record}";
const rangePath = "/tables/{table}/range?lower_bound={lower}&upper_bound={upper}";
const actions = {
  control: [
    { id: "create", name: "Create a table", method: "POST", path: "/tables", body: { table_name: "accounts", table_owner: 1, partition_key_name: "account", sort_key_name: "id" } },
    { id: "write-consistent", name: "Write record · consistent", method: "PUT", path: recordPath, body: { name: "local test" }, write: true },
    { id: "write-inconsistent", name: "Write record · inconsistent", method: "PUT", path: recordPath + "?consistent=false", body: { name: "local test" }, write: true },
    { id: "consistent", name: "Read record · consistent", method: "GET", path: recordPath },
    { id: "inconsistent", name: "Read record · inconsistent", method: "GET", path: recordPath + "?consistent=false" },
    { id: "control-range", name: "Read a range", method: "GET", path: rangePath },
    { id: "delete-table", name: "Delete a table", method: "DELETE", path: "/tables/{table}" },
    { id: "split", name: "Test: split partition", method: "POST", path: "/test/tables/{table}/split", body: { new_partition: 0 }, testOnly: true },
  ],
  data: [
    { id: "write", name: "Write a record", method: "gRPC", path: "WriteRecord", scope: "record", body: { name: "local test" }, write: true },
    { id: "read", name: "Read this node’s copy", method: "gRPC", path: "GetRecord", scope: "record" },
    { id: "read-deleted", name: "Read including tombstones", method: "gRPC", path: "GetRecord", scope: "record", include_deleted: true },
    { id: "delete", name: "Delete a record · tombstone", method: "gRPC", path: "DeleteRecord", scope: "record", write: true },
    { id: "data-range", name: "Read a range", method: "gRPC", path: "GetRange", scope: "range" },
    { id: "count", name: "Count records in a range", method: "gRPC", path: "CountRange", scope: "range" },
    { id: "size", name: "Payload size of a range", method: "gRPC", path: "GetRangeSize", scope: "range" },
    { id: "shard-size", name: "Shard file size", method: "gRPC", path: "GetSize" },
    { id: "telemetry", name: "Shard telemetry", method: "gRPC", path: "GetTelemetry" },
    { id: "table-telemetry", name: "Table telemetry", method: "gRPC", path: "GetTelemetryForTable", scope: "table" },
  ],
};
let cluster = { nodes: [], tables: [], partitions: [], locks: [] };
let lastRequest = null;
let busy = false;
let loadFormError = "";

function node() { return cluster.nodes.find((n) => n.name === $("node").value); }
function action() { return actions[node()?.kind || "control"].find((a) => a.id === $("action").value); }
function option(label, value) { return new Option(label, value); }
function cell(row, value) { const td = row.insertCell(); td.textContent = value; return td; }
function emptyRow(body, columns, text) { const td = body.insertRow().insertCell(); td.colSpan = columns; td.className = "empty"; td.textContent = text; }

function pathForAction(validate = false) {
  const values = { table: $("table").value, partition: $("partition-key").value.trim(), record: $("secondary-key").value.trim(), lower: $("lower-bound").value.trim(), upper: $("upper-bound").value.trim() };
  return action().path.replace(/\{(\w+)\}/g, (_, key) => {
    const value = values[key];
    if (validate && (!/^-?\d+$/.test(value) || BigInt(value) < -(1n << 63n) || BigInt(value) > (1n << 63n) - 1n)) {
      throw new Error(key === "table" ? "Create or select a table first." : `${key} must be a signed 64-bit integer.`);
    }
    return value || `{${key}}`;
  });
}

function dataFields() {
  const scope = action().scope;
  const fields = scope ? { table_id: $("table").value } : {};
  if (scope === "record") Object.assign(fields, { partition_key: $("partition-key").value.trim(), secondary_key: $("secondary-key").value.trim() });
  if (scope === "range") Object.assign(fields, { lower_bound: $("lower-bound").value.trim(), upper_bound: $("upper-bound").value.trim() });
  for (const [key, value] of Object.entries(fields)) {
    if (!/^-?\d+$/.test(value) || BigInt(value) < -(1n << 63n) || BigInt(value) > (1n << 63n) - 1n) {
      throw new Error(`${key} must be a signed 64-bit integer.`);
    }
  }
  return fields;
}

function updateForm(resetBody = false) {
  const current = action();
  if (!current) return;
  $("node-kind").textContent = node().kind === "control" ? "Control plane" : "Data plane";
  $("table-field").hidden = !(current.path.includes("{table}") || current.scope);
  $("key-fields").hidden = !(current.path.includes("{partition}") || current.scope === "record");
  $("range-fields").hidden = !(current.path.includes("{lower}") || current.scope === "range");
  $("body-field").hidden = !current.body;
  $("timestamp-field").hidden = !current.write;
  $("request-telemetry-field").hidden = node().kind !== "control" || !(current.method === "GET" || current.method === "PUT");
  for (const id of ["replicate-field", "replicate-note"]) $(id).hidden = !current.write || node().kind !== "data";
  if (resetBody) $("body").value = current.body ? JSON.stringify(current.body, null, 2) : "";
  $("method-preview").textContent = current.method;
  $("path-preview").textContent = pathForAction();
}

function selectNode(name) {
  $("node").value = name;
  const previous = $("action").value;
  $("action").replaceChildren(...actions[node().kind].filter((a) => !a.testOnly || cluster.test_api).map((a) => option(a.name, a.id)));
  if (actions[node().kind].some((a) => a.id === previous && (!a.testOnly || cluster.test_api))) $("action").value = previous;
  document.querySelectorAll(".node").forEach((button) => {
    const selected = button.dataset.node === name;
    button.classList.toggle("active", selected);
    button.setAttribute("aria-pressed", String(selected));
  });
  updateForm(true);
}

async function refresh() {
  try {
    const response = await fetch("/api/cluster");
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || "Could not read cluster metadata");
    cluster = data;
    const selectedNode = $("node").value;
    const selectedTable = $("table").value;
    $("node").replaceChildren(...cluster.nodes.map((n) => option(`${n.name} · ${n.url}`, n.name)));
    $("node").value = cluster.nodes.some((n) => n.name === selectedNode) ? selectedNode : cluster.nodes[0].name;
    $("table").replaceChildren(...cluster.tables.map((t) => option(`${t.table_name} (#${t.table_id})`, t.table_id)));
    if (!cluster.tables.length) $("table").append(option("No tables yet", ""));
    if (cluster.tables.some((t) => t.table_id === selectedTable)) $("table").value = selectedTable;
    const loadTable = $("load-table").value;
    $("load-table").replaceChildren(...cluster.tables.filter((t) => t.is_ready).map((t) => option(`${t.table_name} (#${t.table_id})`, t.table_id)));
    if (!$("load-table").options.length) $("load-table").append(option("Create a ready table first", ""));
    if (cluster.tables.some((t) => t.table_id === loadTable && t.is_ready)) $("load-table").value = loadTable;
    const loadNode = $("load-node").value;
    $("load-node").replaceChildren(option("All control nodes · round robin", "all"), ...cluster.nodes.filter((n) => n.kind === "control").map((n) => option(n.name, n.name)));
    $("load-node").value = loadNode || "all";
    for (const kind of ["control", "data"]) {
      $(kind + "-nodes").replaceChildren(...cluster.nodes.filter((n) => n.kind === kind).map((n) => {
        const button = document.createElement("button");
        button.className = "node" + (n.name === $("node").value ? " active" : "");
        button.dataset.node = n.name;
        button.setAttribute("aria-pressed", String(n.name === $("node").value));
        const dot = document.createElement("span"); dot.className = "dot" + (n.running ? "" : " down");
        const detail = document.createElement("small"); detail.textContent = n.url.replace("http://", "");
        button.append(dot, n.name, detail);
        button.onclick = () => selectNode(n.name);
        return button;
      }));
    }
    if (!action()) selectNode($("node").value);
    else updateForm();
    $("node-count").textContent = cluster.nodes.filter((n) => n.running).length;
    $("table-count").textContent = cluster.tables.length;
    $("partition-count").textContent = cluster.partitions.length;
    $("lock-count").textContent = cluster.locks.length;
    $("tables").replaceChildren();
    for (const table of cluster.tables) {
      const row = $("tables").insertRow();
      row.classList.toggle("selected-row", table.table_id === $("table").value);
      cell(row, table.table_id); cell(row, table.table_name); cell(row, table.is_ready ? "Ready" : "Not ready");
      const use = document.createElement("button"); use.className = "secondary"; use.textContent = "Use table";
      use.onclick = () => { $("table").value = table.table_id; updateForm(); };
      row.insertCell().append(use);
    }
    if (!cluster.tables.length) emptyRow($("tables"), 4, "No tables yet. Send a Create a table request above.");
    $("partitions").replaceChildren();
    for (const partition of cluster.partitions) {
      const row = $("partitions").insertRow();
      cell(row, partition.table_id); cell(row, partition.partition_id); cell(row, partition.hash_start).style.fontFamily = "ui-monospace, monospace";
      cell(row, partition.replicas.map((id) => `data-${id}`).join(", ")); cell(row, partition.forward_to ?? "—");
    }
    if (!cluster.partitions.length) emptyRow($("partitions"), 5, "Partition placement will appear when you create a table.");
    $("locks").textContent = JSON.stringify(cluster.locks, null, 2);
    $("lease-summary").textContent = cluster.locks.length ? `${cluster.locks.length} lease(s)` : "No locks held";
    $("connection").className = "connection";
    $("connection").textContent = "● Connected to the local rig";
    $("updated").textContent = "Updated " + new Date().toLocaleTimeString();
  } catch (error) {
    $("connection").className = "connection error";
    $("connection").textContent = error.message + ". Check the launcher terminal.";
  }
}

async function send(values) {
  if (busy) return;
  busy = true; $("send").disabled = true; $("retry").disabled = true;
  $("response-status").textContent = "Sending…";
  $("response-status").className = "tag";
  try {
    const response = await fetch("/api/request", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(values) });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || "The dashboard could not send the request");
    lastRequest = { ...values, timestamp: result.timestamp || values.timestamp };
    $("response-status").textContent = `${result.protocol || "HTTP"} ${result.status}`;
    $("response-status").className = "tag" + ((result.protocol === "gRPC" ? result.status !== "OK" : result.status >= 400) ? " bad" : "");
    $("response-time").textContent = `${result.elapsed_ms} ms`;
    $("response-meta").textContent = `${result.method} ${result.url}` + (result.timestamp ? `\nTimestamp: ${result.timestamp}` : "") + (result.replicas.length ? `\nReplicas: ${result.replicas.join(", ")}` : "") + (result.server_timing ? `\nServer timing (ms): ${result.server_timing}` : "");
    $("response-body").textContent = result.body || "Request completed. No response body.";
    // Table IDs remain decimal strings; JSON.parse in the browser can round i64s.
    if (values.method === "POST" && values.path === "/tables" && result.status === 201) {
      const match = result.body.match(/"table_id"\s*:\s*(-?\d+)/);
      await refresh();
      if (match) $("table").value = match[1];
      updateForm();
    } else await refresh();
  } catch (error) {
    $("response-status").textContent = "Request failed";
    $("response-status").className = "tag bad";
    $("response-time").textContent = "";
    $("response-body").textContent = error.message;
  } finally {
    busy = false; $("send").disabled = false; $("retry").disabled = !lastRequest;
  }
}

$("node").onchange = () => selectNode($("node").value);
$("action").onchange = () => updateForm(true);
for (const id of ["table", "partition-key", "secondary-key", "lower-bound", "upper-bound"]) $(id).addEventListener("input", () => updateForm());
$("refresh").onclick = refresh;
$("retry").onclick = () => send(lastRequest);
$("request-form").onsubmit = (event) => {
  event.preventDefault();
  try {
    const current = action();
    if (!current) throw new Error("Wait for the cluster to connect.");
    send({ node: node().name, method: current.method, path: pathForAction(true),
      fields: node().kind === "data" ? dataFields() : {}, include_deleted: Boolean(current.include_deleted),
      body: current.body ? $("body").value : "", timestamp: current.write ? $("timestamp").value.trim() : "",
      telemetry: node().kind === "control" && $("request-telemetry").checked,
      replicate: Boolean(current.write && node().kind === "data" && $("replicate").checked) });
  } catch (error) {
    $("response-body").textContent = error.message;
    $("response-status").textContent = "Check request";
    $("response-status").className = "tag bad";
  }
};
refresh();
setInterval(refresh, 5000);

async function updateLoad() {
  try {
    const response = await fetch("/api/load");
    if (!response.ok) throw new Error("Could not read load generator status");
    const state = await response.json();
    const count = state.counts;
    $("load-settings").disabled = state.running;
    $("load-start").disabled = state.running;
    $("load-stop").disabled = !state.running || state.phase === "stopping";
    $("load-phase").textContent = state.phase;
    $("load-phase").className = "tag" + (state.phase === "failed" ? " bad" : "");
    $("load-actual").textContent = `${state.current_tps} / ${state.average_tps}`;
    $("load-completed").textContent = (count.completed || 0).toLocaleString();
    $("load-errors").textContent = count.errors || 0;
    $("load-misses").textContent = count.misses || 0;
    $("load-latency").textContent = `${state.p95_ms} ms`;
    $("load-inflight").textContent = count.in_flight || 0;
    $("load-progress").max = state.phase === "seeding" ? state.config.records : state.config.duration || 1;
    $("load-progress").value = state.phase === "seeding" ? count.seeded || 0 : state.elapsed;
    $("load-detail").textContent = state.phase === "idle" ? "Choose a ready table to get started." : state.phase === "seeding" ? `Seeding ${count.seeded || 0} / ${state.config.records} records before timed load…` : `${state.elapsed}s · target ${state.config.tps} TPS · ${count.reads || 0} reads / ${count.writes || 0} writes · HTTP ${JSON.stringify(state.statuses)}`;
    $("load-detail").textContent += ` · ${count.skipped || 0} scheduling slots skipped`;
    if (state.report) $("load-detail").textContent += ` · report: ${state.report}`;
    $("load-error").textContent = loadFormError || state.last_error;
  } catch (error) {
    $("load-error").textContent = error.message;
  }
}

$("load-distribution").onchange = () => {
  const hot = $("load-distribution").value === "hot";
  $("load-keys-field").hidden = hot;
  $("load-hot-field").hidden = !hot;
};
$("load-form").onsubmit = async (event) => {
  event.preventDefault();
  $("load-start").disabled = true;
  loadFormError = "";
  try {
    const config = {
      table_id: $("load-table").value, node: $("load-node").value,
      tps: $("load-tps").value, duration: $("load-duration").value,
      workers: $("load-workers").value, read_percent: $("load-reads").value,
      records: $("load-records").value, keys: $("load-keys").value,
      payload_bytes: $("load-payload").value, distribution: $("load-distribution").value,
      hot_key: $("load-hot-key").value, consistent: $("load-consistency").value === "true",
      seed: $("load-seed").checked, telemetry: $("load-telemetry").checked,
    };
    const response = await fetch("/api/load", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(config) });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || "Could not start load");
    await updateLoad();
  } catch (error) {
    loadFormError = error.message;
    $("load-error").textContent = error.message;
    $("load-start").disabled = false;
  }
};
$("load-stop").onclick = async () => {
  try {
    const response = await fetch("/api/load/stop", { method: "POST" });
    if (!response.ok) throw new Error("Could not stop load");
    await updateLoad();
  } catch (error) { loadFormError = error.message; $("load-error").textContent = error.message; }
};
updateLoad();
setInterval(updateLoad, 1000);

function accessStatisticsRows(nodes, combine) {
  const rows = nodes.flatMap((node) => node.telemetry.map((stats) => ({ ...stats, name: node.name })));
  if (!combine) return rows;
  const combined = new Map();
  for (const stats of rows) {
    const key = JSON.stringify([stats.table_id, stats.partition_id]);
    if (!combined.has(key)) combined.set(key, {
      table_id: stats.table_id, partition_id: stats.partition_id,
      reads_per_second: 0, writes_per_second: 0, read_count: 0, write_count: 0,
      write_position_sum: 0n, window_seconds: 0, contributing_nodes: 0,
      median_write_position: null, writing_nodes: 0,
    });
    const total = combined.get(key);
    for (const field of ["reads_per_second", "writes_per_second", "read_count", "write_count", "contributing_nodes"]) total[field] += stats[field];
    total.write_position_sum += BigInt(stats.write_position_sum);
    if (stats.window_seconds > 0) total.window_seconds = total.window_seconds ? Math.min(total.window_seconds, stats.window_seconds) : stats.window_seconds;
    if (stats.write_count) {
      total.writing_nodes += 1;
      total.median_write_position = total.writing_nodes === 1 ? stats.median_write_position : null;
    }
  }
  return [...combined.values()].map((stats) => ({
    ...stats, name: `All nodes (${stats.contributing_nodes})`,
    average_write_position: stats.write_count ? String(stats.write_position_sum / BigInt(stats.write_count)) : null,
  }));
}

let telemetryBusy = false;
let lastTelemetry = null;
async function refreshTelemetry(force = false) {
  if (telemetryBusy || (!force && !$("telemetry-live").checked)) return;
  telemetryBusy = true;
  try {
    const response = await fetch("/api/telemetry");
    if (!response.ok) throw new Error("Could not read node telemetry");
    const result = await response.json();
    lastTelemetry = result;
    renderTelemetry(result);
  } catch (error) { $("telemetry-error").textContent = error.message; }
  finally { telemetryBusy = false; }
}

function renderTelemetry(result) {
  const errors = [];
  $("operation-timings").replaceChildren();
  $("data-telemetry-rows").replaceChildren();
  for (const node of result.nodes) {
    if (node.error) { errors.push(`${node.name}: ${node.error}`); continue; }
    if (node.kind === "control") {
      for (const [operation, stats] of Object.entries(node.telemetry)) {
        const row = $("operation-timings").insertRow();
        cell(row, node.name); cell(row, operation.replaceAll("_", " "));
        cell(row, `${stats.requests} / ${stats.errors}`);
        for (const stage of ["total", "table_lookup", "partition_lookup", "replica_resolution", "replica_lookup", "replica_grpc"]) {
          cell(row, stats.stages[stage] ? (stats.stages[stage].total_ms / stats.requests).toFixed(3) : "—");
        }
      }
    }
  }
  const dataNodes = result.nodes.filter((node) => node.kind === "data" && !node.error);
  const combine = $("telemetry-combine").checked;
  const rows = accessStatisticsRows(dataNodes, combine).sort((a, b) => (b.reads_per_second + b.writes_per_second) - (a.reads_per_second + a.writes_per_second)).slice(0, 100);
  for (const stats of rows) {
    const row = $("data-telemetry-rows").insertRow();
    for (const value of [stats.name, stats.table_id, stats.partition_id, stats.reads_per_second.toFixed(1), stats.writes_per_second.toFixed(1), `${stats.read_count} / ${stats.write_count}`, stats.window_seconds.toFixed(2), stats.average_write_position ?? "—", stats.median_write_position ?? "—"]) cell(row, value);
  }
  if (!$("operation-timings").rows.length) emptyRow($("operation-timings"), 9, "Enable fetch / put measurement above to collect timings.");
  if (!$("data-telemetry-rows").rows.length) emptyRow($("data-telemetry-rows"), 9, "No recent data-node accesses.");
  $("telemetry-error").textContent = errors.join(" · ");
}

let backlogBusy = false;
let lastBacklog = null;
async function refreshWriteBacklog() {
  if (backlogBusy || !$("telemetry-live").checked) return;
  backlogBusy = true;
  try {
    const response = await fetch("/api/write-backlog");
    if (!response.ok) throw new Error("Could not read write backlog");
    lastBacklog = await response.json();
    renderWriteBacklog(lastBacklog);
  } catch (error) { $("write-backlog-error").textContent = error.message; }
  finally { backlogBusy = false; }
}

function renderWriteBacklog(result) {
  $("write-backlog-rows").replaceChildren();
  let backlogs = result.nodes.filter((node) => node.write_backlog);
  if ($("telemetry-combine").checked && backlogs.length) {
    const total = { queued: 0, in_flight: 0, total: 0 };
    for (const node of backlogs) for (const field of Object.keys(total)) total[field] += node.write_backlog[field];
    backlogs = [{ name: `All reporting nodes (${backlogs.length}/${result.nodes.length})`, write_backlog: total }];
  }
  for (const node of backlogs) {
    const row = $("write-backlog-rows").insertRow();
    for (const value of [node.name, node.write_backlog.queued, node.write_backlog.in_flight, node.write_backlog.total]) cell(row, value);
  }
  if (!$("write-backlog-rows").rows.length) emptyRow($("write-backlog-rows"), 4, "Write backlog unavailable.");
  $("write-backlog-error").textContent = result.nodes.filter((node) => node.error).map((node) => `${node.name}: ${node.error}`).join(" · ");
}
$("telemetry-combine").onchange = () => {
  if (lastTelemetry) renderTelemetry(lastTelemetry);
  if (lastBacklog) renderWriteBacklog(lastBacklog);
};
$("telemetry-live").onchange = () => { refreshTelemetry(); refreshWriteBacklog(); };
$("reset-telemetry").onclick = async () => {
  $("reset-telemetry").disabled = true;
  try {
    const response = await fetch("/api/telemetry/reset", { method: "POST" });
    const result = await response.json();
    if (!response.ok) throw new Error(result.errors.join(" · "));
    await refreshTelemetry(true);
  } catch (error) { $("telemetry-error").textContent = error.message; }
  finally { $("reset-telemetry").disabled = false; }
};
refreshTelemetry();
setInterval(refreshTelemetry, 5000);
refreshWriteBacklog();
setInterval(refreshWriteBacklog, 1000);
