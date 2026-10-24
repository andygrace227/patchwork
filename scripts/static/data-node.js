const $ = (id) => document.getElementById(id);
const number = (value) => Number(value || 0).toLocaleString();
let state = null;
let busy = false;
let formError = "";

async function refresh() {
  if (busy) return;
  busy = true;
  try {
    const response = await fetch("/api/load");
    if (!response.ok) throw new Error("Could not read load status");
    state = await response.json();
    const counts = state.counts || {};
    $("phase").textContent = state.phase;
    $("settings").disabled = state.running;
    $("start").disabled = state.running;
    $("stop").disabled = !state.running || state.phase === "stopping";
    $("download").disabled = state.running || !state.report;
    $("current").textContent = number(state.current_tps);
    $("average").textContent = number(state.average_tps);
    $("latency").textContent = `${state.p95_ms || 0} ms`;
    $("inflight").textContent = number(counts.in_flight);
    $("errors").textContent = `${number(counts.errors)} / ${number(counts.misses)}`;
    $("skipped").textContent = number(counts.skipped);
    $("progress").max = state.phase === "seeding" ? state.config.records : state.config.duration || 1;
    $("progress").value = state.phase === "seeding" ? counts.seeded || 0 : state.elapsed || 0;
    $("detail").textContent = state.phase === "idle" ? "Ready to start." : `${state.elapsed}s · ${number(counts.completed)} completed · ${number(counts.reads)} reads / ${number(counts.writes)} writes · seeded ${number(counts.seeded)} · gRPC ${JSON.stringify(state.statuses)}`;
    $("error").textContent = formError || state.last_error;
  } catch (error) {
    $("error").textContent = error.message;
  } finally { busy = false; }
}

$("load-form").onsubmit = async (event) => {
  event.preventDefault();
  formError = "";
  $("start").disabled = true;
  const config = Object.fromEntries(new FormData(event.currentTarget));
  config.seed = event.currentTarget.elements.seed.checked;
  try {
    const response = await fetch("/api/load", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(config),
    });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || "Could not start load");
  } catch (error) { formError = error.message; }
  await refresh();
};

$("stop").onclick = async () => {
  try {
    const response = await fetch("/api/load/stop", { method: "POST" });
    if (!response.ok) throw new Error("Could not stop load");
  } catch (error) { formError = error.message; }
  await refresh();
};

$("download").onclick = () => {
  const url = URL.createObjectURL(new Blob([JSON.stringify(state, null, 2)], { type: "application/json" }));
  const link = document.createElement("a");
  link.href = url;
  link.download = `data-load-${state.config.run_id}.json`;
  link.click();
  URL.revokeObjectURL(url);
};

fetch("/api/node").then((response) => {
  if (!response.ok) throw new Error("Could not read node configuration");
  return response.json();
}).then((node) => {
  $("node").textContent = `${node.url} · data threads ${node.data_threads} · load threads ${node.load_threads} · SQLite readers ${node.readers}`;
  $("directory").textContent = `Logs and reports: ${node.directory}`;
}).catch((error) => { formError = error.message; $("error").textContent = formError; });
refresh();
setInterval(refresh, 1000);
