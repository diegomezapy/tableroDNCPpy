const CONFIG = window.LICITABAYES_CONFIG || {};

const state = {
  summary: null,
  price: [],
  concentration: [],
  series: {},
  tab: "resumen",
  level: "Todos",
  search: ""
};

const $ = (id) => document.getElementById(id);

function money(value) {
  const n = Number(value || 0);
  if (n >= 1e12) return `G. ${(n / 1e12).toFixed(2)} B`;
  if (n >= 1e9) return `G. ${(n / 1e9).toFixed(2)} MM`;
  if (n >= 1e6) return `G. ${(n / 1e6).toFixed(1)} M`;
  return `G. ${Math.round(n).toLocaleString("es-PY")}`;
}

function pct(value) {
  return `${Math.round(Number(value || 0) * 100)}%`;
}

function text(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;"
  }[ch]));
}

async function getJson(name) {
  const base = CONFIG.dataBaseUrl || "data";
  const res = await fetch(`${base}/${name}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`No se pudo cargar ${name}`);
  return res.json();
}

async function loadData() {
  $("loadStatus").textContent = "Cargando JSON publicados...";
  const [summary, prices, concentration, series] = await Promise.all([
    getJson("model_summary.json"),
    getJson("price_alerts.json"),
    getJson("concentration_alerts.json"),
    getJson("series.json")
  ]);
  state.summary = summary;
  state.price = prices.rows || [];
  state.concentration = concentration.rows || [];
  state.series = series;
  $("loadStatus").textContent = "Datos cargados";
  $("lastRun").textContent = `Run ${summary.run_id || ""}`;
  $("appVersion").textContent = summary.app_version || CONFIG.appVersion || "0.1.0";
  render();
}

function filteredPrice() {
  const q = state.search.trim().toLowerCase();
  return state.price.filter((row) => {
    const levelOk = state.level === "Todos" || row.nivel_bayes === state.level;
    if (!levelOk) return false;
    if (!q) return true;
    return [row.articulo, row.entidad, row.proveedor, row.codigo_catalogo, row.ruc]
      .join(" ")
      .toLowerCase()
      .includes(q);
  });
}

function filteredConcentration() {
  const q = state.search.trim().toLowerCase();
  return state.concentration.filter((row) => {
    const levelOk = state.level === "Todos" || row.nivel_concentracion === state.level;
    if (!levelOk) return false;
    if (!q) return true;
    return [row.entidad, row.proveedor, row.ruc].join(" ").toLowerCase().includes(q);
  });
}

function kpi(label, value, note) {
  return `<div class="kpi"><span>${text(label)}</span><strong>${text(value)}</strong><small>${text(note || "")}</small></div>`;
}

function renderKpis() {
  const c = state.summary?.counts || {};
  $("kpiGrid").innerHTML = [
    kpi("Licitaciones", Number(c.licitaciones || 0).toLocaleString("es-PY"), "convocatorias publicadas"),
    kpi("Items", Number(c.items || 0).toLocaleString("es-PY"), "detalle adjudicado"),
    kpi("Alertas precio", Number(c.price_alerts || 0).toLocaleString("es-PY"), "ranking publicado"),
    kpi("Concentracion", Number(c.concentration_alerts || 0).toLocaleString("es-PY"), "relaciones analizadas")
  ].join("");
}

function renderBars(target, rows, labelKey, valueKey, maxRows = 8) {
  const top = rows.slice(0, maxRows);
  const max = Math.max(...top.map((r) => Number(r[valueKey] || 0)), 1);
  target.innerHTML = top.map((row) => {
    const value = Number(row[valueKey] || 0);
    return `
      <div class="bar-row">
        <span>${text(row[labelKey])}</span>
        <div class="bar-track"><div class="bar-fill" style="width:${Math.max(2, (value / max) * 100)}%"></div></div>
        <strong>${value.toLocaleString("es-PY")}</strong>
      </div>`;
  }).join("");
}

function renderLevelChart() {
  const levels = state.summary?.price_stats?.levels || {};
  const rows = Object.entries(levels).map(([level, count]) => ({ level, count }));
  renderBars($("levelChart"), rows, "level", "count", 8);
  $("priceCountLabel").textContent = `${state.price.length.toLocaleString("es-PY")} filas publicadas`;
}

function renderEntityChart() {
  renderBars($("entityChart"), state.series.top_entidades || [], "entidad", "cantidad", 8);
}

function badge(level) {
  const css = String(level || "Normal").split(" ")[0];
  return `<span class="badge ${text(css)}">${text(level)}</span>`;
}

function renderCards() {
  const rows = filteredPrice().slice(0, 12);
  $("alertCards").innerHTML = rows.map((row) => `
    <article class="alert-card">
      ${badge(row.nivel_bayes)}
      <h3>${text(row.articulo)}</h3>
      <p>${text(row.entidad)}</p>
      <strong>${money(row.precio_promedio_ent)}</strong>
      <p>Mediana: ${money(row.precio_mediano)} · Prob.: ${pct(row.prob_alta)}</p>
    </article>
  `).join("");
}

function renderPriceTable() {
  const rows = filteredPrice();
  $("filteredPriceCount").textContent = `${rows.length.toLocaleString("es-PY")} resultados`;
  const maxRows = CONFIG.maxTableRows || 250;
  $("priceTable").innerHTML = rows.slice(0, maxRows).map((row) => `
    <tr>
      <td>${row.rank}</td>
      <td>${badge(row.nivel_bayes)}</td>
      <td>${pct(row.prob_alta)}</td>
      <td>${Number(row.score_bayes || 0).toFixed(1)}</td>
      <td><strong>${text(row.articulo)}</strong><br><small>${text(row.codigo_catalogo)} · ${text(row.unidad)}</small></td>
      <td>${text(row.entidad)}</td>
      <td>${text(row.proveedor)}<br><small>${text(row.ruc)}</small></td>
      <td>${money(row.precio_promedio_ent)}</td>
      <td>${money(row.precio_mediano)}</td>
      <td>${Number(row.ratio_observado || 0).toFixed(2)}x</td>
    </tr>
  `).join("");
}

function renderNetwork() {
  const rows = filteredConcentration().slice(0, 22);
  const h = 470;
  const leftX = 120;
  const rightX = 520;
  const width = 680;
  const entityMap = new Map();
  const providerMap = new Map();
  rows.forEach((row) => {
    if (!entityMap.has(row.entidad)) entityMap.set(row.entidad, entityMap.size);
    if (!providerMap.has(row.proveedor)) providerMap.set(row.proveedor, providerMap.size);
  });
  const yFor = (idx, total) => 50 + idx * ((h - 100) / Math.max(1, total - 1));
  const lines = rows.map((row) => {
    const y1 = yFor(entityMap.get(row.entidad), entityMap.size);
    const y2 = yFor(providerMap.get(row.proveedor), providerMap.size);
    const sw = Math.max(1.5, Math.min(9, Number(row.score_concentracion || 0) / 13));
    return `<line x1="${leftX}" y1="${y1}" x2="${rightX}" y2="${y2}" stroke="#C8A24A" stroke-opacity=".52" stroke-width="${sw}"><title>${text(row.entidad)} -> ${text(row.proveedor)}</title></line>`;
  }).join("");
  const entities = [...entityMap.keys()].map((name, idx) => {
    const y = yFor(idx, entityMap.size);
    return `<circle cx="${leftX}" cy="${y}" r="7" fill="#0B5D3B"></circle><text x="${leftX - 12}" y="${y + 4}" text-anchor="end">${text(name.slice(0, 34))}</text>`;
  }).join("");
  const providers = [...providerMap.keys()].map((name, idx) => {
    const y = yFor(idx, providerMap.size);
    return `<circle cx="${rightX}" cy="${y}" r="7" fill="#1D4ED8"></circle><text x="${rightX + 12}" y="${y + 4}">${text(name.slice(0, 34))}</text>`;
  }).join("");
  $("networkView").innerHTML = `<svg viewBox="0 0 ${width} ${h}" role="img" aria-label="Red entidad proveedor">${lines}${entities}${providers}</svg>`;
}

function renderConcentration() {
  const rows = filteredConcentration();
  $("filteredConcCount").textContent = `${rows.length.toLocaleString("es-PY")} resultados`;
  $("concentrationList").innerHTML = rows.slice(0, 80).map((row) => `
    <article class="stack-item">
      ${badge(row.nivel_concentracion)}
      <strong>${text(row.entidad)}</strong>
      <p>${text(row.proveedor)} · ${text(row.ruc)}</p>
      <p>Score ${Number(row.score_concentracion || 0).toFixed(1)} · Monto ${money(row.monto_total)} · Share ${pct(row.share_monto)}</p>
    </article>
  `).join("");
  renderNetwork();
}

function renderDataQuality() {
  const q = state.summary?.quality || {};
  $("qualityList").innerHTML = Object.entries(q).map(([key, value]) => `
    <div class="definition-row"><span>${text(key)}</span><strong>${text(value)}</strong></div>
  `).join("");
  const s = state.series || {};
  const seriesRows = [
    ["Convocatorias anual", (s.convocatorias_anual || []).length],
    ["Convocatorias mensual", (s.convocatorias_mensual || []).length],
    ["Adjudicaciones anual", (s.adjudicaciones_anual || []).length],
    ["Adjudicaciones mensual", (s.adjudicaciones_mensual || []).length],
    ["Top entidades", (s.top_entidades || []).length],
    ["Top proveedores", (s.top_proveedores || []).length]
  ];
  $("seriesList").innerHTML = seriesRows.map(([key, value]) => `
    <div class="definition-row"><span>${text(key)}</span><strong>${value}</strong></div>
  `).join("");
}

function renderBackup() {
  $("backupInfo").textContent = JSON.stringify({
    spreadsheetId: CONFIG.googleSheetId,
    githubPagesUrl: CONFIG.githubPagesUrl,
    dataBaseUrl: `${CONFIG.githubPagesUrl || ""}data/`,
    appsScript: "Use gas/Code.gs y ejecute syncFromGithub() despues de desplegar.",
    runId: state.summary?.run_id || ""
  }, null, 2);
}

function render() {
  renderKpis();
  renderLevelChart();
  renderEntityChart();
  renderCards();
  renderPriceTable();
  renderConcentration();
  renderDataQuality();
  renderBackup();
}

function setTab(tab) {
  state.tab = tab;
  document.querySelectorAll(".nav-btn").forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === tab));
  document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.id === `tab-${tab}`));
}

function toCsv(rows) {
  const headers = Object.keys(rows[0] || {});
  const clean = (value) => `"${String(value ?? "").replace(/"/g, '""')}"`;
  return [headers.join(","), ...rows.map((row) => headers.map((key) => clean(row[key])).join(","))].join("\n");
}

function download(name, content, type = "text/plain") {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  a.click();
  URL.revokeObjectURL(url);
}

function bindEvents() {
  $("enterApp").addEventListener("click", () => {
    $("accessScreen").hidden = true;
    $("appShell").hidden = false;
  });
  document.querySelectorAll(".nav-btn").forEach((btn) => btn.addEventListener("click", () => setTab(btn.dataset.tab)));
  $("searchBox").addEventListener("input", (event) => {
    state.search = event.target.value;
    renderCards();
    renderPriceTable();
    renderConcentration();
  });
  document.querySelectorAll(".chip").forEach((btn) => btn.addEventListener("click", () => {
    document.querySelectorAll(".chip").forEach((chip) => chip.classList.remove("active"));
    btn.classList.add("active");
    state.level = btn.dataset.level;
    renderCards();
    renderPriceTable();
    renderConcentration();
  }));
  $("clearFilters").addEventListener("click", () => {
    state.search = "";
    state.level = "Todos";
    $("searchBox").value = "";
    document.querySelectorAll(".chip").forEach((chip) => chip.classList.toggle("active", chip.dataset.level === "Todos"));
    renderCards();
    renderPriceTable();
    renderConcentration();
  });
  $("exportBtn").addEventListener("click", () => {
    const rows = state.tab === "concentracion" ? filteredConcentration() : filteredPrice();
    download(`licitabayes_${state.tab}.csv`, toCsv(rows), "text/csv;charset=utf-8");
  });
  $("downloadSnapshot").addEventListener("click", () => {
    download("licitabayes_snapshot.json", JSON.stringify({
      summary: state.summary,
      price: filteredPrice().slice(0, 500),
      concentration: filteredConcentration().slice(0, 500)
    }, null, 2), "application/json");
  });
}

bindEvents();
loadData().catch((error) => {
  $("loadStatus").textContent = "Error al cargar datos";
  console.error(error);
});

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("service-worker.js").catch(() => {});
  });
}
