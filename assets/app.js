const CONFIG = window.LICITABAYES_CONFIG || {};
const VISITOR_KEY = "licitabayes_visitante_v1";
const ADMIN_EMAIL = String(CONFIG.adminEmail || "dmeza.py@gmail.com").toLowerCase();

const state = {
  summary: null,
  price: [],
  concentration: [],
  series: {},
  visitor: null,
  tab: "resumen",
  level: "Todos",
  search: "",
  entity: "Todos",
  product: "",
  category: "Todos",
  year: "Todos",
  month: "Todos",
  provider: "Todos",
  unit: "Todos",
  previousTab: "resumen",
  selectedDetail: null
};

const $ = (id) => document.getElementById(id);

function money(value) {
  return `G. ${formatNumber(value, 0)}`;
}

function formatNumber(value, decimals = 0) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return decimals > 0 ? `0,${"0".repeat(decimals)}` : "0";
  const fixed = Math.abs(n).toFixed(decimals);
  const [whole, fraction] = fixed.split(".");
  const grouped = whole.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  const sign = n < 0 ? "-" : "";
  return `${sign}${grouped}${decimals > 0 ? `,${fraction}` : ""}`;
}

function pct(value) {
  const n = Number(value || 0);
  if (n >= 0.9995) return ">99,9%";
  return `${formatNumber(n * 100, 1)}%`;
}

function decimal(value, decimals = 1) {
  return formatNumber(value, decimals);
}

function displayValue(value) {
  if (typeof value === "number") {
    return Number.isInteger(value) ? formatNumber(value) : decimal(value, 2);
  }
  return value;
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

function normalizedEmail(value) {
  return String(value || "").trim().toLowerCase();
}

function isAdminVisitor(visitor = state.visitor) {
  return normalizedEmail(visitor?.correo) === ADMIN_EMAIL;
}

function sheetUrl() {
  return `https://docs.google.com/spreadsheets/d/${CONFIG.googleSheetId}/edit`;
}

function applyRole(visitor = state.visitor) {
  const isAdmin = isAdminVisitor(visitor);
  document.body.dataset.role = isAdmin ? "admin" : "visitor";
  document.querySelectorAll(".admin-only").forEach((el) => {
    el.hidden = !isAdmin;
  });
  document.querySelectorAll("[data-sheet-link]").forEach((el) => {
    if (isAdmin) el.setAttribute("href", sheetUrl());
    else el.removeAttribute("href");
  });
  if (!isAdmin && state.tab === "respaldo") setTab("resumen");
}

async function getJson(name) {
  const base = CONFIG.dataBaseUrl || "data";
  const res = await fetch(`${base}/${name}`, { cache: "no-store" });
  if (!res.ok) throw new Error(`No se pudo cargar ${name}`);
  return res.json();
}

function rowHasValue(row, key, selected) {
  if (selected === "Todos") return true;
  const value = row[key];
  if (Array.isArray(value)) return value.map(String).includes(String(selected));
  return String(value ?? "") === String(selected);
}

function codeCategory(row) {
  return String(row.rubro || row.codigo_catalogo || "").slice(0, 4);
}

function uniqueOptions(rows, getter, limit = 500) {
  const counts = new Map();
  rows.forEach((row) => {
    const raw = getter(row);
    const values = Array.isArray(raw) ? raw : [raw];
    values.forEach((value) => {
      const clean = String(value ?? "").trim();
      if (!clean) return;
      counts.set(clean, (counts.get(clean) || 0) + 1);
    });
  });
  return [...counts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, limit)
    .map(([value, count]) => ({ value, count }));
}

function setOptions(id, options, allLabel = "Todos", formatter = null) {
  const el = $(id);
  if (!el) return;
  const current = el.value || "Todos";
  el.innerHTML = [`<option value="Todos">${text(allLabel)}</option>`]
    .concat(options.map((item) => {
      const label = formatter ? formatter(item) : `${item.value} (${formatNumber(item.count)})`;
      return `<option value="${text(item.value)}">${text(label)}</option>`;
    }))
    .join("");
  el.value = [...el.options].some((opt) => opt.value === current) ? current : "Todos";
}

function monthName(value) {
  return ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"][Number(value)] || value;
}

function formatList(values, formatter = null) {
  const list = Array.isArray(values) ? values : [values];
  const clean = list.filter((value) => value !== "" && value !== null && value !== undefined);
  if (!clean.length) return "Sin dato";
  return clean.map((value) => formatter ? formatter(value) : value).join(", ");
}

function thresholdInfo(row) {
  const rubro = String(row.rubro || row.codigo_catalogo || "");
  const article = String(row.articulo || "").toLowerCase();
  const isWorks = rubro.startsWith("72") || /obra|construccion|camino|cancha|desague|cerco|plaza/.test(article);
  const upper = isWorks ? 1.10 : 1.15;
  const lower = isWorks ? 0.80 : 0.75;
  const ratio = Number(row.ratio_observado || 0);
  let status = "Dentro de umbral normativo orientativo";
  if (ratio > upper) status = `Supera umbral orientativo alto (+${formatNumber((upper - 1) * 100, 0)}%)`;
  if (ratio < lower) status = `Debajo de umbral orientativo bajo (-${formatNumber((1 - lower) * 100, 0)}%)`;
  return { type: isWorks ? "Obras/servicios constructivos" : "Bienes/servicios generales", upper, lower, status };
}

function explainLevel(row) {
  if (row.observacion_calidad) return row.observacion_calidad;
  const level = row.nivel_bayes || "Normal";
  if (level === "Critico") return "Clasificado como Critico porque la probabilidad posterior o el score superan el umbral alto del modelo.";
  if (level === "Alto") return "Clasificado como Alto porque la evidencia estadistica supera el umbral medio-alto de revision.";
  if (level === "Moderado") return "Clasificado como Moderado porque existe una desviacion posterior que conviene revisar con contexto.";
  if (level === "Verificar dato") return "Clasificado como Verificar dato porque la referencia puede no ser comparable o la evidencia es insuficiente.";
  return "No presenta una senal alta en el ranking publicado.";
}

function detailPeers(row) {
  return state.price
    .filter((peer) => peer.hash_registro !== row.hash_registro)
    .filter((peer) => peer.codigo_catalogo === row.codigo_catalogo && peer.unidad === row.unidad)
    .sort((a, b) => Number(b.ratio_observado || 0) - Number(a.ratio_observado || 0))
    .slice(0, 12);
}

function detailPeerRows(row) {
  const peers = detailPeers(row);
  if (!peers.length) {
    return `<p class="muted-note">No hay otros pares publicados para el mismo codigo y unidad dentro del ranking actual.</p>`;
  }
  return `
    <div class="table-wrap mini">
      <table>
        <thead>
          <tr>
            <th>Entidad</th>
            <th>Proveedor</th>
            <th>Precio</th>
            <th>Referencia</th>
            <th>Ratio</th>
            <th>Nivel</th>
          </tr>
        </thead>
        <tbody>
          ${peers.map((peer) => `
            <tr>
              <td>${text(peer.entidad)}</td>
              <td>${text(peer.proveedor)}</td>
              <td>${money(peer.precio_promedio_ent)}</td>
              <td>${money(peer.precio_mediano)}</td>
              <td>${decimal(peer.ratio_observado || 0, 2)}x</td>
              <td>${badge(peer.nivel_bayes)}</td>
            </tr>
          `).join("")}
        </tbody>
      </table>
    </div>`;
}

function priceBars(row) {
  const observed = Number(row.precio_promedio_ent || 0);
  const ref = Number(row.precio_mediano || 0);
  const max = Math.max(observed, ref, 1);
  const observedW = Math.max(4, (observed / max) * 260);
  const refW = Math.max(4, (ref / max) * 260);
  return `
    <svg class="detail-svg" viewBox="0 0 360 150" role="img" aria-label="Comparacion de precio observado y referencia">
      <text x="0" y="18">Precio observado</text>
      <rect x="0" y="28" width="${observedW}" height="24" rx="4" fill="#0B5D3B"></rect>
      <text x="${Math.min(270, observedW + 8)}" y="46">${money(observed)}</text>
      <text x="0" y="82">Referencia</text>
      <rect x="0" y="92" width="${refW}" height="24" rx="4" fill="#C8A24A"></rect>
      <text x="${Math.min(270, refW + 8)}" y="110">${money(ref)}</text>
      <text x="0" y="142">Ratio observado: ${decimal(row.ratio_observado || 0, 2)}x</text>
    </svg>`;
}

function posteriorFigure(row) {
  const prob = Math.max(0, Math.min(1, Number(row.prob_alta || 0)));
  const score = Math.max(0, Math.min(100, Number(row.score_bayes || 0)));
  const probW = 300 * prob;
  const scoreW = 3 * score;
  return `
    <svg class="detail-svg" viewBox="0 0 360 150" role="img" aria-label="Probabilidad posterior y score">
      <text x="0" y="18">Probabilidad posterior &gt; 1,5x</text>
      <rect x="0" y="28" width="300" height="18" rx="9" fill="#edf2ef"></rect>
      <rect x="0" y="28" width="${probW}" height="18" rx="9" fill="#B42318"></rect>
      <text x="310" y="43">${pct(prob)}</text>
      <text x="0" y="78">Score Bayes</text>
      <rect x="0" y="88" width="300" height="18" rx="9" fill="#edf2ef"></rect>
      <rect x="0" y="88" width="${scoreW}" height="18" rx="9" fill="#1D4ED8"></rect>
      <text x="310" y="103">${decimal(score, 1)}</text>
      <text x="0" y="138">Intervalo posterior ratio: ${decimal(row.intervalo_bajo || 0, 2)}x a ${decimal(row.intervalo_alto || 0, 2)}x</text>
    </svg>`;
}

function posteriorCurve(row) {
  const ratio = Math.max(0.05, Number(row.ratio_observado || 1));
  const low = Math.max(0.05, Number(row.intervalo_bajo || 0.8));
  const high = Math.max(low + 0.05, Number(row.intervalo_alto || 1.2));
  const threshold = 1.5;
  const scale = (value) => Math.max(20, Math.min(330, 20 + (Math.min(value, 4) / 4) * 310));
  const points = Array.from({ length: 42 }, (_, i) => {
    const x = 20 + i * (320 / 41);
    const center = scale(Math.min(ratio, 4));
    const spread = Math.max(36, (scale(Math.min(high, 4)) - scale(Math.min(low, 4))) / 1.4);
    const y = 118 - 78 * Math.exp(-Math.pow((x - center) / spread, 2));
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  return `
    <svg class="posterior-curve" viewBox="0 0 360 150" role="img" aria-label="Curva posterior del ratio">
      <line x1="20" y1="118" x2="340" y2="118"></line>
      <rect x="${scale(threshold)}" y="24" width="${340 - scale(threshold)}" height="94" rx="6"></rect>
      <polyline points="${points}"></polyline>
      <circle cx="${scale(ratio)}" cy="42" r="5"></circle>
      <text x="20" y="138">1,0x</text>
      <text x="${scale(threshold) - 14}" y="138">1,5x</text>
      <text x="${Math.min(286, scale(ratio) + 8)}" y="38">${decimal(ratio, 2)}x observado</text>
      <text x="210" y="18">zona de revision</text>
    </svg>`;
}

function bayesProcess(row) {
  const evidence = Number(row.total_transacciones || 0);
  const entities = Number(row.total_entidades || 0);
  const shrink = evidence < 10 ? "alta" : evidence < 40 ? "media" : "baja";
  return `
    <div class="process-grid">
      <div><span>1</span><strong>Referencia</strong><p>${money(row.precio_mediano)} para codigo ${text(row.codigo_catalogo)} y unidad ${text(row.unidad)}.</p></div>
      <div><span>2</span><strong>Ratio</strong><p>${decimal(row.ratio_observado || 0, 2)}x = precio observado / referencia.</p></div>
      <div><span>3</span><strong>Incertidumbre</strong><p>${formatNumber(evidence)} transacciones y ${formatNumber(entities)} entidades comparables; contraccion ${shrink}.</p></div>
      <div><span>4</span><strong>Posterior</strong><p>${pct(row.prob_alta)} de superar 1,5x; intervalo ${decimal(row.intervalo_bajo || 0, 2)}x a ${decimal(row.intervalo_alto || 0, 2)}x.</p></div>
    </div>`;
}

function peerFigure(row) {
  const peers = [row].concat(detailPeers(row)).slice(0, 8);
  const max = Math.max(...peers.map((peer) => Number(peer.precio_promedio_ent || 0)), 1);
  return `
    <div class="peer-bars">
      ${peers.map((peer) => {
        const width = Math.max(3, (Number(peer.precio_promedio_ent || 0) / max) * 100);
        return `
          <div class="peer-bar-row">
            <span>${text(peer.entidad).slice(0, 42)}</span>
            <div class="peer-track"><div style="width:${width}%"></div></div>
            <strong>${money(peer.precio_promedio_ent)}</strong>
          </div>`;
      }).join("")}
    </div>`;
}

function populateFilters() {
  const priceRows = state.price || [];
  const allRows = priceRows.concat(state.concentration || []);
  setOptions("entityFilter", uniqueOptions(allRows, (row) => row.entidad, 300), "Todas");
  setOptions("providerFilter", uniqueOptions(allRows, (row) => row.proveedor, 300), "Todos");
  setOptions("unitFilter", uniqueOptions(priceRows, (row) => row.unidad, 120), "Todas");
  setOptions("categoryFilter", uniqueOptions(priceRows, codeCategory, 200), "Todos", (item) => `${item.value} (${formatNumber(item.count)})`);
  setOptions("yearFilter", uniqueOptions(priceRows, (row) => row.anios_observados || row.anio, 40), "Todos", (item) => `${item.value} (${formatNumber(item.count)})`);
  setOptions("monthFilter", uniqueOptions(priceRows, (row) => row.meses_observados, 12), "Todos", (item) => `${monthName(item.value)} (${formatNumber(item.count)})`);
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
  populateFilters();
  render();
  const detailHash = new URLSearchParams(window.location.search).get("detail");
  const tabParam = new URLSearchParams(window.location.search).get("tab");
  if (detailHash) openDetail(detailHash, false);
  else if (tabParam && $(`tab-${tabParam}`)) setTab(tabParam);
}

function filteredPrice() {
  const q = state.search.trim().toLowerCase();
  const product = state.product.trim().toLowerCase();
  return state.price.filter((row) => {
    const levelOk = state.level === "Todos" || row.nivel_bayes === state.level;
    if (!levelOk) return false;
    if (state.entity !== "Todos" && row.entidad !== state.entity) return false;
    if (state.provider !== "Todos" && row.proveedor !== state.provider) return false;
    if (state.unit !== "Todos" && row.unidad !== state.unit) return false;
    if (state.category !== "Todos" && codeCategory(row) !== state.category) return false;
    if (!rowHasValue(row, "anios_observados", state.year) && !rowHasValue(row, "anio", state.year)) return false;
    if (!rowHasValue(row, "meses_observados", state.month)) return false;
    if (product && ![row.articulo, row.codigo_catalogo, row.unidad].join(" ").toLowerCase().includes(product)) return false;
    if (!q) return true;
    return [row.articulo, row.entidad, row.proveedor, row.codigo_catalogo, row.ruc, row.unidad, row.rubro]
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
    if (state.entity !== "Todos" && row.entidad !== state.entity) return false;
    if (state.provider !== "Todos" && row.proveedor !== state.provider) return false;
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
    kpi("Licitaciones", formatNumber(c.licitaciones || 0), "convocatorias publicadas"),
    kpi("Items", formatNumber(c.items || 0), "detalle adjudicado"),
    kpi("Alertas precio", formatNumber(c.price_alerts || 0), "ranking publicado"),
    kpi("Concentracion", formatNumber(c.concentration_alerts || 0), "relaciones analizadas")
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
        <strong>${formatNumber(value)}</strong>
      </div>`;
  }).join("");
}

function renderLevelChart() {
  const levels = state.summary?.price_stats?.levels || {};
  const rows = Object.entries(levels).map(([level, count]) => ({ level, count }));
  renderBars($("levelChart"), rows, "level", "count", 8);
  $("priceCountLabel").textContent = `${formatNumber(state.price.length)} filas publicadas`;
}

function renderEntityChart() {
  renderBars($("entityChart"), state.series.top_entidades || [], "entidad", "cantidad", 8);
}

function badge(level) {
  const css = String(level || "Normal").split(" ")[0];
  return `<span class="badge ${text(css)}">${text(level)}</span>`;
}

function riskTone(level) {
  const clean = String(level || "Normal");
  if (clean === "Critico") return "critical";
  if (clean === "Alto") return "high";
  if (clean === "Moderado") return "medium";
  if (clean === "Verificar dato") return "verify";
  return "normal";
}

function meter(label, value, note, tone = "normal") {
  const width = Math.max(2, Math.min(100, Number(value || 0)));
  return `
    <div class="meter ${tone}">
      <div class="meter-head"><span>${text(label)}</span><strong>${text(note)}</strong></div>
      <div class="meter-track"><div style="width:${width}%"></div></div>
    </div>`;
}

function probabilityMeter(prob) {
  const n = Math.max(0, Math.min(1, Number(prob || 0)));
  return meter("Probabilidad", n * 100, pct(n), n > 0.98 ? "critical" : n > 0.85 ? "high" : "medium");
}

function scoreMeter(score) {
  const n = Math.max(0, Math.min(100, Number(score || 0)));
  return meter("Score", n, decimal(n, 1), n > 85 ? "critical" : n > 65 ? "high" : "medium");
}

function ratioMeter(ratio) {
  const n = Number(ratio || 0);
  const capped = Math.min(100, Math.max(2, (Math.min(n, 4) / 4) * 100));
  return meter("Ratio", capped, `${decimal(n, 2)}x`, n > 1.8 ? "critical" : n > 1.3 ? "high" : "normal");
}

function miniBayes(row) {
  const ratio = Number(row.ratio_observado || 0);
  const prob = Number(row.prob_alta || 0);
  const ratioX = Math.max(8, Math.min(94, (Math.min(ratio, 4) / 4) * 100));
  const probX = Math.max(8, Math.min(94, prob * 100));
  return `
    <div class="mini-bayes" aria-label="Resumen visual bayesiano">
      <div class="mini-axis">
        <span style="left:${ratioX}%"></span>
        <small>ratio ${decimal(ratio, 2)}x</small>
      </div>
      <div class="mini-axis posterior">
        <span style="left:${probX}%"></span>
        <small>posterior ${pct(prob)}</small>
      </div>
    </div>`;
}

function renderBayesInsights() {
  const rows = state.price || [];
  const levels = state.summary?.price_stats?.levels || {};
  const usable = Number(state.summary?.price_stats?.usable_rows || rows.length || 0);
  const verify = Number(levels["Verificar dato"] || 0);
  const critical = Number(levels.Critico || 0);
  const highPosterior = rows.filter((row) => Number(row.prob_alta || 0) > 0.95).length;
  const comparable = rows.filter((row) => !row.observacion_calidad).length;
  $("bayesInsightGrid").innerHTML = [
    ["Referencia limpia", `${formatNumber(comparable)} filas`, "codigo + unidad + pares entre entidades"],
    ["Contraccion Bayes", "ruido controlado", "poca evidencia vuelve hacia ratio 1"],
    ["Prob. > 1,5x", `${formatNumber(highPosterior)} publicadas`, "posterior alta para revision humana"],
    ["Verificar dato", formatNumber(verify), "posible unidad, lote o catalogo no comparable"],
    ["Critico", formatNumber(critical), "senal fuerte, no dictamen"],
    ["Base usable", formatNumber(usable), "filas con referencia calculable"]
  ].map(([label, value, note]) => `
    <div class="insight-card">
      <span>${text(label)}</span>
      <strong>${text(value)}</strong>
      <small>${text(note)}</small>
    </div>`).join("");
}

function renderCards() {
  const rows = filteredPrice().slice(0, 12);
  $("alertCards").innerHTML = rows.map((row) => `
    <article class="alert-card clickable-card ${riskTone(row.nivel_bayes)}" data-detail-hash="${text(row.hash_registro)}">
      <div class="card-top">${badge(row.nivel_bayes)}<span>#${formatNumber(row.rank || 0)}</span></div>
      <h3>${text(row.articulo)}</h3>
      <p>${text(row.entidad)}</p>
      <strong>${money(row.precio_promedio_ent)}</strong>
      ${miniBayes(row)}
      <p>${row.observacion_calidad ? text(row.observacion_calidad) : `Referencia: ${money(row.precio_mediano)} · Prob.: ${pct(row.prob_alta)}`}</p>
      <button class="text-btn" type="button" data-detail-hash="${text(row.hash_registro)}">Ver detalle</button>
    </article>
  `).join("");
}

function renderPriceTable() {
  const rows = filteredPrice();
  $("filteredPriceCount").textContent = `${formatNumber(rows.length)} resultados`;
  const maxRows = CONFIG.maxTableRows || 250;
  $("priceTable").innerHTML = rows.slice(0, maxRows).map((row) => `
    <tr class="clickable-row" data-detail-hash="${text(row.hash_registro)}">
      <td>${row.rank}</td>
      <td>${badge(row.nivel_bayes)}</td>
      <td>${probabilityMeter(row.prob_alta)}</td>
      <td>${scoreMeter(row.score_bayes)}</td>
      <td><strong>${text(row.articulo)}</strong><br><small>${text(row.codigo_catalogo)} · ${text(row.unidad)}</small></td>
      <td>${text(row.entidad)}</td>
      <td>${text(row.proveedor)}<br><small>${text(row.ruc)}</small></td>
      <td>${money(row.precio_promedio_ent)}</td>
      <td>${money(row.precio_mediano)}</td>
      <td>${ratioMeter(row.ratio_observado)}</td>
      <td>${text(row.observacion_calidad || row.referencia_usada || "")}</td>
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
  $("filteredConcCount").textContent = `${formatNumber(rows.length)} resultados`;
  $("concentrationList").innerHTML = rows.slice(0, 80).map((row) => `
    <article class="stack-item">
      ${badge(row.nivel_concentracion)}
      <strong>${text(row.entidad)}</strong>
      <p>${text(row.proveedor)} · ${text(row.ruc)}</p>
      <p>Score ${decimal(row.score_concentracion || 0, 1)} · Monto ${money(row.monto_total)} · Share ${pct(row.share_monto)}</p>
    </article>
  `).join("");
  renderNetwork();
}

function renderDetail(row) {
  const threshold = thresholdInfo(row);
  const peerCount = detailPeers(row).length;
  $("detailTitle").textContent = row.articulo || "Detalle del item";
  $("detailSubtitle").textContent = `${row.entidad || ""} · ${row.proveedor || ""}`;
  $("detailContent").innerHTML = `
    <div class="detail-grid">
      <section class="detail-card">
        ${badge(row.nivel_bayes)}
        <h3>Por que aparece en el ranking</h3>
        <p>${text(explainLevel(row))}</p>
        <div class="detail-kpis">
          ${kpi("Precio observado", money(row.precio_promedio_ent), "promedio entidad")}
          ${kpi("Referencia", money(row.precio_mediano), row.referencia_usada || "referencia comparable")}
          ${kpi("Ratio", `${decimal(row.ratio_observado || 0, 2)}x`, "precio / referencia")}
          ${kpi("Probabilidad", pct(row.prob_alta), "posterior > 1,5x")}
        </div>
      </section>

      <section class="detail-card">
        <h3>Comparacion visual</h3>
        ${priceBars(row)}
      </section>

      <section class="detail-card">
        <h3>Modelo bayesiano</h3>
        ${bayesProcess(row)}
        ${posteriorCurve(row)}
        ${posteriorFigure(row)}
      </section>

      <section class="detail-card">
        <h3>Semaforo normativo orientativo</h3>
        <p><strong>${text(threshold.status)}</strong></p>
        <p>Tipo usado: ${text(threshold.type)}. Umbral alto: ${decimal(threshold.upper, 2)}x. Umbral bajo: ${decimal(threshold.lower, 2)}x.</p>
        <p>Este semaforo no reemplaza el dictamen tecnico; sirve para orientar la revision junto con la senal bayesiana.</p>
      </section>
    </div>

    <section class="detail-card">
      <h3>Como se calculo</h3>
      <p>La lectura no sale de una regla fija. Primero se arma una referencia comparable, luego se calcula el ratio observado y despues el modelo ajusta la incertidumbre segun cuanta evidencia comparable existe.</p>
      <div class="formula-box">
        ratio = precio observado / referencia = ${money(row.precio_promedio_ent)} / ${money(row.precio_mediano)} = ${decimal(row.ratio_observado || 0, 2)}x
      </div>
      <div class="table-wrap mini">
        <table>
          <thead>
            <tr>
              <th>Variable</th>
              <th>Valor</th>
              <th>Lectura</th>
            </tr>
          </thead>
          <tbody>
            <tr><td>Codigo catalogo</td><td>${text(row.codigo_catalogo)}</td><td>Identifica la familia del bien, servicio u obra.</td></tr>
            <tr><td>Rubro</td><td>${text(row.rubro || "")}</td><td>Primeros digitos del codigo catalogo para filtrar familias amplias.</td></tr>
            <tr><td>Unidad</td><td>${text(row.unidad)}</td><td>La referencia se calcula evitando mezclar unidades distintas.</td></tr>
            <tr><td>Compras de la entidad</td><td>${formatNumber(row.cantidad_compras)}</td><td>Base observada para la entidad seleccionada.</td></tr>
            <tr><td>Entidades comparables</td><td>${formatNumber(row.total_entidades)}</td><td>Cantidad de entidades consideradas por la referencia comparable.</td></tr>
            <tr><td>Transacciones comparables</td><td>${formatNumber(row.total_transacciones)}</td><td>Volumen de evidencia publicado para esta referencia.</td></tr>
            <tr><td>Anios observados</td><td>${text(formatList(row.anios_observados))}</td><td>Fechas reales detectadas en el detalle de items.</td></tr>
            <tr><td>Meses observados</td><td>${text(formatList(row.meses_observados, monthName))}</td><td>Meses detectados para este item-entidad-unidad.</td></tr>
            <tr><td>Rango de fechas</td><td>${text(row.fecha_min_observada || "Sin dato")} a ${text(row.fecha_max_observada || "Sin dato")}</td><td>Cobertura temporal usada para filtros.</td></tr>
            <tr><td>Mediana original</td><td>${money(row.precio_mediano_original)}</td><td>Referencia anterior amplia; se conserva para auditoria.</td></tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="detail-card">
      <h3>Pares comparables publicados</h3>
      <p>Se muestran hasta 12 filas del ranking publicado con el mismo codigo de catalogo y unidad. No sustituye la reconstruccion completa desde CSV crudos.</p>
      ${peerFigure(row)}
      ${detailPeerRows(row)}
      <p class="muted-note">Pares publicados encontrados: ${formatNumber(peerCount)}.</p>
    </section>

    <section class="detail-card">
      <h3>Lectura responsable</h3>
      <p>La clasificacion prioriza revision humana. Antes de concluir sobreprecio, corresponde verificar pliego, especificaciones tecnicas, presentacion, calidad, marca/modelo, cantidades, forma de pago, plazo, servicios conexos e historial documental.</p>
      <p>${row.observacion_calidad ? text(row.observacion_calidad) : "No hay nota automatica de calidad para esta fila, pero la referencia sigue siendo estadistica y debe contrastarse documentalmente."}</p>
    </section>`;
}

function updateDetailUrl(hash) {
  if (!window.history?.pushState) return;
  const url = new URL(window.location.href);
  if (hash) url.searchParams.set("detail", hash);
  else url.searchParams.delete("detail");
  window.history.pushState({}, "", url.toString());
}

function openDetail(hash, pushUrl = true) {
  const row = state.price.find((item) => item.hash_registro === hash);
  if (!row) return;
  state.previousTab = state.tab === "detalle" ? state.previousTab : state.tab;
  state.selectedDetail = row;
  renderDetail(row);
  setTab("detalle");
  if (pushUrl) updateDetailUrl(hash);
  $("tab-detalle").scrollIntoView({ behavior: "smooth", block: "start" });
}

function closeDetail() {
  state.selectedDetail = null;
  updateDetailUrl("");
  setTab(state.previousTab || "resumen");
}

function renderDataQuality() {
  const q = state.summary?.quality || {};
  $("qualityList").innerHTML = Object.entries(q).map(([key, value]) => `
    <div class="definition-row"><span>${text(key)}</span><strong>${text(displayValue(value))}</strong></div>
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
    <div class="definition-row"><span>${text(key)}</span><strong>${formatNumber(value)}</strong></div>
  `).join("");
}

function renderBackup() {
  if (!isAdminVisitor()) {
    $("backupInfo").textContent = JSON.stringify({
      acceso: "solo_admin",
      admin: ADMIN_EMAIL,
      nota: "La planilla de respaldo no se muestra a visitantes. Drive mantiene permiso propietario para el admin."
    }, null, 2);
    return;
  }
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
  renderBayesInsights();
  renderLevelChart();
  renderEntityChart();
  renderCards();
  renderPriceTable();
  renderConcentration();
  renderDataQuality();
  renderBackup();
}

function rerenderFilteredViews() {
  renderCards();
  renderPriceTable();
  renderConcentration();
}

function setTab(tab) {
  if (tab === "respaldo" && !isAdminVisitor()) tab = "resumen";
  state.tab = tab;
  document.querySelectorAll(".nav-btn").forEach((btn) => btn.classList.toggle("active", btn.dataset.tab === tab));
  document.querySelectorAll(".tab-panel").forEach((panel) => panel.classList.toggle("active", panel.id === `tab-${tab}`));
}

function toCsv(rows) {
  const headers = Object.keys(rows[0] || {});
  const clean = (value) => {
    const normalized = typeof value === "number" ? String(value).replace(".", ",") : String(value ?? "");
    return `"${normalized.replace(/"/g, '""')}"`;
  };
  return [headers.join(";"), ...rows.map((row) => headers.map((key) => clean(row[key])).join(";"))].join("\n");
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

function getStoredVisitor() {
  try {
    return JSON.parse(localStorage.getItem(VISITOR_KEY) || "null");
  } catch {
    return null;
  }
}

function visitorId() {
  if (window.crypto?.randomUUID) return window.crypto.randomUUID();
  return `visit-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function visitorPayload(formData) {
  const now = new Date().toISOString();
  return {
    id_visita: visitorId(),
    fecha_hora: now,
    nombre: String(formData.get("visitorName") || "").trim(),
    correo: String(formData.get("visitorEmail") || "").trim(),
    institucion: String(formData.get("visitorInstitution") || "").trim(),
    motivo: String(formData.get("visitorPurpose") || "").trim(),
    app_version: CONFIG.appVersion || "0.1.7",
    data_version: state.summary?.data_version || "",
    pagina: window.location.href,
    user_agent: navigator.userAgent || "",
    origen: "GitHub Pages"
  };
}

function logVisit(visitor) {
  const endpoint = String(CONFIG.gasEndpoint || "").trim();
  if (!endpoint) return;
  const payload = {
    evento: "visit_register",
    usuario: visitor.correo || visitor.nombre || "visitante",
    modulo: "Registro visitas",
    detalle: `${visitor.nombre} - ${visitor.institucion} - ${visitor.motivo}`,
    observacion: "Registro minimo de visita sin contrasena",
    data_version: visitor.data_version || "",
    visitor
  };
  fetch(endpoint, {
    method: "POST",
    mode: "no-cors",
    body: JSON.stringify(payload)
  }).catch(() => {});
}

function setVisitorLabel(visitor) {
  if (!$("visitorLabel")) return;
  $("visitorLabel").textContent = visitor
    ? `${isAdminVisitor(visitor) ? "Admin" : "Visitante"}: ${visitor.nombre} - ${visitor.institucion}`
    : "Visitante no registrado";
}

function showVisitGate() {
  state.visitor = null;
  $("visitGate").hidden = false;
  $("appShell").hidden = true;
  setVisitorLabel(null);
  applyRole(null);
}

function showRegisteredApp(visitor) {
  state.visitor = visitor;
  $("visitGate").hidden = true;
  $("appShell").hidden = false;
  setVisitorLabel(visitor);
  applyRole(visitor);
}

function handleVisitSubmit(event) {
  event.preventDefault();
  const visitor = visitorPayload(new FormData(event.currentTarget));
  if (!visitor.nombre || !visitor.institucion || !visitor.motivo) {
    $("visitGateStatus").textContent = "Complete nombre, institucion y motivo de consulta.";
    return;
  }
  localStorage.setItem(VISITOR_KEY, JSON.stringify(visitor));
  $("visitGateStatus").textContent = "Registro guardado. Abriendo panel...";
  logVisit(visitor);
  showRegisteredApp(visitor);
  loadData().catch((error) => {
    $("loadStatus").textContent = "Error al cargar datos";
    console.error(error);
  });
}

async function updateVersion() {
  if ($("loadStatus")) $("loadStatus").textContent = "Actualizando version...";
  try {
    if ("caches" in window) {
      const keys = await caches.keys();
      await Promise.all(keys.filter((key) => key.includes("licitabayes")).map((key) => caches.delete(key)));
    }
    if ("serviceWorker" in navigator) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      await Promise.all(registrations.map((registration) => registration.update().catch(() => {})));
    }
  } catch (error) {
    console.warn("No se pudo limpiar cache de version", error);
  }
  const url = new URL(window.location.href);
  url.searchParams.set("v", Date.now().toString());
  window.location.replace(url.toString());
}

function bindEvents() {
  $("visitForm").addEventListener("submit", handleVisitSubmit);
  $("updateVersion").addEventListener("click", updateVersion);
  $("changeVisitor").addEventListener("click", () => {
    localStorage.removeItem(VISITOR_KEY);
    $("visitForm").reset();
    showVisitGate();
  });
  document.querySelectorAll(".nav-btn").forEach((btn) => btn.addEventListener("click", () => setTab(btn.dataset.tab)));
  $("alertCards").addEventListener("click", (event) => {
    const target = event.target.closest("[data-detail-hash]");
    if (target) openDetail(target.dataset.detailHash);
  });
  $("priceTable").addEventListener("click", (event) => {
    const target = event.target.closest("[data-detail-hash]");
    if (target) openDetail(target.dataset.detailHash);
  });
  $("closeDetail").addEventListener("click", closeDetail);
  $("downloadDetail").addEventListener("click", () => {
    if (!state.selectedDetail) return;
    download(`licitabayes_detalle_${state.selectedDetail.hash_registro}.json`, JSON.stringify({
      item: state.selectedDetail,
      pares_publicados: detailPeers(state.selectedDetail),
      semaforo_normativo: thresholdInfo(state.selectedDetail),
      explicacion: explainLevel(state.selectedDetail)
    }, null, 2), "application/json");
  });
  $("searchBox").addEventListener("input", (event) => {
    state.search = event.target.value;
    rerenderFilteredViews();
  });
  $("productBox").addEventListener("input", (event) => {
    state.product = event.target.value;
    rerenderFilteredViews();
  });
  [
    ["entityFilter", "entity"],
    ["categoryFilter", "category"],
    ["yearFilter", "year"],
    ["monthFilter", "month"],
    ["providerFilter", "provider"],
    ["unitFilter", "unit"]
  ].forEach(([id, key]) => {
    $(id).addEventListener("change", (event) => {
      state[key] = event.target.value;
      rerenderFilteredViews();
    });
  });
  document.querySelectorAll(".chip").forEach((btn) => btn.addEventListener("click", () => {
    document.querySelectorAll(".chip").forEach((chip) => chip.classList.remove("active"));
    btn.classList.add("active");
    state.level = btn.dataset.level;
    rerenderFilteredViews();
  }));
  $("clearFilters").addEventListener("click", () => {
    state.search = "";
    state.level = "Todos";
    state.entity = "Todos";
    state.product = "";
    state.category = "Todos";
    state.year = "Todos";
    state.month = "Todos";
    state.provider = "Todos";
    state.unit = "Todos";
    $("searchBox").value = "";
    $("productBox").value = "";
    ["entityFilter", "categoryFilter", "yearFilter", "monthFilter", "providerFilter", "unitFilter"].forEach((id) => {
      $(id).value = "Todos";
    });
    document.querySelectorAll(".chip").forEach((chip) => chip.classList.toggle("active", chip.dataset.level === "Todos"));
    rerenderFilteredViews();
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
const storedVisitor = getStoredVisitor();
if (storedVisitor?.nombre && storedVisitor?.institucion) {
  showRegisteredApp(storedVisitor);
  loadData().catch((error) => {
    $("loadStatus").textContent = "Error al cargar datos";
    console.error(error);
  });
} else {
  showVisitGate();
}

if ("serviceWorker" in navigator) {
  window.addEventListener("load", () => {
    navigator.serviceWorker.register("service-worker.js").catch(() => {});
  });
}
