function syncFromGithub() {
  const ss = ensureSchema_();
  const cfg = getConfigMap_(ss);
  const baseUrl = String(cfg.DATA_BASE_URL || DEFAULT_DATA_BASE_URL).replace(/\/$/, '');
  const limit = Number(cfg.ALERT_LIMIT || DEFAULT_ALERT_LIMIT);
  const now = new Date().toISOString();

  try {
    const summary = fetchJson_(baseUrl + '/model_summary.json');
    const pricePayload = fetchJson_(baseUrl + '/price_alerts.json');
    const concentrationPayload = fetchJson_(baseUrl + '/concentration_alerts.json');
    const runId = summary.run_id || pricePayload.run_id || Utilities.getUuid();
    const dataVersion = summary.data_version || '';
    const priceRows = (pricePayload.rows || []).slice(0, limit).map((row) => mapPriceRow_(runId, row, now));
    const concentrationRows = (concentrationPayload.rows || []).slice(0, limit).map((row) => mapConcentrationRow_(runId, row, now));

    replaceRows_(ss.getSheetByName('ALERTAS_BAYES'), priceRows, SHEET_HEADERS.ALERTAS_BAYES.length);
    replaceRows_(ss.getSheetByName('CONCENTRACION'), concentrationRows, SHEET_HEADERS.CONCENTRACION.length);
    appendRun_(ss, summary, priceRows.length, concentrationRows.length, now);
    appendSnapshot_(ss, runId, 'model_summary', 1, baseUrl + '/model_summary.json', summary, now);
    setConfigValue_(ss, 'LAST_SYNC', now);
    appendLog_(ss, 'sync_success', 'Apps Script', 'Sync', 'Sincronizacion desde GitHub Pages', APP_VERSION, dataVersion, baseUrl, '', 'success', '', summary);

    return {
      success: true,
      run_id: runId,
      price_rows: priceRows.length,
      concentration_rows: concentrationRows.length,
      synced_at: now
    };
  } catch (err) {
    appendError_(ss, 'Sync', 'syncFromGithub', 'No se pudo sincronizar desde GitHub Pages', err, { baseUrl: baseUrl }, now);
    throw err;
  }
}

function fetchJson_(url) {
  const res = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: { Accept: 'application/json' }
  });
  const code = res.getResponseCode();
  const text = res.getContentText();
  if (code < 200 || code >= 300) {
    throw new Error('HTTP ' + code + ' al leer ' + url + ': ' + text.slice(0, 200));
  }
  return JSON.parse(text);
}

function replaceRows_(sheet, rows, width) {
  const last = sheet.getLastRow();
  if (last > 1) sheet.getRange(2, 1, last - 1, width).clearContent();
  if (rows.length) sheet.getRange(2, 1, rows.length, width).setValues(rows);
}

function mapPriceRow_(runId, row, now) {
  return [
    runId,
    row.rank || '',
    row.nivel_bayes || '',
    row.prob_alta || 0,
    row.score_bayes || 0,
    row.codigo_catalogo || '',
    row.articulo || '',
    row.unidad || '',
    row.entidad || '',
    row.proveedor || '',
    row.ruc || '',
    row.precio_promedio_ent || 0,
    row.precio_mediano || 0,
    row.ratio_observado || 0,
    row.intervalo_bajo || 0,
    row.intervalo_alto || 0,
    row.cantidad_compras || 0,
    row.total_entidades || 0,
    row.total_transacciones || 0,
    row.anio || '',
    'price_alerts.json',
    'Senal estadistica para revision',
    now,
    row.hash_registro || ''
  ];
}

function mapConcentrationRow_(runId, row, now) {
  return [
    runId,
    row.rank || '',
    row.nivel_concentracion || '',
    row.prob_concentracion || 0,
    row.score_concentracion || 0,
    row.entidad || '',
    row.proveedor || '',
    row.ruc || '',
    row.contratos || 0,
    row.monto_total || 0,
    row.share_monto || 0,
    row.share_contratos || 0,
    row.proveedores_entidad || 0,
    row.monto_entidad || 0,
    'concentration_alerts.json',
    'Senal estadistica para revision',
    now,
    row.hash_registro || ''
  ];
}

function appendRun_(ss, summary, priceCount, concentrationCount, now) {
  const c = summary.counts || {};
  const models = summary.models || {};
  ss.getSheetByName('RUNS_MODELO').appendRow([
    summary.run_id || '',
    now,
    summary.app_version || APP_VERSION,
    summary.data_version || '',
    summary.source_url || '',
    c.licitaciones || 0,
    c.items || 0,
    priceCount,
    concentrationCount,
    models.price || '',
    models.concentration || '',
    summary.responsible_notice || '',
    '',
    summary.github_pages_url || '',
    'SINCRONIZADO',
    Session.getEffectiveUser().getEmail()
  ]);
}

function appendSnapshot_(ss, id, type, rows, url, payload, now) {
  ss.getSheetByName('SNAPSHOTS').appendRow([
    id,
    now,
    type,
    rows,
    url,
    JSON.stringify(payload).slice(0, 45000),
    'Snapshot resumido',
    APP_VERSION,
    payload.data_version || '',
    'OK'
  ]);
}

function appendLog_(ss, evento, usuario, modulo, detalle, appVersion, dataVersion, origen, commit, resultado, observacion, raw) {
  ss.getSheetByName('LOG').appendRow([
    new Date().toISOString(),
    evento,
    usuario,
    modulo,
    detalle,
    appVersion,
    dataVersion,
    origen,
    commit,
    resultado,
    observacion,
    raw ? JSON.stringify(raw).slice(0, 45000) : ''
  ]);
}

function appendError_(ss, modulo, accion, mensaje, err, payload, now) {
  ss.getSheetByName('ERRORES').appendRow([
    now,
    modulo,
    accion,
    mensaje,
    err && err.stack ? err.stack : String(err),
    JSON.stringify(payload || {}).slice(0, 45000),
    APP_VERSION,
    '',
    'Apps Script',
    'ABIERTO',
    'FALSE',
    ''
  ]);
}
