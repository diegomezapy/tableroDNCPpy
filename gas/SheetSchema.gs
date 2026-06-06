const SHEET_HEADERS = {
  CONFIG: ['parametro', 'valor', 'descripcion', 'activo'],
  RUNS_MODELO: ['run_id', 'fecha_hora', 'app_version', 'data_version', 'fuente', 'total_licitaciones', 'total_items', 'total_alertas', 'total_concentracion', 'modelo_precio', 'modelo_concentracion', 'observacion', 'github_commit', 'github_pages_url', 'estado', 'usuario'],
  ALERTAS_BAYES: ['run_id', 'rank', 'nivel_bayes', 'prob_alta', 'score_bayes', 'codigo_catalogo', 'articulo', 'unidad', 'entidad', 'proveedor', 'ruc', 'precio_promedio_ent', 'precio_mediano', 'ratio_observado', 'intervalo_bajo', 'intervalo_alto', 'cantidad_compras', 'total_entidades', 'total_transacciones', 'anio', 'fuente_json', 'observacion', 'fecha_sync', 'hash_registro'],
  CONCENTRACION: ['run_id', 'rank', 'nivel_concentracion', 'prob_concentracion', 'score_concentracion', 'entidad', 'proveedor', 'ruc', 'contratos', 'monto_total', 'share_monto', 'share_contratos', 'proveedores_entidad', 'monto_entidad', 'fuente_json', 'observacion', 'fecha_sync', 'hash_registro'],
  SNAPSHOTS: ['snapshot_id', 'fecha_hora', 'tipo', 'total_filas', 'url_fuente', 'payload_json', 'observacion', 'app_version', 'data_version', 'estado'],
  LOG: ['fecha_hora', 'evento', 'usuario', 'modulo', 'detalle', 'app_version', 'data_version', 'origen', 'github_commit', 'resultado', 'observacion', 'raw_json'],
  ERRORES: ['fecha_hora', 'modulo', 'accion', 'mensaje_usuario', 'detalle_tecnico', 'payload_json', 'app_version', 'data_version', 'origen', 'estado', 'resuelto', 'observacion'],
  VERSIONES: ['fecha_hora', 'app_version', 'data_version', 'github_commit', 'cambio', 'fuente_datos', 'modelo_precio', 'modelo_concentracion', 'sheets_schema_version', 'observacion']
};

function ensureSchema_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID);
  Object.keys(SHEET_HEADERS).forEach((name) => {
    let sh = ss.getSheetByName(name);
    if (!sh) sh = ss.insertSheet(name);
    const headers = SHEET_HEADERS[name];
    sh.getRange(1, 1, 1, headers.length).setValues([headers]);
    sh.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#eeeeee');
    sh.setFrozenRows(1);
    if (!sh.getFilter()) {
      sh.getRange(1, 1, Math.max(sh.getMaxRows(), 2), headers.length).createFilter();
    }
  });
  seedConfig_(ss);
  return ss;
}

function seedConfig_(ss) {
  const sh = ss.getSheetByName('CONFIG');
  const current = getConfigMap_(ss);
  const defaults = [
    ['APP_NAME', APP_NAME, 'Nombre de la aplicacion', 'TRUE'],
    ['APP_VERSION', APP_VERSION, 'Version del frontend/modelo', 'TRUE'],
    ['SHEET_ID', SPREADSHEET_ID, 'Planilla de respaldo operativo', 'TRUE'],
    ['DATA_BASE_URL', DEFAULT_DATA_BASE_URL, 'Base URL de JSON publicados', 'TRUE'],
    ['ALERT_LIMIT', DEFAULT_ALERT_LIMIT, 'Cantidad maxima de alertas copiadas a Sheets', 'TRUE'],
    ['LAST_SYNC', '', 'Ultima sincronizacion Apps Script', 'TRUE']
  ];
  const missing = defaults.filter((row) => !current[row[0]]);
  if (missing.length) sh.getRange(sh.getLastRow() + 1, 1, missing.length, 4).setValues(missing);
}

function getConfigMap_(ss) {
  const sheet = ss.getSheetByName('CONFIG');
  if (!sheet || sheet.getLastRow() < 2) return {};
  const values = sheet.getRange(2, 1, sheet.getLastRow() - 1, 4).getValues();
  return values.reduce((acc, row) => {
    if (row[0]) acc[String(row[0])] = row[1];
    return acc;
  }, {});
}

function setConfigValue_(ss, key, value) {
  const sheet = ss.getSheetByName('CONFIG');
  const values = sheet.getRange(1, 1, Math.max(sheet.getLastRow(), 1), 1).getValues();
  for (let i = 0; i < values.length; i++) {
    if (String(values[i][0]) === key) {
      sheet.getRange(i + 1, 2).setValue(value);
      return;
    }
  }
  sheet.appendRow([key, value, 'Agregado por Apps Script', 'TRUE']);
}
