function doGet() {
  const ss = ensureSchema_();
  const cfg = getConfigMap_(ss);
  return json_({
    success: true,
    app: APP_NAME,
    version: APP_VERSION,
    spreadsheet_id: SPREADSHEET_ID,
    last_sync: cfg.LAST_SYNC || '',
    data_base_url: cfg.DATA_BASE_URL || DEFAULT_DATA_BASE_URL
  });
}

function doPost(e) {
  const ss = ensureSchema_();
  const now = new Date().toISOString();
  try {
    const payload = e && e.postData && e.postData.contents ? JSON.parse(e.postData.contents) : {};
    if (API_TOKEN && payload.token !== API_TOKEN) {
      throw new Error('Token invalido');
    }
    if (payload.evento === 'visit_register' && payload.visitor) {
      appendVisit_(ss, payload.visitor);
    }
    appendLog_(ss, payload.evento || 'client_event', payload.usuario || 'web', payload.modulo || 'Frontend', payload.detalle || '', APP_VERSION, payload.data_version || '', 'Web', payload.github_commit || '', 'success', payload.observacion || '', payload);
    return json_({ success: true, logged_at: now });
  } catch (err) {
    appendError_(ss, 'Code', 'doPost', 'No se pudo registrar evento del cliente', err, {}, now);
    return json_({ success: false, error: String(err) });
  }
}

function appendVisit_(ss, visitor) {
  ss.getSheetByName('VISITAS').appendRow([
    visitor.id_visita || Utilities.getUuid(),
    visitor.fecha_hora || new Date().toISOString(),
    visitor.nombre || '',
    visitor.correo || '',
    visitor.institucion || '',
    visitor.motivo || '',
    visitor.app_version || APP_VERSION,
    visitor.data_version || '',
    visitor.pagina || '',
    visitor.user_agent || '',
    visitor.origen || 'Web',
    JSON.stringify(visitor).slice(0, 45000)
  ]);
}

function installHourlySyncTrigger() {
  ScriptApp.newTrigger('syncFromGithub').timeBased().everyHours(1).create();
}

function json_(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
