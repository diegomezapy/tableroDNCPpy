# Bitacora

## 2026-06-06

- Objetivo: convertir `tableroDNCPpy` en app web estatica LicitaBayes DNCP, sin Streamlit.
- Repositorio base: `https://github.com/diegomezapy/tableroDNCPpy`.
- Cambio de arquitectura: GitHub Pages + JSON estaticos + Google Sheets + Apps Script.
- Planilla creada: `LicitaBayes DNCP - Respaldo operativo completo`.
- Spreadsheet ID: `1QJ_xagB5ze4ugYIpOosYPHBsp-o7TfQM8W8FUYSUnmQ`.
- Se agregaron tabs: CONFIG, RUNS_MODELO, ALERTAS_BAYES, CONCENTRACION, SNAPSHOTS, LOG, ERRORES, VERSIONES.
- Se agrego `scripts/build_static_data.py`.
- Se genero `data/model_summary.json`, `data/price_alerts.json`, `data/concentration_alerts.json`, `data/series.json`.
- Se reemplazo el uso operativo de Streamlit por `index.html`, `assets/`, `manifest.json` y `service-worker.js`.
- Se agrego Apps Script en `gas/` para sincronizar desde GitHub Pages hacia Google Sheets.
- Validacion ejecutada: `py -3 -m py_compile dashboard.py downloader.py processor.py scripts/build_static_data.py`.
- Validacion local HTTP: `http://127.0.0.1:8766/` respondio 200 para HTML, JS, service worker y `data/model_summary.json`.
- Validacion Playwright: app renderizo 4 KPIs, 12 tarjetas, 250 filas en `Modelo Bayes` y sin errores de consola.
- Se importo respaldo completo a Google Sheets con 1000 alertas de precio y 1000 relaciones de concentracion.
- Commit publicado en main: `feeef0b`.
- Rama `gh-pages` publicada con commit estatico: `e5010b6`.
- URL publica verificada: `https://diegomezapy.github.io/tableroDNCPpy/`.
- Validacion publica Playwright: 4 KPIs, 250 filas en `Modelo Bayes` y sin errores de consola.
- Proyecto Apps Script creado con `clasp`: `1RyCnpi2EgClc8HDbHnnEFJ6cU5nKyOW82lIWBXZYBOfbnVA5YIEk4iD1`.
- Deployment Apps Script creado: `AKfycbwKV8HoLAK336enxHmcYOKoF45-1c2H6SHFV7_UY_No-3dxEfAPGzPzVgEcI7YwOWVIdg`.
- Pendiente GAS: primera autorizacion manual desde editor Apps Script; el endpoint devuelve 403 hasta autorizar/aclarar acceso.
