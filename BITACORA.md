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

### Correccion de formato numerico y acceso

- Problema reportado: el boton `Entrar al panel` no funcionaba para el usuario.
- Correccion: la app ahora carga directamente el panel, sin pantalla de acceso intermedia.
- Problema reportado: montos como `G. 71.9 M` eran ambiguos e inconsistentes.
- Correccion: todos los montos visibles se muestran completos en guaranies con punto de miles y coma decimal, por ejemplo `G. 71.900.000`.
- Correccion adicional: scores y ratios usan coma decimal (`100,0`, `425,69x`) y los conteos usan punto de miles.
- Validacion local Playwright: panel visible al cargar, 250 filas en `Modelo Bayes`, primer monto `G. 90.200.000`, sin abreviaturas monetarias y sin errores de consola.
- Hoja de respaldo actualizada a `APP_VERSION=0.1.1`.
- Deployment Apps Script v0.1.1 creado: `AKfycbwROIhGtWqEc2UakPVrw1qsnazj9FT7PXhGqO1rEIfx84fmWBIwivEwtJpdiS1kef29zQ`.

### Rastreo de descarga de datos

- Se reviso `downloader.py`: la descarga configurada usa ZIP masivos OCDS de DNCP desde `https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/{file}`.
- Modulos rastreados: `ten-masivo.zip` para convocatorias, `awa-masivo.zip` para adjudicaciones y `con-masivo.zip` para contratos.
- Años por defecto del descargador: 2023, 2024 y 2025.
- Verificacion por `HEAD`: los 9 ZIP configurados para 2023-2025 respondieron `200`.
- Se reviso `processor.py`: el pipeline versionado reconstruye agregados basicos desde CSV, pero no reconstruye todos los caches avanzados del modelo bayesiano.
- Brecha detectada: `items_detalle.parquet`, `comparacion_precios.parquet`, `red_actores.parquet`, `catalogo_ruc.parquet` y `licitaciones_full.parquet` fueron agregados ya construidos en el commit inicial visible `c20af15`.
- Documento agregado: `docs/trazabilidad_datos.md`.
- Recomendacion pendiente: agregar generador reproducible de caches avanzados y manifiestos `data/download_manifest.json` y `cache/cache_manifest.json` con URL, fecha, bytes, SHA256, comando, version y cobertura temporal.

### Bitacora central espejo

- Se creo bitacora central del proyecto en `I:\Mi unidad\MANUAL_MAESTRO_FORMATOS_FUNCIONES_APPWEB\BITACORAS_PROYECTOS\BITACORA_LICITABAYES_DNCP_TABLERODNCPPY.md`.
- Se creo indice central en `I:\Mi unidad\MANUAL_MAESTRO_FORMATOS_FUNCIONES_APPWEB\BITACORAS_PROYECTOS\INDICE_BITACORAS.md`.
- Regla operativa nueva: desde este hito, cada proyecto debe mantener una bitacora local y una copia central claramente identificada en la carpeta maestra.

### Correccion de referencia estadistica de precios

- Problema reportado: los primeros resultados mostraban referencias evidentemente sospechosas, por ejemplo rastra aradora comparada contra una mediana de repuestos de G. 211.893.
- Diagnostico: el cache `comparacion_precios.parquet` usaba una mediana transaccional por catalogo que podia mezclar maquinas completas, repuestos, presentaciones y unidades.
- Diagnostico adicional: `items_detalle.parquet` tiene `anio = 2025` para todas las filas, pero las fechas reales incluyen varios anios y algunas fechas futuras; esto queda como senal de calidad a auditar.
- Correccion: `scripts/build_static_data.py` ahora usa una referencia comparable por codigo y unidad, calculada como mediana entre entidades.
- Correccion: si una unidad no tiene pares suficientes, o si el ratio sigue siendo extremo, el caso queda como `Verificar dato` con score y probabilidad bayesiana no accionables.
- Correccion visual: la app muestra `Referencia` en lugar de `Mediana` y evita mostrar `100%`, usando `>99,9%`.
- Version actualizada: `APP_VERSION=0.1.2`, `DATA_VERSION=dncp-cache-2025-ref-v2`.
- Validacion: `Rastra aradora`, `Papel de seguridad` y `Municipalidad de Eusebio Ayala / Mantenimiento de camino terraplenado` dejaron de aparecer en el ranking publicado como alertas bayesianas principales.
- Commit main: `e02f4e3`.
- Commit gh-pages: `272cdb9`.
- Deployment Apps Script v0.1.2 creado: `AKfycbxAO0hU6oscY3MEVrOTW1Nb5JPLq9d7X0GOVl-yB6mHLdL15F6vHnuURtmoFnqzzj00KQ`.
- Pendiente: reconstruir caches avanzados desde CSV crudos y agregar clustering o reglas semanticas de descripcion para mejorar comparabilidad fina.

### Estudio de referencias normativas sobre precios referenciales

- Se revisaron el compromiso OGP PY0041, la Resolucion DNCP 454/2024 y la Resolucion DNCP 1890/2020.
- Hallazgo: la mediana historica no debe tratarse como precio referencial definitivo; debe ser solo un insumo con trazabilidad, comparabilidad y nivel de confianza.
- Ideas incorporables: minimo tres precios documentados, combinacion de dos o mas fuentes, caracteristicas similares, separacion de servicios conexos, ajuste por IPC, ajuste por plazo/forma de pago y umbrales normativos de revision.
- Documento agregado: `docs/referencias_normativas_precios.md`.
- Propuesta tecnica: crear `cache/referencias_precio.parquet` y `data/reference_quality.json` con confianza de referencia, fuentes, dispersion, reglas de comparabilidad y semaforo normativo.

### Filtros avanzados del panel

- Problema reportado: el panel de filtros necesitaba filtros por entidad/institucion, anio, mes, articulo/producto y rubro.
- Correccion: se agregaron filtros laterales por entidad o institucion, articulo/producto, rubro/codigo catalogo, anio, mes, proveedor, unidad y nivel de senal.
- Correccion tecnica: `price_alerts.json` ahora incluye `anios_observados`, `meses_observados`, `fecha_min_observada`, `fecha_max_observada` y `rubro`.
- Criterio: anio y mes se aplican al ranking de precios usando fechas observadas del detalle de items; concentracion aplica entidad, proveedor, nivel y texto, porque no tiene periodo confiable en el cache actual.
- Version actualizada: `APP_VERSION=0.1.3`, `DATA_VERSION=dncp-cache-2025-ref-v3`.
- Commit main: `18e807b`.
- Commit gh-pages: `564c824`.
- Deployment Apps Script v0.1.3 creado: `AKfycbzcb_DuKJR_CXAkkQrgaBJWkaxoC1CKN4YL39VI-sa9ckNkRVPepESmQaAE6bDN8m4nXg`.

### Ficha explicativa por item

- Problema reportado: el usuario necesita hacer clic en cualquier item y ver una vista detallada que explique como se obtuvieron los calculos.
- Correccion: cada tarjeta y cada fila del ranking de precios abre una ficha explicativa.
- La ficha muestra formula de ratio, precio observado, referencia, probabilidad posterior, score, intervalo posterior, semaforo normativo orientativo, tabla de variables, pares comparables publicados y lectura responsable.
- Se agrego enlace directo por `?detail=<hash_registro>` para compartir o validar una ficha especifica.
- Version actualizada: `APP_VERSION=0.1.4`; `DATA_VERSION` se mantiene en `dncp-cache-2025-ref-v3`.
- Commit main: `8cd9ae3`.
- Commit gh-pages: `4f76762`.
- Deployment Apps Script v0.1.4 creado: `AKfycbzF0hj24Wyw10yIse0QpS1UqYcYdhgpBDNeWWa8kHpTN2qzbzDeeBVFtUJscB4bs5wVaQ`.

### Registro minimo obligatorio de visita

- Problema reportado: la app no pedia ningun registro al usuario antes de entrar al panel.
- Decision tecnica: exigir un registro minimo de visita en vez de contrasena, porque GitHub Pages es frontend estatico y una contrasena real requiere backend con sesiones y verificacion del lado servidor.
- Correccion: se agrego pantalla inicial obligatoria con nombre y apellido, institucion u organizacion, motivo de consulta, correo opcional y aceptacion de trazabilidad.
- Correccion: el panel queda oculto hasta que exista visitante registrado; se agrego regla CSS `[hidden]` para evitar que el layout se muestre debajo del formulario.
- Correccion: el visitante se guarda en `localStorage` para continuidad de sesion local y puede cambiarse desde el panel.
- Correccion adicional: se agrego boton `Actualizar version` para limpiar caches `licitabayes`, pedir actualizacion del service worker y recargar con parametro de version.
- Respaldo operativo: Apps Script incorpora hoja `VISITAS` con `id_visita`, `fecha_hora`, `nombre`, `correo`, `institucion`, `motivo`, `app_version`, `data_version`, `pagina`, `user_agent`, `origen` y `raw_json`.
- Version actualizada: `APP_VERSION=0.1.5`; `DATA_VERSION` se mantiene en `dncp-cache-2025-ref-v3`.
- Validacion ejecutada: `node --check assets/app.js`.
- Validacion ejecutada: `py -3 -m py_compile scripts/build_static_data.py dashboard.py downloader.py processor.py`.
- Regeneracion ejecutada: `py -3 scripts/build_static_data.py`.
- Validacion local Playwright: primer acceso muestra solo el registro obligatorio; con visitante guardado carga KPIs y alertas de la version `0.1.5`.
- Commit main de implementacion: `3e9d7b7`.
- Commit main de configuracion final: `283601b`.
- Commit gh-pages publicado: `6bd483f`.
- Deployment Apps Script v0.1.5 creado: `AKfycbyF7Z-QBLhtTnnezx8AJBLGbudPkQbGpEbjbBPjMxfYEW3i0NQMu7iZZi2peIIHdbuOgA`.
- Verificacion GAS: `GET /exec` del deployment v0.1.5 respondio `403 Prohibido`.
- Pendiente GAS: autorizar manualmente el endpoint si Google mantiene bloqueo 403 antes de escribir en la hoja `VISITAS`.

### Boton de actualizacion de version y cache

- Objetivo: evitar que usuarios queden viendo una version vieja por cache del navegador, GitHub Pages o service worker.
- Correccion: se agrego boton `Actualizar version` en el panel de estado.
- Funcionamiento: borra caches propios que contengan `licitabayes`, solicita actualizacion de service workers registrados y recarga la URL con parametro `v=<timestamp>`.
- Criterio: no borra visitante registrado, borradores ni otros datos locales criticos.
- Version actualizada: `APP_VERSION=0.1.6`; cache del service worker `licitabayes-dncp-v0-1-6`.
- README actualizado con el comportamiento del boton.
- Regeneracion ejecutada: `py -3 scripts/build_static_data.py`.
- Validacion ejecutada: `node --check assets/app.js`.
- Validacion ejecutada: `py -3 -m py_compile scripts/build_static_data.py dashboard.py downloader.py processor.py`.
- Commit main: `d0105f9`.
- Commit gh-pages: `44c4cb0`.
- Deployment Apps Script v0.1.6 creado: `AKfycbykTsry-BM_EVS5lhyt72BKMDJ4ayHEuGyIDzEYhhVXjjNqgeKxi0PsWafdS5_a0Cyc0w`.
- Verificacion GAS: `GET /exec` del deployment v0.1.6 respondio `403 Prohibido`.
- Pendiente GAS: autorizar manualmente el endpoint si Google mantiene bloqueo 403.

### Vista admin de respaldo y mejora visual Bayes

- Problema reportado: la hoja de respaldo solo debe poder verla el admin `dmeza.py@gmail.com`.
- Verificacion Drive: metadata de la planilla `1QJ_xagB5ze4ugYIpOosYPHBsp-o7TfQM8W8FUYSUnmQ` muestra permiso unico de propietario para `dmeza.py@gmail.com`.
- Correccion UI: el modulo `Respaldo` y los enlaces `Hoja respaldo` / `Abrir planilla` quedan ocultos para visitantes no admin.
- Correccion UI: los enlaces a Google Sheets ya no quedan como URL directa en HTML; se hidratan por JS solo si el visitante registrado usa el correo admin.
- Criterio de seguridad: la restriccion real es Drive; el control frontend evita exposicion visual, pero no sustituye permisos del archivo.
- Problema reportado: la app se sentia aburrida y no era clara la aplicacion bayesiana.
- Mejora: se agrego bloque `Motor Bayes` en resumen con flujo referencia -> ratio -> contraccion -> posterior.
- Mejora: tarjetas principales incorporan medidores de ratio y posterior, borde por nivel de riesgo y ranking visible.
- Mejora: tabla de precios incorpora medidores visuales para probabilidad, score y ratio.
- Mejora: ficha de detalle agrega proceso bayesiano paso a paso, curva posterior, zona de revision y explicacion textual de incertidumbre.
- Version actualizada: `APP_VERSION=0.1.7`; cache del service worker `licitabayes-dncp-v0-1-7`.
- Regeneracion ejecutada: `py -3 scripts/build_static_data.py`.
- Validacion ejecutada: `node --check assets/app.js`.
- Validacion ejecutada: `py -3 -m py_compile scripts/build_static_data.py dashboard.py downloader.py processor.py`.
- Validacion local Playwright: visitante comun no ve `Respaldo`; admin `dmeza.py@gmail.com` ve `Respaldo` y enlace Sheets hidratado.
- Validacion local Playwright: ficha de detalle carga `.process-grid` con explicacion Bayes.
- Commit main: `ad83dd5`.
- Commit gh-pages: `ed151cb`.
- Deployment Apps Script v0.1.7 creado: `AKfycbxvAUgPdUTrzv66QBA79BMoUmvvvWOQP4O1DPf-zkF8u93GF35ZcCvZSrk3DIa8Omf8cA`.
- Verificacion GAS: `GET /exec` del deployment v0.1.7 respondio `403 Prohibido`.
- Pendiente GAS: autorizar manualmente el endpoint si Google mantiene bloqueo 403.

### Metodologia ampliada con formulas y bibliografia

- Problema reportado: la vista `Metodologia` estaba demasiado pobre y no explicaba autores, formulas ni fundamentos.
- Correccion: se reemplazo la vista metodologica por un manual tecnico dentro de la app.
- Contenido agregado: fundamento bayesiano, formula de Bayes, referencia comparable, ratio observado, escala logaritmica, contraccion empirical Bayes, probabilidad posterior, intervalo posterior, concentracion entidad-proveedor, niveles de senal y limitaciones.
- Bibliografia agregada en UI: Bayes (1763), Efron y Morris (1972/1973), Gelman et al. `Bayesian Data Analysis`, Robert `The Bayesian Choice` y referencia Dirichlet-multinomial.
- Mejora tecnica: se agrego deep link por `?tab=metodologia` para compartir/probar la vista.
- Version actualizada: `APP_VERSION=0.1.8`; cache del service worker `licitabayes-dncp-v0-1-8`.
- Regeneracion ejecutada: `py -3 scripts/build_static_data.py`.
- Validacion ejecutada: `node --check assets/app.js`.
- Validacion ejecutada: `py -3 -m py_compile scripts/build_static_data.py dashboard.py downloader.py processor.py`.
- Validacion local Playwright: capturas desktop y movil de `?tab=metodologia` cargaron referencias y formulas sin solapamientos.
- Commit main: `dd1c0ce`.
- Commit gh-pages: `a7d5099`.
- Deployment Apps Script v0.1.8 creado: `AKfycbwVWTZi6vCVBJMDpGbJb4DShxxH2GrsB0MFONtRQgal7AMrZscaiXC_IpTRm9m9QI648A`.
- Verificacion GAS: `GET /exec` del deployment v0.1.8 respondio `403 Prohibido`.
- Pendiente GAS: autorizar manualmente el endpoint si Google mantiene bloqueo 403.

### Boton obligatorio Actualizar version

- Problema reportado: la app no tenia boton visible para actualizar la version.
- Correccion: se agrego boton `Actualizar version` en la caja lateral de estado.
- Comportamiento: limpia caches propios que contengan `licitabayes`, solicita actualizacion del service worker y recarga con parametro `v=<timestamp>`.
- Criterio: no borra el visitante registrado ni datos locales criticos, para no romper trazabilidad ni continuidad de uso.
- Version actualizada: `APP_VERSION=0.1.6`; `DATA_VERSION` se mantiene en `dncp-cache-2025-ref-v3`.
- Manual maestro actualizado: se agrego regla obligatoria para boton `Actualizar version` en toda app web publicada.
