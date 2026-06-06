# Apps Script LicitaBayes

## Instalacion

Proyecto Apps Script asociado al libro:

https://script.google.com/d/1DUbhm-6S67wk4NYbL_nAkWa5wlXjz4GaemSdbsz2oEPn1Rii2qmKLIs4/edit

Deployment activo:

https://script.google.com/macros/s/AKfycbzweRdG2sBKnIDSnj7GFaBRT3YAeKgOylce8CXuHZ5bH-tNltwHpweT1AB1K7VGKhmb/exec

1. Abrir el proyecto Apps Script.
2. Autorizar manualmente `ensureSchema_()` o `syncFromGithub()` la primera vez.
3. Verificar en `Config.gs`:
   - `SPREADSHEET_ID`
   - `DEFAULT_DATA_BASE_URL`
4. Ejecutar `ensureSchema_()` una vez.
5. Ejecutar `syncFromGithub()` para cargar las alertas desde GitHub Pages.
6. Opcional: ejecutar `installHourlySyncTrigger()` para sincronizacion horaria.

## Planilla

La planilla operativa completa creada para este proyecto es:

https://docs.google.com/spreadsheets/d/1QJ_xagB5ze4ugYIpOosYPHBsp-o7TfQM8W8FUYSUnmQ/edit

## Seguridad

El flujo recomendado es `pull_from_github`: Apps Script lee los JSON publicados y los copia a Sheets. Asi no se expone un token en el frontend publico.
