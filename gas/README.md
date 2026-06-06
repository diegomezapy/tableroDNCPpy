# Apps Script LicitaBayes

## Instalacion

1. Crear un proyecto de Google Apps Script.
2. Copiar `Code.gs`, `Config.gs`, `SheetSchema.gs`, `Sync.gs` y `appsscript.json`.
3. Verificar en `Config.gs`:
   - `SPREADSHEET_ID`
   - `DEFAULT_DATA_BASE_URL`
4. Ejecutar `ensureSchema_()` una vez.
5. Ejecutar `syncFromGithub()` para cargar las alertas desde GitHub Pages.
6. Opcional: ejecutar `installHourlySyncTrigger()` para sincronizacion horaria.

## Planilla

La planilla operativa creada para este proyecto es:

https://docs.google.com/spreadsheets/d/1QJ_xagB5ze4ugYIpOosYPHBsp-o7TfQM8W8FUYSUnmQ/edit

## Seguridad

El flujo recomendado es `pull_from_github`: Apps Script lee los JSON publicados y los copia a Sheets. Asi no se expone un token en el frontend publico.
