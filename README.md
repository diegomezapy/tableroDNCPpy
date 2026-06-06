# LicitaBayes DNCP

App web estatica para explorar senales bayesianas en datos abiertos de contrataciones publicas de Paraguay.

La app no usa Streamlit. Se publica como sitio estatico en GitHub Pages y usa Google Sheets + Apps Script como respaldo operativo.

## Objetivo

Priorizar revision humana sobre:

- precios institucionales atipicos por item de catalogo;
- concentracion entidad-proveedor;
- calidad y cobertura de datos usados por el modelo.
- exploracion filtrable por entidad, anio, mes observado, articulo, rubro, proveedor, unidad y nivel de senal.
- fichas explicativas clicables para entender cada clasificacion, con formula, tablas, graficos, pares comparables y lectura responsable.
- registro minimo obligatorio de visita antes de entrar al panel.

Las alertas son senales estadisticas exploratorias. No constituyen denuncia, prueba de irregularidad ni dictamen legal.

## Arquitectura

```text
index.html              App web estatica
assets/                 CSS, JS y configuracion publica
data/                   JSON publicados para GitHub Pages
cache/                  Parquet procesados desde datos DNCP
scripts/                Generadores reproducibles
gas/                    Google Apps Script para respaldo en Sheets
docs/                   Manuales y metodologia
```

## Datos

Fuente: DNCP Paraguay, datos abiertos OCDS.

URL: https://contrataciones.gov.py/datos

Los Parquet incluidos en `cache/` alimentan el generador:

```bash
py -3 scripts/build_static_data.py
```

Salidas publicadas:

```text
data/model_summary.json
data/price_alerts.json
data/concentration_alerts.json
data/series.json
```

## Modelos MVP

### Precio esperado

Empirical Bayes log-normal sobre el ratio:

```text
precio promedio entidad / referencia comparable por item y unidad
```

El modelo contrae resultados hacia ratio 1 cuando hay poca evidencia y calcula probabilidad posterior de precio alto. Los casos con unidad sin pares suficientes o ratios extremos se separan como `Verificar dato` para evitar tratar posibles errores de catalogacion como alertas bayesianas.

### Concentracion

Suavizado tipo Dirichlet sobre participacion entidad-proveedor, combinando monto, cantidad de contratos y cantidad de proveedores por entidad.

## Google Sheets

Planilla de respaldo:

https://docs.google.com/spreadsheets/d/1QJ_xagB5ze4ugYIpOosYPHBsp-o7TfQM8W8FUYSUnmQ/edit

Apps Script:

```text
gas/Code.gs
gas/Config.gs
gas/SheetSchema.gs
gas/Sync.gs
```

Proyecto Apps Script creado:

```text
https://script.google.com/d/1RyCnpi2EgClc8HDbHnnEFJ6cU5nKyOW82lIWBXZYBOfbnVA5YIEk4iD1/edit
```

Despues de publicar GitHub Pages, ejecutar en Apps Script:

```text
ensureSchema_()
syncFromGithub()
```

Nota: Google puede exigir la primera autorizacion desde el editor Apps Script antes de permitir ejecucion web publica.

## Registro de visita

El acceso al panel exige nombre, institucion u organizacion, motivo de consulta y aceptacion de trazabilidad. El correo es opcional.

El registro queda guardado en `localStorage` para no pedir los datos en cada recarga y se intenta respaldar en la hoja `VISITAS` mediante Apps Script. No se implementa contrasena en esta version porque el frontend es estatico en GitHub Pages; una contrasena real requiere backend con sesiones y verificacion del lado servidor.

## Actualizacion de version

El panel incluye el boton `Actualizar version`. Limpia caches propios de LicitaBayes, solicita actualizacion del service worker y recarga con cache-busting sin borrar el visitante registrado ni otros datos locales criticos.

## Publicacion GitHub Pages

La URL publica es:

```text
https://diegomezapy.github.io/tableroDNCPpy/
```

El sitio se publica desde la rama `gh-pages`, que contiene solo los archivos estaticos necesarios.

Si GitHub Pages se desactiva, activar Pages con fuente `Deploy from a branch` y seleccionar:

```text
Branch: gh-pages
Folder: /
```

## Validacion local

```bash
py -3 -m py_compile dashboard.py downloader.py processor.py scripts/build_static_data.py
py -3 scripts/build_static_data.py
py -3 -m http.server 8765
```

Abrir:

```text
http://127.0.0.1:8765/
```

## Nota historica

El proyecto original tenia tablero Streamlit. Esta version lo reemplaza por una app estatica publicable en GitHub Pages.
