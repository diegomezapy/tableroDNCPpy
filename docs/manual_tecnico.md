# Manual tecnico

## Requisitos

- Python 3.10+
- pandas
- pyarrow
- requests
- tqdm

Instalar:

```bash
pip install -r requirements.txt
```

## Regenerar datos

```bash
py -3 scripts/build_static_data.py
```

El script lee `cache/` y escribe JSON en `data/`.

## Trazabilidad de datos

La fuente de descarga rastreada esta documentada en:

```text
docs/trazabilidad_datos.md
```

Resumen tecnico: `downloader.py` baja ZIP masivos OCDS de DNCP para convocatorias, adjudicaciones y contratos. `processor.py` reconstruye agregados basicos desde los CSV extraidos. Los caches avanzados usados por LicitaBayes fueron incorporados ya construidos en el commit inicial visible; por eso el siguiente paso recomendado es agregar un generador reproducible para `items_detalle`, `comparacion_precios`, `red_actores`, `catalogo_ruc` y `licitaciones_full`.

## Servir localmente

```bash
py -3 -m http.server 8765
```

Abrir `http://127.0.0.1:8765/`.

No abrir `index.html` directamente si el navegador bloquea `fetch()` sobre archivos locales.

## Google Sheets

La planilla de respaldo contiene:

- CONFIG
- RUNS_MODELO
- ALERTAS_BAYES
- CONCENTRACION
- SNAPSHOTS
- LOG
- ERRORES
- VERSIONES

Apps Script sincroniza desde `DATA_BASE_URL`, por defecto:

```text
https://diegomezapy.github.io/tableroDNCPpy/data
```

## GitHub Pages

La app esta publicada desde la rama `gh-pages`.

URL:

```text
https://diegomezapy.github.io/tableroDNCPpy/
```

Para republicar manualmente, copiar `index.html`, `assets/`, `data/`, `docs/`, `manifest.json` y `service-worker.js` a la rama `gh-pages`.

## Despliegue Apps Script

1. Crear proyecto Apps Script.
2. Copiar archivos de `gas/`.
3. Ejecutar `ensureSchema_()`.
4. Ejecutar `syncFromGithub()`.
5. Opcional: crear trigger con `installHourlySyncTrigger()`.

## Seguridad

El respaldo recomendado es `pull_from_github`: el script lee datos publicados. No se expone token en el frontend.
