# Trazabilidad de datos DNCP

## Fuente de descarga identificada

El repositorio contiene un descargador versionado en `downloader.py`. Ese script usa descarga masiva CSV/OCDS desde DNCP con este patron:

```text
https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/{file}
```

Modulos configurados:

```text
convocatorias -> ten-masivo.zip
adjudicaciones -> awa-masivo.zip
contratos -> con-masivo.zip
```

Anios por defecto:

```text
2023, 2024, 2025
```

Comando base:

```bash
python downloader.py
```

Ejemplos:

```bash
python downloader.py --years 2024 2025
python downloader.py --modules convocatorias
python downloader.py --years 2025 --force
```

## Verificacion de URLs

El 2026-06-06 se verifico por `HEAD` que los ZIP configurados para 2023, 2024 y 2025 respondian `200`:

| Anio | Modulo | Archivo | Estado | Bytes |
| --- | --- | --- | --- | ---: |
| 2023 | convocatorias | ten-masivo.zip | 200 | 67.642.719 |
| 2023 | adjudicaciones | awa-masivo.zip | 200 | 50.129.609 |
| 2023 | contratos | con-masivo.zip | 200 | 29.035.623 |
| 2024 | convocatorias | ten-masivo.zip | 200 | 109.926.652 |
| 2024 | adjudicaciones | awa-masivo.zip | 200 | 82.100.794 |
| 2024 | contratos | con-masivo.zip | 200 | 40.538.838 |
| 2025 | convocatorias | ten-masivo.zip | 200 | 117.740.372 |
| 2025 | adjudicaciones | awa-masivo.zip | 200 | 86.181.711 |
| 2025 | contratos | con-masivo.zip | 200 | 33.771.860 |

## Procesamiento versionado actual

`processor.py` lee CSV extraidos bajo:

```text
data/{anio}/{modulo}/
```

Archivos esperados:

```text
convocatorias/records.csv
adjudicaciones/awards.csv
adjudicaciones/awa_suppliers.csv
adjudicaciones/records.csv
contratos/records.csv
```

Ese procesador genera agregados basicos en `cache/`, por ejemplo evolucion anual, evolucion mensual, modalidades, top entidades, top proveedores y muestras.

## Brecha de reproducibilidad detectada

La app LicitaBayes usa caches avanzados:

```text
cache/adjudicaciones/items_detalle.parquet
cache/adjudicaciones/comparacion_precios.parquet
cache/adjudicaciones/red_actores.parquet
cache/adjudicaciones/catalogo_ruc.parquet
cache/convocatorias/licitaciones_full.parquet
```

Estos archivos fueron agregados ya construidos en el commit inicial visible `c20af15`. En el repositorio actual no hay un script versionado que permita regenerarlos exactamente desde los CSV crudos.

Conclusion operativa: la fuente DNCP y la descarga masiva estan identificadas, pero la reconstruccion completa de los caches avanzados todavia no es totalmente auditable.

## Recomendacion

Agregar un script reproducible para reconstruir los caches avanzados y generar manifiestos:

```text
data/download_manifest.json
cache/cache_manifest.json
```

Cada manifiesto debe registrar URL, modulo, anio, fecha de descarga, bytes, SHA256, comando ejecutado, version del script y cobertura temporal real.
