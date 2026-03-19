# 🇵🇾 Tablero de Contrataciones Públicas — DNCP Paraguay

Proyecto Python para descargar, procesar y visualizar los datos abiertos de la **Dirección Nacional de Contrataciones Públicas (DNCP)** del Paraguay.

## 📦 Estructura del Proyecto

```
dncp-dashboard/
├── downloader.py     # Descarga CSVs desde el portal de DNCP
├── processor.py      # Limpieza y métricas con Pandas
├── dashboard.py      # Tablero interactivo con Streamlit + Plotly
├── requirements.txt  # Dependencias
└── data/             # Directorio de datos (auto-creado)
    └── {año}/
        ├── convocatorias/   # CSVs de llamados/licitaciones
        ├── adjudicaciones/  # CSVs de adjudicaciones
        └── contratos/       # CSVs de contratos
```

## 🚀 Instalación y Uso Rápido

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Descargar datos

```bash
# Años por defecto: 2023, 2024, 2025 — todos los módulos
python downloader.py

# Personalizar años y módulos
python downloader.py --years 2024 2025 --modules convocatorias adjudicaciones

# Forzar re-descarga
python downloader.py --years 2025 --force
```

### 3. (Opcional) Verificar el procesamiento

```bash
python processor.py
```

### 4. Abrir el tablero

```bash
streamlit run dashboard.py
```

El tablero se abrirá en `http://localhost:8501`

## 📊 Módulos de Datos Disponibles

| Módulo | Descripción | Cobertura |
|---|---|---|
| `convocatorias` | Llamados / licitaciones publicadas | 2010–2026 |
| `adjudicaciones` | Procesos adjudicados a proveedores | 2010–2026 |
| `contratos` | Contratos firmados | 2010–2026 |

> **Fuente de datos:** Portal de Datos Abiertos de la DNCP — https://contrataciones.gov.py/datos  
> **Licencia:** Creative Commons Attribution 4.0 Internacional (CC BY 4.0)  
> **Estándar:** Open Contracting Data Standard (OCDS) — https://standard.open-contracting.org

## 🖥️ Secciones del Tablero

1. **Indicadores Generales (KPIs):** Total de llamados, montos estimados/adjudicados/contratados, proveedores y entidades únicas.
2. **Convocatorias:** Evolución anual, top entidades públicas, distribución de modalidades.
3. **Adjudicaciones:** Top proveedores por monto, evolución mensual, distribución por cantidad.
4. **Contratos:** Estado de contratos, evolución mensual, monto total.
5. **Filtros:** Por año, visibles desde el panel lateral.

## 🔑 API Key (Opcional)

Para usar la API REST con paginación (en lugar de la descarga masiva), podés registrar una aplicación en:  
https://contrataciones.gov.py/datos/adm/aplicaciones

Para la descarga masiva de CSV (lo que usa este proyecto), **no se necesita API key**.

## 📋 Requisitos del Sistema

- Python 3.10+
- Conexión a internet para la descarga
- ~500 MB de espacio por año descargado (aproximado)
