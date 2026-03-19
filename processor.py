"""
processor.py — Procesador de datos DNCP con bajo consumo de memoria
====================================================================
Lee los CSV en CHUNKS (nunca carga el archivo completo en RAM),
agrega los datos y guarda resultados pre-calculados como Parquet
liviano. El tablero solo lee los Parquet (pocos MB), no los CSV.

Uso:
    python processor.py                 # procesa todos los años disponibles
    python processor.py --years 2025    # solo un año
    python processor.py --force         # re-procesa aunque ya existan Parquet
"""

import gc
import argparse
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

DATA_DIR  = Path(__file__).parent / "data"
CACHE_DIR = Path(__file__).parent / "cache"
CHUNK_SIZE = 50_000  # filas por chunk — ajustar según RAM disponible

# ─────────────────────────────────────────────────────────────────────────────
# Columnas de interés por módulo (OCDS flatten, prefijo compiledRelease/)
# ─────────────────────────────────────────────────────────────────────────────
CONVOCATORIA_COLS = {
    "compiledRelease/tender/id":                       "id_llamado",
    "compiledRelease/tender/title":                    "titulo",
    "compiledRelease/tender/procuringEntity/name":     "entidad",
    "compiledRelease/tender/procurementMethodDetails": "modalidad",
    "compiledRelease/tender/value/amount":             "monto_estimado",
    "compiledRelease/tender/tenderPeriod/startDate":   "fecha_publicacion",
    "compiledRelease/tender/status":                   "estado",
}

ADJUDICACION_COLS = {
    "compiledRelease/awards/0/id":           "id_adjudicacion",
    "compiledRelease/awards/0/value/amount": "monto_adjudicado",
    "compiledRelease/awards/0/date":         "fecha_adjudicacion",
    "compiledRelease/awards/0/status":       "estado",
}

ADJUDICACION_SUPPLIER_COLS = {
    "compiledRelease/awards/0/id":             "id_adjudicacion",
    "compiledRelease/awards/0/suppliers/0/name": "proveedor",
}

ADJUDICACION_TENDER_COLS = {
    "compiledRelease/id":                              "compiledRelease_id",
    "compiledRelease/tender/procuringEntity/name":     "entidad",
    "compiledRelease/tender/procurementMethodDetails": "modalidad",
}

CONTRATO_COLS = {
    "compiledRelease/contracts/0/id":           "id_contrato",
    "compiledRelease/contracts/0/value/amount": "monto_contrato",
    "compiledRelease/contracts/0/dateSigned":   "fecha_firma",
    "compiledRelease/contracts/0/status":       "estado",
}

CATEGORY_COLS = {"entidad", "modalidad", "estado", "proveedor"}

# ─────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────────────

def _csv_files(module: str, filename: str, years: list) -> list[Path]:
    """Devuelve los CSVs de un nombre de archivo específico para los años dados."""
    files = []
    year_dirs = [DATA_DIR / str(y) / module for y in years] if years else [
        d / module for d in sorted(DATA_DIR.iterdir()) if d.is_dir()
    ]
    for d in year_dirs:
        fp = d / filename
        if fp.exists():
            files.append(fp)
    return files


def _available_cols(path: Path, col_map: dict) -> dict:
    """Devuelve solo las columnas del col_map que existen en el CSV."""
    header = pd.read_csv(path, nrows=0)
    return {k: v for k, v in col_map.items() if k in header.columns}


def _parse_date_col(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True).dt.tz_convert(None)


def _agg_chunks(csv_path: Path, col_map: dict, date_col: str, amount_col: str) -> dict:
    """
    Lee un CSV en chunks y devuelve agregados parciales:
    - conteo y suma por año/mes (para evolución)
    - top entidades parciales
    - top proveedores parciales (si aplica)
    - registros mínimos para tabla (primeras 200 filas)
    """
    available = _available_cols(csv_path, col_map)
    if not available:
        return {}

    # Acumuladores
    by_year   = {}   # {año: {count, amount}}
    by_month  = {}   # {"YYYY-MM": {count, amount}}
    by_entity = {}   # {entidad: count}
    by_modal  = {}   # {modalidad: count}
    by_prov   = {}   # {proveedor: amount}
    sample_rows = []
    sampled = False

    reader = pd.read_csv(
        csv_path,
        usecols=list(available.keys()),
        chunksize=CHUNK_SIZE,
        low_memory=False,
    )

    for chunk in reader:
        chunk.rename(columns=available, inplace=True)

        # Guardar muestra pequeña para la tabla del tablero
        if not sampled:
            sample_rows.append(chunk.head(200))
            sampled = True

        # Parsear fecha y monto
        if date_col in chunk.columns:
            chunk[date_col] = _parse_date_col(chunk[date_col])
            chunk["_anio"] = chunk[date_col].dt.year.astype("Int16")
            chunk["_mes"]  = chunk[date_col].dt.to_period("M").astype(str)
        else:
            chunk["_anio"] = pd.NA
            chunk["_mes"]  = pd.NA

        if amount_col in chunk.columns:
            chunk[amount_col] = pd.to_numeric(chunk[amount_col], errors="coerce")
        else:
            chunk[amount_col] = 0.0

        # Agregar por año
        for anio, grp in chunk.groupby("_anio", dropna=True):
            key = int(anio)
            prev = by_year.get(key, {"count": 0, "amount": 0.0})
            by_year[key] = {
                "count":  prev["count"]  + len(grp),
                "amount": prev["amount"] + grp[amount_col].sum(),
            }

        # Agregar por mes
        for mes, grp in chunk.groupby("_mes", dropna=True):
            prev = by_month.get(mes, {"count": 0, "amount": 0.0})
            by_month[mes] = {
                "count":  prev["count"]  + len(grp),
                "amount": prev["amount"] + grp[amount_col].sum(),
            }

        # Top entidades
        if "entidad" in chunk.columns:
            for ent, cnt in chunk["entidad"].value_counts().items():
                by_entity[ent] = by_entity.get(ent, 0) + cnt

        # Modalidades
        if "modalidad" in chunk.columns:
            for mod, cnt in chunk["modalidad"].value_counts().items():
                by_modal[mod] = by_modal.get(mod, 0) + cnt

        # Top proveedores por monto
        if "proveedor" in chunk.columns and amount_col in chunk.columns:
            for prov, grp in chunk.groupby("proveedor", sort=False):
                prev_amt = by_prov.get(prov, {"amount": 0.0, "count": 0})
                by_prov[prov] = {
                    "amount": prev_amt["amount"] + grp[amount_col].sum(),
                    "count":  prev_amt["count"]  + len(grp),
                }

        del chunk
        gc.collect()

    return {
        "by_year":   by_year,
        "by_month":  by_month,
        "by_entity": by_entity,
        "by_modal":  by_modal,
        "by_prov":   by_prov,
        "sample":    pd.concat(sample_rows, ignore_index=True) if sample_rows else pd.DataFrame(),
    }


def _merge_aggs(agg_list: list[dict]) -> dict:
    """Combina la lista de agregados parciales de múltiples CSV."""
    merged = {"by_year": {}, "by_month": {}, "by_entity": {}, "by_modal": {}, "by_prov": {}, "samples": []}
    for a in agg_list:
        for k in ("by_year", "by_month"):
            for key, vals in a.get(k, {}).items():
                prev = merged[k].get(key, {"count": 0, "amount": 0.0})
                merged[k][key] = {"count": prev["count"] + vals["count"], "amount": prev["amount"] + vals["amount"]}
        for k in ("by_entity", "by_modal"):
            for key, cnt in a.get(k, {}).items():
                merged[k][key] = merged[k].get(key, 0) + cnt
        for prov, vals in a.get("by_prov", {}).items():
            prev = merged["by_prov"].get(prov, {"amount": 0.0, "count": 0})
            merged["by_prov"][prov] = {"amount": prev["amount"] + vals["amount"], "count": prev["count"] + vals["count"]}
        if "sample" in a and not a["sample"].empty:
            merged["samples"].append(a["sample"])
    return merged


def _to_dataframes(merged: dict, amount_label: str) -> dict[str, pd.DataFrame]:
    """Convierte los dicts de agregados a DataFrames."""
    dfs = {}

    by_year  = merged.get("by_year",  {})
    by_month = merged.get("by_month", {})
    by_ent   = merged.get("by_entity",{})
    by_mod   = merged.get("by_modal", {})
    by_prov  = merged.get("by_prov",  {})
    samples  = merged.get("samples",  [])

    dfs["evolucion_anual"] = pd.DataFrame([
        {"anio": k, "cantidad": v["count"], "monto": v["amount"]}
        for k, v in by_year.items()
    ]).sort_values("anio") if by_year else pd.DataFrame(columns=["anio","cantidad","monto"])

    dfs["evolucion_mensual"] = pd.DataFrame([
        {"mes": k, "cantidad": v["count"], "monto": v["amount"]}
        for k, v in by_month.items()
    ]).sort_values("mes") if by_month else pd.DataFrame(columns=["mes","cantidad","monto"])

    dfs["top_entidades"] = (
        pd.DataFrame({"entidad": list(by_ent.keys()), "cantidad": list(by_ent.values())})
        .sort_values("cantidad", ascending=False).head(20)
    ) if by_ent else pd.DataFrame(columns=["entidad","cantidad"])

    dfs["modalidades"] = (
        pd.DataFrame({"modalidad": list(by_mod.keys()), "cantidad": list(by_mod.values())})
        .sort_values("cantidad", ascending=False).head(20)
    ) if by_mod else pd.DataFrame(columns=["modalidad","cantidad"])

    dfs["top_proveedores"] = (
        pd.DataFrame([
            {"proveedor": k, "monto": v["amount"], "cantidad": v["count"]}
            for k, v in by_prov.items()
        ]).sort_values("monto", ascending=False).head(20)
    ) if by_prov else pd.DataFrame(columns=["proveedor","monto","cantidad"])

    if samples:
        dfs["muestra"] = pd.concat(samples, ignore_index=True).head(500)

    return dfs


# ─────────────────────────────────────────────────────────────────────────────
# Procesadores por módulo
# ─────────────────────────────────────────────────────────────────────────────

def _process_single(module: str, filename: str, col_map: dict,
                     date_col: str, amount_col: str, years: list) -> list[dict]:
    """Procesa en chunks un único CSV de un módulo."""
    files = _csv_files(module, filename, years)
    agg_list = []
    for f in files:
        print(f"     → {f.parent.parent.name}/{f.name}", end=" ", flush=True)
        agg = _agg_chunks(f, col_map, date_col, amount_col)
        if agg:
            agg_list.append(agg)
            print("✓")
        else:
            print("(sin columnas)")
    return agg_list


def _process_convocatorias(years: list, force: bool) -> bool:
    out_dir = CACHE_DIR / "convocatorias"
    marker  = out_dir / "_done"
    if marker.exists() and not force:
        print("  ✅ convocatorias: cache ya existe")
        return True
    files = _csv_files("convocatorias", "records.csv", years)
    if not files:
        print("  ⚠️  convocatorias: no se encontraron records.csv")
        return False
    print(f"  📂 convocatorias: {len(files)} archivo(s)...")
    agg_list = _process_single("convocatorias", "records.csv", CONVOCATORIA_COLS,
                               "fecha_publicacion", "monto_estimado", years)
    if not agg_list:
        return False
    merged = _merge_aggs(agg_list)
    dfs    = _to_dataframes(merged, "monto_estimado")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in dfs.items():
        if not df.empty:
            df.to_parquet(out_dir / f"{name}.parquet", index=False)
    marker.touch()
    total_kb = sum(p.stat().st_size for p in out_dir.glob("*.parquet")) // 1024
    print(f"  💾 convocatorias: cache {total_kb} KB")
    return True


def _process_adjudicaciones(years: list, force: bool) -> bool:
    """
    Adjudicaciones usa 3 archivos:
    - awards.csv: id, monto, fecha, estado
    - awa_suppliers.csv: id_adjudicacion → proveedor
    - records.csv: id → entidad, modalidad
    Se unen en memoria por lotes pequeños.
    """
    out_dir = CACHE_DIR / "adjudicaciones"
    marker  = out_dir / "_done"
    if marker.exists() and not force:
        print("  ✅ adjudicaciones: cache ya existe")
        return True

    award_files = _csv_files("adjudicaciones", "awards.csv", years)
    if not award_files:
        print("  ⚠️  adjudicaciones: no se encontraron awards.csv")
        return False

    print(f"  📂 adjudicaciones: {len(award_files)} archivo(s) (awards + suppliers + records)...")

    by_year, by_month, by_entity, by_modal, by_prov = {}, {}, {}, {}, {}
    sample_rows = []

    for awards_f in award_files:
        year_dir = awards_f.parent

        # Cargar proveedores completo (archivo pequeño)
        sup_f   = year_dir / "awa_suppliers.csv"
        rec_f   = year_dir / "records.csv"

        sup_df  = pd.read_csv(sup_f, usecols=list(ADJUDICACION_SUPPLIER_COLS.keys())) \
                    .rename(columns=ADJUDICACION_SUPPLIER_COLS) if sup_f.exists() else pd.DataFrame()
        rec_cols = {k: v for k, v in ADJUDICACION_TENDER_COLS.items()}
        if rec_f.exists():
            sample_hdr = pd.read_csv(rec_f, nrows=0).columns
            avail_rec  = {k: v for k, v in rec_cols.items() if k in sample_hdr}
            rec_df = pd.read_csv(rec_f, usecols=list(avail_rec.keys())).rename(columns=avail_rec) \
                     if avail_rec else pd.DataFrame()
        else:
            rec_df = pd.DataFrame()

        # Leer awards en chunks
        available = _available_cols(awards_f, ADJUDICACION_COLS)
        if not available:
            continue

        sampled = False
        for chunk in pd.read_csv(awards_f, usecols=list(available.keys()),
                                  chunksize=CHUNK_SIZE, low_memory=False):
            chunk.rename(columns=available, inplace=True)

            # Unir proveedores y entidad
            if not sup_df.empty and "id_adjudicacion" in chunk.columns:
                chunk = chunk.merge(sup_df, on="id_adjudicacion", how="left")
            if not rec_df.empty and "compiledRelease_id" in rec_df.columns:
                chunk["compiledRelease/id"] = chunk.get("compiledRelease/id", pd.NA)
                chunk = chunk.merge(
                    rec_df.rename(columns={"compiledRelease_id": "compiledRelease/id"}),
                    on="compiledRelease/id", how="left"
                ) if "compiledRelease/id" in chunk.columns else chunk

            if not sampled:
                sample_rows.append(chunk.head(200))
                sampled = True

            chunk["fecha_adjudicacion"] = _parse_date_col(chunk["fecha_adjudicacion"]) \
                                          if "fecha_adjudicacion" in chunk.columns else pd.NaT
            chunk["monto_adjudicado"]   = pd.to_numeric(chunk.get("monto_adjudicado", 0), errors="coerce").fillna(0)
            chunk["_anio"] = chunk["fecha_adjudicacion"].dt.year.astype("Int16") \
                             if "fecha_adjudicacion" in chunk.columns else pd.NA
            chunk["_mes"]  = chunk["fecha_adjudicacion"].dt.to_period("M").astype(str) \
                             if "fecha_adjudicacion" in chunk.columns else pd.NA

            for anio, grp in chunk.groupby("_anio", dropna=True):
                k = int(anio)
                p = by_year.get(k, {"count": 0, "amount": 0.0})
                by_year[k] = {"count": p["count"] + len(grp), "amount": p["amount"] + grp["monto_adjudicado"].sum()}
            for mes, grp in chunk.groupby("_mes", dropna=True):
                p = by_month.get(mes, {"count": 0, "amount": 0.0})
                by_month[mes] = {"count": p["count"] + len(grp), "amount": p["amount"] + grp["monto_adjudicado"].sum()}
            if "entidad" in chunk.columns:
                for ent, cnt in chunk["entidad"].value_counts().items():
                    by_entity[ent] = by_entity.get(ent, 0) + cnt
            if "modalidad" in chunk.columns:
                for mod, cnt in chunk["modalidad"].value_counts().items():
                    by_modal[mod] = by_modal.get(mod, 0) + cnt
            if "proveedor" in chunk.columns:
                for prov, grp in chunk.groupby("proveedor", sort=False):
                    p = by_prov.get(prov, {"amount": 0.0, "count": 0})
                    by_prov[prov] = {"amount": p["amount"] + grp["monto_adjudicado"].sum(),
                                     "count":  p["count"]  + len(grp)}
            del chunk
            gc.collect()

    merged = {"by_year": by_year, "by_month": by_month, "by_entity": by_entity,
              "by_modal": by_modal, "by_prov": by_prov, "samples": sample_rows}
    dfs    = _to_dataframes(merged, "monto_adjudicado")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in dfs.items():
        if not df.empty:
            df.to_parquet(out_dir / f"{name}.parquet", index=False)
    out_dir.joinpath("_done").touch()
    total_kb = sum(p.stat().st_size for p in out_dir.glob("*.parquet")) // 1024
    print(f"  💾 adjudicaciones: cache {total_kb} KB")
    return True


def _process_contratos(years: list, force: bool) -> bool:
    """Contratos leídos desde el records.csv del módulo contratos si está disponible."""
    out_dir = CACHE_DIR / "contratos"
    marker  = out_dir / "_done"
    if marker.exists() and not force:
        print("  ✅ contratos: cache ya existe")
        return True
    # contratos no se descargaron todavía — omitir sin error
    files = _csv_files("contratos", "records.csv", years)
    if not files:
        print("  ⚠️  contratos: sin datos (descargá el módulo 'contratos' primero)")
        return False
    print(f"  📂 contratos: {len(files)} archivo(s)...")
    agg_list = _process_single("contratos", "records.csv", CONTRATO_COLS,
                               "fecha_firma", "monto_contrato", years)
    if not agg_list:
        return False
    merged = _merge_aggs(agg_list)
    dfs    = _to_dataframes(merged, "monto_contrato")
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in dfs.items():
        if not df.empty:
            df.to_parquet(out_dir / f"{name}.parquet", index=False)
    marker.touch()
    total_kb = sum(p.stat().st_size for p in out_dir.glob("*.parquet")) // 1024
    print(f"  💾 contratos: cache {total_kb} KB")
    return True


def process_all(years: list, force: bool = False):
    CACHE_DIR.mkdir(exist_ok=True)
    print(f"\n⚙️  Procesando módulos para años: {years or 'todos'}\n")
    _process_convocatorias(years, force)
    gc.collect()
    _process_adjudicaciones(years, force)
    gc.collect()
    _process_contratos(years, force)
    gc.collect()
    print("\n✅ Procesamiento completo. Ejecuta: streamlit run dashboard.py\n")

    print("\n✅ Procesamiento completo. Ejecuta: streamlit run dashboard.py\n")


# ─────────────────────────────────────────────────────────────────────────────
# Funciones de lectura para el tablero (solo Parquet livianos)
# ─────────────────────────────────────────────────────────────────────────────

def _load_parquet(module: str, name: str) -> pd.DataFrame:
    path = CACHE_DIR / module / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


# Convocatorias
def get_evolucion_anual_conv()   : return _load_parquet("convocatorias", "evolucion_anual")
def get_evolucion_mensual_conv() : return _load_parquet("convocatorias", "evolucion_mensual")
def get_top_entidades()          : return _load_parquet("convocatorias", "top_entidades")
def get_modalidades()            : return _load_parquet("convocatorias", "modalidades")
def get_muestra_conv()           : return _load_parquet("convocatorias", "muestra")

# Adjudicaciones
def get_top_proveedores()        : return _load_parquet("adjudicaciones", "top_proveedores")
def get_evolucion_mensual_adj()  : return _load_parquet("adjudicaciones", "evolucion_mensual")
def get_muestra_adj()            : return _load_parquet("adjudicaciones", "muestra")

# Contratos
def get_evolucion_anual_cont()   : return _load_parquet("contratos", "evolucion_anual")
def get_evolucion_mensual_cont() : return _load_parquet("contratos", "evolucion_mensual")
def get_modalidades_cont()       : return _load_parquet("contratos", "modalidades")
def get_muestra_cont()           : return _load_parquet("contratos", "muestra")


def kpis_generales() -> dict:
    """Lee solo los totales de los Parquet de evolución (mínima RAM)."""
    ea = get_evolucion_anual_conv()
    aa = get_evolucion_anual_cont()
    ad = _load_parquet("adjudicaciones", "evolucion_anual")
    tp = get_top_proveedores()
    te = get_top_entidades()
    return {
        "total_llamados":         int(ea["cantidad"].sum())    if not ea.empty else 0,
        "monto_estimado_total":   float(ea["monto"].sum())     if not ea.empty else 0.0,
        "total_adjudicaciones":   int(ad["cantidad"].sum())    if not ad.empty else 0,
        "monto_adjudicado_total": float(ad["monto"].sum())     if not ad.empty else 0.0,
        "total_contratos":        int(aa["cantidad"].sum())    if not aa.empty else 0,
        "monto_contratos_total":  float(aa["monto"].sum())     if not aa.empty else 0.0,
        "proveedores_unicos":     len(tp)                      if not tp.empty else 0,
        "entidades_unicas":       len(te)                      if not te.empty else 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Procesador DNCP (bajo consumo de memoria)")
    parser.add_argument("--years", nargs="+", type=int, default=[], help="Años a procesar (default: todos)")
    parser.add_argument("--force", action="store_true", help="Re-procesar aunque ya exista cache")
    args = parser.parse_args()
    process_all(years=args.years, force=args.force)
