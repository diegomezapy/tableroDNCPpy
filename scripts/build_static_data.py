"""
Genera los JSON estaticos usados por LicitaBayes.

Entrada: Parquet livianos en cache/.
Salida: data/model_summary.json, data/price_alerts.json,
        data/concentration_alerts.json y data/series.json.

El modelo es un MVP de Empirical Bayes para priorizar revision humana. No
produce denuncias ni conclusiones juridicas.
"""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "cache"
DATA_DIR = ROOT / "data"

APP_VERSION = "0.1.1"
DATA_VERSION = "dncp-cache-2025"
PAGES_URL = "https://diegomezapy.github.io/tableroDNCPpy/"
SOURCE_URL = "https://contrataciones.gov.py/datos"


def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def sigmoid(x: float) -> float:
    if x < -60:
        return 0.0
    if x > 60:
        return 1.0
    return 1.0 / (1.0 + math.exp(-x))


def finite(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def clean_text(value: Any, max_len: int = 180) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    text = str(value).replace("\n", " ").replace("\r", " ").strip()
    while "  " in text:
        text = text.replace("  ", " ")
    return text[:max_len]


def record_hash(*parts: Any) -> str:
    raw = "|".join(clean_text(p, 500) for p in parts)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]


def parquet(name: str) -> pd.DataFrame:
    path = CACHE_DIR / name
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def robust_group_scale(comp: pd.DataFrame) -> pd.Series:
    ratios = comp[["codigo_catalogo", "_log_ratio"]].dropna()
    if ratios.empty:
        return pd.Series(dtype="float64")

    def mad(values: pd.Series) -> float:
        vals = values.dropna()
        if len(vals) < 4:
            return 0.55
        med = vals.median()
        dev = (vals - med).abs().median()
        return max(0.25, min(1.75, 1.4826 * float(dev)))

    return ratios.groupby("codigo_catalogo")["_log_ratio"].apply(mad)


def build_price_alerts(comp: pd.DataFrame, limit: int = 5000) -> tuple[list[dict], dict]:
    if comp.empty:
        return [], {"rows": 0, "usable_rows": 0}

    df = comp.copy()
    for col in ["precio_promedio_ent", "precio_mediano", "cantidad_compras", "total_entidades", "total_transacciones"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")

    usable = (
        (df["precio_promedio_ent"] > 0)
        & (df["precio_mediano"] > 0)
        & (df["cantidad_compras"].fillna(0) > 0)
        & (df["total_entidades"].fillna(0) >= 2)
    )
    df = df[usable].copy()
    if df.empty:
        return [], {"rows": len(comp), "usable_rows": 0}

    df["_ratio"] = df["precio_promedio_ent"] / df["precio_mediano"]
    df["_log_ratio"] = df["_ratio"].clip(lower=0.001, upper=1000).map(math.log)
    scale = robust_group_scale(df)
    df["_scale"] = df["codigo_catalogo"].map(scale).fillna(0.55).clip(0.25, 1.75)

    prior_mean = 0.0
    prior_var = 0.55**2
    records: list[dict] = []
    threshold_12 = math.log(1.2)
    threshold_15 = math.log(1.5)

    for row in df.to_dict(orient="records"):
        n = max(1.0, finite(row.get("cantidad_compras", 1)))
        total_ent = max(1.0, finite(row.get("total_entidades", 1)))
        total_tx = max(1.0, finite(row.get("total_transacciones", 1)))
        log_ratio = finite(row.get("_log_ratio", 0.0))
        ratio_value = finite(row.get("_ratio", 0.0))
        scale_value = finite(row.get("_scale", 0.55), 0.55)

        obs_var = (scale_value**2 / min(n, 25.0)) + 0.035
        post_var = 1.0 / ((1.0 / prior_var) + (1.0 / obs_var))
        post_mean = post_var * ((prior_mean / prior_var) + (log_ratio / obs_var))
        post_sd = math.sqrt(max(post_var, 1e-9))

        prob_12 = 1.0 - norm_cdf((threshold_12 - post_mean) / post_sd)
        prob_15 = 1.0 - norm_cdf((threshold_15 - post_mean) / post_sd)
        confidence = min(1.0, math.sqrt(total_tx / 12.0)) * min(1.0, math.sqrt(total_ent / 4.0))
        score = max(0.0, min(100.0, 100.0 * (0.72 * prob_15 + 0.28 * prob_12) * confidence))

        if ratio_value >= 50:
            level = "Verificar dato"
        elif prob_15 >= 0.85 or score >= 75:
            level = "Critico"
        elif prob_15 >= 0.60 or score >= 50:
            level = "Alto"
        elif prob_12 >= 0.45 or score >= 25:
            level = "Moderado"
        elif n < 2 and ratio_value >= 3:
            level = "Verificar dato"
        else:
            level = "Normal"

        codigo = clean_text(row.get("codigo_catalogo", ""))
        entidad = clean_text(row.get("entidad", ""))
        proveedor = clean_text(row.get("proveedor_mas_frecuente", ""))
        articulo = clean_text(row.get("nombre_catalogo", ""))

        records.append(
            {
                "nivel_bayes": level,
                "prob_alta": round(prob_15, 4),
                "prob_moderada": round(prob_12, 4),
                "score_bayes": round(score, 2),
                "codigo_catalogo": codigo,
                "articulo": articulo,
                "unidad": clean_text(row.get("unidad", ""), 40),
                "entidad": entidad,
                "proveedor": proveedor,
                "ruc": clean_text(row.get("ruc_proveedor", "")),
                "precio_promedio_ent": round(finite(row.get("precio_promedio_ent", 0)), 2),
                "precio_mediano": round(finite(row.get("precio_mediano", 0)), 2),
                "ratio_observado": round(ratio_value, 3),
                "intervalo_bajo": round(math.exp(post_mean - 1.96 * post_sd), 3),
                "intervalo_alto": round(math.exp(post_mean + 1.96 * post_sd), 3),
                "cantidad_compras": int(n),
                "total_entidades": int(total_ent),
                "total_transacciones": int(total_tx),
                "anio": int(finite(row.get("anio", 0))),
                "hash_registro": record_hash(codigo, entidad, proveedor, articulo),
            }
        )

    records.sort(key=lambda item: (item["score_bayes"], item["prob_alta"], item["ratio_observado"]), reverse=True)
    for idx, item in enumerate(records, start=1):
        item["rank"] = idx

    stats = {
        "rows": len(comp),
        "usable_rows": len(df),
        "published_rows": min(limit, len(records)),
        "levels": pd.Series([r["nivel_bayes"] for r in records]).value_counts().to_dict(),
    }
    return records[:limit], stats


def build_concentration(red: pd.DataFrame, limit: int = 1500) -> tuple[list[dict], dict]:
    if red.empty:
        return [], {"rows": 0}

    df = red.copy()
    df["monto_total"] = pd.to_numeric(df.get("monto_total"), errors="coerce").fillna(0)
    df["contratos"] = pd.to_numeric(df.get("contratos"), errors="coerce").fillna(0)
    df = df[(df["monto_total"] > 0) & (df["contratos"] > 0)].copy()
    if df.empty:
        return [], {"rows": len(red), "usable_rows": 0}

    totals = df.groupby("entidad").agg(
        monto_entidad=("monto_total", "sum"),
        contratos_entidad=("contratos", "sum"),
        proveedores_entidad=("proveedor", "nunique"),
    )
    df = df.join(totals, on="entidad")
    df["share_monto"] = df["monto_total"] / df["monto_entidad"].replace(0, pd.NA)
    df["share_contratos"] = df["contratos"] / df["contratos_entidad"].replace(0, pd.NA)

    records: list[dict] = []
    for row in df.itertuples(index=False):
        share_monto = finite(getattr(row, "share_monto", 0))
        share_contratos = finite(getattr(row, "share_contratos", 0))
        providers = max(1, int(finite(getattr(row, "proveedores_entidad", 1))))
        contracts = finite(getattr(row, "contratos", 0))
        monto = finite(getattr(row, "monto_total", 0))
        monto_ent = finite(getattr(row, "monto_entidad", 0))

        expected_share = 1.0 / providers
        shrinkage = min(0.25, 1.0 / max(4.0, math.sqrt(providers)))
        posterior_share = (1.0 - shrinkage) * share_monto + shrinkage * expected_share
        evidence = min(1.0, math.log1p(contracts) / math.log1p(40)) * min(1.0, math.log1p(monto) / math.log1p(1_000_000_000_000))
        prob = sigmoid((posterior_share - 0.35) / 0.08) * evidence
        score = max(0.0, min(100.0, 100.0 * (0.78 * prob + 0.22 * min(1.0, share_contratos))))

        if providers < 3:
            level = "Baja base"
        elif score >= 80:
            level = "Critico"
        elif score >= 60:
            level = "Alto"
        elif score >= 35:
            level = "Moderado"
        else:
            level = "Normal"

        entidad = clean_text(getattr(row, "entidad", ""))
        proveedor = clean_text(getattr(row, "proveedor", ""))
        records.append(
            {
                "nivel_concentracion": level,
                "prob_concentracion": round(prob, 4),
                "score_concentracion": round(score, 2),
                "entidad": entidad,
                "proveedor": proveedor,
                "ruc": clean_text(getattr(row, "ruc", "")),
                "contratos": int(contracts),
                "monto_total": round(monto, 2),
                "share_monto": round(share_monto, 4),
                "share_contratos": round(share_contratos, 4),
                "proveedores_entidad": providers,
                "monto_entidad": round(monto_ent, 2),
                "hash_registro": record_hash(entidad, proveedor, monto, contracts),
            }
        )

    records.sort(key=lambda item: (item["score_concentracion"], item["monto_total"]), reverse=True)
    for idx, item in enumerate(records, start=1):
        item["rank"] = idx
    stats = {
        "rows": len(red),
        "usable_rows": len(df),
        "published_rows": min(limit, len(records)),
        "levels": pd.Series([r["nivel_concentracion"] for r in records]).value_counts().to_dict(),
    }
    return records[:limit], stats


def build_series() -> dict:
    out: dict[str, Any] = {}
    for key, rel in {
        "convocatorias_anual": "convocatorias/evolucion_anual.parquet",
        "convocatorias_mensual": "convocatorias/evolucion_mensual.parquet",
        "adjudicaciones_anual": "adjudicaciones/evolucion_anual.parquet",
        "adjudicaciones_mensual": "adjudicaciones/evolucion_mensual.parquet",
        "modalidades": "convocatorias/modalidades.parquet",
        "top_entidades": "convocatorias/top_entidades.parquet",
        "top_proveedores": "adjudicaciones/top_proveedores.parquet",
    }.items():
        df = parquet(rel)
        out[key] = df.head(40).fillna("").to_dict(orient="records") if not df.empty else []
    return out


def quality_report(items: pd.DataFrame, licit: pd.DataFrame) -> dict:
    now = pd.Timestamp.now(tz="UTC")
    report: dict[str, Any] = {}
    if not items.empty and "fecha_adjudicacion" in items.columns:
        dt = pd.to_datetime(items["fecha_adjudicacion"], errors="coerce", utc=True)
        report["items_fecha_min"] = str(dt.min()) if dt.notna().any() else ""
        report["items_fecha_max"] = str(dt.max()) if dt.notna().any() else ""
        report["items_fechas_futuras"] = int((dt > now).sum())
        for col in ["cantidad", "precio_unitario", "monto_item"]:
            series = pd.to_numeric(items.get(col), errors="coerce")
            report[f"items_{col}_nulos"] = int(series.isna().sum())
            report[f"items_{col}_ceros"] = int((series == 0).sum())
    if not licit.empty and "fecha_publicacion" in licit.columns:
        dt = pd.to_datetime(licit["fecha_publicacion"], errors="coerce", utc=True)
        report["licitaciones_fecha_min"] = str(dt.min()) if dt.notna().any() else ""
        report["licitaciones_fecha_max"] = str(dt.max()) if dt.notna().any() else ""
        report["licitaciones_fechas_futuras"] = int((dt > now).sum())
    return report


def write_json(name: str, payload: Any) -> None:
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / name
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    generated_at = datetime.now(timezone.utc).isoformat()
    comp = parquet("adjudicaciones/comparacion_precios.parquet")
    red = parquet("adjudicaciones/red_actores.parquet")
    items = parquet("adjudicaciones/items_detalle.parquet")
    licit = parquet("convocatorias/licitaciones_full.parquet")

    price_alerts, price_stats = build_price_alerts(comp)
    concentration, concentration_stats = build_concentration(red)
    series = build_series()
    quality = quality_report(items, licit)

    run_id = record_hash(generated_at, APP_VERSION, DATA_VERSION, len(price_alerts), len(concentration))
    summary = {
        "run_id": run_id,
        "generated_at": generated_at,
        "app_version": APP_VERSION,
        "data_version": DATA_VERSION,
        "source_url": SOURCE_URL,
        "github_pages_url": PAGES_URL,
        "models": {
            "price": "Empirical Bayes log-normal sobre ratio precio institucional / mediana por item",
            "concentration": "Suavizado Dirichlet empirico sobre participacion entidad-proveedor",
        },
        "counts": {
            "licitaciones": int(len(licit)),
            "items": int(len(items)),
            "price_alerts": int(len(price_alerts)),
            "concentration_alerts": int(len(concentration)),
        },
        "price_stats": price_stats,
        "concentration_stats": concentration_stats,
        "quality": quality,
        "responsible_notice": "Las alertas son senales estadisticas para revision; no son prueba de irregularidad.",
    }

    write_json("model_summary.json", summary)
    write_json("price_alerts.json", {"run_id": run_id, "generated_at": generated_at, "rows": price_alerts})
    write_json("concentration_alerts.json", {"run_id": run_id, "generated_at": generated_at, "rows": concentration})
    write_json("series.json", {"run_id": run_id, "generated_at": generated_at, **series})
    print(f"Generated static data in {DATA_DIR}")
    print(json.dumps(summary["counts"], indent=2))


if __name__ == "__main__":
    main()
