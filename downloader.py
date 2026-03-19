"""
downloader.py — Descargador de datos abiertos de la DNCP Paraguay
=================================================================
Descarga los archivos CSV comprimidos del portal de datos abiertos
de la Dirección Nacional de Contrataciones Públicas (DNCP).

Uso:
    python downloader.py                         # Descarga años 2023-2025 (todos los módulos)
    python downloader.py --years 2024 2025       # Años específicos
    python downloader.py --modules convocatorias # Solo un módulo
"""

import os
import sys
import zipfile
import argparse
import requests
from pathlib import Path
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------

BASE_URL = "https://www.contrataciones.gov.py/images/opendata-v3/final/ocds/{year}/{file}"

MODULES = {
    "convocatorias": "ten-masivo.zip",
    "adjudicaciones": "awa-masivo.zip",
    "contratos":      "con-masivo.zip",
}

DATA_DIR = Path(__file__).parent / "data"

DEFAULT_YEARS = [2023, 2024, 2025]


# ---------------------------------------------------------------------------
# Funciones
# ---------------------------------------------------------------------------

def download_file(url: str, dest: Path) -> bool:
    """Descarga un archivo desde `url` hacia `dest` con barra de progreso."""
    try:
        resp = requests.get(url, stream=True, timeout=60)
        if resp.status_code == 404:
            print(f"  ⚠️  No encontrado (404): {url}")
            return False
        resp.raise_for_status()

        total = int(resp.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            total=total,
            unit="B",
            unit_scale=True,
            desc=dest.name,
            leave=False,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        return True
    except requests.RequestException as e:
        print(f"  ❌ Error descargando {url}: {e}")
        return False


def extract_zip(zip_path: Path, extract_dir: Path) -> bool:
    """Descomprime un archivo ZIP en `extract_dir`."""
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(extract_dir)
        return True
    except zipfile.BadZipFile as e:
        print(f"  ❌ ZIP inválido {zip_path}: {e}")
        return False


def download_module(year: int, module: str, force: bool = False) -> Path | None:
    """
    Descarga y descomprime el módulo indicado para el año dado.
    Retorna el directorio con los CSVs extraídos, o None si falla.
    """
    filename = MODULES[module]
    url = BASE_URL.format(year=year, file=filename)

    year_dir = DATA_DIR / str(year)
    extract_dir = year_dir / module
    zip_path = year_dir / filename

    year_dir.mkdir(parents=True, exist_ok=True)

    # Verificar caché
    if extract_dir.exists() and any(extract_dir.glob("*.csv")) and not force:
        print(f"  ✅ {year}/{module}: ya descargado (usa --force para re-descargar)")
        return extract_dir

    # Descargar
    print(f"  ⬇️  Descargando {year}/{module} desde {url}")
    if not download_file(url, zip_path):
        return None

    # Descomprimir
    print(f"  📂 Extrayendo {zip_path.name}...")
    extract_dir.mkdir(exist_ok=True)
    if not extract_zip(zip_path, extract_dir):
        return None

    # Limpiar ZIP
    zip_path.unlink(missing_ok=True)

    csvs = list(extract_dir.glob("*.csv"))
    print(f"  ✅ {year}/{module}: {len(csvs)} archivo(s) CSV extraído(s)")
    return extract_dir


def download_all(years: list[int], modules: list[str], force: bool = False):
    """Descarga todos los módulos para los años indicados."""
    print(f"\n🚀 Iniciando descarga — Años: {years} | Módulos: {modules}\n")
    results = {}
    for year in years:
        print(f"\n📅 Año {year}:")
        for module in modules:
            path = download_module(year, module, force=force)
            results[(year, module)] = path

    ok = sum(1 for v in results.values() if v is not None)
    total = len(results)
    print(f"\n✅ Descarga finalizada: {ok}/{total} módulos disponibles.")
    print(f"📁 Datos guardados en: {DATA_DIR.resolve()}\n")
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Descargador de datos abiertos de la DNCP Paraguay"
    )
    parser.add_argument(
        "--years", nargs="+", type=int, default=DEFAULT_YEARS,
        help=f"Años a descargar (default: {DEFAULT_YEARS})"
    )
    parser.add_argument(
        "--modules", nargs="+", choices=list(MODULES.keys()), default=list(MODULES.keys()),
        help="Módulos a descargar (default: todos)"
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Forzar re-descarga aunque ya existan los datos"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    download_all(years=args.years, modules=args.modules, force=args.force)
