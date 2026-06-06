"""
Compatibility entrypoint.

LicitaBayes no usa Streamlit. La app publica vive en index.html y se sirve
como sitio estatico de GitHub Pages. Este archivo queda solo para evitar que
usuarios antiguos intenten abrir el tablero anterior sin contexto.
"""

from pathlib import Path


def main() -> None:
    root = Path(__file__).parent
    index = root / "index.html"
    print("LicitaBayes DNCP es una app web estatica, no Streamlit.")
    print(f"Abra {index} o publique el repositorio en GitHub Pages.")
    print("Para regenerar datos: py -3 scripts/build_static_data.py")


if __name__ == "__main__":
    main()
