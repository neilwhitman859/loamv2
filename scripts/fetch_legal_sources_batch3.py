"""
Path A batch 3: Spanish MAPA pliegos.

URLs confirmed via MAPA search.

Usage: python scripts/fetch_legal_sources_batch3.py
"""
from __future__ import annotations

import sys
import time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pypdf

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "legal_sources"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TARGETS: list[tuple[str, str]] = [
    # Spanish MAPA pliegos (confirmed URLs)
    ("rueda_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/rueda_2023_11_29.pdf"),
    ("penedes_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/penedes_2022_05_17.pdf"),
    ("navarra_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/navarra_2024_10_11.pdf"),
    ("somontano_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/somontano_2011_01_01.pdf"),
    ("bierzo_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/bierzo_2021_07_23.pdf"),
    # Cava via Galician path
    ("cava_pliego", "https://www.mapa.gob.es/gl/alimentacion/temas/calidad-diferenciada/pliegodecondicionesdopcava_tcm37-564756.pdf"),
]

# Toro — try multiple naming patterns
GUESSES: dict[str, list[str]] = {
    "toro_pliego": [
        "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2023_11_29.pdf",
        "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2022_03_25.pdf",
        "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2021_07_23.pdf",
        "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2020_03_04.pdf",
        "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2024_10_11.pdf",
        "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2025_01_01.pdf",
    ],
}


def fetch_pdf(url: str) -> bytes | None:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Loam Path A research bot)"})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read()
    except HTTPError as e:
        print(f"  HTTP {e.code}: {url[-80:]}")
        return None
    except URLError as e:
        print(f"  URL error: {e}")
        return None
    except Exception as e:
        print(f"  Error: {type(e).__name__}: {e}")
        return None


def extract_text(pdf_bytes: bytes) -> str:
    from io import BytesIO
    reader = pypdf.PdfReader(BytesIO(pdf_bytes))
    pages_out = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            txt = page.extract_text() or ""
        except Exception as e:
            txt = f"[extraction error: {e}]"
        pages_out.append(f"=== PAGE {i} ===\n{txt}")
    return "\n\n".join(pages_out)


def save_fetch(slug: str, url: str) -> bool:
    pdf_bytes = fetch_pdf(url)
    if pdf_bytes is None:
        return False
    if not pdf_bytes.startswith(b"%PDF-"):
        print(f"  Not a PDF (first bytes: {pdf_bytes[:20]!r})")
        return False
    try:
        text = extract_text(pdf_bytes)
    except Exception as e:
        print(f"  pypdf error: {e}")
        return False
    out_path = OUT_DIR / f"{slug}.txt"
    out_path.write_text(text, encoding="utf-8")
    print(f"  Saved {out_path.name} ({len(text):,} chars)")
    return True


def main() -> int:
    successes, failures = [], []

    for slug, url in TARGETS:
        out_path = OUT_DIR / f"{slug}.txt"
        if out_path.exists() and out_path.stat().st_size > 1000:
            print(f"Skipping {slug} (exists)")
            continue
        print(f"Fetching {slug}: {url[-80:]}")
        if save_fetch(slug, url):
            successes.append(slug)
        else:
            failures.append(slug)
        time.sleep(1)

    print("\n=== Guess attempts ===")
    for slug, urls in GUESSES.items():
        out_path = OUT_DIR / f"{slug}.txt"
        if out_path.exists() and out_path.stat().st_size > 1000:
            print(f"Skipping {slug} (exists)")
            continue
        print(f"\n{slug}:")
        ok = False
        for url in urls:
            print(f"  Trying {url[-60:]}")
            if save_fetch(slug, url):
                ok = True
                break
            time.sleep(0.5)
        if ok:
            successes.append(slug)
        else:
            failures.append(slug)

    print(f"\n=== Summary ===")
    print(f"Succeeded: {len(successes)}: {successes}")
    print(f"Failed: {len(failures)}: {failures}")
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
