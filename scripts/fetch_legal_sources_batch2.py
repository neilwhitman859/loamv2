"""
Path A batch 2: Fetch more INAO CDCs + Spanish MAPA pliegos + try URL guesses
for AOCs not directly found via search.

Usage: python scripts/fetch_legal_sources_batch2.py
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

# Primary URLs (confirmed via INAO search)
TARGETS: list[tuple[str, str]] = [
    # Bordeaux — confirmed URLs
    ("pomerol_cdc", "https://extranet.inao.gouv.fr/fichier/4-CDC-Pomerol-PNO.pdf"),
    ("sauternes_full_cdc", "https://extranet.inao.gouv.fr/fichier/4-CDC-Sauternes-PNO.pdf"),
    ("barsac_cdc", "https://extranet.inao.gouv.fr/fichier/4-CDC-Barsac-PNO.pdf"),
    ("saint_emilion_grand_cru_cdc", "https://extranet.inao.gouv.fr/fichier/CDCSaint-Emilion-Grand-cru-PNO2023.pdf"),
    # Northern Rhône — confirmed URLs
    ("saint_joseph_cdc", "https://extranet.inao.gouv.fr/fichier/PNO2023SaintJoseph.pdf"),
    ("cote_rotie_cdc", "https://extranet.inao.gouv.fr/fichier/PNO2023AOPCoteRotie.pdf"),
    ("hermitage_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCAOC-Hermitage.pdf"),
    # Beaujolais + Southern Rhône
    ("moulin_a_vent_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCMoulinaVent.pdf"),
    ("vacqueyras_cdc", "https://extranet.inao.gouv.fr/fichier/pnocdcaoc-vacqueyras.pdf"),
    # Spanish MAPA pliegos (URL pattern: mapa.gob.es/.../dops/{name}_{yyyy-mm-dd}.pdf)
    # Cava
    ("cava_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/cava_2022_09_06.pdf"),
    # Penedès
    ("penedes_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/penedes_2022_09_06.pdf"),
    # Rueda
    ("rueda_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/rueda_2022_09_06.pdf"),
    # Toro
    ("toro_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/toro_2022_09_06.pdf"),
    # Bierzo
    ("bierzo_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/bierzo_2022_09_06.pdf"),
    # Navarra
    ("navarra_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/navarra_2022_09_06.pdf"),
    # Somontano
    ("somontano_pliego", "https://www.mapa.gob.es/dam/mapa/contenido/alimentacion/temas/calidad-agroalimentaria/2017-calidad-diferenciada/nuevo_denominaciones/pliegos-de-condiciones/pliego-condiciones-vinos/dops/somontano_2022_09_06.pdf"),
]

# URL guesses for AOCs where search didn't return an exact URL
# Try multiple patterns per slug; first one that returns a valid PDF wins.
GUESSES: dict[str, list[str]] = {
    "saint_estephe_cdc": [
        "https://extranet.inao.gouv.fr/fichier/PNOCDCSaint-Estephe.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCSaintEstephe.pdf",
        "https://extranet.inao.gouv.fr/fichier/CDC-Saint-Estephe-PNO.pdf",
        "https://extranet.inao.gouv.fr/fichier/4-CDC-Saint-Estephe-PNO.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCSaintEstephe.pdf",
    ],
    "fleurie_cdc": [
        "https://extranet.inao.gouv.fr/fichier/PNOCDCFleurie.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDC-Fleurie.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNO-CDCFleurie.pdf",
        "https://extranet.inao.gouv.fr/fichier/CDC-Fleurie-PNO.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCFleurie.pdf",
    ],
    "saint_emilion_cdc": [
        "https://extranet.inao.gouv.fr/fichier/PNOCDCSaintEmilion.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDC-Saint-Emilion.pdf",
        "https://extranet.inao.gouv.fr/fichier/CDC-Saint-Emilion-PNO.pdf",
        "https://extranet.inao.gouv.fr/fichier/CDCSaint-Emilion-PNO2023.pdf",
    ],
}


def fetch_pdf(url: str) -> bytes | None:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Loam Path A research bot)"})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read()
    except HTTPError as e:
        print(f"  HTTP {e.code}: {url}")
        return None
    except URLError as e:
        print(f"  URL error: {e} on {url}")
        return None
    except Exception as e:
        print(f"  Error: {type(e).__name__}: {e} on {url}")
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


def try_fetch(slug: str, urls: list[str]) -> tuple[bool, str | None]:
    for url in urls:
        print(f"  Trying {url}")
        pdf_bytes = fetch_pdf(url)
        if pdf_bytes is None:
            continue
        if not pdf_bytes.startswith(b"%PDF-"):
            print(f"    Not a PDF (first bytes: {pdf_bytes[:20]!r})")
            continue
        try:
            text = extract_text(pdf_bytes)
        except Exception as e:
            print(f"    pypdf error: {e}")
            continue
        out_path = OUT_DIR / f"{slug}.txt"
        out_path.write_text(text, encoding="utf-8")
        print(f"    Saved {out_path.name} ({len(text):,} chars)")
        return True, url
    return False, None


def main() -> int:
    successes: list[tuple[str, str]] = []
    failures: list[str] = []

    for slug, url in TARGETS:
        out_path = OUT_DIR / f"{slug}.txt"
        if out_path.exists() and out_path.stat().st_size > 1000:
            print(f"Skipping {slug} (exists)")
            continue
        print(f"Fetching {slug}: {url}")
        pdf_bytes = fetch_pdf(url)
        if pdf_bytes is None or not pdf_bytes.startswith(b"%PDF-"):
            if pdf_bytes is not None:
                print(f"  Not a PDF (first bytes: {pdf_bytes[:20]!r})")
            failures.append(slug)
            continue
        try:
            text = extract_text(pdf_bytes)
        except Exception as e:
            print(f"  pypdf error: {e}")
            failures.append(slug)
            continue
        out_path.write_text(text, encoding="utf-8")
        print(f"  Saved {out_path.name} ({len(text):,} chars)")
        successes.append((slug, url))
        time.sleep(1)

    print()
    print("=== Guess attempts ===")
    for slug, urls in GUESSES.items():
        out_path = OUT_DIR / f"{slug}.txt"
        if out_path.exists() and out_path.stat().st_size > 1000:
            print(f"Skipping {slug} (exists)")
            continue
        print(f"\n{slug}:")
        ok, url = try_fetch(slug, urls)
        if ok:
            successes.append((slug, url))
        else:
            failures.append(slug)
        time.sleep(1)

    print()
    print(f"=== Summary ===")
    print(f"Succeeded: {len(successes)}")
    for s, u in successes:
        print(f"  + {s}  <-  {u}")
    print(f"Failed: {len(failures)}")
    for s in failures:
        print(f"  - {s}")

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
