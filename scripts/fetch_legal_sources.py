"""
Path A: Fetch legal source PDFs for appellation rules and extract text via pypdf.

URLs confirmed via INAO site search. Pattern is wildly inconsistent, so we maintain
a target list of (slug, url) pairs rather than constructing URLs.

Usage: python scripts/fetch_legal_sources.py
Saves output to data/legal_sources/{slug}.txt (plain text extraction).
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

# (slug, url) — slug becomes {slug}.txt
TARGETS: list[tuple[str, str]] = [
    # Bordeaux Left Bank — Médoc communal AOCs
    ("pauillac_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCPauillac.pdf"),
    ("margaux_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCMargaux.pdf"),
    ("saint_julien_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCSaint-Julien.pdf"),
    # Bordeaux Graves
    ("graves_cdc", "https://extranet.inao.gouv.fr/fichier/CDC---Graves-et-Graves-sup%C3%A9rieures---PNO-2023.pdf"),
    ("pessac_leognan_cdc", "https://extranet.inao.gouv.fr/fichier/CDC---Pessac-L%C3%A9ognan---PNO-2024.pdf"),
    # Northern Rhône
    ("crozes_hermitage_cdc", "https://extranet.inao.gouv.fr/fichier/PNO2020CDCCrozesHermitage.pdf"),
    ("cornas_cdc", "https://extranet.inao.gouv.fr/fichier/PNO2023AOPCornas.pdf"),
    ("condrieu_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCCondrieu.pdf"),
    # Beaujolais
    ("morgon_cdc", "https://extranet.inao.gouv.fr/fichier/PNO-CDCMorgon-221130.pdf"),
    # Provence / Languedoc
    ("bandol_cdc", "https://extranet.inao.gouv.fr/fichier/PNO2022AOPBANDOL.pdf"),
    ("minervois_cdc", "https://extranet.inao.gouv.fr/fichier/PNO-AOC-MINERVOIS-2019.docx.pdf"),
]


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


def main() -> int:
    successes: list[str] = []
    failures: list[tuple[str, str]] = []
    skips: list[str] = []

    for slug, url in TARGETS:
        out_path = OUT_DIR / f"{slug}.txt"
        if out_path.exists() and out_path.stat().st_size > 1000:
            skips.append(slug)
            continue

        print(f"Fetching {slug}: {url}")
        pdf_bytes = fetch_pdf(url)
        if pdf_bytes is None:
            failures.append((slug, url))
            continue

        # Sanity check: is this actually a PDF?
        if not pdf_bytes.startswith(b"%PDF-"):
            print(f"  Not a PDF (first bytes: {pdf_bytes[:20]!r}); skipping")
            failures.append((slug, url))
            continue

        try:
            text = extract_text(pdf_bytes)
        except Exception as e:
            print(f"  pypdf error: {e}")
            failures.append((slug, url))
            continue

        out_path.write_text(text, encoding="utf-8")
        print(f"  Saved {out_path.name} ({len(text):,} chars)")
        successes.append(slug)
        time.sleep(1)  # gentle to the server

    print()
    print(f"=== Summary ===")
    print(f"Succeeded: {len(successes)}")
    for s in successes:
        print(f"  + {s}")
    print(f"Skipped (already existed): {len(skips)}")
    for s in skips:
        print(f"  . {s}")
    print(f"Failed: {len(failures)}")
    for slug, url in failures:
        print(f"  - {slug}: {url}")

    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
