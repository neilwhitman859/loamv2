"""
Path A batch 5: Portuguese DOPs (IVV) + Campania DOCGs (Regione Campania) +
more Italian DOCs.

IVV.gov.pt hosts Portuguese cadernos de especificações via template URL.
Regione Campania hosts DOCG PDFs on agricoltura.regione.campania.it/pubblicazioni.

Usage: python scripts/fetch_legal_sources_batch5.py
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

IVV_BASE = "https://www.ivv.gov.pt/np4/file.tpl?fileName={filename}&newsId=8617"

TARGETS: list[tuple[str, str]] = [
    # === PORTUGUESE DOPs via IVV ===
    ("douro_caderno", IVV_BASE.format(filename="PDO_Douro.pdf")),
    ("porto_caderno", IVV_BASE.format(filename="DO_Porto.pdf")),
    ("vinho_verde_caderno", IVV_BASE.format(filename="DO_Vinho_Verde.pdf")),
    ("dao_caderno", IVV_BASE.format(filename="DOP_D_O___Caderno_de_especifica__es_fina.pdf")),
    ("madeira_caderno", IVV_BASE.format(filename="DO_Madeira.pdf")),
    ("alentejo_caderno", IVV_BASE.format(filename="DO_Alentejo.pdf")),
    ("bairrada_caderno", IVV_BASE.format(filename="DO_Bairrada.pdf")),
    # === CAMPANIA DOCGs via Regione Campania ===
    ("fiano_di_avellino_docg", "https://agricoltura.regione.campania.it/pubblicazioni/pdf/DOCGFIA1.PDF"),
    ("greco_di_tufo_docg", "https://agricoltura.regione.campania.it/pubblicazioni/pdf/DOCGGRE1.PDF"),
    ("taurasi_docg", "https://agricoltura.regione.campania.it/pubblicazioni/pdf/DOCGTAU1.PDF"),
]

# IVV URL guesses for other Portuguese DOPs
GUESSES: dict[str, list[str]] = {
    "tejo_caderno": [
        IVV_BASE.format(filename="DO_Tejo.pdf"),
        IVV_BASE.format(filename="PDO_Tejo.pdf"),
    ],
    "lisboa_caderno": [
        IVV_BASE.format(filename="DO_Lisboa.pdf"),
        IVV_BASE.format(filename="PDO_Lisboa.pdf"),
    ],
    "peninsula_setubal_caderno": [
        IVV_BASE.format(filename="DO_Peninsula_de_Setubal.pdf"),
        IVV_BASE.format(filename="PDO_Peninsula_de_Setubal.pdf"),
        IVV_BASE.format(filename="DO_Pen__nsula_de_Set__bal.pdf"),
    ],
    "colares_caderno": [
        IVV_BASE.format(filename="DO_Colares.pdf"),
        IVV_BASE.format(filename="PDO_Colares.pdf"),
    ],
    "bucelas_caderno": [
        IVV_BASE.format(filename="DO_Bucelas.pdf"),
        IVV_BASE.format(filename="PDO_Bucelas.pdf"),
    ],
    "carcavelos_caderno": [
        IVV_BASE.format(filename="DO_Carcavelos.pdf"),
    ],
    # Campania guesses for other DOCs
    "aglianico_taburno_docg": [
        "https://agricoltura.regione.campania.it/pubblicazioni/pdf/DOCGAGL1.PDF",
        "https://agricoltura.regione.campania.it/pubblicazioni/pdf/DOCG_Aglianico_Taburno.pdf",
    ],
}


def fetch_pdf(url: str) -> bytes | None:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Loam Path A research bot)"})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read()
    except HTTPError as e:
        print(f"  HTTP {e.code}")
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
        print(f"  Not a PDF ({pdf_bytes[:20]!r})")
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
            continue
        print(f"\n{slug}:")
        ok = False
        for url in urls:
            print(f"  Trying {url[-70:]}")
            if save_fetch(slug, url):
                ok = True
                break
            time.sleep(0.3)
        if ok:
            successes.append(slug)
        else:
            failures.append(slug)

    print(f"\n=== Summary ===")
    print(f"Succeeded: {len(successes)}: {successes}")
    print(f"Failed: {len(failures)}: {failures}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
