"""
Path A batch 4: Italian DOCs via regional gov sites + more French AOCs + Hermitage via JORF.

Italian DOCs use regional government PDF hosting (MASAF-subordinate):
- Veneto: regione.veneto.it sharing system (Nextcloud shares with /download suffix)
- Lombardia: valoritalia.it wp-content or Consorzio Franciacorta

French AOCs from INAO extranet (extranet.inao.gouv.fr/fichier/*).
Hermitage from JORF/Légifrance or info.agriculture.gouv.fr.

Usage: python scripts/fetch_legal_sources_batch4.py
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
    # === ITALIAN DOCs via MASAF-subordinate regional gov ===
    # Veneto (Regione del Veneto — official regional government; MASAF-subordinate)
    ("valpolicella_doc", "https://sharing.regione.veneto.it/index.php/s/yABaGJKHrga9jxQ/download"),
    ("soave_doc", "https://sharing.regione.veneto.it/index.php/s/PxRP64A5pjymRYw/download"),
    ("soave_superiore_docg", "https://sharing.regione.veneto.it/index.php/s/3cKJDw6BCWMDQsf/download"),
    ("bardolino_doc", "https://sharing.regione.veneto.it/index.php/s/2KE5myjcgjbdmyB/download"),
    ("prosecco_doc", "https://sharing.regione.veneto.it/index.php/s/GMSo6kKiEMDzckA/download"),
    ("conegliano_valdobbiadene_docg", "https://sharing.regione.veneto.it/index.php/s/NbiQeKGfZymyFs5/download"),
    ("asolo_prosecco_docg", "https://sharing.regione.veneto.it/index.php/s/Kqgpc6afGJ5i4BD/download"),
    ("amarone_valpolicella_alt", "https://sharing.regione.veneto.it/index.php/s/WpHKzRnRyAGLTyM/download"),
    # Lombardia / Franciacorta via Valoritalia (MASAF-designated control body)
    # Franciacorta disciplinare PDF on valoritalia.it - try known URL patterns
    ("franciacorta_docg", "https://www.valoritalia.it/wp-content/uploads/2023/03/Disciplinare-di-produzione.pdf"),
    # === MORE FRENCH AOCs via INAO ===
    ("corbieres_cdc", "https://extranet.inao.gouv.fr/fichier/CDCCorbieresPNO2019.pdf"),
    ("faugeres_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCdC-Faugeres.pdf"),
    ("saint_peray_cdc", "https://extranet.inao.gouv.fr/fichier/PNO2025AOPSaintPeray.pdf"),
    ("cairanne_cdc", "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCCairanne.pdf"),
]

# URL guesses for missing AOCs (no direct search hit)
GUESSES: dict[str, list[str]] = {
    "savennieres_cdc": [
        "https://extranet.inao.gouv.fr/fichier/PNOCDCSavennieres.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDC-Savennieres.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCAOC-Savennieres.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCSavennieres.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDC-SavenniereS.pdf",
        "https://extranet.inao.gouv.fr/fichier/CDC-Savennieres-PNO.pdf",
    ],
    "menetou_salon_cdc": [
        "https://extranet.inao.gouv.fr/fichier/PNOCDCMenetouSalon.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDC-Menetou-Salon.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCMenetouSalon.pdf",
        "https://extranet.inao.gouv.fr/fichier/CDC-Menetou-Salon-PNO.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNO2022AOPMenetouSalon.pdf",
    ],
    "quincy_cdc": [
        "https://extranet.inao.gouv.fr/fichier/PNOCDCQuincy.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDC-Quincy.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNOCDCAOCQuincy.pdf",
        "https://extranet.inao.gouv.fr/fichier/PNO2022AOPQuincy.pdf",
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
        print(f"Fetching {slug}: {url[:90]}")
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
    return 0 if not failures else 1


if __name__ == "__main__":
    sys.exit(main())
