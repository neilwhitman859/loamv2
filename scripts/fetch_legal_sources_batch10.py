"""
Path A batch 10: Mass fetch Italian DOCs/DOCGs via MASAF catalogoviti
(catalogoviti.politicheagricole.it is BACK ONLINE — this is the direct
official Italian Ministry source for all wine disciplinari).

URL pattern: http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q={numeric_id}

Usage: python scripts/fetch_legal_sources_batch10.py
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

MASAF_BASE = "http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q={id}"

# Confirmed IDs from WebSearch
TARGETS: list[tuple[str, int]] = [
    # Piemonte
    ("asti_docg_masaf", 1005),
    ("barbera_dasti_docg", 1008),
    ("gavi_docg_masaf", 1040),
    ("roero_docg_masaf", 1057),
    ("barbera_monferrato", 2021),
    # Tuscany
    ("chianti_docg_masaf", 1022),
    ("bolgheri_doc_masaf", 2033),
    # Umbria
    ("montefalco_sagrantino_masaf", 1045),
    # Campania
    ("fiano_di_avellino_masaf", 1036),
    ("irpinia_doc", 2156),
    # Lombardia
    ("franciacorta_docg_masaf", 1037),
    ("oltrepo_pavese_metodo_classico", 1050),
    ("valtellina_superiore_masaf", 1068),
    # Veneto
    ("asolo_prosecco_docg_masaf", 1024),
    # Sardegna
    ("vermentino_di_gallura_masaf", 1070),
    ("vermentino_di_sardegna_masaf", 2323),
    # Alto Adige
    ("alto_adige_doc", 2010),
    # Friuli
    ("colli_orientali_friuli_picolit", 1028),
    ("friuli_colli_orientali", 2134),
    ("friuli_grave", 2135),
    # Abruzzo
    ("montepulciano_dabruzzo_doc", 2200),
]


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


def main() -> int:
    successes, failures = [], []
    for slug, doc_id in TARGETS:
        out_path = OUT_DIR / f"{slug}.txt"
        if out_path.exists() and out_path.stat().st_size > 1000:
            continue
        url = MASAF_BASE.format(id=doc_id)
        print(f"Fetching {slug} (q={doc_id})")
        pdf_bytes = fetch_pdf(url)
        if pdf_bytes is None:
            failures.append(slug)
            continue
        if not pdf_bytes.startswith(b"%PDF-"):
            print(f"  Not a PDF ({pdf_bytes[:20]!r})")
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
        successes.append(slug)
        time.sleep(0.5)

    print(f"\n=== Summary ===")
    print(f"Succeeded ({len(successes)}): {successes}")
    print(f"Failed ({len(failures)}): {failures}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
