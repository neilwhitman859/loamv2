"""
Path A batch 8: Austrian DACs (Bundeskellereiinspektion) + Italian Tuscan DOCs (Valoritalia).

Austria: All 18 DAC Verordnungen published by BML via Bundeskellereiinspektion:
  https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_{name}.pdf

Italy Tuscany: Valoritalia (MASAF-designated control body) hosts disciplinari:
  https://www.valoritalia.it/wp-content/uploads/{YYYY}/{MM}/{Name}.pdf

Usage: python scripts/fetch_legal_sources_batch8.py
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
    # === AUSTRIAN DACs via Bundeskellereiinspektion (BML subordinate) ===
    # Niederösterreich (Lower Austria)
    ("dac_wachau", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_wachau.pdf"),
    ("dac_kamptal", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_kamptal.pdf"),
    ("dac_kremstal", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_kremstal.pdf"),
    ("dac_traisental", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_traisental.pdf"),
    ("dac_wagram", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_wagram.pdf"),
    ("dac_weinviertel", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_weinviertel.pdf"),
    ("dac_carnuntum", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_carnuntum.pdf"),
    ("dac_thermenregion", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_thermenregion.pdf"),
    # Burgenland
    ("dac_neusiedlersee", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_neusiedlersee.pdf"),
    ("dac_leithaberg", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_leithaberg.pdf"),
    ("dac_ruster_ausbruch", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_ruster_ausbruch.pdf"),
    ("dac_mittelburgenland", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_mittelburgenland.pdf"),
    ("dac_rosalia", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_rosalia.pdf"),
    ("dac_eisenberg", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_eisenberg.pdf"),
    # Wien
    ("dac_wien", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_wien.pdf"),
    # Steiermark
    ("dac_suedsteiermark", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_suedsteiermark.pdf"),
    ("dac_weststeiermark", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_weststeiermark.pdf"),
    ("dac_vulkanland_steiermark", "https://www.bundeskellereiinspektion.at/downloads/dac_vo/dac_vo_vulkanland_steiermark.pdf"),

    # === ITALIAN TUSCAN DOCs via Valoritalia ===
    ("bolgheri_doc", "https://www.valoritalia.it/wp-content/uploads/2021/03/Bolgheri-Nuovo-disciplinare.pdf"),
    ("vernaccia_san_gimignano_docg", "https://www.valoritalia.it/wp-content/uploads/2022/10/Vernaccia-di-San-Gimignano-1.pdf"),
    ("morellino_di_scansano_docg", "https://www.valoritalia.it/wp-content/uploads/2022/10/Morellino-di-Scansano-1.pdf"),
    ("san_gimignano_doc", "https://www.valoritalia.it/wp-content/uploads/2022/10/San-Gimignano-1.pdf"),
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
            continue
        print(f"Fetching {slug}: {url[-90:]}")
        if save_fetch(slug, url):
            successes.append(slug)
        else:
            failures.append(slug)
        time.sleep(0.5)

    print(f"\n=== Summary ===")
    print(f"Succeeded ({len(successes)}): {successes}")
    print(f"Failed ({len(failures)}): {failures}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
