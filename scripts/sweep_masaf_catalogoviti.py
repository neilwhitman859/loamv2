"""
Sweep the MASAF catalogoviti.politicheagricole.it q= ID space to discover
all Italian DOC/DOCG disciplinari.

URL pattern: http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q={id}

Strategy:
1. Sweep q=1000-1100 (DOCGs are typically in this range)
2. Sweep q=2000-2500 (DOCs are typically in this range)
3. For each valid PDF, extract the denomination name from the first page
4. Save to data/legal_sources/masaf_q{id}.txt
5. Build a JSON index mapping q_id → name for later seeding

Usage: python scripts/sweep_masaf_catalogoviti.py [--range 1000-1100]
"""
from __future__ import annotations

import json
import re
import sys
import time
from io import BytesIO
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import pypdf

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "legal_sources"
INDEX_PATH = ROOT / "data" / "legal_sources" / "_masaf_catalogoviti_index.json"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MASAF_BASE = "http://catalogoviti.politicheagricole.it/scheda_denom.php?t=dsc&q={id}"


def fetch_pdf(url: str) -> bytes | None:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (Loam Path A research bot)"})
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read()
    except HTTPError as e:
        return None
    except URLError:
        return None
    except Exception:
        return None


def extract_text(pdf_bytes: bytes) -> str:
    reader = pypdf.PdfReader(BytesIO(pdf_bytes))
    pages_out = []
    for i, page in enumerate(reader.pages, start=1):
        try:
            txt = page.extract_text() or ""
        except Exception:
            txt = ""
        pages_out.append(f"=== PAGE {i} ===\n{txt}")
    return "\n\n".join(pages_out)


NAME_PATTERNS = [
    re.compile(r'denominazione di origine[^"]*?\s*[«"]([^"»]+)[»"]', re.IGNORECASE),
    re.compile(r'["«]([^»"]{3,60})[»"]\s*DOC', re.IGNORECASE),
    re.compile(r'DOCG[^"]*?\s*[«"]([^"»]+)[»"]', re.IGNORECASE),
    re.compile(r'DOC\s*[«"]([^"»]+)[»"]', re.IGNORECASE),
]


def extract_denomination_name(text: str) -> str:
    # Look in first 2000 characters
    snippet = text[:3000]
    for pattern in NAME_PATTERNS:
        match = pattern.search(snippet)
        if match:
            name = match.group(1).strip()
            if 3 <= len(name) <= 60:
                return name
    return ""


def main() -> int:
    # Default: sweep common DOCG + DOC ranges
    ranges = [(1001, 1100), (2000, 2500)]

    if len(sys.argv) > 1 and sys.argv[1] == "--range":
        parts = sys.argv[2].split("-")
        ranges = [(int(parts[0]), int(parts[1]))]

    # Load existing index
    if INDEX_PATH.exists():
        index = json.loads(INDEX_PATH.read_text())
    else:
        index = {}

    hits = 0
    misses = 0
    for low, high in ranges:
        print(f"\n=== Sweeping q={low}..{high} ===")
        for q in range(low, high + 1):
            str_q = str(q)
            if str_q in index and index[str_q].get("status") == "ok":
                continue

            url = MASAF_BASE.format(id=q)
            pdf_bytes = fetch_pdf(url)

            if pdf_bytes is None or not pdf_bytes.startswith(b"%PDF-"):
                index[str_q] = {"status": "missing"}
                misses += 1
                if misses % 20 == 0:
                    print(f"  q={q}: still sweeping ({hits} hits, {misses} misses)")
                time.sleep(0.2)
                continue

            try:
                text = extract_text(pdf_bytes)
            except Exception:
                index[str_q] = {"status": "extraction_error"}
                time.sleep(0.2)
                continue

            name = extract_denomination_name(text)
            out_path = OUT_DIR / f"masaf_q{q}.txt"
            out_path.write_text(text, encoding="utf-8")

            index[str_q] = {
                "status": "ok",
                "name": name,
                "chars": len(text),
                "file": out_path.name,
            }
            hits += 1
            print(f"  q={q}: {name or '(no name extracted)'} — {len(text):,} chars")
            time.sleep(0.3)

        # Flush index after each range
        INDEX_PATH.write_text(json.dumps(index, indent=2, ensure_ascii=False))

    print(f"\n=== Summary ===")
    print(f"Total index entries: {len(index)}")
    print(f"Hits this run: {hits}")
    print(f"Misses this run: {misses}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
