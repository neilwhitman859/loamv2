#!/usr/bin/env python3
"""
OCR Bake-off: Compare OCR tools on TTB wine label images.

Tests EasyOCR, RapidOCR, and Claude Vision (Haiku) on a set of
test label images. Measures accuracy, speed, and text coverage.

Usage:
    python -m pipeline.analyze.ocr_bakeoff
    python -m pipeline.analyze.ocr_bakeoff --image-dir data/test_labels --limit 5
    python -m pipeline.analyze.ocr_bakeoff --skip-claude  # skip API calls
"""

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path

from PIL import Image

# Force UTF-8 on Windows
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

# ─── Constants ───────────────────────────────────────────────────────────────

DEFAULT_IMAGE_DIR = Path(__file__).resolve().parents[2] / "data" / "test_labels"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[2] / "data" / "stats" / "ocr_bakeoff_results.json"
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp"}


# ─── OCR Runners ─────────────────────────────────────────────────────────────

def run_easyocr(img_path: str, languages: list[str]) -> dict:
    """Run EasyOCR on a single image."""
    import easyocr

    # EasyOCR caches model on first call
    if not hasattr(run_easyocr, "_reader"):
        print("  [EasyOCR] Loading models (first time)...")
        run_easyocr._reader = easyocr.Reader(languages, gpu=False, verbose=False)

    reader = run_easyocr._reader
    t0 = time.perf_counter()
    results = reader.readtext(img_path, detail=1)
    elapsed = time.perf_counter() - t0

    # Collect text with confidence
    lines = []
    total_conf = 0
    for bbox, text, conf in results:
        lines.append({"text": text.strip(), "confidence": round(conf, 3)})
        total_conf += conf

    full_text = " ".join(r["text"] for r in lines if r["text"])
    avg_conf = total_conf / len(results) if results else 0

    return {
        "tool": "EasyOCR",
        "elapsed_sec": round(elapsed, 2),
        "text": full_text,
        "char_count": len(full_text),
        "word_count": len(full_text.split()),
        "line_count": len(lines),
        "avg_confidence": round(avg_conf, 3),
        "lines": lines,
    }


def run_rapidocr(img_path: str) -> dict:
    """Run RapidOCR (PaddleOCR models via ONNX Runtime) on a single image."""
    from rapidocr_onnxruntime import RapidOCR

    if not hasattr(run_rapidocr, "_engine"):
        print("  [RapidOCR] Loading models (first time)...")
        run_rapidocr._engine = RapidOCR()

    engine = run_rapidocr._engine
    t0 = time.perf_counter()
    result, elapse = engine(img_path)
    elapsed = time.perf_counter() - t0

    lines = []
    total_conf = 0
    if result:
        for bbox, text, conf in result:
            lines.append({"text": text.strip(), "confidence": round(conf, 3)})
            total_conf += conf

    full_text = " ".join(r["text"] for r in lines if r["text"])
    avg_conf = total_conf / len(lines) if lines else 0

    return {
        "tool": "RapidOCR",
        "elapsed_sec": round(elapsed, 2),
        "text": full_text,
        "char_count": len(full_text),
        "word_count": len(full_text.split()),
        "line_count": len(lines),
        "avg_confidence": round(avg_conf, 3),
        "lines": lines,
    }


def run_claude_vision(img_path: str) -> dict:
    """Run Claude Vision (Haiku) on a single image as gold standard."""
    import anthropic
    from pipeline.lib.db import get_env

    if not hasattr(run_claude_vision, "_client"):
        run_claude_vision._client = anthropic.Anthropic(
            api_key=get_env("ANTHROPIC_API_KEY")
        )

    client = run_claude_vision._client

    # Read and encode image
    with open(img_path, "rb") as f:
        img_data = base64.standard_b64encode(f.read()).decode("utf-8")

    ext = Path(img_path).suffix.lower()
    media_type = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".gif": "image/gif",
        ".webp": "image/webp", ".bmp": "image/bmp",
    }.get(ext, "image/jpeg")

    t0 = time.perf_counter()
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=2000,
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": img_data,
                    },
                },
                {
                    "type": "text",
                    "text": (
                        "Extract ALL visible text from this wine label image. "
                        "Transcribe everything you can read — producer name, wine name, "
                        "vintage, appellation, grape varieties, percentages, ABV, "
                        "winemaker notes, food pairings, technical data (pH, TA, RS), "
                        "certifications, awards, importer info, government warnings, "
                        "and any other text. Preserve the original language. "
                        "Output the raw text only, no commentary."
                    ),
                },
            ],
        }],
    )
    elapsed = time.perf_counter() - t0

    text = response.content[0].text.strip()
    usage = response.usage

    return {
        "tool": "Claude Vision (Haiku)",
        "elapsed_sec": round(elapsed, 2),
        "text": text,
        "char_count": len(text),
        "word_count": len(text.split()),
        "line_count": text.count("\n") + 1,
        "avg_confidence": 1.0,  # Claude doesn't report per-line confidence
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "cost_usd": round(
            usage.input_tokens * 0.80 / 1_000_000
            + usage.output_tokens * 4.00 / 1_000_000, 5
        ),
    }


# ─── Main ────────────────────────────────────────────────────────────────────

def find_images(image_dir: Path) -> list[Path]:
    """Find all image files in directory."""
    images = []
    for f in sorted(image_dir.iterdir()):
        if f.suffix.lower() in IMAGE_EXTS:
            images.append(f)
    return images


def print_comparison(filename: str, results: list[dict]):
    """Print side-by-side comparison for one image."""
    print(f"\n{'='*80}")
    print(f"  {filename}")
    print(f"{'='*80}")

    for r in results:
        tool = r["tool"]
        chars = r["char_count"]
        words = r["word_count"]
        elapsed = r["elapsed_sec"]
        conf = r.get("avg_confidence", 0)

        cost_str = ""
        if "cost_usd" in r:
            cost_str = f"  cost=${r['cost_usd']:.4f}"

        print(f"\n  [{tool}]  {chars} chars, {words} words, {elapsed:.1f}s, conf={conf:.2f}{cost_str}")
        print(f"  {'-'*70}")

        # Show first 300 chars of extracted text
        text = r["text"]
        preview = text[:300].replace("\n", " \\n ")
        if len(text) > 300:
            preview += "..."
        print(f"  {preview}")


def main():
    parser = argparse.ArgumentParser(description="OCR Bake-off on wine labels")
    parser.add_argument("--image-dir", type=Path, default=DEFAULT_IMAGE_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=None, help="Max images to process")
    parser.add_argument("--skip-claude", action="store_true", help="Skip Claude Vision API calls")
    parser.add_argument("--languages", default="en,fr,it,es,de,pt",
                        help="EasyOCR languages (comma-separated)")
    args = parser.parse_args()

    images = find_images(args.image_dir)
    if args.limit:
        images = images[:args.limit]

    print(f"OCR Bake-off: {len(images)} images from {args.image_dir}")
    print(f"Tools: EasyOCR, RapidOCR" + ("" if args.skip_claude else ", Claude Vision (Haiku)"))
    print()

    languages = args.languages.split(",")
    all_results = []

    # Totals for summary
    totals = {}
    tool_names = ["EasyOCR", "RapidOCR"]
    if not args.skip_claude:
        tool_names.append("Claude Vision (Haiku)")
    for t in tool_names:
        totals[t] = {"time": 0, "chars": 0, "words": 0, "cost": 0, "count": 0}

    for i, img_path in enumerate(images):
        filename = img_path.name
        print(f"\n[{i+1}/{len(images)}] Processing: {filename}")

        image_results = {"filename": filename, "results": []}

        # Run each OCR tool
        try:
            r = run_easyocr(str(img_path), languages)
            image_results["results"].append(r)
            totals["EasyOCR"]["time"] += r["elapsed_sec"]
            totals["EasyOCR"]["chars"] += r["char_count"]
            totals["EasyOCR"]["words"] += r["word_count"]
            totals["EasyOCR"]["count"] += 1
        except Exception as e:
            print(f"  EasyOCR ERROR: {e}")
            image_results["results"].append({"tool": "EasyOCR", "error": str(e)})

        try:
            r = run_rapidocr(str(img_path))
            image_results["results"].append(r)
            totals["RapidOCR"]["time"] += r["elapsed_sec"]
            totals["RapidOCR"]["chars"] += r["char_count"]
            totals["RapidOCR"]["words"] += r["word_count"]
            totals["RapidOCR"]["count"] += 1
        except Exception as e:
            print(f"  RapidOCR ERROR: {e}")
            image_results["results"].append({"tool": "RapidOCR", "error": str(e)})

        if not args.skip_claude:
            try:
                r = run_claude_vision(str(img_path))
                image_results["results"].append(r)
                totals["Claude Vision (Haiku)"]["time"] += r["elapsed_sec"]
                totals["Claude Vision (Haiku)"]["chars"] += r["char_count"]
                totals["Claude Vision (Haiku)"]["words"] += r["word_count"]
                totals["Claude Vision (Haiku)"]["cost"] += r.get("cost_usd", 0)
                totals["Claude Vision (Haiku)"]["count"] += 1
            except Exception as e:
                print(f"  Claude Vision ERROR: {e}")
                image_results["results"].append({"tool": "Claude Vision (Haiku)", "error": str(e)})

        # Print comparison for this image
        valid = [r for r in image_results["results"] if "error" not in r]
        if valid:
            print_comparison(filename, valid)

        all_results.append(image_results)

    # ─── Summary ─────────────────────────────────────────────────────────
    print(f"\n\n{'='*80}")
    print(f"  SUMMARY ({len(images)} images)")
    print(f"{'='*80}\n")

    # Use Claude as baseline for comparison
    claude_total_chars = totals.get("Claude Vision (Haiku)", {}).get("chars", 0)

    fmt = "  {:<25s}  {:>8s}  {:>8s}  {:>10s}  {:>8s}  {:>10s}"
    print(fmt.format("Tool", "Time", "Avg/img", "Total chars", "Words", "vs Claude"))
    print(fmt.format("-" * 25, "-" * 8, "-" * 8, "-" * 10, "-" * 8, "-" * 10))

    for tool_name in tool_names:
        t = totals[tool_name]
        n = t["count"] or 1
        avg = t["time"] / n
        vs = ""
        if claude_total_chars > 0 and tool_name != "Claude Vision (Haiku)":
            vs = f"{t['chars'] / claude_total_chars * 100:.0f}%"
        elif tool_name == "Claude Vision (Haiku)":
            vs = "baseline"

        cost_str = ""
        if t["cost"] > 0:
            cost_str = f"  (${t['cost']:.4f})"

        print(fmt.format(
            tool_name,
            f"{t['time']:.1f}s",
            f"{avg:.1f}s",
            str(t["chars"]),
            str(t["words"]),
            vs,
        ) + cost_str)

    # ─── Save results ────────────────────────────────────────────────────
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output_data = {
        "metadata": {
            "run_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "image_count": len(images),
            "image_dir": str(args.image_dir),
            "tools": tool_names,
            "totals": totals,
        },
        "results": all_results,
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    print(f"\n  Results saved to: {args.output}")


if __name__ == "__main__":
    main()
