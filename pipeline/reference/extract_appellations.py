"""
Extract appellation names from nested JSON structure to text + JSON files.

Reads a JSON file containing appellation data (from a Supabase MCP query result),
groups by country ISO code, and writes both a text listing and a JSON lookup.

Usage:
    python -m pipeline.reference.extract_appellations <input.json> [output.txt]
"""

import argparse
import json
import sys
from pathlib import Path

# Allow running as module from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main():
    parser = argparse.ArgumentParser(description="Extract appellations from nested JSON")
    parser.add_argument("input_file", help="Input JSON file (nested MCP result)")
    parser.add_argument("output_file", nargs="?", default="appellations_export.txt",
                        help="Output text file (default: appellations_export.txt)")
    args = parser.parse_args()

    raw = Path(args.input_file).read_text(encoding="utf-8")

    # Parse nested structure: [{type:"text", text: "{\"result\":\"...\\n[{...}]\\n...\"}"}]
    outer = json.loads(raw)
    inner = json.loads(outer[0]["text"])  # {result: "...\n[...]\n..."}
    result_str = inner["result"]

    # Find the JSON array between the untrusted-data tags
    import re
    array_match = re.search(r"\[[\s\S]*\]", result_str)
    if not array_match:
        print("No JSON array found in result")
        sys.exit(1)
    data = json.loads(array_match.group(0))

    by_country: dict[str, dict] = {}
    for r in data:
        iso = r["iso_code"]
        if iso not in by_country:
            by_country[iso] = {"country": r["country"], "appellations": []}
        by_country[iso]["appellations"].append({"name": r["appellation"], "type": r["designation_type"]})

    lines = []
    for code, info in sorted(by_country.items(), key=lambda x: -len(x[1]["appellations"])):
        lines.append(f"\n=== {info['country']} ({code}) -- {len(info['appellations'])} appellations ===")
        for a in info["appellations"]:
            lines.append(f"  {a['type']}: {a['name']}")

    out_path = Path(args.output_file)
    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Written to {out_path} ({len(by_country)} countries, {len(data)} appellations)")

    # Also output as JSON for easy consumption
    json_out = {}
    for code, info in by_country.items():
        json_out[code] = [a["name"] for a in info["appellations"]]
    json_path = out_path.with_suffix(".json")
    json_path.write_text(json.dumps(json_out, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
