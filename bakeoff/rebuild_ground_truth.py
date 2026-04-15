#!/usr/bin/env python3
"""Rebuild Task 2 ground truth using model consensus + HTML text verification.

For each page:
1. Collect what all completed models extracted
2. Find consensus values (50%+ models agree)
3. Verify consensus values appear in the HTML text
4. Flag uncertain cases for human review

Writes updated truth_page_*.json files and a review log.
"""

import json
import os
import re
from collections import Counter
from html.parser import HTMLParser
from pathlib import Path

DATA_DIR = Path("bakeoff/data/task2")
RESULTS_DIR = Path("bakeoff/results/task2")


class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self.skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self.skip = False

    def handle_data(self, data):
        if not self.skip:
            t = data.strip()
            if t:
                self.text.append(t)


def get_page_text(html):
    ext = TextExtractor()
    ext.feed(html)
    return " ".join(ext.text)


def consensus_numeric(field, wines, text, tolerance=0.1):
    vals = [w.get(field) for w in wines if w.get(field) is not None]
    if not vals:
        return None, None
    rounded = [round(v, 2) if isinstance(v, float) else v for v in vals]
    c = Counter(str(v) for v in rounded)
    top_val, top_count = c.most_common(1)[0]
    agreement = top_count / len(wines)

    try:
        num = float(top_val)
    except (ValueError, TypeError):
        return None, None

    # Verify in text
    check_strs = [top_val]
    if "." in top_val:
        check_strs.append(top_val.rstrip("0").rstrip("."))
    else:
        check_strs.append(str(int(num)))
    in_text = any(s in text for s in check_strs)

    if agreement >= 0.5 and in_text:
        result = num if "." in top_val else int(num)
        return result, None
    elif agreement >= 0.5 and not in_text:
        return None, f"Consensus {top_val} ({top_count}/{len(wines)}) but NOT found in HTML text"
    elif agreement >= 0.3 and in_text:
        result = num if "." in top_val else int(num)
        return result, f"Weak consensus {top_val} ({top_count}/{len(wines)}) but confirmed in HTML"
    return None, None


def consensus_text_field(field, wines, text):
    vals = [w.get(field) for w in wines if w.get(field) is not None and w.get(field) != ""]
    if not vals:
        return None, None
    c = Counter(str(v).lower().strip() for v in vals)
    top_val, top_count = c.most_common(1)[0]
    agreement = top_count / len(wines)
    in_text = top_val.lower() in text.lower()

    if agreement >= 0.4 and in_text:
        # Return original casing
        for v in vals:
            if str(v).lower().strip() == top_val:
                return str(v).strip(), None
        return top_val, None
    elif agreement >= 0.4 and not in_text:
        return None, f"Consensus '{top_val}' ({top_count}/{len(wines)}) but NOT in HTML"
    return None, None


def consensus_farming(wines, text):
    vals = [w.get("farming") for w in wines if w.get("farming") is not None]
    if not vals:
        return None, None
    c = Counter(str(v).lower().strip() for v in vals)
    top_val, top_count = c.most_common(1)[0]
    agreement = top_count / len(wines)

    keywords = {
        "organic": ["organic", "organically", "organically grown"],
        "biodynamic": ["biodynamic"],
        "sustainable": ["sustainable", "sustainability", "sustainably"],
        "conventional": [],
    }
    kws = keywords.get(top_val, [top_val])
    in_text = any(kw.lower() in text.lower() for kw in kws) if kws else False

    if agreement >= 0.4 and in_text:
        return top_val, None
    elif agreement >= 0.4 and not in_text:
        return None, f"Farming consensus '{top_val}' ({top_count}/{len(wines)}) but NOT in HTML"
    return None, None


def consensus_blend(wines, text):
    blends = [w.get("blend") for w in wines
              if isinstance(w.get("blend"), list) and len(w.get("blend", [])) > 0]
    null_count = sum(1 for w in wines if w.get("blend") is None)

    if len(blends) < len(wines) * 0.3:
        return None, None

    # Find most common grape count
    grape_counts = Counter(len(b) for b in blends)
    target_count = grape_counts.most_common(1)[0][0]
    candidates = [b for b in blends if len(b) == target_count]

    if not candidates:
        return None, None

    # Verify grape names in text
    best = candidates[0]
    verified_grapes = []
    for g in best:
        if not isinstance(g, dict):
            continue
        gname = g.get("grape", "")
        if gname.lower() in text.lower():
            verified_grapes.append(g)

    if len(verified_grapes) == len(best) and len(best) > 0:
        return best, None
    elif len(verified_grapes) > 0:
        return verified_grapes, f"Partial blend verified ({len(verified_grapes)}/{len(best)} grapes in HTML)"
    elif len(candidates) >= len(wines) * 0.5:
        return None, f"Blend consensus ({len(candidates)}/{len(wines)}) but grapes NOT in HTML"
    return None, None


def consensus_oak(wines, text):
    oaks = [w.get("oak") for w in wines
            if isinstance(w.get("oak"), dict) and any(v is not None for v in (w.get("oak") or {}).values())]
    if len(oaks) < len(wines) * 0.3:
        return None, None

    result = {}
    flags = []
    for field in ["vessel", "months", "new_pct", "origin"]:
        vals = [o.get(field) for o in oaks if o.get(field) is not None]
        if not vals:
            result[field] = None
            continue
        c = Counter(str(v).lower() for v in vals)
        top_val, top_count = c.most_common(1)[0]
        in_text = top_val.lower() in text.lower()

        if top_count >= len(oaks) * 0.4:
            if field in ("months", "new_pct"):
                try:
                    num = float(top_val)
                    num_str = str(int(num)) if num == int(num) else str(num)
                    if num_str in text:
                        result[field] = int(num) if field == "months" else num
                    else:
                        result[field] = None
                        flags.append(f"Oak {field}={top_val} not in HTML")
                except (ValueError, TypeError):
                    result[field] = None
            else:
                if in_text:
                    result[field] = top_val
                else:
                    result[field] = None
                    flags.append(f"Oak {field}='{top_val}' not in HTML")
        else:
            result[field] = None

    if any(v is not None for v in result.values()):
        flag = "; ".join(flags) if flags else None
        return result, flag
    return None, None


def consensus_chemistry(wines, text):
    chems = [w.get("chemistry") for w in wines
             if isinstance(w.get("chemistry"), dict) and any(v is not None for v in (w.get("chemistry") or {}).values())]
    if len(chems) < len(wines) * 0.3:
        return None, None

    result = {}
    flags = []
    for field in ["ph", "ta_g_l", "rs_g_l", "so2_total_mg_l"]:
        vals = [c.get(field) for c in chems if c.get(field) is not None and isinstance(c.get(field), (int, float))]
        if not vals:
            result[field] = None
            continue
        c = Counter(str(round(v, 2)) for v in vals)
        top_val, top_count = c.most_common(1)[0]
        if top_count >= len(chems) * 0.4 and top_val in text:
            result[field] = float(top_val)
        else:
            result[field] = None
            if top_count >= len(chems) * 0.4:
                flags.append(f"Chem {field}={top_val} not in HTML")

    if any(v is not None for v in result.values()):
        flag = "; ".join(flags) if flags else None
        return result, flag
    return None, None


def main():
    # Load all complete model results
    model_dirs = [d for d in os.listdir(RESULTS_DIR)
                  if os.path.isdir(RESULTS_DIR / d)
                  and len([f for f in os.listdir(RESULTS_DIR / d) if f.endswith(".json")]) >= 50]
    print(f"Using {len(model_dirs)} complete models for consensus\n")

    new_truths = []
    review_items = []
    changes = 0

    for page_num in range(1, 51):
        page_id = f"page_{page_num:03d}"

        html_file = DATA_DIR / f"final_page_{page_num:03d}.html"
        truth_file = DATA_DIR / f"truth_{page_id}.json"
        meta_file = DATA_DIR / f"final_page_{page_num:03d}_meta.json"

        if not html_file.exists() or not truth_file.exists():
            continue

        html = html_file.read_text(encoding="utf-8", errors="ignore")
        page_text = get_page_text(html)
        old_truth = json.loads(truth_file.read_text(encoding="utf-8"))
        meta = json.loads(meta_file.read_text(encoding="utf-8")) if meta_file.exists() else {}

        # Collect model extractions
        model_wines = []
        for md in model_dirs:
            rf = RESULTS_DIR / md / f"{page_id}.json"
            if not rf.exists():
                continue
            r = json.loads(rf.read_text(encoding="utf-8"))
            if r.get("wines_found") and r.get("parse_ok"):
                model_wines.append(r["wines_found"][0])

        if not model_wines:
            new_truths.append(old_truth)
            continue

        # Build consensus
        page_flags = []

        vintage, flag = consensus_numeric("vintage_year", model_wines, page_text)
        if flag:
            page_flags.append(("vintage_year", flag))

        abv, flag = consensus_numeric("abv_pct", model_wines, page_text)
        if flag:
            page_flags.append(("abv_pct", flag))

        blend, flag = consensus_blend(model_wines, page_text)
        if flag:
            page_flags.append(("blend", flag))

        oak, flag = consensus_oak(model_wines, page_text)
        if flag:
            page_flags.append(("oak", flag))

        cases, flag = consensus_numeric("cases_produced", model_wines, page_text)
        if flag:
            page_flags.append(("cases_produced", flag))

        chem, flag = consensus_chemistry(model_wines, page_text)
        if flag:
            page_flags.append(("chemistry", flag))

        winemaker, flag = consensus_text_field("winemaker", model_wines, page_text)
        if flag:
            page_flags.append(("winemaker", flag))

        farming, flag = consensus_farming(model_wines, page_text)
        if flag:
            page_flags.append(("farming", flag))

        new_ext = {
            "name": old_truth["extraction"].get("name", meta.get("wine", "")),
            "producer": old_truth["extraction"].get("producer", meta.get("producer", "")),
            "vintage_year": vintage,
            "abv_pct": abv,
            "blend": blend,
            "oak": oak,
            "cases_produced": cases,
            "chemistry": chem,
            "winemaker": winemaker,
            "farming": farming,
        }

        non_null = sum(1 for k, v in new_ext.items()
                       if v is not None and k not in ("name", "producer"))
        old_non_null = sum(1 for k, v in old_truth["extraction"].items()
                          if v is not None and k not in ("name", "producer"))

        if non_null != old_non_null:
            changes += 1

        new_truth = {
            "page_id": page_id,
            "url": old_truth.get("url", ""),
            "producer": meta.get("producer", ""),
            "wine_name": meta.get("wine", ""),
            "extraction": new_ext,
            "data_richness": non_null,
            "total_fields": 8,
            "text_length": len(page_text),
            "consensus_models": len(model_dirs),
        }
        new_truths.append(new_truth)

        if page_flags:
            review_items.append({
                "page_id": page_id,
                "producer": meta.get("producer", ""),
                "wine": meta.get("wine", ""),
                "flags": page_flags,
            })

    # Save updated truths
    for t in new_truths:
        pid = t["page_id"]
        out = DATA_DIR / f"truth_{pid}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(t, f, indent=2, ensure_ascii=False)

    # Summary
    richness_dist = Counter(t["data_richness"] for t in new_truths)
    print(f"Rebuilt {len(new_truths)} ground truth files ({changes} changed)")
    print(f"Richness distribution: {dict(sorted(richness_dist.items()))}")

    print(f"\nPages with 3+ fields:")
    for t in sorted(new_truths, key=lambda x: x["data_richness"], reverse=True):
        if t["data_richness"] >= 3:
            ext = t["extraction"]
            fields = [k for k, v in ext.items() if v is not None and k not in ("name", "producer")]
            print(f"  [{t['data_richness']}] {t['producer']} - {t['wine_name']}: {', '.join(fields)}")

    # Print review items
    if review_items:
        print(f"\n{'='*80}")
        print(f"ITEMS FLAGGED FOR HUMAN REVIEW ({len(review_items)} pages)")
        print(f"{'='*80}")
        for item in review_items:
            print(f"\n{item['page_id']}: {item['producer']} - {item['wine']}")
            for field, flag in item["flags"]:
                print(f"  [{field}] {flag}")

    # Save review log
    with open(DATA_DIR / "ground_truth_review_log.json", "w") as f:
        json.dump(review_items, f, indent=2)
    print(f"\nReview log saved to ground_truth_review_log.json")


if __name__ == "__main__":
    main()
