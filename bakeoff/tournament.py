#!/usr/bin/env python3
"""B5.6 Tournament helper — compute cumulative composites across wines for ranking.

Not the judge itself (that's run_task3_judge.py). This reads existing judge JSON
files and produces cumulative rankings per round. DeepSeek v3.2 is protected.

Usage:
    python -m bakeoff.tournament --wines <id1,id2,...>             # rank all 21 models
    python -m bakeoff.tournament --wines <ids> --survivors <slugs> # rank subset
    python -m bakeoff.tournament --round 1                         # preset: Shafer + Terroir + Plantagenet
"""

import argparse
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
JUDGE_DIR = BASE_DIR / "scores" / "task3_judge"
CONTEXTS_FILE = BASE_DIR / "data" / "task3" / "contexts.json"

# Wine IDs per round
SHAFER = "1231e371-af5f-4bde-9d2d-be094605cc21"       # Tier A calibration
TERROIR_AL_LIMIT = "e998b872-3ce8-419e-ba0c-9904a4638629"  # Tier B R1
PLANTAGENET = "9934442d-7786-489b-aca3-e329b1c12da5"  # Tier C R1
TYRRELL = "14823b73-f705-4b4d-8a11-e0079c33ab99"      # Tier A R2 (oak fabrication test)
DES_BOSQUETS = "29c755be-e57c-4d38-8f28-3ec6d54d5268"  # Tier B R2 (grape fabrication test)
HOWARD_PARK = "ebde8e00-350b-40fc-a762-0987259419ac"  # Tier C R2 (naming test)

R1_WINES = [SHAFER, TERROIR_AL_LIMIT, PLANTAGENET]
R2_WINES = R1_WINES + [TYRRELL, DES_BOSQUETS, HOWARD_PARK]

ALL_MODELS = [
    "anthropic/claude-sonnet-4.6",
    "anthropic/claude-haiku-4.5",
    "anthropic/claude-opus-4.6",
    "deepseek/deepseek-v3.2",
    "google/gemini-2.5-flash",
    "google/gemini-2.5-pro",
    "meta-llama/llama-4-maverick",
    "mistralai/mistral-large",
    "mistralai/mistral-large-2512",
    "mistralai/mistral-nemo",
    "openai/gpt-5.4",
    "openai/gpt-5.4-mini",
    "openai/gpt-5-mini",
    "qwen/qwen3.5-plus-02-15",
    "qwen/qwen3.6-plus",
    "google/gemini-3.1-pro-preview",
    "google/gemini-3-flash-preview",
    "xiaomi/mimo-v2-pro",
    "xiaomi/mimo-v2-flash",
    "minimax/minimax-m2.7",
    "x-ai/grok-4.1-fast",
]

DEEPSEEK = "deepseek/deepseek-v3.2"
TIE_TOLERANCE = 0.1  # composite within 0.1 of cut line → bring extra forward


def model_slug(m: str) -> str:
    return m.replace("/", "__")


def load_scores(wine_ids, models):
    """Load judge scores per (model, wine). Returns dict[model] -> list of score dicts."""
    by_model = {}
    for m in models:
        slug = model_slug(m)
        lst = []
        for wid in wine_ids:
            f = JUDGE_DIR / f"{slug}__{wid}.json"
            if f.exists():
                lst.append(json.load(open(f, encoding="utf-8")))
        by_model[m] = lst
    return by_model


def compute_cumulative(by_model, wine_ids):
    """Compute average composite across given wines per model.

    Only includes models with scores on ALL given wines (otherwise comparisons
    are unfair). Models missing scores are marked 'incomplete'.
    """
    rows = []
    for m, scores in by_model.items():
        if len(scores) < len(wine_ids):
            rows.append({
                "model": m,
                "composite": None,
                "incomplete": True,
                "n_wines": len(scores),
                "missing": len(wine_ids) - len(scores),
            })
            continue
        avg_comp = sum(s["composite"] for s in scores) / len(scores)
        avg_corr = sum(s["correctness"] for s in scores) / len(scores)
        avg_voice = sum(s["voice"] for s in scores) / len(scores)
        avg_spec = sum(s["specificity"] for s in scores) / len(scores)
        avg_use = sum(s["usefulness"] for s in scores) / len(scores)
        per_wine = [(s["wine_id"][:8], s["tier"], round(s["composite"], 3)) for s in scores]
        rows.append({
            "model": m,
            "composite": round(avg_comp, 3),
            "correctness": round(avg_corr, 2),
            "voice": round(avg_voice, 2),
            "specificity": round(avg_spec, 2),
            "usefulness": round(avg_use, 2),
            "n_wines": len(scores),
            "per_wine": per_wine,
            "incomplete": False,
        })
    # Sort: complete first by composite desc, incomplete last
    rows.sort(key=lambda r: (r["incomplete"], -(r["composite"] or 0)))
    return rows


def cut_with_ties(ranked, target_n, protect=None):
    """Advance top target_n; plus any within TIE_TOLERANCE of the cut line.

    protect: model id to always advance (DeepSeek protection).
    Returns (survivors, eliminated, notes).
    """
    protect = protect or DEEPSEEK
    complete = [r for r in ranked if not r["incomplete"]]
    incomplete = [r for r in ranked if r["incomplete"]]

    if len(complete) <= target_n:
        survivors = [r["model"] for r in complete]
        # Also include protected if missing
        if protect not in survivors and any(r["model"] == protect for r in ranked):
            survivors.append(protect)
        eliminated = [r["model"] for r in complete if r["model"] not in survivors]
        return survivors, eliminated, [f"All {len(complete)} complete models advance"]

    # Normal cut
    cut_line = complete[target_n - 1]["composite"]
    base_survivors = [r["model"] for r in complete[:target_n]]

    # Ties: anyone within TIE_TOLERANCE below the cut line
    extras = []
    for r in complete[target_n:]:
        if cut_line - r["composite"] < TIE_TOLERANCE:
            extras.append(r["model"])
        else:
            break

    survivors = list(base_survivors)
    # Add extras
    for e in extras:
        if e not in survivors:
            survivors.append(e)
    # Add DeepSeek if not already in
    deepseek_in_top = protect in base_survivors
    deepseek_added = False
    if not deepseek_in_top and any(r["model"] == protect for r in complete):
        survivors.append(protect)
        deepseek_added = True

    eliminated = [r["model"] for r in complete if r["model"] not in survivors]
    notes = []
    notes.append(f"Cut line: {cut_line:.3f} (rank {target_n})")
    if extras:
        notes.append(f"Ties within {TIE_TOLERANCE}: +{extras}")
    if deepseek_added:
        notes.append(f"DeepSeek protected (composite {next(r['composite'] for r in complete if r['model']==protect):.3f})")
    if incomplete:
        notes.append(f"Incomplete: {[r['model'] for r in incomplete]} (need full scores to advance)")
    return survivors, eliminated, notes


def print_table(rows, title):
    print(f"\n{'=' * 90}")
    print(title)
    print("=" * 90)
    print(f"{'#':>2}  {'Model':<35} {'Comp':>6} {'Corr':>5} {'Voice':>5} {'Spec':>5} {'Use':>5} {'N':>3}")
    print("-" * 90)
    for i, r in enumerate(rows):
        short = r["model"].split("/")[-1][:33]
        if r["incomplete"]:
            print(f"{i+1:>2}  {short:<35} {'INCOMPLETE':>6} (n={r['n_wines']}, missing={r['missing']})")
        else:
            print(f"{i+1:>2}  {short:<35} {r['composite']:>6.3f} {r['correctness']:>5.2f} "
                  f"{r['voice']:>5.2f} {r['specificity']:>5.2f} {r['usefulness']:>5.2f} {r['n_wines']:>3}")
    print("=" * 90)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--round", type=int, choices=[1, 2, 3], help="Preset round (1=R1 3 wines, 2=R2 6 wines, 3=R3 all 30)")
    parser.add_argument("--wines", help="Comma-separated wine IDs")
    parser.add_argument("--survivors", help="Comma-separated OpenRouter model slugs to restrict to")
    parser.add_argument("--cut", type=int, help="Target survivor count (for tie-aware cut)")
    args = parser.parse_args()

    # Determine wines
    if args.round == 1:
        wine_ids = R1_WINES
        default_cut = 10
        round_name = "Round 1 (3 wines: Shafer + Terroir Al Limit + Plantagenet)"
    elif args.round == 2:
        wine_ids = R2_WINES
        default_cut = 5
        round_name = "Round 2 (6 wines: R1 + Tyrrell's + des Bosquets + Howard Park)"
    elif args.round == 3:
        with open(CONTEXTS_FILE, encoding="utf-8") as f:
            wine_ids = [w["wine_id"] for w in json.load(f)]
        default_cut = None  # no cut — full leaderboard
        round_name = f"Round 3 / Final ({len(wine_ids)} wines)"
    elif args.wines:
        wine_ids = args.wines.split(",")
        default_cut = args.cut
        round_name = f"Custom ({len(wine_ids)} wines)"
    else:
        parser.error("Must specify --round or --wines")

    # Determine models
    if args.survivors:
        models = args.survivors.split(",")
    else:
        models = ALL_MODELS

    print(f"{round_name} — {len(models)} models")
    print(f"Wines: {[w[:8] for w in wine_ids]}")

    # Load + compute
    by_model = load_scores(wine_ids, models)
    rows = compute_cumulative(by_model, wine_ids)
    print_table(rows, f"Cumulative Leaderboard — {round_name}")

    cut_n = args.cut if args.cut else default_cut
    if cut_n:
        survivors, eliminated, notes = cut_with_ties(rows, cut_n)
        print(f"\n{'=' * 90}")
        print(f"CUT (target top {cut_n} + DeepSeek protected)")
        print("=" * 90)
        for n in notes:
            print(f"  {n}")
        print(f"\nSURVIVORS ({len(survivors)}):")
        for s in survivors:
            print(f"  ✓ {s}")
        print(f"\nELIMINATED ({len(eliminated)}):")
        for e in eliminated:
            print(f"  ✗ {e}")


if __name__ == "__main__":
    main()
