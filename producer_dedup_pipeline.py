#!/usr/bin/env python3
"""
Producer dedup pipeline for Loam v2.
Sends fuzzy-matched producer name pairs to Claude Haiku for merge/keep_separate verdicts.
Reads from and writes to the producer_dedup_pairs table in Supabase.
"""

import os
import json
import time
import anthropic
from supabase import create_client

# Config
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://vgbppjhmvbggfjztzobl.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZnYnBwamhtdmJnZ2ZqenR6b2JsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI1ODU1NDYsImV4cCI6MjA4ODE2MTU0Nn0.KHZiqk6B7XYDnkFcDNJtMIKoT-hf7s8MGkmpOsjgVDk")
BATCH_SIZE = 50
MODEL = "claude-haiku-4-5-20251001"

SYSTEM_PROMPT = """You are a wine industry data expert helping deduplicate wine producer names.

For each numbered pair of producer names (from the same country), decide if they are the SAME producer or DIFFERENT producers.

Key rules:
- "Château X" and "Chateau X" = SAME (just accent difference)
- "Château X" and "Domaine X" = DIFFERENT (different business types in France)
- "Tenuta X" and "Fattoria X" = DIFFERENT (different estate types in Italy)
- "Cantina X" and "Cantine X" = SAME (singular vs plural)
- "X Winery" and "X Vineyards" = probably SAME (just suffix)
- "X" and "X Wines" = probably SAME
- "Domaine de X" and "Domaine X" = probably SAME
- "Cascina Ca' Rossa" and "Cascina Rossa" = DIFFERENT (Ca' Rossa is a specific name)
- "Castello di Gabiano" and "Castello di Gabbiano" = DIFFERENT (different places)
- Short names that are common words (e.g., "Aurora", "Carmen") matching longer names = usually DIFFERENT

Respond with ONLY a JSON array. Each element: {"pair": <number>, "verdict": "merge" or "separate", "reason": "<brief reason>"}
No other text, no markdown fences, just the JSON array."""

def get_pending_pairs(supabase, limit=None):
    """Fetch all pending pairs from the database."""
    query = supabase.table("producer_dedup_pairs").select("id, name_a, name_b, country, similarity").eq("verdict", "pending").order("similarity", desc=True)
    if limit:
        query = query.limit(limit)
    result = query.execute()
    return result.data

def build_batch_prompt(pairs):
    """Build the user prompt for a batch of pairs."""
    lines = []
    for i, pair in enumerate(pairs, 1):
        lines.append(f"{i}. \"{pair['name_a']}\" vs \"{pair['name_b']}\" (country: {pair['country']}, similarity: {pair['similarity']})")
    return "\n".join(lines)

def call_haiku(client, prompt):
    """Send a batch to Haiku and parse the response."""
    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}]
    )
    
    text = response.content[0].text.strip()
    # Clean potential markdown fences
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    
    try:
        verdicts = json.loads(text)
        return verdicts, response.usage
    except json.JSONDecodeError as e:
        print(f"  WARNING: Failed to parse Haiku response: {e}")
        print(f"  Response was: {text[:200]}...")
        return None, response.usage

def update_verdicts(supabase, pairs, verdicts):
    """Write verdicts back to the database."""
    if not verdicts:
        return 0
    
    updated = 0
    for v in verdicts:
        pair_idx = v["pair"] - 1  # 1-indexed in response
        if pair_idx < 0 or pair_idx >= len(pairs):
            continue
        
        pair = pairs[pair_idx]
        verdict = "merge" if v["verdict"] == "merge" else "keep_separate"
        
        supabase.table("producer_dedup_pairs").update({
            "verdict": verdict,
            "verdict_source": "haiku"
        }).eq("id", pair["id"]).execute()
        
        updated += 1
    
    return updated

def run_pipeline(dry_run=False, limit=None):
    """Run the full dedup pipeline."""
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    client = anthropic.Anthropic()  # Uses ANTHROPIC_API_KEY env var
    
    # Fetch pending pairs
    pairs = get_pending_pairs(supabase, limit=limit)
    total = len(pairs)
    print(f"Fetched {total} pending pairs")
    
    if total == 0:
        print("Nothing to process!")
        return
    
    # Process in batches
    total_input_tokens = 0
    total_output_tokens = 0
    total_merges = 0
    total_separates = 0
    total_updated = 0
    batch_num = 0
    
    for i in range(0, total, BATCH_SIZE):
        batch = pairs[i:i+BATCH_SIZE]
        batch_num += 1
        batch_count = len(batch)
        
        prompt = build_batch_prompt(batch)
        
        if dry_run:
            print(f"  Batch {batch_num}: {batch_count} pairs (dry run, skipping API call)")
            continue
        
        print(f"  Batch {batch_num}/{(total + BATCH_SIZE - 1) // BATCH_SIZE}: {batch_count} pairs...", end="", flush=True)
        
        try:
            verdicts, usage = call_haiku(client, prompt)
            total_input_tokens += usage.input_tokens
            total_output_tokens += usage.output_tokens
            
            if verdicts:
                merges = sum(1 for v in verdicts if v.get("verdict") == "merge")
                separates = sum(1 for v in verdicts if v.get("verdict") != "merge")
                total_merges += merges
                total_separates += separates
                
                updated = update_verdicts(supabase, batch, verdicts)
                total_updated += updated
                
                print(f" ✓ merge:{merges} separate:{separates} (updated:{updated})")
            else:
                print(f" ✗ parse error")
            
            # Rate limiting - be gentle
            time.sleep(0.5)
            
        except Exception as e:
            print(f" ✗ error: {e}")
            time.sleep(2)  # Back off on errors
    
    # Summary
    print(f"\n{'='*50}")
    print(f"PIPELINE COMPLETE")
    print(f"{'='*50}")
    print(f"Total pairs processed: {total_updated}/{total}")
    print(f"Merges: {total_merges}")
    print(f"Separates: {total_separates}")
    print(f"Input tokens: {total_input_tokens:,}")
    print(f"Output tokens: {total_output_tokens:,}")
    
    input_cost = (total_input_tokens / 1_000_000) * 0.25
    output_cost = (total_output_tokens / 1_000_000) * 1.25
    print(f"Cost: ${input_cost + output_cost:.2f}")

if __name__ == "__main__":
    import sys
    
    dry_run = "--dry-run" in sys.argv
    limit = None
    for arg in sys.argv[1:]:
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
    
    if dry_run:
        print("DRY RUN MODE - no API calls will be made")
    
    run_pipeline(dry_run=dry_run, limit=limit)
