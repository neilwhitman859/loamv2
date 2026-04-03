"""Fix missed COLA IDs: add to record list, remove from checkpoint."""
import json
import os

RECORD_LIST = os.path.expanduser("~/Desktop/Loam Cowork/data/imports/ttb_cola_labels/record_list.json")
CHECKPOINT = os.path.expanduser("~/Desktop/Loam Cowork/data/imports/ttb_cola_labels/checkpoint_v3.txt")
MISSED_FILE = os.path.expanduser("~/Desktop/Loam Cowork/data/imports/ttb_cola_labels/missed_long_ids.json")

def main():
    # Read missed IDs from the JSON file created by the SQL export
    if not os.path.exists(MISSED_FILE):
        print(f"ERROR: {MISSED_FILE} not found. Run the SQL export first.")
        return

    with open(MISSED_FILE) as f:
        missed_ids = json.load(f)
    print(f"Missed long IDs: {len(missed_ids)}")

    # Add to record list
    with open(RECORD_LIST) as f:
        records = json.load(f)
    record_ids = set(r["ttb_id"] for r in records)

    added = 0
    for ttb_id in missed_ids:
        if ttb_id not in record_ids:
            records.append({"ttb_id": ttb_id, "brand": "", "era": "unknown"})
            added += 1

    with open(RECORD_LIST, "w") as f:
        json.dump(records, f)
    print(f"Added {added} to record list (now {len(records):,})")

    # Remove from checkpoint
    with open(CHECKPOINT) as f:
        checkpointed = set(line.strip() for line in f if line.strip())
    before = len(checkpointed)
    checkpointed -= set(missed_ids)
    with open(CHECKPOINT, "w") as f:
        for cid in sorted(checkpointed):
            f.write(cid + "\n")
    print(f"Checkpoint: {before:,} -> {len(checkpointed):,} (removed {before - len(checkpointed)})")

if __name__ == "__main__":
    main()
