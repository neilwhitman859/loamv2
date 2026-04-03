"""Fix detail scrape DB write failures by removing failed IDs from checkpoint."""
import os, json

CHECKPOINT = os.path.expanduser("~/Desktop/Loam Cowork/data/imports/ttb_cola_labels/checkpoint_v3.txt")
RECORD_LIST = os.path.expanduser("~/Desktop/Loam Cowork/data/imports/ttb_cola_labels/record_list.json")

# Load checkpoint
with open(CHECKPOINT) as f:
    checkpoint_ids = set(line.strip() for line in f if line.strip())
print(f"Checkpoint has {len(checkpoint_ids):,} IDs")

# Load record list to get wine IDs
with open(RECORD_LIST) as f:
    records = json.load(f)
print(f"Record list has {len(records):,} records")

# Query Supabase for wine IDs that are missing detail_scraped_at
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '../../.env'))
from supabase import create_client
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE"])

# Get wine records missing detail_scraped_at using keyset pagination
missing_ids = []
last_id = ""
while True:
    resp = sb.table("source_ttb_colas") \
        .select("ttb_id") \
        .is_("detail_scraped_at", "null") \
        .in_("class_type", ["80","81","80A","84","88","8000","8100","8400","8800"]) \
        .gt("ttb_id", last_id) \
        .order("ttb_id") \
        .limit(1000) \
        .execute()
    if not resp.data:
        break
    for r in resp.data:
        missing_ids.append(r["ttb_id"])
    last_id = resp.data[-1]["ttb_id"]
    print(f"  Found {len(missing_ids)} missing so far...")

print(f"\nTotal wine records missing detail_scraped_at: {len(missing_ids)}")

# Remove these from checkpoint so scraper will re-process them
removed = 0
for mid in missing_ids:
    if mid in checkpoint_ids:
        checkpoint_ids.remove(mid)
        removed += 1

print(f"Removed {removed} IDs from checkpoint")
print(f"Checkpoint now has {len(checkpoint_ids):,} IDs")

# Write updated checkpoint
with open(CHECKPOINT, "w") as f:
    for cid in sorted(checkpoint_ids):
        f.write(cid + "\n")

print(f"Checkpoint saved. Run the detail scraper to re-process these {len(missing_ids)} records.")
