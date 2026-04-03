"""Query DB for long-format missed wine COLAs and save to file."""
import json
import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE"])

all_ids = []
for ct in ["80", "81", "84", "88", "80A", "8000", "8100", "8400", "8800"]:
    last_id = ""
    ct_count = 0
    while True:
        try:
            resp = (
                sb.table("source_ttb_colas")
                .select("ttb_id")
                .is_("detail_scraped_at", "null")
                .eq("class_type", ct)
                .gt("ttb_id", last_id)
                .order("ttb_id")
                .limit(500)
                .execute()
            )
            if not resp.data:
                break
            # Only long IDs (001 format)
            long_ids = [r["ttb_id"] for r in resp.data if len(r["ttb_id"]) > 10]
            all_ids.extend(long_ids)
            ct_count += len(long_ids)
            last_id = resp.data[-1]["ttb_id"]
        except Exception as e:
            print(f"  Error on {ct}: {e}")
            break
    if ct_count:
        print(f"  Class {ct}: {ct_count} long IDs")

all_ids = list(set(all_ids))
print(f"Total unique long IDs: {len(all_ids)}")

outpath = os.path.expanduser(
    "~/Desktop/Loam Cowork/data/imports/ttb_cola_labels/missed_long_ids.json"
)
with open(outpath, "w") as f:
    json.dump(all_ids, f)
print(f"Saved to {outpath}")
