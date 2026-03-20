"""
Create canonical producer records from dedup results.
1. Reads merge pairs + exact-match groups
2. Builds Union-Find merge groups
3. Picks canonical name (most wines) per group
4. Inserts producers + aliases

Usage:
    python -m pipeline.vivino.create_producers --dry-run
    python -m pipeline.vivino.create_producers
"""

import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify


class UnionFind:
    def __init__(self):
        self.parent: dict[str, str] = {}
        self.rank: dict[str, int] = {}

    def make(self, x: str):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0

    def find(self, x: str) -> str:
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a: str, b: str):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            self.parent[ra] = rb
        elif self.rank[ra] > self.rank[rb]:
            self.parent[rb] = ra
        else:
            self.parent[rb] = ra
            self.rank[ra] += 1


def fetch_all(table, columns="*", filters=None, batch_size=1000):
    sb = get_supabase()
    rows, offset = [], 0
    while True:
        query = sb.table(table).select(columns).range(offset, offset + batch_size - 1)
        if filters:
            for k, v in filters.items():
                query = query.eq(k, v)
        result = query.execute()
        if not result.data:
            break
        rows.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += len(result.data)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Create producers from dedup results")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    sb = get_supabase()

    # 1. Fetch all data
    print("Fetching data...")
    staging = fetch_all("producer_dedup_staging", "id, producer_name, country, norm, wine_count")
    merge_pairs = fetch_all("producer_dedup_pairs", "name_a, name_b, country", filters={"verdict": "merge"})
    countries = fetch_all("countries", "id, name")

    print(f"  {len(staging)} staging producers, {len(merge_pairs)} merge pairs, {len(countries)} countries")

    country_map = {c["name"]: c["id"] for c in countries}

    staging_map = {}
    for s in staging:
        staging_map[f"{s['producer_name']}|{s['country']}"] = s

    # 2. Find exact-match groups
    false_exact_matches = {
        "Chateau Belle-Vue|Chateau Bellevue|France",
        "Chateau Bellevue|Chateau Belle-Vue|France",
    }

    norm_groups: dict[str, list] = {}
    for s in staging:
        key = f"{s['norm']}|{s['country']}"
        norm_groups.setdefault(key, []).append(s)

    exact_match_edges = []
    for group in norm_groups.values():
        if len(group) < 2:
            continue
        for i in range(1, len(group)):
            pair_key1 = f"{group[0]['producer_name']}|{group[i]['producer_name']}|{group[0]['country']}"
            pair_key2 = f"{group[i]['producer_name']}|{group[0]['producer_name']}|{group[0]['country']}"
            if pair_key1 in false_exact_matches or pair_key2 in false_exact_matches:
                print(f'  SKIP false exact match: "{group[0]["producer_name"]}" <-> "{group[i]["producer_name"]}" ({group[0]["country"]})')
                continue
            exact_match_edges.append({
                "name_a": group[0]["producer_name"],
                "name_b": group[i]["producer_name"],
                "country": group[0]["country"],
            })
    print(f"  {len(exact_match_edges)} exact-match edges from normalization")

    # 3. Build Union-Find
    uf = UnionFind()
    for s in staging:
        uf.make(f"{s['producer_name']}|{s['country']}")

    for p in merge_pairs:
        ka = f"{p['name_a']}|{p['country']}"
        kb = f"{p['name_b']}|{p['country']}"
        uf.make(ka)
        uf.make(kb)
        uf.union(ka, kb)

    for e in exact_match_edges:
        ka = f"{e['name_a']}|{e['country']}"
        kb = f"{e['name_b']}|{e['country']}"
        uf.union(ka, kb)

    # 4. Collect groups
    groups: dict[str, list] = {}
    for s in staging:
        key = f"{s['producer_name']}|{s['country']}"
        root = uf.find(key)
        groups.setdefault(root, []).append({
            "name": s["producer_name"], "country": s["country"],
            "wine_count": s.get("wine_count", 0), "norm": s.get("norm"),
        })

    # 5. Review transitive chains
    print(f"\n-- Transitive chain review (groups of 3+) --")
    large_groups = sorted(
        [(root, members) for root, members in groups.items() if len(members) >= 3],
        key=lambda x: -len(x[1]),
    )
    for root, members in large_groups:
        names = ", ".join(f'"{m["name"]}" ({m["wine_count"]}w)' for m in members)
        print(f"  [{members[0]['country']}] {len(members)} names: {names}")
    print(f"  {len(large_groups)} groups with 3+ names\n")

    # 6. Pick canonical name per group
    producer_list = []
    merged_away = 0

    for members in groups.values():
        members.sort(key=lambda m: -m["wine_count"])
        canonical = members[0]
        country_id = country_map.get(canonical["country"])

        if not country_id:
            print(f'  WARNING: No country_id for "{canonical["country"]}"')

        producer_list.append({
            "name": canonical["name"],
            "country": canonical["country"],
            "countryId": country_id,
            "slug": slugify(canonical["name"]),
            "nameNormalized": normalize(canonical["name"]),
            "totalWines": sum(m["wine_count"] for m in members),
            "aliases": [{"name": m["name"], "wineCount": m["wine_count"]} for m in members] if len(members) > 1 else [],
        })
        if len(members) > 1:
            merged_away += len(members) - 1

    # Check slug collisions
    slug_counts: dict[str, int] = {}
    for p in producer_list:
        slug_counts[p["slug"]] = slug_counts.get(p["slug"], 0) + 1

    collisions = [(s, c) for s, c in slug_counts.items() if c > 1]
    if collisions:
        print(f"-- Slug collisions ({len(collisions)}) --")
        for slug, count in collisions:
            dupes = [p for p in producer_list if p["slug"] == slug]
            for i in range(1, len(dupes)):
                dupes[i]["slug"] = f"{dupes[i]['slug']}-{slugify(dupes[i]['country'])}"
            print(f'  "{slug}" x {count} -> disambiguated with country suffix')

        slug_set = set()
        for p in producer_list:
            if p["slug"] in slug_set:
                n = 2
                while f"{p['slug']}-{n}" in slug_set:
                    n += 1
                p["slug"] = f"{p['slug']}-{n}"
            slug_set.add(p["slug"])

    print(f"-- Summary --")
    print(f"  Total staging names: {len(staging)}")
    print(f"  Merged away: {merged_away}")
    print(f"  Canonical producers to create: {len(producer_list)}")
    print(f"  Producers with aliases: {sum(1 for p in producer_list if p['aliases'])}")
    print(f"  Total aliases: {sum(len(p['aliases']) for p in producer_list)}")

    if args.dry_run:
        print("\nDRY RUN -- no database changes made.")
        print("\nSample producers:")
        for p in producer_list[:10]:
            print(f"  {p['name']} ({p['country']}) -- {p['totalWines']} wines, slug: {p['slug']}")
            if p["aliases"]:
                print(f"    aliases: {', '.join(a['name'] for a in p['aliases'])}")
        return

    # 7. Insert producers in batches
    print("\nInserting producers...")
    BATCH = 500
    inserted = 0
    producer_id_map = {}

    for i in range(0, len(producer_list), BATCH):
        batch = producer_list[i:i + BATCH]
        rows = [{"slug": p["slug"], "name": p["name"], "name_normalized": p["nameNormalized"], "country_id": p["countryId"]} for p in batch]

        try:
            result = sb.table("producers").insert(rows).select("id, name").execute()
            if result.data:
                for row in result.data:
                    p_entry = next((p for p in batch if p["name"] == row["name"]), None)
                    if p_entry:
                        producer_id_map[f"{row['name']}|{p_entry['country']}"] = row["id"]
                inserted += len(result.data)
        except Exception as e:
            print(f"  ERROR at batch {i // BATCH + 1}: {e}")
            for row in rows:
                try:
                    result = sb.table("producers").insert(row).select("id, name").execute()
                    if result.data and result.data[0]:
                        p_entry = next((p for p in batch if p["name"] == result.data[0]["name"]), None)
                        if p_entry:
                            producer_id_map[f"{result.data[0]['name']}|{p_entry['country']}"] = result.data[0]["id"]
                    inserted += 1
                except Exception as e2:
                    print(f'    SKIP "{row["name"]}": {e2}')

        print(f"\r  {inserted}/{len(producer_list)}", end="", flush=True)

    print(f"\n  Inserted {inserted} producers")

    # 8. Insert aliases
    alias_rows = []
    for p in producer_list:
        if not p["aliases"]:
            continue
        producer_id = producer_id_map.get(f"{p['name']}|{p['country']}")
        if not producer_id:
            continue
        for alias in p["aliases"]:
            if alias["name"] == p["name"]:
                continue
            alias_rows.append({
                "producer_id": producer_id,
                "name": alias["name"],
                "name_normalized": normalize(alias["name"]),
                "source": "xwines_dedup",
            })

    if alias_rows:
        print(f"\nInserting {len(alias_rows)} aliases...")
        alias_inserted = 0
        for i in range(0, len(alias_rows), BATCH):
            batch = alias_rows[i:i + BATCH]
            try:
                sb.table("producer_aliases").insert(batch).execute()
                alias_inserted += len(batch)
            except Exception as e:
                print(f"  ERROR: {e}")
                for row in batch:
                    try:
                        sb.table("producer_aliases").insert(row).execute()
                        alias_inserted += 1
                    except Exception as e2:
                        print(f'    SKIP alias "{row["name"]}": {e2}')
            print(f"\r  {alias_inserted}/{len(alias_rows)}", end="", flush=True)
        print(f"\n  Inserted {alias_inserted} aliases")

    print("\nDone!")


if __name__ == "__main__":
    main()
