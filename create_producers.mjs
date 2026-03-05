#!/usr/bin/env node
/**
 * Create canonical producer records from dedup results.
 * 1. Reads merge pairs + exact-match groups
 * 2. Builds Union-Find merge groups
 * 3. Picks canonical name (most wines) per group
 * 4. Inserts producers + aliases
 */

import { createClient } from "@supabase/supabase-js";
import { readFileSync } from "fs";

// Load .env
const envPath = new URL(".env", import.meta.url).pathname.replace(/^\/([A-Z]:)/, "$1");
const envContent = readFileSync(envPath, "utf-8");
for (const line of envContent.split("\n")) {
  const trimmed = line.trim();
  if (!trimmed || trimmed.startsWith("#")) continue;
  const eqIdx = trimmed.indexOf("=");
  if (eqIdx === -1) continue;
  const key = trimmed.slice(0, eqIdx);
  const val = trimmed.slice(eqIdx + 1);
  if (!process.env[key]) process.env[key] = val;
}

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE || process.env.SUPABASE_ANON_KEY;

// ── Union-Find ──────────────────────────────────────────────
class UnionFind {
  constructor() {
    this.parent = new Map();
    this.rank = new Map();
  }
  make(x) {
    if (!this.parent.has(x)) {
      this.parent.set(x, x);
      this.rank.set(x, 0);
    }
  }
  find(x) {
    if (this.parent.get(x) !== x) {
      this.parent.set(x, this.find(this.parent.get(x))); // path compression
    }
    return this.parent.get(x);
  }
  union(a, b) {
    const ra = this.find(a);
    const rb = this.find(b);
    if (ra === rb) return;
    // union by rank
    if (this.rank.get(ra) < this.rank.get(rb)) {
      this.parent.set(ra, rb);
    } else if (this.rank.get(ra) > this.rank.get(rb)) {
      this.parent.set(rb, ra);
    } else {
      this.parent.set(rb, ra);
      this.rank.set(ra, this.rank.get(ra) + 1);
    }
  }
}

// ── Helpers ──────────────────────────────────────────────────
function slugify(name) {
  return name
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "") // strip accents
    .toLowerCase()
    .replace(/['']/g, "")           // remove apostrophes
    .replace(/[^a-z0-9]+/g, "-")    // non-alphanumeric → dash
    .replace(/^-+|-+$/g, "");       // trim dashes
}

function normalize(name) {
  return name
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .trim();
}

async function fetchAll(supabase, table, select, filters = {}) {
  const allData = [];
  const pageSize = 1000;
  let offset = 0;
  while (true) {
    let query = supabase.from(table).select(select).range(offset, offset + pageSize - 1);
    for (const [col, val] of Object.entries(filters)) {
      query = query.eq(col, val);
    }
    const { data, error } = await query;
    if (error) throw new Error(`Fetch ${table}: ${error.message}`);
    if (!data || data.length === 0) break;
    allData.push(...data);
    offset += data.length;
    if (data.length < pageSize) break;
  }
  return allData;
}

// ── Main ────────────────────────────────────────────────────
async function main() {
  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  const dryRun = process.argv.includes("--dry-run");

  // 1. Fetch all data
  console.log("Fetching data...");
  const [staging, mergePairs, countries] = await Promise.all([
    fetchAll(supabase, "producer_dedup_staging", "id, producer_name, country, norm, wine_count"),
    fetchAll(supabase, "producer_dedup_pairs", "name_a, name_b, country", { verdict: "merge" }),
    fetchAll(supabase, "countries", "id, name"),
  ]);
  console.log(`  ${staging.length} staging producers, ${mergePairs.length} merge pairs, ${countries.length} countries`);

  // Build country lookup
  const countryMap = new Map();
  for (const c of countries) countryMap.set(c.name, c.id);

  // Build staging lookup: key = "name|country"
  const stagingMap = new Map();
  for (const s of staging) {
    stagingMap.set(`${s.producer_name}|${s.country}`, s);
  }

  // 2. Find exact-match groups (same norm + country, different names)
  // Exclusions: name pairs that share a norm due to normalization artifacts (e.g., hyphen removal)
  // but are actually different producers.
  const falseExactMatches = new Set([
    "Château Belle-Vue|Château Bellevue|France",
    "Château Bellevue|Château Belle-Vue|France",
  ]);

  const normGroups = new Map(); // key = "norm|country" → [staging rows]
  for (const s of staging) {
    const key = `${s.norm}|${s.country}`;
    if (!normGroups.has(key)) normGroups.set(key, []);
    normGroups.get(key).push(s);
  }

  const exactMatchEdges = [];
  for (const [, group] of normGroups) {
    if (group.length < 2) continue;
    // Connect all pairs in the group, skipping false exact matches
    for (let i = 1; i < group.length; i++) {
      const pairKey1 = `${group[0].producer_name}|${group[i].producer_name}|${group[0].country}`;
      const pairKey2 = `${group[i].producer_name}|${group[0].producer_name}|${group[0].country}`;
      if (falseExactMatches.has(pairKey1) || falseExactMatches.has(pairKey2)) {
        console.log(`  SKIP false exact match: "${group[0].producer_name}" ↔ "${group[i].producer_name}" (${group[0].country})`);
        continue;
      }
      exactMatchEdges.push({
        name_a: group[0].producer_name,
        name_b: group[i].producer_name,
        country: group[0].country
      });
    }
  }
  console.log(`  ${exactMatchEdges.length} exact-match edges from normalization`);

  // 3. Build Union-Find with country-scoped keys
  const uf = new UnionFind();

  // Add all staging producers as nodes
  for (const s of staging) {
    uf.make(`${s.producer_name}|${s.country}`);
  }

  // Add fuzzy merge edges
  for (const p of mergePairs) {
    const ka = `${p.name_a}|${p.country}`;
    const kb = `${p.name_b}|${p.country}`;
    uf.make(ka);
    uf.make(kb);
    uf.union(ka, kb);
  }

  // Add exact-match edges
  for (const e of exactMatchEdges) {
    const ka = `${e.name_a}|${e.country}`;
    const kb = `${e.name_b}|${e.country}`;
    uf.union(ka, kb);
  }

  // 4. Collect groups
  const groups = new Map(); // root → [{name, country, wine_count}]
  for (const s of staging) {
    const key = `${s.producer_name}|${s.country}`;
    const root = uf.find(key);
    if (!groups.has(root)) groups.set(root, []);
    groups.get(root).push({
      name: s.producer_name,
      country: s.country,
      wine_count: s.wine_count,
      norm: s.norm
    });
  }

  // 5. Review transitive chains (groups of 3+)
  console.log(`\n── Transitive chain review (groups of 3+) ──`);
  const largeGroups = [...groups.entries()]
    .filter(([, members]) => members.length >= 3)
    .sort((a, b) => b[1].length - a[1].length);

  let chainIssues = 0;
  const groupsToSplit = []; // will collect problematic groups

  for (const [root, members] of largeGroups) {
    const names = members.map(m => `"${m.name}" (${m.wine_count}w)`).join(", ");
    const country = members[0].country;
    console.log(`  [${country}] ${members.length} names: ${names}`);

    // Check for potential false transitive merges:
    // If group has names from fuzzy pairs that were NOT directly paired, flag it
    // For now, just log them for review
    chainIssues++;
  }
  console.log(`  ${chainIssues} groups with 3+ names (review above)\n`);

  // 6. Pick canonical name per group (most wines)
  const producers = []; // {canonicalName, country, countryId, aliases: [{name, wineCount}], totalWines}
  let mergedAway = 0;

  for (const [, members] of groups) {
    // Sort by wine_count desc
    members.sort((a, b) => b.wine_count - a.wine_count);
    const canonical = members[0];
    const countryId = countryMap.get(canonical.country) || null;

    if (!countryId) {
      console.log(`  WARNING: No country_id for "${canonical.country}"`);
    }

    producers.push({
      name: canonical.name,
      country: canonical.country,
      countryId,
      slug: slugify(canonical.name),
      nameNormalized: normalize(canonical.name),
      totalWines: members.reduce((sum, m) => sum + m.wine_count, 0),
      aliases: members.length > 1 ? members.map(m => ({ name: m.name, wineCount: m.wine_count })) : []
    });

    if (members.length > 1) mergedAway += members.length - 1;
  }

  // Check for slug collisions
  const slugCounts = new Map();
  for (const p of producers) {
    slugCounts.set(p.slug, (slugCounts.get(p.slug) || 0) + 1);
  }
  const collisions = [...slugCounts.entries()].filter(([, c]) => c > 1);
  if (collisions.length > 0) {
    console.log(`── Slug collisions (${collisions.length}) ──`);
    for (const [slug, count] of collisions) {
      const dupes = producers.filter(p => p.slug === slug);
      // Fix: append country slug
      for (let i = 1; i < dupes.length; i++) {
        dupes[i].slug = `${dupes[i].slug}-${slugify(dupes[i].country)}`;
      }
      console.log(`  "${slug}" × ${count} → disambiguated with country suffix`);
    }
    // Re-check after fix
    const slugSet = new Set();
    for (const p of producers) {
      if (slugSet.has(p.slug)) {
        // Still colliding — add a number
        let n = 2;
        while (slugSet.has(`${p.slug}-${n}`)) n++;
        p.slug = `${p.slug}-${n}`;
      }
      slugSet.add(p.slug);
    }
  }

  console.log(`── Summary ──`);
  console.log(`  Total staging names: ${staging.length}`);
  console.log(`  Merged away: ${mergedAway}`);
  console.log(`  Canonical producers to create: ${producers.length}`);
  console.log(`  Producers with aliases: ${producers.filter(p => p.aliases.length > 0).length}`);
  console.log(`  Total aliases: ${producers.reduce((sum, p) => sum + p.aliases.length, 0)}`);

  if (dryRun) {
    console.log("\nDRY RUN — no database changes made.");
    // Show a sample
    console.log("\nSample producers:");
    for (const p of producers.slice(0, 10)) {
      console.log(`  ${p.name} (${p.country}) — ${p.totalWines} wines, slug: ${p.slug}`);
      if (p.aliases.length > 0) {
        console.log(`    aliases: ${p.aliases.map(a => a.name).join(", ")}`);
      }
    }
    return;
  }

  // 7. Insert producers in batches
  console.log("\nInserting producers...");
  const BATCH = 500;
  let inserted = 0;
  const producerIdMap = new Map(); // "name|country" → producer uuid

  for (let i = 0; i < producers.length; i += BATCH) {
    const batch = producers.slice(i, i + BATCH).map(p => ({
      slug: p.slug,
      name: p.name,
      name_normalized: p.nameNormalized,
      country_id: p.countryId,
    }));

    const { data, error } = await supabase
      .from("producers")
      .insert(batch)
      .select("id, name");

    if (error) {
      console.log(`  ERROR at batch ${Math.floor(i/BATCH)+1}: ${error.message}`);
      // Try one by one to find the problem
      for (const row of batch) {
        const { data: d, error: e } = await supabase
          .from("producers")
          .insert(row)
          .select("id, name");
        if (e) {
          console.log(`    SKIP "${row.name}": ${e.message}`);
        } else if (d && d[0]) {
          producerIdMap.set(`${d[0].name}|${producers.find(p => p.name === d[0].name)?.country}`, d[0].id);
          inserted++;
        }
      }
      continue;
    }

    if (data) {
      for (const row of data) {
        // Find the producer entry to get country
        const pEntry = producers.find(p => p.name === row.name && p.slug === batch.find(b => b.name === row.name)?.slug);
        if (pEntry) {
          producerIdMap.set(`${row.name}|${pEntry.country}`, row.id);
        }
      }
      inserted += data.length;
    }
    process.stdout.write(`  ${inserted}/${producers.length}\r`);
  }
  console.log(`  Inserted ${inserted} producers`);

  // 8. Insert aliases
  const aliasRows = [];
  for (const p of producers) {
    if (p.aliases.length === 0) continue;
    const producerId = producerIdMap.get(`${p.name}|${p.country}`);
    if (!producerId) continue;
    for (const alias of p.aliases) {
      if (alias.name === p.name) continue; // skip canonical name as alias
      aliasRows.push({
        producer_id: producerId,
        name: alias.name,
        name_normalized: normalize(alias.name),
        source: "xwines_dedup"
      });
    }
  }

  if (aliasRows.length > 0) {
    console.log(`\nInserting ${aliasRows.length} aliases...`);
    let aliasInserted = 0;
    for (let i = 0; i < aliasRows.length; i += BATCH) {
      const batch = aliasRows.slice(i, i + BATCH);
      const { error } = await supabase.from("producer_aliases").insert(batch);
      if (error) {
        console.log(`  ERROR: ${error.message}`);
        // Try one by one
        for (const row of batch) {
          const { error: e } = await supabase.from("producer_aliases").insert(row);
          if (e) console.log(`    SKIP alias "${row.name}": ${e.message}`);
          else aliasInserted++;
        }
      } else {
        aliasInserted += batch.length;
      }
      process.stdout.write(`  ${aliasInserted}/${aliasRows.length}\r`);
    }
    console.log(`  Inserted ${aliasInserted} aliases`);
  }

  console.log("\n✅ Done!");
}

main().catch(e => {
  console.error("Failed:", e);
  process.exit(1);
});
