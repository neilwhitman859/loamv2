import fs from 'fs';
import readline from 'readline';

async function main() {
  // 1. Check slug_match entries — these are the riskiest (slug resolved but name didn't match)
  console.log("=== AUDIT: slug_match entries (highest risk of false positives) ===\n");
  const rl = readline.createInterface({ input: fs.createReadStream('producer_winery_map.jsonl') });
  const slugMatches = [];
  const allResolved = [];
  for await (const line of rl) {
    try {
      const j = JSON.parse(line);
      if (j.match_confidence === 'slug_match') slugMatches.push(j);
      if (j.vivino_winery_id) allResolved.push(j);
    } catch {}
  }

  console.log(`slug_match entries: ${slugMatches.length}`);
  console.log("Sample slug_match (potential false positives):");
  for (const m of slugMatches.slice(0, 20)) {
    const match = m.producer_name === m.vivino_winery_name ? '✓' : '✗';
    console.log(`  ${match} Loam: "${m.producer_name}" → Vivino: "${m.vivino_winery_name}" (${m.vivino_winery_id}, ${m.wines_count} wines)`);
  }

  // 2. Check substring matches
  console.log("\n=== AUDIT: substring entries ===\n");
  const rl2 = readline.createInterface({ input: fs.createReadStream('producer_winery_map.jsonl') });
  const substringMatches = [];
  for await (const line of rl2) {
    try {
      const j = JSON.parse(line);
      if (j.match_confidence === 'substring') substringMatches.push(j);
    } catch {}
  }
  console.log(`substring entries: ${substringMatches.length}`);
  console.log("Sample substring matches:");
  for (const m of substringMatches.slice(0, 20)) {
    console.log(`  Loam: "${m.producer_name}" → Vivino: "${m.vivino_winery_name}" (${m.wines_count} wines)`);
  }

  // 3. Check for duplicate vivino_winery_ids (multiple Loam producers → same Vivino winery)
  console.log("\n=== AUDIT: Duplicate Vivino winery mappings ===\n");
  const wineryToProducers = new Map();
  for (const m of allResolved) {
    if (!wineryToProducers.has(m.vivino_winery_id)) wineryToProducers.set(m.vivino_winery_id, []);
    wineryToProducers.get(m.vivino_winery_id).push(m);
  }
  const dupes = [...wineryToProducers.entries()].filter(([_, v]) => v.length > 1);
  console.log(`Wineries mapped to multiple Loam producers: ${dupes.length}`);
  for (const [wid, producers] of dupes.slice(0, 10)) {
    console.log(`  Vivino ${wid} "${producers[0].vivino_winery_name}" → ${producers.map(p => `"${p.producer_name}"`).join(', ')}`);
  }

  // 4. Check for obviously wrong matches in "exact" category
  console.log("\n=== AUDIT: 'exact' matches with high wine counts ===\n");
  const exactBigWineries = allResolved.filter(m => m.match_confidence === 'exact' && m.wines_count > 100);
  console.log(`Exact matches with >100 wines: ${exactBigWineries.length}`);
  for (const m of exactBigWineries.slice(0, 10)) {
    console.log(`  "${m.producer_name}" → "${m.vivino_winery_name}" (${m.wines_count} wines)`);
  }
}

main();
