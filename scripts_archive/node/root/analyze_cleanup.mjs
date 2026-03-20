import fs from 'fs';
import readline from 'readline';

function normalize(s) {
  // The vivino names have literal \u0026 (backslash + u0026) stored as text
  // We need to replace that literal sequence with &
  let out = s;
  // Match literal backslash followed by u0026
  out = out.replace(/\x5cu0026/g, '&');
  out = out.replace(/&amp;/g, '&');
  return out.normalize('NFC').toLowerCase().trim();
}

async function main() {
  const rl = readline.createInterface({ input: fs.createReadStream('producer_winery_map.jsonl') });
  const slugMatches = [];
  const substringMatches = [];
  const suffixStripped = [];
  const allEntries = [];

  for await (const line of rl) {
    try {
      const j = JSON.parse(line);
      allEntries.push(j);
      if (j.match_confidence === 'slug_match') slugMatches.push(j);
      if (j.match_confidence === 'substring') substringMatches.push(j);
      if (j.match_confidence === 'suffix_stripped') suffixStripped.push(j);
    } catch {}
  }

  // Test normalization on first &-containing entry
  const testEntry = slugMatches.find(m => m.vivino_winery_name.includes('0026'));
  if (testEntry) {
    const rawVivino = testEntry.vivino_winery_name;
    console.log('=== NORMALIZATION TEST ===');
    console.log('Raw vivino name:', JSON.stringify(rawVivino));
    console.log('Char codes of first 20:', [...rawVivino].slice(0, 20).map(c => c.charCodeAt(0)).join(','));
    console.log('Normalized:', normalize(rawVivino));
    console.log('Producer normalized:', normalize(testEntry.producer_name));
    console.log('Match?', normalize(testEntry.producer_name) === normalize(testEntry.vivino_winery_name));
    console.log('');
  }

  // === SLUG_MATCH ANALYSIS ===
  const slugSafe = [];
  const slugFalsePositives = [];
  for (const m of slugMatches) {
    if (normalize(m.producer_name) === normalize(m.vivino_winery_name)) {
      slugSafe.push(m);
    } else {
      slugFalsePositives.push(m);
    }
  }

  console.log('=== SLUG_MATCH ANALYSIS ===');
  console.log(`Total: ${slugMatches.length}`);
  console.log(`Encoding-safe (names match after normalizing \\u0026→&): ${slugSafe.length}`);
  console.log(`FALSE POSITIVES (names genuinely different): ${slugFalsePositives.length}`);
  console.log('');
  if (slugFalsePositives.length > 0) {
    console.log('All false positive slug_matches:');
    for (const m of slugFalsePositives) {
      console.log(`  ✗ "${m.producer_name}" → "${normalize(m.vivino_winery_name)}" (winery ${m.vivino_winery_id}, ${m.wines_count} wines)`);
    }
  }

  // === SUFFIX_STRIPPED ANALYSIS ===
  console.log('');
  console.log('=== SUFFIX_STRIPPED ANALYSIS ===');
  console.log(`Total: ${suffixStripped.length}`);
  for (const m of suffixStripped) {
    console.log(`  "${m.producer_name}" → "${normalize(m.vivino_winery_name)}" (winery ${m.vivino_winery_id}, ${m.wines_count} wines)`);
  }

  // === SUBSTRING ANALYSIS ===
  const subSafe = [];
  const subRisky = [];
  for (const m of substringMatches) {
    const a = normalize(m.producer_name);
    const b = normalize(m.vivino_winery_name);
    // Safe if one fully contains the other
    if (a.includes(b) || b.includes(a)) {
      subSafe.push(m);
    } else {
      subRisky.push(m);
    }
  }

  console.log('');
  console.log('=== SUBSTRING ANALYSIS ===');
  console.log(`Total: ${substringMatches.length}`);
  console.log(`Contained (one name inside the other): ${subSafe.length}`);
  console.log(`Risky (partial/weak overlap): ${subRisky.length}`);
  console.log('');
  if (subRisky.length > 0) {
    console.log('Risky substring matches:');
    for (const m of subRisky) {
      console.log(`  ? "${m.producer_name}" → "${normalize(m.vivino_winery_name)}" (winery ${m.vivino_winery_id}, ${m.wines_count} wines)`);
    }
  }

  // === SUMMARY ===
  const totalResolved = allEntries.filter(e => e.vivino_winery_id).length;
  const totalNone = allEntries.filter(e => e.match_confidence === 'none').length;
  const totalExact = allEntries.filter(e => e.match_confidence === 'exact').length;

  console.log('');
  console.log('=== SUMMARY ===');
  console.log(`Total producers: ${allEntries.length}`);
  console.log(`Exact matches (safe): ${totalExact}`);
  console.log(`Slug_match encoding-safe: ${slugSafe.length}`);
  console.log(`Slug_match FALSE POSITIVES: ${slugFalsePositives.length}`);
  console.log(`Suffix_stripped: ${suffixStripped.length}`);
  console.log(`Substring contained (likely safe): ${subSafe.length}`);
  console.log(`Substring risky: ${subRisky.length}`);
  console.log(`Unresolved: ${totalNone}`);
  console.log('');
  console.log(`ENTRIES TO DELETE: ${slugFalsePositives.length} slug_match FPs + ${subRisky.length} risky substring = ${slugFalsePositives.length + subRisky.length}`);
  console.log(`ENTRIES TO KEEP: ${totalExact} exact + ${slugSafe.length} slug safe + ${subSafe.length} substring safe + ${suffixStripped.length} suffix = ${totalExact + slugSafe.length + subSafe.length + suffixStripped.length}`);
}

main();
