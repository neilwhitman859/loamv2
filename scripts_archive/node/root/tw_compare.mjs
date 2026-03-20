#!/usr/bin/env node
/**
 * Compares Total Wine Lexington Green inventory against Loam wine database.
 * Read-only — no DB writes.
 */
import { readFileSync } from 'fs';

// Load .env manually
const envLines = readFileSync('.env', 'utf8').split('\n');
const env = {};
for (const l of envLines) {
  const m = l.replace(/\r/g, '').match(/^(\w+)=(.*)$/);
  if (m) env[m[1]] = m[2].trim();
}

const SUPABASE_URL = env.SUPABASE_URL;
const SUPABASE_KEY = env.SUPABASE_SERVICE_ROLE;
const JSONL_FILE = 'totalwine_lexington_green.jsonl';

async function sql(query) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/`, {
    method: 'POST',
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query })
  });
  // Use the direct postgres endpoint instead
  const pgRes = await fetch(`${SUPABASE_URL}/rest/v1/`, {
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
    }
  });
  return null;
}

// Use pg REST API to query
async function pgQuery(endpoint, params = {}) {
  const url = new URL(`${SUPABASE_URL}/rest/v1/${endpoint}`);
  for (const [k, v] of Object.entries(params)) {
    url.searchParams.set(k, v);
  }
  const res = await fetch(url, {
    headers: {
      'apikey': SUPABASE_KEY,
      'Authorization': `Bearer ${SUPABASE_KEY}`,
      'Prefer': 'count=exact'
    }
  });
  const count = res.headers.get('content-range');
  const data = await res.json();
  return { data, count };
}

// Normalize a name for comparison
function normalize(name) {
  return name
    .toLowerCase()
    .replace(/[,.''\-–—]/g, ' ')
    .replace(/\s+/g, ' ')
    .replace(/\b(20[12]\d|19\d{2})\b/g, '') // strip vintage years
    .trim();
}

async function main() {
  // 1. Load TW data
  const lines = readFileSync(JSONL_FILE, 'utf8').trim().split('\n');
  const twWines = lines.map(l => JSON.parse(l));
  console.log(`Total Wine inventory: ${twWines.length} wines\n`);

  // 2. Load all producers from Loam (paginated)
  console.log('Loading Loam producers...');
  let allProducers = [];
  let offset = 0;
  const PAGE = 1000;
  while (true) {
    const { data } = await pgQuery('producers', {
      select: 'id,name,name_normalized',
      'deleted_at': 'is.null',
      order: 'name',
      offset: String(offset),
      limit: String(PAGE)
    });
    allProducers.push(...data);
    if (data.length < PAGE) break;
    offset += PAGE;
  }
  console.log(`Loaded ${allProducers.length} Loam producers`);

  // 3. Build producer lookup (normalized name -> producer)
  const producerByNorm = new Map();
  const producerById = new Map();
  for (const p of allProducers) {
    const norm = normalize(p.name);
    producerByNorm.set(norm, p);
    producerById.set(p.id, p);
  }

  // Sort producer names by length DESC for longest-prefix matching
  const producerNames = [...producerByNorm.keys()].sort((a, b) => b.length - a.length);

  // 4. Match TW wines to producers by prefix
  console.log('Matching TW wines to Loam producers...\n');
  let producerMatches = 0;
  let producerMisses = 0;
  const matchedProducerIds = new Set();
  const unmatchedProducers = new Map(); // first word(s) -> count
  const twWithProducer = []; // {tw, producer, winePart}

  for (const tw of twWines) {
    const twNorm = normalize(tw.name);
    let matched = false;

    for (const pName of producerNames) {
      if (twNorm.startsWith(pName + ' ') || twNorm === pName) {
        const producer = producerByNorm.get(pName);
        const winePart = twNorm.slice(pName.length).trim();
        twWithProducer.push({ tw, producer, winePart });
        matchedProducerIds.add(producer.id);
        producerMatches++;
        matched = true;
        break;
      }
    }

    if (!matched) {
      producerMisses++;
      // Track unmatched producer prefixes (first 2 words)
      const prefix = tw.name.split(/\s+/).slice(0, 2).join(' ');
      unmatchedProducers.set(prefix, (unmatchedProducers.get(prefix) || 0) + 1);
    }
  }

  console.log(`Producer matching:`);
  console.log(`  Matched: ${producerMatches} / ${twWines.length} (${(producerMatches/twWines.length*100).toFixed(1)}%)`);
  console.log(`  Unmatched: ${producerMisses}`);
  console.log(`  Unique Loam producers matched: ${matchedProducerIds.size}`);

  // Show top unmatched producer prefixes
  const topUnmatched = [...unmatchedProducers.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20);
  console.log(`\n  Top unmatched producer prefixes:`);
  for (const [prefix, count] of topUnmatched) {
    console.log(`    "${prefix}" — ${count} wines`);
  }

  // 5. Now match wines within matched producers
  console.log('\nMatching wines within matched producers...');

  // Batch: get wines for matched producers
  const producerIdList = [...matchedProducerIds];
  const loamWinesByProducer = new Map(); // producer_id -> [{name, name_normalized}]

  // Fetch in batches of 50 producer IDs
  for (let i = 0; i < producerIdList.length; i += 50) {
    const batch = producerIdList.slice(i, i + 50);
    const idFilter = `in.(${batch.join(',')})`;
    const { data } = await pgQuery('wines', {
      select: 'name,name_normalized,producer_id',
      'deleted_at': 'is.null',
      'producer_id': idFilter,
      limit: '10000'
    });
    for (const w of data) {
      if (!loamWinesByProducer.has(w.producer_id)) {
        loamWinesByProducer.set(w.producer_id, []);
      }
      loamWinesByProducer.get(w.producer_id).push(w);
    }
    if ((i + 50) % 500 === 0 || i + 50 >= producerIdList.length) {
      process.stdout.write(`  Fetched wines for ${Math.min(i + 50, producerIdList.length)}/${producerIdList.length} producers\r`);
    }
  }
  console.log('');

  // Match wine names
  let fullMatches = 0;
  let fuzzyMatches = 0;
  let wineOnlyMisses = 0;
  const fullMatchList = [];
  const fuzzyMatchList = [];
  const missedWines = [];

  for (const { tw, producer, winePart } of twWithProducer) {
    const producerWines = loamWinesByProducer.get(producer.id) || [];

    // Try exact match on normalized wine name
    const exactMatch = producerWines.find(w =>
      normalize(w.name) === winePart ||
      w.name_normalized === winePart
    );

    if (exactMatch) {
      fullMatches++;
      fullMatchList.push({ tw: tw.name, loam: `${producer.name} — ${exactMatch.name}`, price: tw.price });
      continue;
    }

    // Try fuzzy match: check if wine part is contained in or contains a Loam wine name
    let bestFuzzy = null;
    let bestScore = 0;
    for (const w of producerWines) {
      const wNorm = normalize(w.name);
      // Trigram-like similarity: count shared 3-grams
      const trgA = trigrams(winePart);
      const trgB = trigrams(wNorm);
      const intersection = [...trgA].filter(t => trgB.has(t)).length;
      const union = new Set([...trgA, ...trgB]).size;
      const sim = union > 0 ? intersection / union : 0;
      if (sim > bestScore) {
        bestScore = sim;
        bestFuzzy = w;
      }
    }

    if (bestScore >= 0.4) {
      fuzzyMatches++;
      fuzzyMatchList.push({
        tw: tw.name,
        loam: `${producer.name} — ${bestFuzzy.name}`,
        score: bestScore.toFixed(2),
        price: tw.price
      });
    } else {
      wineOnlyMisses++;
      missedWines.push({ name: tw.name, producer: producer.name, winePart, price: tw.price });
    }
  }

  // 6. Summary
  const totalMatched = fullMatches + fuzzyMatches;
  const totalUnmatched = producerMisses + wineOnlyMisses;

  console.log('\n========================================');
  console.log('   TOTAL WINE vs LOAM COMPARISON');
  console.log('========================================\n');
  console.log(`Total Wine Lexington Green: ${twWines.length} wines`);
  console.log(`Loam Database: ${allProducers.length} producers\n`);

  console.log(`MATCHING RESULTS:`);
  console.log(`  Exact matches:       ${fullMatches} (${(fullMatches/twWines.length*100).toFixed(1)}%)`);
  console.log(`  Fuzzy matches:       ${fuzzyMatches} (${(fuzzyMatches/twWines.length*100).toFixed(1)}%)`);
  console.log(`  Total matched:       ${totalMatched} (${(totalMatched/twWines.length*100).toFixed(1)}%)`);
  console.log(`  No match (wine):     ${wineOnlyMisses} (producer found, wine not)`);
  console.log(`  No match (producer): ${producerMisses} (producer not in Loam)`);
  console.log(`  Total unmatched:     ${totalUnmatched} (${(totalUnmatched/twWines.length*100).toFixed(1)}%)\n`);

  // Price analysis on matched wines
  const matchedPrices = [...fullMatchList, ...fuzzyMatchList]
    .map(m => parseFloat((m.price || '').replace('$', '')))
    .filter(p => p > 0);
  const unmatchedPrices = missedWines
    .map(m => parseFloat((m.price || '').replace('$', '')))
    .filter(p => p > 0);

  if (matchedPrices.length > 0) {
    matchedPrices.sort((a, b) => a - b);
    unmatchedPrices.sort((a, b) => a - b);
    const avg = arr => arr.reduce((s, v) => s + v, 0) / arr.length;
    const median = arr => arr[Math.floor(arr.length / 2)];

    console.log(`PRICE ANALYSIS:`);
    console.log(`  Matched wines avg price:   $${avg(matchedPrices).toFixed(2)} (median $${median(matchedPrices).toFixed(2)})`);
    if (unmatchedPrices.length > 0) {
      console.log(`  Unmatched wines avg price: $${avg(unmatchedPrices).toFixed(2)} (median $${median(unmatchedPrices).toFixed(2)})`);
    }
  }

  // Category breakdown
  console.log(`\nCATEGORY BREAKDOWN (matched vs unmatched):`);
  const catStats = new Map();
  for (const tw of twWines) {
    const mainCat = (tw.categories || [])[0] || 'Unknown';
    if (!catStats.has(mainCat)) catStats.set(mainCat, { total: 0, matched: 0 });
    catStats.get(mainCat).total++;
  }
  // Mark matched ones
  for (const m of fullMatchList) {
    const tw = twWines.find(t => t.name === m.tw);
    if (tw) {
      const cat = (tw.categories || [])[0] || 'Unknown';
      catStats.get(cat).matched++;
    }
  }
  for (const m of fuzzyMatchList) {
    const tw = twWines.find(t => t.name === m.tw);
    if (tw) {
      const cat = (tw.categories || [])[0] || 'Unknown';
      catStats.get(cat).matched++;
    }
  }

  const catSorted = [...catStats.entries()].sort((a, b) => b[1].total - a[1].total);
  console.log(`  ${'Category'.padEnd(35)} Total  Matched  Rate`);
  console.log(`  ${'─'.repeat(35)} ${'─'.repeat(6)} ${'─'.repeat(8)} ${'─'.repeat(5)}`);
  for (const [cat, s] of catSorted.slice(0, 20)) {
    const rate = (s.matched / s.total * 100).toFixed(0);
    console.log(`  ${cat.padEnd(35)} ${String(s.total).padStart(5)}  ${String(s.matched).padStart(7)}  ${rate.padStart(4)}%`);
  }

  // Sample unmatched wines
  console.log(`\nSAMPLE UNMATCHED WINES (producer not in Loam):`);
  const unmatchedSamples = [...unmatchedProducers.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 15);
  for (const [prefix, count] of unmatchedSamples) {
    console.log(`  "${prefix}..." — ${count} wines`);
  }

  console.log(`\nSAMPLE UNMATCHED WINES (producer found, wine not):`);
  for (const m of missedWines.slice(0, 15)) {
    console.log(`  ${m.name} (producer: ${m.producer}, looking for: "${m.winePart}") ${m.price}`);
  }

  // Sample fuzzy matches for review
  console.log(`\nSAMPLE FUZZY MATCHES (review quality):`);
  for (const m of fuzzyMatchList.slice(0, 15)) {
    console.log(`  TW: "${m.tw}" → Loam: "${m.loam}" (score: ${m.score}) ${m.price}`);
  }
}

function trigrams(s) {
  const set = new Set();
  const padded = '  ' + s + ' ';
  for (let i = 0; i < padded.length - 2; i++) {
    set.add(padded.substring(i, i + 3));
  }
  return set;
}

main().catch(console.error);
