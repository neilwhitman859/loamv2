#!/usr/bin/env node
/**
 * Reads the TSV data extracted from the browser localStorage
 * and converts it back to JSONL format, appending to the main output file.
 *
 * Input: tw_remaining_tsv.txt (tab-separated: name, sku, size, price, starRating, reviews, page)
 * Output: appends to totalwine_lexington_green.jsonl
 */
import { readFileSync, appendFileSync } from 'fs';

const INPUT = 'tw_remaining.txt';
const OUTPUT = 'totalwine_lexington_green.jsonl';

const raw = readFileSync(INPUT, 'utf8').trim();
const lines = raw.split('\n').filter(l => l.trim());

let count = 0;
for (const line of lines) {
  const [name, price, sku, starRating, reviews, page] = line.split('|');
  if (!name) continue;

  const obj = {
    name: name.trim(),
    sku: sku || '',
    size: '',
    price: price || '',
    starRating: starRating || '',
    reviews: reviews || '',
    wineryDirect: false,
    categories: [],
    page: parseInt(page) || 0
  };

  appendFileSync(OUTPUT, JSON.stringify(obj) + '\n');
  count++;
}

console.log(`Appended ${count} wines to ${OUTPUT}`);
