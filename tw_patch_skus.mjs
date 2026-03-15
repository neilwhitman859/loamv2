#!/usr/bin/env node
/**
 * Patches pages 35-43 in totalwine_lexington_green.jsonl with
 * SKU, size, and URL data from the browser localStorage export.
 */
import { readFileSync, writeFileSync } from 'fs';

const JSONL_FILE = 'totalwine_lexington_green.jsonl';
const PATCH_FILE = 'C:/Users/neilw/Downloads/tw_full_data.txt';

// Build lookup from patch file: name -> {sku, size, url}
const patchLines = readFileSync(PATCH_FILE, 'utf8').trim().split('\n');
const patchMap = new Map();
for (const line of patchLines) {
  const [name, sku, size, url, page] = line.split('|');
  if (name) {
    patchMap.set(name.trim(), { sku: sku || '', size: size || '', url: url || '' });
  }
}
console.log(`Loaded ${patchMap.size} patch entries`);

// Parse category from URL path
function parseCategoriesFromUrl(url) {
  const categories = [];
  if (!url) return categories;
  const cleaned = url.replace(/\?.*$/, '').replace(/^\/wine\//, '');
  const parts = cleaned.split('/');
  const skip = new Set(['deals', 'gift-center', 'p']);
  for (const part of parts) {
    if (skip.has(part)) continue;
    if (/^\d+$/.test(part)) continue;
    const idx = parts.indexOf(part);
    if (idx === parts.length - 1 || (idx === parts.length - 3 && parts[parts.length - 2] === 'p')) continue;
    const readable = part.split('-').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
    categories.push(readable);
  }
  return categories;
}

// Read and patch JSONL
const lines = readFileSync(JSONL_FILE, 'utf8').trim().split('\n');
let patched = 0, skuAdded = 0, sizeAdded = 0, urlAdded = 0;

const updated = lines.map(line => {
  const obj = JSON.parse(line);
  if (obj.page < 35) return line; // Leave pages 1-34 untouched

  const patch = patchMap.get(obj.name);
  if (!patch) return line;

  patched++;
  if (patch.sku && !obj.sku) { obj.sku = patch.sku; skuAdded++; }
  else if (patch.sku && obj.sku !== patch.sku) { obj.sku = patch.sku; } // update if different
  if (patch.size && !obj.size) { obj.size = patch.size; sizeAdded++; }
  if (patch.url && !obj.url) { obj.url = patch.url; urlAdded++; }
  if (patch.url && (!obj.categories || obj.categories.length === 0)) {
    obj.categories = parseCategoriesFromUrl(patch.url);
  }

  return JSON.stringify(obj);
});

writeFileSync(JSONL_FILE, updated.join('\n') + '\n');
console.log(`Patched ${patched} entries:`);
console.log(`  SKUs added/updated: ${skuAdded}`);
console.log(`  Sizes added: ${sizeAdded}`);
console.log(`  URLs added: ${urlAdded}`);
