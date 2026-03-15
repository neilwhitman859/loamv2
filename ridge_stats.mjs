#!/usr/bin/env node
/** Stats on scraped Ridge data */
import { readFileSync } from 'fs';

const lines = readFileSync('ridge_wines.jsonl', 'utf8').trim().split('\n');
const wines = lines.map(l => JSON.parse(l));

console.log('=== RIDGE SCRAPE STATS ===\n');
console.log(`Total entries: ${wines.length}`);

// Unique wine names
const names = new Set(wines.map(w => w.wineName));
console.log(`Unique wines: ${names.size}`);

// Vintage range
const years = wines.map(w => w.vintage).filter(Boolean);
console.log(`Vintage range: ${Math.min(...years)} - ${Math.max(...years)}`);

// Data completeness
console.log(`\nData Completeness:`);
console.log(`  With grapes: ${wines.filter(w => w.grapes.length > 0).length} (${(wines.filter(w => w.grapes.length > 0).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With scores: ${wines.filter(w => w.scores.length > 0).length} (${(wines.filter(w => w.scores.length > 0).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With winemaker notes: ${wines.filter(w => w.winemakerNotes).length} (${(wines.filter(w => w.winemakerNotes).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With vintage notes: ${wines.filter(w => w.vintageNotes).length} (${(wines.filter(w => w.vintageNotes).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With ABV: ${wines.filter(w => w.abv).length} (${(wines.filter(w => w.abv).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With winemaking: ${wines.filter(w => w.winemaking).length} (${(wines.filter(w => w.winemaking).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With growing season: ${wines.filter(w => w.growingSeason).length} (${(wines.filter(w => w.growingSeason).length / wines.length * 100).toFixed(1)}%)`);
console.log(`  With pH: ${wines.filter(w => w.winemaking?.ph).length}`);
console.log(`  With Brix: ${wines.filter(w => w.winemaking?.brix).length}`);
console.log(`  Members only: ${wines.filter(w => w.membersOnly).length}`);

// Total scores
const totalScores = wines.reduce((s, w) => s + w.scores.length, 0);
console.log(`\nTotal scores: ${totalScores}`);

// Publications
const pubs = new Map();
for (const w of wines) {
  for (const s of w.scores) {
    const pub = s.publication || '(no publication)';
    pubs.set(pub, (pubs.get(pub) || 0) + 1);
  }
}
console.log(`\nPublications (${pubs.size}):`);
[...pubs.entries()].sort((a, b) => b[1] - a[1]).forEach(([p, c]) => console.log(`  ${p}: ${c} scores`));

// Appellations
const apps = new Map();
for (const w of wines) {
  const app = w.appellation || '(none)';
  apps.set(app, (apps.get(app) || 0) + 1);
}
console.log(`\nAppellations (${apps.size}):`);
[...apps.entries()].sort((a, b) => b[1] - a[1]).forEach(([a, c]) => console.log(`  ${a}: ${c} vintages`));

// Grapes
const grapeSet = new Set();
for (const w of wines) {
  for (const g of w.grapes) grapeSet.add(g.grape);
}
console.log(`\nUnique grapes (${grapeSet.size}): ${[...grapeSet].sort().join(', ')}`);
