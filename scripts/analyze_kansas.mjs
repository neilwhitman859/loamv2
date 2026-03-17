import { readFileSync } from 'fs';

const data = JSON.parse(readFileSync('data/imports/kansas_active_brands.json', 'utf8'));
console.log('=== KANSAS ACTIVE BRANDS ANALYSIS ===');
console.log('Total records:', data.length);

const labels = {
  a: 'COLA Number', b: 'KS License', c: 'Brand Name', d: 'Fanciful Name',
  e: 'Type', f: 'ABV', g: 'unknown_g', h: 'Size', i: 'Unit', j: 'Vintage',
  k: 'Appellation', l: 'Expiration', m: 'unknown_m', n: 'Container',
  o: 'unknown_o', p: 'unknown_p', q: 'Distributor1', r: 'Distributor2'
};

console.log('\n--- Field Fill Rates ---');
for (const k of 'abcdefghijklmnopqr'.split('')) {
  const filled = data.filter(r => r[k] && r[k].toString().trim().length > 0).length;
  const pct = (filled / data.length * 100).toFixed(1);
  console.log(`  ${k} (${labels[k] || '?'}): ${filled} (${pct}%)`);
}

// Type breakdown
console.log('\n--- Type Breakdown ---');
const types = {};
data.forEach(r => { types[r.e] = (types[r.e] || 0) + 1; });
Object.entries(types).sort((a, b) => b[1] - a[1]).forEach(([k, v]) => console.log(`  ${k}: ${v}`));

// Wine-only subset
const wineTypes = ['Light Wine', 'Sparkling Wine', 'Wine', 'Table Wine'];
const wines = data.filter(r => {
  const t = (r.e || '').trim();
  return t.includes('Wine') || t.includes('wine');
});
console.log('\n--- Wine Records ---');
console.log(`Wine-type records: ${wines.length} of ${data.length} (${(wines.length/data.length*100).toFixed(1)}%)`);

// COLA number analysis for wines
const winesWithCola = wines.filter(r => r.a && r.a.trim().length > 0);
console.log(`Wines with COLA number: ${winesWithCola.length} (${(winesWithCola.length/wines.length*100).toFixed(1)}%)`);

// Vintage analysis for wines
const winesWithVintage = wines.filter(r => r.j && r.j.trim().length > 0 && r.j.trim() !== '0');
console.log(`Wines with vintage: ${winesWithVintage.length} (${(winesWithVintage.length/wines.length*100).toFixed(1)}%)`);

// Appellation analysis for wines
const winesWithAppellation = wines.filter(r => r.k && r.k.trim().length > 0);
console.log(`Wines with appellation: ${winesWithAppellation.length} (${(winesWithAppellation.length/wines.length*100).toFixed(1)}%)`);

// ABV analysis for wines
const winesWithAbv = wines.filter(r => r.f && parseFloat(r.f) > 0);
console.log(`Wines with ABV: ${winesWithAbv.length} (${(winesWithAbv.length/wines.length*100).toFixed(1)}%)`);

// Sample wines
console.log('\n--- Sample Wine Records ---');
for (const w of wines.slice(0, 5)) {
  console.log(JSON.stringify(w));
}

// Appellation distribution for wines
console.log('\n--- Top 30 Wine Appellations ---');
const appellations = {};
wines.forEach(r => {
  const app = (r.k || '').trim();
  if (app) appellations[app] = (appellations[app] || 0) + 1;
});
Object.entries(appellations).sort((a, b) => b[1] - a[1]).slice(0, 30)
  .forEach(([k, v]) => console.log(`  ${k}: ${v}`));

// Brand name distribution (top 20)
console.log('\n--- Top 20 Wine Brands ---');
const brands = {};
wines.forEach(r => {
  const b = (r.c || '').trim();
  if (b) brands[b] = (brands[b] || 0) + 1;
});
Object.entries(brands).sort((a, b) => b[1] - a[1]).slice(0, 20)
  .forEach(([k, v]) => console.log(`  ${k}: ${v}`));

// Unknown field analysis
console.log('\n--- Unknown Field Samples ---');
const gVals = [...new Set(wines.slice(0, 100).map(r => r.g))];
console.log('g values (first 100 wines):', gVals.slice(0, 20));
const mVals = [...new Set(wines.slice(0, 100).map(r => r.m))];
console.log('m values (first 100 wines):', mVals.slice(0, 20));
const oVals = [...new Set(wines.slice(0, 100).map(r => r.o))];
console.log('o values (first 100 wines):', oVals.slice(0, 20));
const pVals = [...new Set(wines.slice(0, 100).map(r => r.p))];
console.log('p values (first 100 wines):', pVals.slice(0, 20));

// Unique brand count
const uniqueBrands = new Set(wines.map(r => (r.c || '').trim().toUpperCase()));
console.log('\n--- Unique Counts (wines only) ---');
console.log(`Unique brands: ${uniqueBrands.size}`);
const uniqueColas = new Set(winesWithCola.map(r => r.a.trim()));
console.log(`Unique COLA numbers: ${uniqueColas.size}`);
