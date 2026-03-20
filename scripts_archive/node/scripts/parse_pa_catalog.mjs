#!/usr/bin/env node
/**
 * Parse Pennsylvania PLCB Wholesale Catalog Excel → JSON
 * Extracts wine products with UPC barcodes, prices, vintage, country, region.
 * Output: data/imports/pa_wines_parsed.json
 */
import XLSX from 'xlsx';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const INPUT = path.join(__dirname, '..', 'data', 'imports', 'pa_wine_catalog.xlsx');
const OUTPUT = path.join(__dirname, '..', 'data', 'imports', 'pa_wines_parsed.json');

const wb = XLSX.readFile(INPUT);
const ws = wb.Sheets[wb.SheetNames[0]];
const data = XLSX.utils.sheet_to_json(ws, { header: 1 });
const rows = data.slice(1).filter(r => r[0] === 'Stock Wine');

const wineGroups = new Set(['Table Wine', 'Fortified Wine', 'Other-Dessert Wines', 'Sparkling Wine']);
const wines = rows.filter(r => wineGroups.has(r[1]));

const output = wines.map(r => {
  const upcs = [r[16], r[17], r[18], r[19], r[20]]
    .filter(u => u)
    .map(u => String(u).trim());

  const vintage = r[28];
  let vintageYear = null;
  if (vintage && /^\d{4}$/.test(vintage)) vintageYear = parseInt(vintage);

  return {
    plcb_item: r[3] ? String(r[3]).trim() : null,
    name: r[4] || null,
    plcb_scc: r[5] ? String(r[5]).trim() : null,
    manufacturer_scc: r[6] ? String(r[6]).trim() : null,
    volume: r[7] || null,
    case_pack: r[8] ? parseInt(r[8]) : null,
    price_usd: r[9] ? parseFloat(r[9]) : null,
    upcs,
    upc_primary: upcs[0] || null,
    proof: r[27] || null,
    vintage: vintage || null,
    vintage_year: vintageYear,
    brand: r[29] || null,
    import_domestic: r[30] || null,
    country: r[31] || null,
    region: r[32] || null,
    group: r[1] || null,
    class_name: r[2] || null,
  };
});

// Stats
const allUpcs = new Set();
output.forEach(w => w.upcs.forEach(u => allUpcs.add(u)));

const stats = {
  total: output.length,
  has_upc: output.filter(w => w.upc_primary).length,
  unique_upcs: allUpcs.size,
  has_vintage_year: output.filter(w => w.vintage_year).length,
  has_country: output.filter(w => w.country).length,
  has_region: output.filter(w => w.region).length,
  has_brand: output.filter(w => w.brand).length,
  has_proof: output.filter(w => w.proof !== 'N/A' && w.proof != null).length,
  has_price: output.filter(w => w.price_usd).length,
};

console.log('=== PA PLCB Wine Catalog ===');
console.log(`Total wines: ${stats.total}`);
console.log(`Has UPC: ${stats.has_upc} (${(stats.has_upc / stats.total * 100).toFixed(1)}%)`);
console.log(`Unique UPCs: ${stats.unique_upcs}`);
console.log(`Has vintage year: ${stats.has_vintage_year}`);
console.log(`Has country: ${stats.has_country}`);
console.log(`Has region: ${stats.has_region}`);
console.log(`Has brand: ${stats.has_brand}`);
console.log(`Has proof: ${stats.has_proof}`);
console.log(`Has price: ${stats.has_price}`);

// Country breakdown
const countries = {};
output.forEach(w => { if (w.country) countries[w.country] = (countries[w.country] || 0) + 1; });
console.log('\nTop countries:', Object.fromEntries(
  Object.entries(countries).sort((a, b) => b[1] - a[1]).slice(0, 15)
));

// Group breakdown
const groups = {};
output.forEach(w => { groups[w.group] = (groups[w.group] || 0) + 1; });
console.log('Groups:', groups);

// Write output
const file = {
  metadata: {
    source: 'Pennsylvania PLCB Wholesale Catalog',
    file: 'pa_wine_catalog.xlsx',
    extracted_at: new Date().toISOString(),
    stats
  },
  wines: output
};
fs.writeFileSync(OUTPUT, JSON.stringify(file, null, 2));
console.log(`\nSaved to ${OUTPUT}`);
console.log(`File size: ${(fs.statSync(OUTPUT).size / 1024 / 1024).toFixed(1)} MB`);
