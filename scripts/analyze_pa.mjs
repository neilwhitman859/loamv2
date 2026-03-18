import { readFileSync } from 'fs';
import XLSX from 'xlsx';

const wb = XLSX.readFile('data/imports/pa_wine_catalog.xlsx');
const data = XLSX.utils.sheet_to_json(wb.Sheets[wb.SheetNames[0]]);

// Group breakdown
const groups = {};
data.forEach(r => { const g = r['Group Name'] || 'null'; groups[g] = (groups[g] || 0) + 1; });
console.log('Group Names:', JSON.stringify(groups, null, 2));
console.log('Total rows:', data.length);

// UPC coverage
let hasUpc = 0;
let totalUpcs = 0;
data.forEach(r => {
  const keys = Object.keys(r).filter(k => k === 'UPC' || k.startsWith('UPC'));
  const upcs = keys.map(k => r[k]).filter(Boolean);
  if (upcs.length > 0) { hasUpc++; totalUpcs += upcs.length; }
});
console.log('\nRows with UPC:', hasUpc, '/', data.length);
console.log('Total UPC values:', totalUpcs);

// Vintage coverage
let hasVintage = 0;
data.forEach(r => {
  if (r['Vintage'] && r['Vintage'] !== 'Nonvintage') hasVintage++;
});
console.log('Rows with vintage:', hasVintage);

// Country/region
const countries = {};
data.forEach(r => { const c = r['Country'] || 'null'; countries[c] = (countries[c] || 0) + 1; });
console.log('\nCountries:', JSON.stringify(countries));

// Sample real wine with UPC and vintage
const sample = data.find(r => r['Vintage'] && r['Vintage'] !== 'Nonvintage' && r['Region'] && r['UPC']);
console.log('\nSample wine with UPC + vintage:', JSON.stringify(sample, null, 2));
