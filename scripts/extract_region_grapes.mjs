/**
 * Extract grape varieties by region from Anderson & Aryal dataset (2023)
 * Source: University of Adelaide Wine Economics Research Centre
 * "Database of Regional, National and Global Winegrape Bearing Areas by Variety, 2000 to 2023"
 * https://economics.adelaide.edu.au/wine-economics/databases
 *
 * Outputs JSON with top grapes per region and per country for seeding
 * region_grapes and country_grapes tables.
 */

import XLSX from 'xlsx';
import { writeFileSync } from 'fs';

const REGIONAL_FILE = 'C:/Users/neilw/.claude/projects/C--Users-neilw-Documents-GitHub-loamv2/e9288814-96ee-4aa2-b7f9-6d0f936fdefd/tool-results/webfetch-1773525788884-c3gex1.xlsx';
const NATIONAL_FILE = 'C:/Users/neilw/.claude/projects/C--Users-neilw-Documents-GitHub-loamv2/e9288814-96ee-4aa2-b7f9-6d0f936fdefd/tool-results/webfetch-1773525788849-6szmva.xlsx';

// Min hectares to count as "typical" for a region
const MIN_HECTARES_REGION = 50;
// Min share of region's total to count as "typical"
const MIN_SHARE_REGION = 0.03; // 3%
// Max grapes per region (don't want 50 grapes for a region)
const MAX_GRAPES_PER_REGION = 15;
// Min hectares for country level
const MIN_HECTARES_COUNTRY = 500;
const MIN_SHARE_COUNTRY = 0.02; // 2%
const MAX_GRAPES_PER_COUNTRY = 20;

// Read regional data
const wb = XLSX.readFile(REGIONAL_FILE);
const countrySheets = wb.SheetNames.filter(s => s !== 'Title page' && s !== 'All countries');

const regionData = {}; // { country: { region: { grape: hectares } } }
const countryData = {}; // { country: { grape: hectares } }

for (const sheet of countrySheets) {
  const ws = wb.Sheets[sheet];
  const rows = XLSX.utils.sheet_to_json(ws);

  const country = sheet;
  if (!countryData[country]) countryData[country] = {};
  if (!regionData[country]) regionData[country] = {};

  for (const row of rows) {
    const region = row.region || 'Unknown';
    const grape = row.prime;
    const area = parseFloat(row.area) || 0;

    if (!grape || area <= 0) continue;

    // Aggregate by region
    if (!regionData[country][region]) regionData[country][region] = {};
    regionData[country][region][grape] = (regionData[country][region][grape] || 0) + area;

    // Aggregate by country
    countryData[country][grape] = (countryData[country][grape] || 0) + area;
  }
}

// Also read national data for countries not in regional file
const nbw = XLSX.readFile(NATIONAL_FILE);
console.log('National file sheets:', nbw.SheetNames.slice(0, 5));
// Check structure of first data sheet
const firstNatSheet = nbw.Sheets[nbw.SheetNames[1]];
const natRows = XLSX.utils.sheet_to_json(firstNatSheet, {header: 1});
console.log('National first rows:');
natRows.slice(0, 5).forEach((r, i) => console.log(i, JSON.stringify(r?.slice(0, 10))));

// Process regional data into output
const output = {
  source: 'Anderson, Nelgen & Puga (2023). Database of Regional, National and Global Winegrape Bearing Areas by Variety, 2000 to 2023. University of Adelaide Wine Economics Research Centre.',
  url: 'https://economics.adelaide.edu.au/wine-economics/databases',
  countries: {},
  regions: {}
};

// Country-level: top grapes by hectares
for (const [country, grapes] of Object.entries(countryData)) {
  const sorted = Object.entries(grapes).sort((a, b) => b[1] - a[1]);
  const totalHa = sorted.reduce((sum, [, ha]) => sum + ha, 0);

  const topGrapes = sorted
    .filter(([, ha]) => ha >= MIN_HECTARES_COUNTRY && (ha / totalHa) >= MIN_SHARE_COUNTRY)
    .slice(0, MAX_GRAPES_PER_COUNTRY)
    .map(([grape, ha]) => ({
      grape,
      hectares: Math.round(ha),
      share: Math.round((ha / totalHa) * 1000) / 10 + '%'
    }));

  output.countries[country] = {
    total_hectares: Math.round(totalHa),
    grapes: topGrapes
  };
}

// Region-level: top grapes by hectares
for (const [country, regions] of Object.entries(regionData)) {
  for (const [region, grapes] of Object.entries(regions)) {
    const sorted = Object.entries(grapes).sort((a, b) => b[1] - a[1]);
    const totalHa = sorted.reduce((sum, [, ha]) => sum + ha, 0);

    const topGrapes = sorted
      .filter(([, ha]) => ha >= MIN_HECTARES_REGION && (ha / totalHa) >= MIN_SHARE_REGION)
      .slice(0, MAX_GRAPES_PER_REGION)
      .map(([grape, ha]) => ({
        grape,
        hectares: Math.round(ha),
        share: Math.round((ha / totalHa) * 1000) / 10 + '%'
      }));

    if (topGrapes.length > 0) {
      const key = `${country}|${region}`;
      output.regions[key] = {
        country,
        region,
        total_hectares: Math.round(totalHa),
        grapes: topGrapes
      };
    }
  }
}

// Write output
writeFileSync('data/anderson_aryal_grapes.json', JSON.stringify(output, null, 2));

console.log('\n=== SUMMARY ===');
console.log(`Countries: ${Object.keys(output.countries).length}`);
console.log(`Regions: ${Object.keys(output.regions).length}`);

// Print country summaries
console.log('\n=== COUNTRY GRAPES (top 5 each) ===');
for (const [country, data] of Object.entries(output.countries).sort((a, b) => b[1].total_hectares - a[1].total_hectares)) {
  const topNames = data.grapes.slice(0, 5).map(g => `${g.grape} (${g.share})`).join(', ');
  console.log(`${country} [${data.total_hectares} ha]: ${topNames}`);
}

console.log('\n=== REGIONS WITH DATA ===');
for (const [key, data] of Object.entries(output.regions).sort((a, b) => a[0].localeCompare(b[0]))) {
  const topNames = data.grapes.slice(0, 3).map(g => g.grape).join(', ');
  console.log(`  ${data.country} > ${data.region}: ${data.grapes.length} grapes (${topNames}...)`);
}
