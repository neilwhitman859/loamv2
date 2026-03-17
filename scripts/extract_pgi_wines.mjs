import { readFileSync, writeFileSync } from 'fs';

const raw = readFileSync('C:/Users/neilw/.claude/projects/C--Users-neilw-Documents-GitHub-loamv2/fe51cf62-7e9b-4cc6-b236-7f3c2be9d97e/tool-results/bh721bw84.txt', 'utf8');
const data = JSON.parse(raw);

const pgiWines = data.filter(d => d.giType === 'PGI' && d.productType === 'WINE' && d.status === 'registered');

const extracted = pgiWines.map(d => ({
  name: d.protectedNames[0],
  file_number: d.fileNumber,
  country: d.countries[0],
  eu_protection_date: d.euProtectionDate,
  gi_identifier: d.giIdentifier,
  transcriptions: d.transcriptions || null,
})).sort((a, b) => {
  if (a.country !== b.country) return a.country.localeCompare(b.country);
  return a.name.localeCompare(b.name);
});

writeFileSync('data/eambrosia_pgi_wines.json', JSON.stringify(extracted, null, 2));
console.log('Saved', extracted.length, 'PGI wines to data/eambrosia_pgi_wines.json');

// Show by country
for (const country of ['IT', 'FR', 'ES', 'PT', 'DE', 'GR', 'AT', 'RO', 'HU', 'SI', 'BG', 'CZ', 'CY', 'GB', 'NL', 'DK', 'BE', 'MT', 'SK', 'CN', 'US']) {
  const entries = extracted.filter(e => e.country === country);
  if (entries.length === 0) continue;
  console.log(`\n--- ${country} (${entries.length}) ---`);
  entries.forEach(e => console.log(`  ${e.name}`));
}
