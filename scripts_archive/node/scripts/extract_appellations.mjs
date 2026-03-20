import { readFileSync, writeFileSync } from 'fs';

const file = process.argv[2];
const outFile = process.argv[3] || 'appellations_export.txt';
const raw = readFileSync(file, 'utf8');

// Parse nested structure: [{type:"text", text: "{\"result\":\"...\\n[{...}]\\n...\"}"}]
const outer = JSON.parse(raw);
const inner = JSON.parse(outer[0].text);  // {result: "...\\n[...]\\n..."}
const resultStr = inner.result;

// Find the JSON array between the untrusted-data tags
const arrayMatch = resultStr.match(/\[[\s\S]*\]/);
if (!arrayMatch) {
  console.log('No JSON array found in result');
  process.exit(1);
}
const data = JSON.parse(arrayMatch[0]);

const byCountry = {};
for (const r of data) {
  if (!byCountry[r.iso_code]) byCountry[r.iso_code] = { country: r.country, appellations: [] };
  byCountry[r.iso_code].appellations.push({ name: r.appellation, type: r.designation_type });
}

const lines = [];
for (const [code, info] of Object.entries(byCountry).sort((a, b) => b[1].appellations.length - a[1].appellations.length)) {
  lines.push(`\n=== ${info.country} (${code}) — ${info.appellations.length} appellations ===`);
  info.appellations.forEach(a => lines.push(`  ${a.type}: ${a.name}`));
}

writeFileSync(outFile, lines.join('\n'), 'utf8');
console.log(`Written to ${outFile} (${Object.keys(byCountry).length} countries, ${data.length} appellations)`);

// Also output as JSON for easy consumption
const jsonOut = {};
for (const [code, info] of Object.entries(byCountry)) {
  jsonOut[code] = info.appellations.map(a => a.name);
}
writeFileSync(outFile.replace('.txt', '.json'), JSON.stringify(jsonOut, null, 2), 'utf8');
