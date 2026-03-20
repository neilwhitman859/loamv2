import { readFileSync } from 'fs';
const raw = readFileSync(process.argv[2], 'utf8');
const parsed = JSON.parse(raw);
const text = parsed[0].text;
const match = text.match(/\[(\{.*\})\]/s);
const inner = JSON.parse('[' + match[1] + ']');

const byCountry = {};
for (const r of inner) {
  const k = r.country;
  if (!byCountry[k]) byCountry[k] = [];
  byCountry[k].push(r);
}

for (const [country, rows] of Object.entries(byCountry).sort((a,b) => a[0].localeCompare(b[0]))) {
  const catchAll = rows.find(r => r.is_catch_all);
  const real = rows.filter(r => !r.is_catch_all);
  const totalApps = rows.reduce((s, r) => s + r.app_count, 0);

  console.log(`\n## ${country} (${real.length} regions, ${totalApps} appellations)`);
  if (catchAll && catchAll.app_count > 0) {
    console.log(`  WARNING: ${catchAll.app_count} appellations on catch-all`);
  }

  for (const r of real.sort((a, b) => (a.parent_region || '').localeCompare(b.parent_region || ''))) {
    const indent = r.parent_region ? '    ' : '  ';
    const parent = r.parent_region ? ` (under ${r.parent_region})` : '';
    console.log(`${indent}${r.region}${parent} — ${r.app_count} apps, ${r.child_count} children`);
  }
}
