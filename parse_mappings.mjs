import { readFileSync } from 'fs';
const raw = readFileSync('C:\\Users\\neilw\\.claude\\projects\\C--Users-neilw-Documents-GitHub-loamv2\\2fc0b9c8-321b-4022-baf5-1ae0f9958732\\tool-results\\toolu_01JzqNDFX3w6LmAtEh1bJtVw.json', 'utf-8');
const parsed = JSON.parse(raw);
const text = parsed[0].text;
const match = text.match(/\[\{.*\}\]/s);
if (match) {
  const data = JSON.parse(match[0]);
  console.log('Total mappings with NULL appellation:', data.length);
  console.log('\n--- All entries by candidate_count ---');
  for (let i = 0; i < data.length; i++) {
    const d = data[i];
    const idx = String(i + 1).padStart(3);
    const rn = (d.region_name || '').padEnd(45);
    const co = (d.country || '').padEnd(18);
    const cnt = String(d.candidate_count).padStart(5);
    const res = d.resolved_region || 'NULL';
    const ca = d.is_catch_all ? 'catch-all' : 'real';
    const mt = d.match_type || '';
    console.log(`${idx}. ${rn} | ${co} | ${cnt} wines | → ${res} (${ca}) | ${mt}`);
  }
}
