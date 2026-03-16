import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';

const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
const vars = {};
for (const line of envContent.split('\n')) {
  const trimmed = line.replace(/\r/g, '').trim();
  if (!trimmed || trimmed.startsWith('#')) continue;
  const eqIdx = trimmed.indexOf('=');
  if (eqIdx === -1) continue;
  vars[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
}

const sb = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);

const tables = [
  'producers', 'wines', 'wine_vintages', 'wine_vintage_scores',
  'wine_grapes', 'winemakers', 'entity_classifications',
  'producer_farming_certifications', 'wine_label_designations',
  'wine_vintage_prices', 'wine_vintage_grapes', 'producer_winemakers',
  'wine_aliases', 'producer_importers',
];

const results = {};
for (const t of tables) {
  const { count, error } = await sb.from(t).select('*', { count: 'exact', head: true });
  results[t] = error ? `ERROR: ${error.message}` : count;
}

console.log('\n📊 Current DB State:');
for (const [t, c] of Object.entries(results)) {
  console.log(`  ${t}: ${c}`);
}
