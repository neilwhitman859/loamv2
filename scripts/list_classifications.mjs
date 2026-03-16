import { createClient } from '@supabase/supabase-js';
import { readFileSync } from 'fs';
const envPath = new URL('../.env', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1');
const envContent = readFileSync(envPath, 'utf-8');
const vars = {};
for (const line of envContent.split('\n')) {
  const t = line.replace(/\r/g, '').trim();
  if (!t || t.startsWith('#')) continue;
  const i = t.indexOf('=');
  if (i > 0) vars[t.slice(0, i)] = t.slice(i + 1);
}
const sb = createClient(vars.SUPABASE_URL, vars.SUPABASE_SERVICE_ROLE || vars.SUPABASE_ANON_KEY);
const { data } = await sb.from('classification_levels').select('id,classification_id,level_name,level_rank').order('classification_id');
const { data: systems } = await sb.from('classifications').select('id,name');
const sysMap = new Map(systems.map(s => [s.id, s.name]));
for (const cl of data) {
  console.log(`${sysMap.get(cl.classification_id)} | ${cl.level_name} | rank ${cl.level_rank}`);
}
