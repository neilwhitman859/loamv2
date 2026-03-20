#!/usr/bin/env node
/**
 * Fix HTML entities in vinification_notes and other text fields.
 * KL data contained &ldquo; &rdquo; &ocirc; etc.
 */

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

const ENTITIES = {
  '&ldquo;': '\u201C',
  '&rdquo;': '\u201D',
  '&lsquo;': '\u2018',
  '&rsquo;': '\u2019',
  '&amp;': '&',
  '&lt;': '<',
  '&gt;': '>',
  '&nbsp;': ' ',
  '&eacute;': 'é',
  '&egrave;': 'è',
  '&ocirc;': 'ô',
  '&agrave;': 'à',
  '&uuml;': 'ü',
  '&ouml;': 'ö',
  '&aacute;': 'á',
  '&iacute;': 'í',
  '&ntilde;': 'ñ',
  '&ccedil;': 'ç',
  '&mdash;': '—',
  '&ndash;': '–',
  '&hellip;': '…',
  '&deg;': '°',
  '<br>': ' ',
  '<br/>': ' ',
  '<br />': ' ',
};

function decodeEntities(text) {
  if (!text) return text;
  let result = text;
  for (const [entity, replacement] of Object.entries(ENTITIES)) {
    result = result.split(entity).join(replacement);
  }
  // Also handle numeric entities like &#8217;
  result = result.replace(/&#(\d+);/g, (_, code) => String.fromCharCode(parseInt(code, 10)));
  // Clean up double spaces
  result = result.replace(/\s{2,}/g, ' ').trim();
  return result;
}

async function fetchAll(table, columns) {
  const all = [];
  let offset = 0;
  while (true) {
    const { data, error } = await sb.from(table).select(columns).range(offset, offset + 999);
    if (error) throw error;
    all.push(...data);
    if (data.length < 1000) break;
    offset += 1000;
  }
  return all;
}

async function main() {
  console.log('Fixing HTML entities...\n');

  // Fix wines.vinification_notes
  const wines = await fetchAll('wines', 'id,name,vinification_notes');
  let wineFixed = 0;
  for (const w of wines) {
    if (!w.vinification_notes) continue;
    const fixed = decodeEntities(w.vinification_notes);
    if (fixed === w.vinification_notes) continue;

    const { error } = await sb.from('wines').update({ vinification_notes: fixed }).eq('id', w.id);
    if (error) {
      console.log(`  ⚠ ${w.name}: ${error.message}`);
    } else {
      wineFixed++;
    }
  }
  console.log(`Wines vinification_notes fixed: ${wineFixed}`);

  // Fix wines.name (some KL wine names have HTML entities)
  let nameFixed = 0;
  for (const w of wines) {
    const fixed = decodeEntities(w.name);
    if (fixed === w.name) continue;

    const { error } = await sb.from('wines').update({ name: fixed }).eq('id', w.id);
    if (error) {
      console.log(`  ⚠ Name fix ${w.name}: ${error.message}`);
    } else {
      console.log(`  Fixed name: "${w.name}" → "${fixed}"`);
      nameFixed++;
    }
  }
  console.log(`Wine names fixed: ${nameFixed}`);
}

main().catch(console.error);
