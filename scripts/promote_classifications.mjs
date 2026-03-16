#!/usr/bin/env node
/**
 * Promote classification data from wine metadata to entity_classifications table.
 * Handles:
 *   - metadata.classification (77 wines — mostly Jadot/Antinori)
 *   - metadata.vdp_level (28 wines — Dönnhoff)
 *
 * Usage: node scripts/promote_classifications.mjs [--dry-run]
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
const DRY_RUN = process.argv.includes('--dry-run');

async function fetchAll(table, columns = '*') {
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

function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

async function main() {
  console.log(`Promoting classifications from metadata...${DRY_RUN ? ' (DRY RUN)' : ''}\n`);

  // Load classification levels
  const clLevels = await fetchAll('classification_levels', 'id,classification_id,level_name,level_rank');
  const clSystems = await fetchAll('classifications', 'id,name,country_id');
  const systemMap = new Map(clSystems.map(s => [s.id, s]));

  // Build lookup map: various key formats → level
  const levelLookup = new Map();
  for (const cl of clLevels) {
    const sys = systemMap.get(cl.classification_id);
    if (!sys) continue;
    const entry = { levelId: cl.id, systemName: sys.name, levelName: cl.level_name };
    levelLookup.set(`${sys.name.toLowerCase()}|${cl.level_name.toLowerCase()}`, entry);
  }

  // VDP level mapping: metadata value → [system, level]
  const VDP_MAP = {
    'gutswein': ['VDP Classification', 'Gutswein'],
    'ortswein': ['VDP Classification', 'Ortswein'],
    'erste lage': ['VDP Classification', 'Erste Lage'],
    'grosse lage': ['VDP Classification', 'Grosse Lage'],
    'grosses gewächs': ['VDP Classification', 'Grosse Lage'],
    'grosses gewachs': ['VDP Classification', 'Grosse Lage'],
    'gg': ['VDP Classification', 'Grosse Lage'],
    'vdp.gutswein': ['VDP Classification', 'Gutswein'],
    'vdp.ortswein': ['VDP Classification', 'Ortswein'],
    'vdp.erste lage': ['VDP Classification', 'Erste Lage'],
    'vdp.grosse lage': ['VDP Classification', 'Grosse Lage'],
  };

  // Burgundy classification mapping
  const BURG_MAP = {
    'grand cru': ['Burgundy Vineyard Classification', 'Grand Cru'],
    'premier cru': ['Burgundy Vineyard Classification', 'Premier Cru'],
    '1er cru': ['Burgundy Vineyard Classification', 'Premier Cru'],
  };

  // Load wines with metadata
  const wines = await fetchAll('wines', 'id,name,metadata');

  // Check which wines already have classifications
  const existingClassifs = await fetchAll('entity_classifications', 'entity_id,entity_type,classification_level_id');
  const classified = new Set(
    existingClassifs.filter(ec => ec.entity_type === 'wine').map(ec => ec.entity_id)
  );

  let promoted = 0, skipped = 0, notFound = 0;

  for (const w of wines) {
    if (!w.metadata) continue;
    if (classified.has(w.id)) {
      if (w.metadata.vdp_level || w.metadata.classification) skipped++;
      continue;
    }

    // VDP levels
    if (w.metadata.vdp_level) {
      const vdpKey = w.metadata.vdp_level.toLowerCase().trim();
      const mapping = VDP_MAP[vdpKey];
      if (mapping) {
        const key = `${mapping[0].toLowerCase()}|${mapping[1].toLowerCase()}`;
        const level = levelLookup.get(key);
        if (level) {
          if (DRY_RUN) {
            console.log(`  [DRY] ${w.name} → ${level.systemName} / ${level.levelName}`);
          } else {
            const { error } = await sb.from('entity_classifications').insert({
              classification_level_id: level.levelId,
              entity_type: 'wine',
              entity_id: w.id,
            });
            if (error && !error.message.includes('duplicate')) {
              console.log(`  ⚠ ${w.name}: ${error.message}`);
            }
          }
          promoted++;

          // Clean metadata
          if (!DRY_RUN) {
            const newMeta = { ...w.metadata };
            delete newMeta.vdp_level;
            await sb.from('wines').update({
              metadata: Object.keys(newMeta).length > 0 ? newMeta : null,
            }).eq('id', w.id);
          }
          continue;
        }
      }
      console.log(`  ⚠ VDP level not mapped: "${w.metadata.vdp_level}" for ${w.name}`);
      notFound++;
    }

    // Burgundy classifications
    if (w.metadata.classification) {
      const classif = w.metadata.classification.toLowerCase().trim();
      // Check Burgundy first
      const burgMapping = BURG_MAP[classif];
      if (burgMapping) {
        const key = `${burgMapping[0].toLowerCase()}|${burgMapping[1].toLowerCase()}`;
        const level = levelLookup.get(key);
        if (level) {
          if (DRY_RUN) {
            console.log(`  [DRY] ${w.name} → ${level.systemName} / ${level.levelName}`);
          } else {
            const { error } = await sb.from('entity_classifications').insert({
              classification_level_id: level.levelId,
              entity_type: 'wine',
              entity_id: w.id,
            });
            if (error && !error.message.includes('duplicate')) {
              console.log(`  ⚠ ${w.name}: ${error.message}`);
            }
          }
          promoted++;

          if (!DRY_RUN) {
            const newMeta = { ...w.metadata };
            delete newMeta.classification;
            await sb.from('wines').update({
              metadata: Object.keys(newMeta).length > 0 ? newMeta : null,
            }).eq('id', w.id);
          }
          continue;
        }
      }

      // Not a simple Burgundy classification — log it for manual review
      if (!['bolgheri superiore doc', 'chianti classico docg', 'brunello di montalcino docg',
            'igt toscana', 'docg', 'doc', 'premier cru supérieur (1855)'].includes(classif)) {
        console.log(`  ℹ Classification not auto-mapped: "${w.metadata.classification}" for ${w.name}`);
      }
      notFound++;
    }
  }

  console.log(`\nDone. Promoted: ${promoted}, Skipped (already classified): ${skipped}, Not mapped: ${notFound}`);
}

main().catch(console.error);
