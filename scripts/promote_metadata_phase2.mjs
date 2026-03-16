#!/usr/bin/env node
/**
 * Phase 2 metadata promotion — requires new columns from pending_migrations.sql Migration 5.
 * Run AFTER Migration 5 has been applied.
 *
 * Promotes:
 *   - wine.soil → wines.soil_description (~1,489 entries)
 *   - wine.vine_age → wines.vine_age_description (~1,469 entries)
 *   - wine.vineyard_area → wines.vineyard_area_ha (~1,468 entries)
 *   - wine.commune → wines.commune (~53 entries)
 *   - wine.altitude_m → wines.altitude_m_low / altitude_m_high (~23 entries)
 *   - wine.aspect → wines.aspect (~23 entries)
 *   - wine.slope_pct → wines.slope_pct (~22 entries)
 *   - wine.monopole → wines.monopole (~8 entries)
 *   - producer.location → producers.address (~198 entries)
 *
 * Usage: node scripts/promote_metadata_phase2.mjs [--dry-run]
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

function parseAltitudeRange(val) {
  if (!val) return { low: null, high: null };
  const str = String(val).replace(/,/g, '');
  // "330-600" or "550" or "200-350m"
  const rangeMatch = str.match(/(\d+)\s*[-–]\s*(\d+)/);
  if (rangeMatch) return { low: parseInt(rangeMatch[1]), high: parseInt(rangeMatch[2]) };
  const singleMatch = str.match(/(\d+)/);
  if (singleMatch) { const n = parseInt(singleMatch[1]); return { low: n, high: n }; }
  return { low: null, high: null };
}

function parseVineyardArea(val) {
  if (!val) return null;
  const str = String(val).replace(/,/g, '').trim();
  // "5 ha" or "1.8 ha" or "5"
  const match = str.match(/([\d.]+)\s*(ha|hectares?)?/i);
  if (match) return parseFloat(match[1]);
  return null;
}

function parseSlopePct(val) {
  if (!val) return null;
  const str = String(val).replace(/%/g, '').trim();
  // "10-55" → take average? No, take the max for slope_pct
  const rangeMatch = str.match(/([\d.]+)\s*[-–]\s*([\d.]+)/);
  if (rangeMatch) return parseFloat(rangeMatch[2]); // use high end
  const num = parseFloat(str);
  return isNaN(num) ? null : num;
}

async function main() {
  console.log(`Phase 2 metadata promotion...${DRY_RUN ? ' (DRY RUN)' : ''}\n`);

  // Check if new columns exist
  const { error: testError } = await sb.from('wines')
    .select('soil_description').limit(1);
  if (testError) {
    console.error('ERROR: New columns not found. Run Migration 5 from pending_migrations.sql first.');
    console.error(`  ${testError.message}`);
    process.exit(1);
  }

  const wines = await fetchAll('wines', 'id,name,metadata,soil_description,vine_age_description,vineyard_area_ha,commune,altitude_m_low,altitude_m_high,aspect,slope_pct,monopole');
  const stats = { soil: 0, vine_age: 0, vineyard_area: 0, commune: 0, altitude: 0, aspect: 0, slope: 0, monopole: 0, address: 0 };

  for (const w of wines) {
    if (!w.metadata) continue;
    const updates = {};
    const metaDeletes = [];

    // Soil
    if (w.metadata.soil && !w.soil_description) {
      updates.soil_description = String(w.metadata.soil).replace(/<br\s*\/?>/g, ' ').trim();
      metaDeletes.push('soil');
      stats.soil++;
    }

    // Vine age
    if (w.metadata.vine_age && !w.vine_age_description) {
      updates.vine_age_description = String(w.metadata.vine_age).trim();
      metaDeletes.push('vine_age');
      stats.vine_age++;
    }

    // Vineyard area
    if (w.metadata.vineyard_area && w.vineyard_area_ha == null) {
      const ha = parseVineyardArea(w.metadata.vineyard_area);
      if (ha) {
        updates.vineyard_area_ha = ha;
        metaDeletes.push('vineyard_area');
        stats.vineyard_area++;
      }
    }

    // Commune
    if (w.metadata.commune && !w.commune) {
      updates.commune = w.metadata.commune;
      metaDeletes.push('commune');
      stats.commune++;
    }

    // Altitude
    if (w.metadata.altitude_m && w.altitude_m_low == null) {
      const { low, high } = parseAltitudeRange(w.metadata.altitude_m);
      if (low != null) {
        updates.altitude_m_low = low;
        updates.altitude_m_high = high;
        metaDeletes.push('altitude_m');
        stats.altitude++;
      }
    }

    // Aspect
    if (w.metadata.aspect && !w.aspect) {
      updates.aspect = w.metadata.aspect;
      metaDeletes.push('aspect');
      stats.aspect++;
    }

    // Slope
    if (w.metadata.slope_pct && w.slope_pct == null) {
      const slope = parseSlopePct(w.metadata.slope_pct);
      if (slope != null) {
        updates.slope_pct = slope;
        metaDeletes.push('slope_pct');
        stats.slope++;
      }
    }

    // Monopole
    if (w.metadata.monopole && !w.monopole) {
      updates.monopole = w.metadata.monopole === true || w.metadata.monopole === 'true';
      metaDeletes.push('monopole');
      stats.monopole++;
    }

    if (Object.keys(updates).length === 0) continue;

    // Clean metadata
    const newMeta = { ...w.metadata };
    for (const key of metaDeletes) delete newMeta[key];
    updates.metadata = Object.keys(newMeta).length > 0 ? newMeta : null;

    if (DRY_RUN) {
      if (Object.values(stats).reduce((a, b) => a + b, 0) <= 10) {
        console.log(`  [DRY] ${w.name}: ${Object.keys(updates).filter(k => k !== 'metadata').join(', ')}`);
      }
    } else {
      const { error } = await sb.from('wines').update(updates).eq('id', w.id);
      if (error) console.log(`  ⚠ ${w.name}: ${error.message}`);
    }
  }

  // Producer addresses
  const producers = await fetchAll('producers', 'id,name,metadata,address');
  for (const p of producers) {
    if (!p.metadata?.location || p.address) continue;
    const addr = String(p.metadata.location).trim();
    if (!addr) continue;

    if (DRY_RUN) {
      if (stats.address < 5) console.log(`  [DRY] ${p.name}: address → ${addr.slice(0, 60)}`);
    } else {
      const newMeta = { ...p.metadata };
      delete newMeta.location;
      const { error } = await sb.from('producers').update({
        address: addr,
        metadata: Object.keys(newMeta).length > 0 ? newMeta : null,
      }).eq('id', p.id);
      if (error) console.log(`  ⚠ ${p.name}: ${error.message}`);
    }
    stats.address++;
  }

  console.log('\n═══════════════════════════════════════');
  console.log('PHASE 2 SUMMARY');
  console.log('═══════════════════════════════════════');
  for (const [key, count] of Object.entries(stats)) {
    console.log(`  ${key}: ${count}`);
  }
}

main().catch(console.error);
