#!/usr/bin/env node
/**
 * Seed label_designation_aliases with common abbreviations and alternate spellings.
 *
 * Sources:
 * - EU wine regulations (Commission Delegated Regulation 2019/33)
 * - WSET Level 3 terminology
 * - Common retailer/LWIN abbreviations
 *
 * Usage: node scripts/seed_label_designation_aliases.mjs [--dry-run]
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

function normalize(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').replace(/\s+/g, ' ').trim();
}

// Format: [alias, canonical_designation_name, alias_type, language_code]
const ALIASES = [
  // German Prädikats — abbreviations
  ['TBA', 'Trockenbeerenauslese', 'abbreviation', 'de'],
  ['BA', 'Beerenauslese', 'abbreviation', 'de'],
  ['Spatlese', 'Spätlese', 'alternate_spelling', 'de'],
  ['Spaetlese', 'Spätlese', 'alternate_spelling', 'de'],
  ['Kab', 'Kabinett', 'abbreviation', 'de'],
  ['Kab.', 'Kabinett', 'abbreviation', 'de'],

  // French late harvest
  ['VT', 'Vendanges Tardives', 'abbreviation', 'fr'],
  ['SGN', 'Sélection de Grains Nobles', 'abbreviation', 'fr'],
  ['Selection de Grains Nobles', 'Sélection de Grains Nobles', 'alternate_spelling', 'fr'],

  // Spanish aging tiers
  ['GR', 'Gran Reserva', 'abbreviation', 'es'],
  ['Crza', 'Crianza', 'abbreviation', 'es'],
  ['Res', 'Reserva', 'abbreviation', 'es'],
  ['Res.', 'Reserva', 'abbreviation', 'es'],
  ['Grande Reserva', 'Gran Reserva', 'alternate_spelling', null],
  ['Grande Réserve', 'Gran Reserva', 'translation', 'fr'],

  // Italian aging tiers
  ['Ris', 'Riserva', 'abbreviation', 'it'],
  ['Ris.', 'Riserva', 'abbreviation', 'it'],
  ['Sup', 'Superiore', 'abbreviation', 'it'],
  ['Sup.', 'Superiore', 'abbreviation', 'it'],

  // Sparkling sweetness — synonyms and translations
  ['Brut Zero', 'Brut Nature', 'synonym', null],
  ['Dosage Zero', 'Brut Nature', 'synonym', null],
  ['Dosage Zéro', 'Brut Nature', 'synonym', 'fr'],
  ['Non Dosé', 'Brut Nature', 'synonym', 'fr'],
  ['Non Dose', 'Brut Nature', 'alternate_spelling', null],
  ['Pas Dosé', 'Brut Nature', 'synonym', 'fr'],
  ['Pas Dose', 'Brut Nature', 'alternate_spelling', null],
  ['Zero Dosage', 'Brut Nature', 'synonym', null],
  ['BN', 'Brut Nature', 'abbreviation', null],
  ['EB', 'Extra Brut', 'abbreviation', null],
  ['Extra Sec', 'Extra Dry', 'translation', 'fr'],
  ['Extra Seco', 'Extra Dry', 'translation', 'es'],
  ['Extra Trocken', 'Extra Dry', 'translation', 'de'],
  ['Sec', 'Dry', 'translation', 'fr'],
  ['Seco', 'Dry', 'translation', 'es'],
  ['Trocken', 'Dry', 'translation', 'de'],
  ['Demi-Sec', 'Demi-Sec', 'alternate_name', 'fr'],
  ['Halbtrocken', 'Demi-Sec', 'translation', 'de'],
  ['Semi-Seco', 'Demi-Sec', 'translation', 'es'],
  ['Dolce', 'Doux', 'translation', 'it'],
  ['Dulce', 'Doux', 'translation', 'es'],

  // Production methods
  ['Méthode Traditionnelle', 'Traditional Method', 'translation', 'fr'],
  ['Methode Traditionnelle', 'Traditional Method', 'alternate_spelling', 'fr'],
  ['Metodo Classico', 'Traditional Method', 'translation', 'it'],
  ['Méthode Champenoise', 'Traditional Method', 'synonym', 'fr'],
  ['Methode Champenoise', 'Traditional Method', 'alternate_spelling', 'fr'],
  ['Metodo Charmat', 'Charmat Method', 'translation', 'it'],
  ['Méthode Charmat', 'Charmat Method', 'translation', 'fr'],
  ['Tank Method', 'Charmat Method', 'synonym', 'en'],
  ['Método Ancestral', 'Pétillant Naturel', 'translation', 'es'],
  ['Metodo Ancestrale', 'Pétillant Naturel', 'translation', 'it'],
  ['Méthode Ancestrale', 'Pétillant Naturel', 'translation', 'fr'],
  ['Pet-Nat', 'Pétillant Naturel', 'abbreviation', null],
  ['Pet Nat', 'Pétillant Naturel', 'abbreviation', null],
  ['Petillant Naturel', 'Pétillant Naturel', 'alternate_spelling', null],

  // Estate bottling
  ['Mis en Bouteille au Château', 'Château Bottled', 'translation', 'fr'],
  ['Mis en Bouteille au Domaine', 'Estate Bottled', 'translation', 'fr'],
  ['Erzeugerabfüllung', 'Estate Bottled', 'translation', 'de'],
  ['Gutsabfüllung', 'Estate Bottled', 'translation', 'de'],
  ['Imbottigliato all\'Origine', 'Estate Bottled', 'translation', 'it'],

  // Vineyard designations
  ['Vieilles Vignes', 'Old Vines', 'translation', 'fr'],
  ['VV', 'Old Vines', 'abbreviation', null],
  ['Viñas Viejas', 'Old Vines', 'translation', 'es'],
  ['Vecchie Viti', 'Old Vines', 'translation', 'it'],
  ['Alte Reben', 'Old Vines', 'translation', 'de'],

  // Ice wine
  ['Icewine', 'Ice Wine', 'alternate_spelling', null],
  ['Vin de Glace', 'Ice Wine', 'translation', 'fr'],

  // Port-specific (if we have these as designations)
  ['Colheita', 'Colheita', 'alternate_name', 'pt'],
  ['LBV', 'Late Bottled Vintage', 'abbreviation', null],
  ['L.B.V.', 'Late Bottled Vintage', 'abbreviation', null],
];

async function main() {
  // Load label designations
  const { data: desigs, error } = await sb.from('label_designations').select('id,name');
  if (error) throw error;

  const byName = new Map();
  const byNorm = new Map();
  for (const d of desigs) {
    byName.set(d.name.toLowerCase(), d);
    byNorm.set(normalize(d.name), d);
  }

  let inserted = 0, skipped = 0, notFound = 0;

  for (const [alias, canonical, aliasType, lang] of ALIASES) {
    const desig = byName.get(canonical.toLowerCase()) || byNorm.get(normalize(canonical));
    if (!desig) {
      console.log(`  ⚠ Designation not found: "${canonical}" (alias: "${alias}")`);
      notFound++;
      continue;
    }

    const aliasNorm = normalize(alias);
    if (aliasNorm === normalize(desig.name)) {
      skipped++;
      continue;
    }

    if (DRY_RUN) {
      console.log(`  [DRY RUN] "${alias}" → ${desig.name} (${aliasType})`);
      inserted++;
      continue;
    }

    const { error: insertError } = await sb.from('label_designation_aliases').upsert({
      label_designation_id: desig.id,
      alias: alias,
      alias_normalized: aliasNorm,
      alias_type: aliasType,
      language_code: lang,
      source: 'eu-reg-2019-33-wset-l3',
    }, { onConflict: 'alias_normalized' });

    if (insertError) {
      console.log(`  ⚠ Error for "${alias}": ${insertError.message}`);
    } else {
      inserted++;
    }
  }

  console.log(`\nDone. Inserted: ${inserted}, Skipped: ${skipped}, Not found: ${notFound}`);
}

main().catch(console.error);
