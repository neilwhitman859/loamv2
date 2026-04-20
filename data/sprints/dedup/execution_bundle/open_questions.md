# Open Questions — Flagged for Sprint 7+

Issues surfaced during B6.6 that we deliberately did not resolve in this
bundle. Each needs a decision or a schema change before it can be cleanly
addressed.

## 1. Multi-parent collaboration producers (the DVO case)

**Problem:** `producers.parent_producer_id` is single-valued. Joint-venture
brands with two equal principals (Opus One = Mondavi + Rothschild, DVO =
Dalla Valle + Ornellaia, Almaviva = Mouton + Concha y Toro) can only record
one parent.

**Short-term handling in this bundle:** yellow#3 Dalla Valle/DVO is set with
`parent_producer_id = Dalla Valle` and a metadata bread-crumb
`{"co_producer_ids": ["<Ornellaia UUID>"]}` in the child's `metadata` JSONB.
Loses the Ornellaia side semantically.

**Recommendation:** Sprint 7 schema migration. Add a junction table:
```sql
CREATE TABLE producer_collaborators (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    producer_id uuid NOT NULL REFERENCES producers(id),
    collaborator_producer_id uuid NOT NULL REFERENCES producers(id),
    relationship_type text NOT NULL CHECK (relationship_type IN (
        'jv_equal_partner', 'jv_dominant_partner', 'jv_subordinate_partner',
        'collaborator', 'importer', 'distributor'
    )),
    start_year integer,
    end_year integer,
    notes text,
    UNIQUE (producer_id, collaborator_producer_id, relationship_type)
);
```

Then migrate the DVO metadata to a proper row.

**Estimated JV prevalence:** ~20-50 true multi-entity JVs across the corpus
(Opus One, Almaviva, Sena, Caro, DVO, Cheval des Andes, etc.). Not urgent for
the current demo set.

## 2. Generic-château-name rows that need per-wine re-linking

**Problem:** B6.6 flipped several MERGEs to SKIP because the loser row was a
"dumpster" of wines from multiple distinct estates sharing a common name.
Example: the `Beausejour` row contains wines from ≥6 different French châteaux
named Beausejour (Fronsac, Puisseguin-SE ×2, Montagne-SE, Saint-Estèphe, etc.).
The row needs to be split: each wine re-linked to its correct estate.

**Affected rows (flagged `sprint7_flag` in ledger):**
- `beausejour_row_needs_per_wine_split` — pair 62908
- Likely others surfaced during B6.6 Mid/Tail re-Chrome (see `rechrome_flips.md`
  when the subagent completes — add them here)

**Recommendation:** Sprint 7 task. Build a per-wine re-linking tool that:
1. Lists wines under the dumpster row
2. For each wine, runs a producer-resolution pass (appellation + wine name)
3. Creates new canonical producer rows where they don't exist
4. Re-links the wine

## 3. Moreau family 5-row cleanup

**Problem:** The Moreau family of Chassagne-Montrachet has 5 related DB rows
(`Alex Moreau`, `Benoit Moreau`, `Bernard Moreau`, `Alex et Benoit Moreau`,
`Alex Moreau (Bernard Moreau)`). Chrome verified that:
- Bernard Moreau = the father's estate (Domaine Bernard Moreau et Fils)
- Alex and Benoît are sibling winemakers with distinct DB rows (§11.4.m split)
- `Alex et Benoit Moreau` = a single-wine cuvée of Bernard Moreau et Fils
- `Alex Moreau (Bernard Moreau)` = merchant-prefix row

**Recommendation:** Sprint 7. Merge `Alex Moreau (Bernard Moreau)` (1w) into
`Alex Moreau` (47w). Re-link the `Alex et Benoit Moreau` 1 wine to
`Bernard Moreau`, soft-delete the cuvée-row. Consider PC linkage of
`Alex Moreau` and `Benoit Moreau` under `Bernard Moreau` (sibling spin-offs).

## 4. Goldschmidt family 6-row cleanup

**Problem:** `Goldschmidt Vineyards` (canonical) has ~6 variant rows. Pair
29511 only validated Nick Goldschmidt ⇔ Chelsea Goldschmidt; the full cluster
needs a bigger pass.

**Recommendation:** Sprint 7. Single cleanup session to resolve all Goldschmidt
rows at once.

## 5. Taylor's / Taylor Fladgate cluster

**Problem:** The Fladgate Partnership (Port house) has 7+ related DB rows
(`Taylor's`, `Taylor Fladgate`, `Taylor`, `Taylor's (Berry Bros Rudd)`, etc.)
plus distinct unrelated rows (`Taylors` Australia, `Taylor` US Sherry/Madeira).

**Current bundle handling:** yellow#59 merges Taylor's + 4 additional rows into
a single "Taylor Fladgate" producer.

**Sprint 7 flags:** resolve the Sherry/Madeira `Taylor` (4f11b3d5), disambiguate
`Taylors` (Australia, 68w), handle other Taylor Port variants.

## 6. Reversibility per-table row ID recording

**Problem:** the execute script as spec'd records row *counts* per FK table in
`producer_merge_history.repointed_rows`. Reversal would be sharper with row
*IDs*, so you could un-move specific rows without a full-table scan.

**Recommendation:** extend the execute script to store, per FK table, the
list of row IDs that were re-pointed. JSONB is fine as long as counts remain
readable. Cost: minor increase in `producer_merge_history` row size; worth it.

## 7. Bordeaux second-wine rows (§11.4.e violations)

**Problem:** Several producer rows are actually second wines (e.g.,
`Pauillac De Pichon Lalande`, `Lys Lafaurie Peyraguey`, `Lafaurie Peyraguey
Exceptionnelle`, `Carruades de Lafite`, `Les Forts de Latour`, `Petit Mouton`,
`Pavillon Rouge de Château Margaux`). Per §11.4.e these should be wines of
the parent estate, not standalone producer rows.

**Current bundle handling:** PC links some of these under the parent. But
per §11.4.e the correct fix is to un-producer-ify them.

**Recommendation:** Sprint 7. Build a "second-wine re-link" tool that:
1. Identifies second-wine rows (curated list + §11.4.e heuristics)
2. Creates `wines` rows under the parent producer
3. Moves the vintages and wines from the pseudo-producer
4. Soft-deletes the pseudo-producer row

## 8. Hospices de Beaune auction bottlings (§11.4.q)

**Status:** §11.4.q codified in this sprint. PC verdicts applied in ledger
where Chrome identified specific négociant bottlers. But there are likely
more Hospices de Beaune rows in the corpus that weren't in the blocking
pairs — they'll need a separate sweep.

**Recommendation:** Sprint 7 or 8. Build a one-shot HdB query: find all rows
matching `name ILIKE '%hospices de beaune%'` and PC them to their bottler.

## 9. Shared-surname family splits at scale

**Problem:** §11.4.m (shared-surname split → default SKIP) was codified in
B6.6 based on ~120 pairs. The rest of the 600K blocking pool likely contains
many more cases (de Montille/Deux Montille, Brundlmayer siblings, Haselgrove
branches, etc.) where the original L1+L1.5+L2+L2.5 verdicts over-called MERGE.

**Recommendation:** Sprint 7. Apply §11.4.m as a post-hoc override to the
existing routing_stage3 SKIP queue: any pair where both sides share a surname
token AND are in different appellations should be auto-SKIP regardless of
prior verdict. This can be a SQL-only pass.

## 10. Monitoring post-execution

**Problem:** once ~150 producer merges land, we need to watch for symptoms:
- User-reported "why does this wine look wrong"
- Search results that now show 0 producers for a name that previously worked
- Wine pages that reference a soft-deleted producer somehow

**Recommendation:** add a dashboard widget showing `producer_merge_history`
volume + reversals per day + a queue of user-reported issues.

---

## How to add items to this list

If new issues surface during testing, add them here with:
1. A short title
2. A description of the problem
3. A proposed Sprint 7+ treatment
4. A rough cost/scope estimate

This list is the canonical record of what Sprint 6 deliberately deferred.
