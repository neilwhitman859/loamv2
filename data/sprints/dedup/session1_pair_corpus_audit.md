# Session 1 Pair Corpus Audit

**Date:** 2026-04-20  
**Deliverable:** keep/tune/drop report for the current producer blocking strategies  
**Not doing:** model bakeoff, evidence-packet design, execution planning, or parent/child policy  
**Stop when:** every current blocking strategy has a disposition, rationale, and recommended next action

## Method

- Read [`data/sprints/dedup/rebuild_roadmap.md`](/C:/Users/neilw/Documents/GitHub/loamv2/data/sprints/dedup/rebuild_roadmap.md) and [`data/sprints/dedup/session_playbook.md`](/C:/Users/neilw/Documents/GitHub/loamv2/data/sprints/dedup/session_playbook.md).
- Audited [`pipeline/identity/producer_blocking.py`](/C:/Users/neilw/Documents/GitHub/loamv2/pipeline/identity/producer_blocking.py) against live DB state.
- Used `producer_dedup_routing_stage3` only as a downstream noise proxy, not as authoritative truth.
- Cross-checked against known false-merge and missed-merge patterns from:
  - [`data/sprints/dedup/b6_5a_stage2_analysis.md`](/C:/Users/neilw/Documents/GitHub/loamv2/data/sprints/dedup/b6_5a_stage2_analysis.md)
  - [`data/sprints/dedup/step9_pattern_audit.md`](/C:/Users/neilw/Documents/GitHub/loamv2/data/sprints/dedup/step9_pattern_audit.md)

## Corpus Snapshot

- Live DB state: `producers=33,225`, `wines=224,316`, `blocking_pairs=151,150`, `producer_dedup_pairs=600,103`
- Current blocking corpus is mostly weak single-signal pairs: `139,281 / 151,150 = 92.1%`
- Multi-signal overlap is sparse:
  - `s2+s9 = 8,874`
  - `s7+s11 = 1,645`
  - `s10+s8 = 625`

## Keep / Tune / Drop

### S1 `exact_normalized` — DROP

- Current behavior: `0` pairs, `0` unique.
- Catches well: nothing current corpus is missing.
- Floods in: nothing.
- Misses: no distinct miss class; same-country exact-name dupes should already collapse before blocking.
- Recommended v1 action: keep as a smoke-test query only, not as a candidate generator.

### S2 `trigram` — TUNE

- Current behavior: `114,494` pairs, `105,016` unique, only `2.31%` auto-merge proxy.
- Catches well: spelling, punctuation, accent, and short/full-form aliases.
  - Examples: `Pico della Mirandola` / `Pico Mirandola`, `Muga` / `Bodegas Muga`, `Churchill's (Berry Bros. & Rudd)` / `Churchill's`
- Floods in:
  - shared-surname splits
  - Hospices permutations
  - word-order swaps
  - same-first-word siblings
- Misses:
  - low-lexical-sim aliases and abbreviations like `DRC` / `de la Romanee-Conti`
  - cross-country same-brand splits
- Recommended v1 action:
  - split into a high-confidence lexical-alias band and a lower-confidence exploratory band
  - raise the floor for raw trigram-only pairs
  - strip merchant/importer/parenthetical wrappers before comparison
  - suppress known junk classes before enqueueing: Hospices, collaboration labels, shared-surname family splits

### S5 `shared_wine_lwin` — DROP

- Current behavior: `1` pair, `0` unique.
- Catches well: theoretical exact same-wine identity collisions.
- Floods in: none, but it is functionally dead.
- Misses: almost everything, because it almost never fires.
- Recommended v1 action: fold wine-level shared-LWIN into the evidence packet or into cross-country logic, not as its own generator.

### S6 `shared_ttb_permit` — DROP as standalone, KEEP as evidence

- Current behavior: `7,595` pairs, `7,475` unique, `94.6%` auto-skip proxy.
- Catches well: some real DBA/estate alias cases.
  - Examples: `Conn Valley` / `Anderson's Conn Valley Vineyards`, `Bedrock` / `Bedrock Wine Co.`
- Floods in:
  - custom-crush and shared-facility collisions
  - unrelated brands sharing a BW permit
  - a lot of parent/child-shaped noise that does not help merge-only execution
  - Examples: `Valdez` / `Hobbs`, `Gamba` / `Olema`, `Maison Champy` / `Midnight Cellars`
- Misses:
  - non-US producers
  - anything without a usable TTB footprint
- Recommended v1 action:
  - remove it as a primary merge-only candidate generator
  - keep permit overlap as supporting evidence or only allow it to generate when paired with lexical overlap

### S7 `cross_country_strong` — TUNE

- Current behavior: `8,227` pairs, `6,527` unique.
- Catches well: global-brand country splits and some exact-name cross-country duplicates.
  - Examples: `Cupcake Vineyards`, `Tussock Jumper`, `Layer Cake`, `90+ Cellars`
- Floods in:
  - unrelated same-name cross-country producers
  - collaboration/JV labels
  - ownership/brand-family patterns that are not merge-ready
  - Examples: `M. Chapoutier` / `M. Chapoutier & Laughton`, `William Cole` cross-country
- Misses:
  - cross-country same-brand rows whose names diverge materially and do not share LWIN
- Recommended v1 action:
  - split exact cross-country same-name/shared-LWIN from raw cross-country trigram
  - keep the exact-name/shared-LWIN branch
  - demote plain cross-country trigram to a secondary branch or evidence-only unless a second signal fires

### S8 `catalog_overlap` — TUNE HARD

- Current behavior: `2,018` pairs, `1,154` unique, `91.9%` auto-skip proxy.
- Catches well: a few weak-name, strong-catalog coherence cases.
- Floods in:
  - generic wine names that survive the current frequency filter
  - appellation-like and style-like names
  - Examples: `old vine grenache`, `brut reserva`, `cannonau di sardegna`, shared Burgundy lieu-dit names
- Misses:
  - thin producers with only one strong shared wine
  - real duplicates with sparse or disjoint canonical wine names
- Recommended v1 action:
  - keep only if benchmark results justify it
  - otherwise move it behind stricter gates:
    - lower `max_name_frequency`
    - exclude appellation/classification-driven names
    - require higher overlap or a weighted rarity score

### S9 `substring_contain` — KEEP, but TUNE

- Current behavior: `11,786` pairs, only `2,623` unique, highest merge yield among real strategies.
- Catches well: short/full-form aliases and merchant/parenthetical variants.
  - Examples: `Obsidian Ridge` / `Obsidian`, `Clavelier` / `Clavelier et Fils`, `Churchill's` / `Churchill's (Berry Bros. & Rudd)`
- Floods in:
  - collaboration labels
  - shared-surname splits
  - longer names that merely contain the shorter brand token
  - Examples: `Faustino Rivero Ulecia` / `Faustino`, `Savart & Chartogne` / `Savart`
- Misses:
  - aliases with no literal containment
- Recommended v1 action:
  - keep as a primary alias generator
  - pre-strip merchant/importer wrappers
  - explicitly suppress collaboration-label, Hospices, and shared-surname-family patterns before enqueueing

### S10 `shared_rare_wine` — KEEP, but TUNE

- Current behavior: `15,693` pairs, `14,542` unique, `96.5%` auto-skip proxy.
- Catches well: the low-lexical-sim duplicates the name-based strategies miss.
  - Key example: `de la Romanee-Conti` / `DRC`
  - Also catches portfolio alias cases like `Catena` / `Catena Zapata`
- Floods in:
  - unrelated producers that share one rare site or one named cuvee
  - Examples: `Clos Rougeard` / `des Sables Verts` via `les poyeux`, one-off rare wine collisions across Burgundy and Germany
- Misses:
  - duplicates with no shared canonical wine names
  - thin rows with sparse catalogs
- Recommended v1 action:
  - keep it as a specialty anchor strategy
  - require `2` shared rare wines, or `1` ultra-rare wine plus lexical/brand support
  - do not keep the current “any one rare shared wine” rule

### S11 `cross_word_subset` — DROP as standalone, KEEP as evidence

- Current behavior: `3,619` pairs, `1,944` unique, `96.8%` auto-skip proxy.
- Catches well: a few parenthetical or extended-name cross-country variants.
- Floods in:
  - collaboration labels
  - ownership/family structures
  - a lot of parent/child-shaped cases that are explicitly out of scope for merge-only
  - Examples: `Ste. Michelle & Antinori`, `Thienot X Penfolds`, `Domaine Carneros by Taittinger`
- Misses:
  - cross-country same-brand cases without literal token containment
- Recommended v1 action:
  - remove as a merge-only generator
  - preserve the token relationship as evidence for later metadata/parent-child work

## Recommended Candidate Generation v1

### Generator families to keep

1. **Same-country lexical alias**
   - merge the useful parts of `S2` + `S9`
   - optimized for spelling, suffix, punctuation, and short/full-form variants
2. **Cross-country same-brand**
   - keep the strong branch of `S7`: exact normalized name and shared-LWIN support
3. **Rare-wine anchor**
   - keep a retuned `S10` for low-lexical-sim but high-value duplicate clusters
4. **Catalog coherence (probationary)**
   - keep only if the frozen benchmark proves it adds recall that the other three families miss

### Signals to demote to evidence-only

- `S5 shared_wine_lwin`
- `S6 shared_ttb_permit`
- `S11 cross_word_subset`

### Pre-filters v1 should add before pair creation

- Merchant/importer/parenthetical stripping
- Collaboration-label suppression (`&`, `x`, `with`, JV forms)
- Hospices de Beaune / Hospices de Nuits special handling
- Shared-surname family-split suppressor
- Generic wine-name blacklist using appellation/classification/reference dictionaries

## Bottom Line

- **Keep:** `S9`, `S10`
- **Tune:** `S2`, `S7`, `S8`
- **Drop as standalone:** `S1`, `S5`, `S6`, `S11`

The main structural issue is not just thresholds. The current corpus is too dependent on raw single-signal generation, especially `S2`, and too willing to treat facility/ownership/collaboration clues as pair-generation signals instead of supporting evidence.

## Single Next Artifact

Build **`data/sprints/dedup/benchmark_v1.json`**.

That file should freeze the evaluation target for the rebuild:
- the 100-pair blind core audit
- named false-merge pattern cases
- named missed-merge pattern cases
- a clean random tail sample

We should not rewrite the candidate generator before that benchmark exists.
