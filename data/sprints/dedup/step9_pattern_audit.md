# Sprint 6 Step 9 — Pattern audit & §11 amendments

**Source data:** 740-pair user-review pile from `producer_dedup_routing_stage3` + 71 yellow-flag producers from `producer_sanity_scorecard.json`.

**Done by:** Opus inline pattern analysis, 2026-04-18.

---

## Observed pattern clusters

### Cluster 1 — Hospices de Beaune négociant variants (40 pairs)

**What:** HdB auction-bottled wines where the négociant bottling is parenthetically tagged. Appears as `"Hospices de Beaune (X)"` where X is the négociant / broker that bottled that year's lot, e.g., `Hospices de Beaune (Camille Giroud)`, `Hospices de Beaune (Drouhin)`, `Hospices de Beaune (Faiveley)`.

**Sub-patterns:**
- `HdB (X)` vs `HdB (Y)` where X ≠ Y → distinct négociant bottlings of same HdB cuvées → **SKIP**
- `HdB (X)` vs `X` (the négociant itself) → the HdB lot is a sub-catalog of X → **PARENT_CHILD** (X is parent, HdB row is child) OR **MERGE** (if we want HdB bottles under the négociant's catalog)

**Samples:**
- `Hospices de Beaune (Camille Giroud)` vs `Camille Giroud`
- `Hospices de Beaune (Pierre Morey)` vs `Hospices de Beaune (Morey Blanc)` — SKIP (distinct négociants)
- `Hospices de Beaune (Joseph Drouhin)` vs `Joseph Drouhin` — merge candidate
- `Hospice de Nuits (Romain Taupenot)` vs `Hospices de Nuits` — punctuation variant

**Decision:** SKIP for `HdB(X)` vs `HdB(Y)`. PARENT_CHILD for `HdB(X)` vs `X` (X as parent). Rule §11.4.q.

### Cluster 2 — Global commercial brands in multiple countries (87 cross-country pairs)

**What:** International wine brands that are sold in multiple countries with separate producer rows tagged to each country. These are SAME BRAND.

**Samples:**
- `Layer Cake` [AR/US, AU/IT] — Treasury Americas international brand
- `Tussock Jumper` [AU/US, CL/US, DE/US, ES/US, NZ/US, ZA/US] — global brand, 10+ country pairs
- `Wine Spots` [AU/US]
- `Sea Change` [DE/FR, ES/FR, etc.]
- `Baron Philippe de Rothschild` [CL/AR] — global brand
- `Osborne` [ES/PT]
- `Niepoort` [ES/PT] — Dutch-family estate in Port
- `Selaks` [AU/NZ]

**Decision:** MERGE when web evidence converges on single brand identity (one website, one KG entry). Rule §11.4.n update.

### Cluster 3 — "& Fils" / "et Fils" / "Père et Fils" suffix variants (~20 pairs)

**What:** Producer suffix variants where "& Fils" / "et Fils" / "Père et Fils" is added/omitted.

**Samples:**
- `Clavelier` vs `Clavelier et Fils`
- `Daniel Dampt & Fils` vs `Dampt`
- `Alex Moreau` vs `Alex et Benoit Moreau` — FAMILY VARIANT (next generation)
- `Fery-Meunier` vs `Jean Fery & Fils` — likely different estates
- `Protheau & Fils` vs `Jean-Francois Protheau` — generation split
- `Grivelet Pere & Fils` vs `Grivelet-Cusset` — distinct
- `Guinaudeau` vs `Guinaudeau S&J` — likely same, shop initial variation
- `Metrat et Fils` vs `Metrat B` — ambiguous

**Decision:** MERGE if web evidence converges on single domain; SKIP if site/estate difference confirmed. Most are MERGE at Haiku+Serper's 0.89 conf (borderline). Rule §11.4.r.

### Cluster 4 — Collaboration / JV labels ("A & B" or "X & Y") (~15 pairs)

**What:** Joint venture or guest-winemaker labels that pair a chef/winemaker with a producer.

**Samples:**
- `Chapoutier` vs `Anne Sophie Pic & Michel Chapoutier` — JV label (Pic-Chapoutier collab)
- `Giaconda` vs `M. Chapoutier & Giaconda` — AU-FR collab
- `Vinos Atlantico & Rodrigo Mendez` vs `Rodrigo Mendez`
- `Tournon M.Chapoutier` vs `M. Chapoutier & Giaconda`
- `Lombardi` vs `Tendil & Lombardi`
- `Marie et Vincent Tricot` vs `No Control (Vincent Marie)`

**Decision:** JV label is a **distinct producer** (§11.4.l precedent). Default PARENT_CHILD if ownership clear; otherwise SKIP. Rule §11.4.o (strengthened).

### Cluster 5 — Merchant bundled names (~5 pairs)

**What:** Wine merchant bundled into producer name via suffix parens, e.g., `Churchill's (Berry Bros. & Rudd)`.

**Samples:**
- `Churchill's (Berry Bros. & Rudd)` vs `Churchill's` — BBR is merchant; Churchill's is producer → MERGE to Churchill's
- `Corney & Barrow (Sichel)` vs `Berry Bros. & Rudd (Roche Bellene)` — merchant-tagged rows
- `Berry Bros. & Rudd (Rosa)` vs `Berry Bros. & Rudd (Roche Bellene)` — distinct producer rows behind same merchant

**Decision:** Strip merchant suffix, MERGE to the core producer name (§11.4.p reinforced, existing §11.4.i covers this pattern).

### Cluster 6 — Shared-surname family splits (~12 pairs)

**What:** Two producers share a surname but represent distinct family branches / estates, with no ownership relation.

**Samples:**
- `Brundlmayer` vs `Josef & Philip Brundlmayer` — probably family branch
- `Willi Brundlmayer` vs `Josef & Philip Brundlmayer`
- `Valentin Bianchi` vs `Elsa Bianchi` — family split
- `Gebruder Nittnaus` vs `Martin Nittnaus`
- `Jacques Germanier` vs `Jean Rene Germanier` — CH
- `Benedikt Baltes` vs `Bertram-Baltes`

**Decision:** Default SKIP when geography or estate confirms distinction. MERGE if web evidence shows continuity. Rule §11.4.m.

### Cluster 7 — Producer + sub-brand / sub-vineyard rows (~15 pairs)

**What:** Main producer row + separate row for a named sub-brand, single-vineyard bottling, or second label.

**Samples:**
- `Obsidian Ridge` vs `Obsidian` — Obsidian Wine Co. (MERGE)
- `Knappstein` vs `Knappstein Lenswood Vineyards` — lineup label (MERGE or PC)
- `Carlei` vs `Carlei Green Vineyards` vs `Carlei Estate` — same producer, line label → MERGE
- `CVNE (Contino)` vs `Contino` — Contino is CVNE-owned sub-brand → PARENT_CHILD
- `Andrew Januik` vs `Januik` — winemaker + family winery → MERGE
- `Decelle Villa` vs `Decelle & Fils` — sub-brand/line (MERGE or PC)

**Decision:** MERGE when it's a product line name. PARENT_CHILD when the sub-brand has its own commercial identity. Rule §11.4.s.

### Cluster 8 — Accent / punctuation / compound-name variants (~10 pairs)

**What:** Same name, different rendering (accents, hyphens, compound words).

**Samples:**
- `Pico della Mirandola` vs `Pico Mirandola` — missing "della" particle
- `Hospice de Nuits` vs `Hospices de Nuits` — "Hospice" vs "Hospices"
- `Vermillon` vs `Vermillion` — spelling variant
- `Compania Vinos Atlantico` vs `Compania de Vinos del Atlantic` — preposition variants

**Decision:** MERGE. Covered by existing §11.4.h (accent variants).

### Cluster 9 — Low wine count yellow flags (36 producers, scorecard)

**What:** Famous producers showing 1-3 wines in DB — the producer row we matched might not be the primary canonical row, OR LWIN wine recovery (B6.2.2) didn't capture all variants.

**Samples:**
- `Château Pavie` — 1 wine (should be 20+)
- `Château Ausone` — 2 wines
- `Trotanoy` — 2 wines
- `Marques de Murrieta` — 1 wine
- `Pingus` — 2 wines
- `La Conseillante` — 2 wines
- `Almaviva` — 2 wines

**Interpretation:** These are likely DB state issues, not dedup decision problems. The routing is probably correct; the producer row just doesn't have all wines attached yet. Safety Net B + a future wine-backfill sprint would address.

**Decision:** Not actionable in Sprint 6. Flag for Sprint 7 wine dedup. No §11 rule change needed.

### Cluster 10 — Same-first-word sibling still in DB (30 producers)

**What:** Two producers share first normalized word, are in same country, but routing decided SKIP.

**Samples:**
- `Château Haut-Brion` vs `La Mission Haut-Brion` — DISTINCT (separate estate) → SKIP is correct
- `de Chevalier` has 5 same-first-word siblings — these are likely "Château de Chevalier" variants OR distinct "de Chevalier" estates
- `Pierre Gaillard` vs another `Gaillard` — distinct (multiple Gaillard vignerons in Rhône)
- `Il Poggione` — similar-named Italian estates
- `Mont-Redon` — hyphen ambiguity

**Decision:** Most are correctly SKIP'd. Spot-check 5 in user review to confirm. No blanket rule change.

---

## Proposed §11.4 amendments (m-s)

### §11.4.m — Shared-surname family splits

When two producer rows share a surname but represent **distinct family branches**, **distinct estates**, or **distinct generations under different legal entities**, treat as SKIP.

**Signals for family split (→ SKIP):**
- Different websites confirmed via web search
- Different addresses or communes
- Different full legal/brand identities on labels (e.g., "Willi Brundlmayer" vs "Josef & Philip Brundlmayer")
- Shared-surname with no overlapping wines

**Signals for continuous family operation (→ MERGE):**
- Same website/address
- Brand carried forward across generations
- Shared LWIN_7 or TTB permit

**Examples:** `Brundlmayer` vs `Josef & Philip Brundlmayer` → SKIP (family branches); `Clos Vougeot` renamed through generations → MERGE.

### §11.4.n — Cross-country same-name (Rule update)

**Two producer rows in different country codes with the same or near-same name** may be:

1. **Same global brand** sold in multiple countries → MERGE if web evidence converges on a single website/KG entry
2. **Independent producers sharing a name coincidentally** → SKIP

**Signals for same global brand (→ MERGE):**
- Single website hosting both country variants
- Shared LWIN_7 or corporate parent
- Same Wikipedia entry

**Signals for independent coincidence (→ SKIP):**
- Different websites and KG entries
- Different corporate parents
- Distinct legal/TTB status

**Examples:**
- `Layer Cake` [AR/US] → MERGE (Treasury Americas brand, single website)
- `Tussock Jumper` [AU/US/NZ/etc.] → MERGE (global brand)
- `Jordan` [US/ZA] → SKIP (distinct estates, different websites)
- `Miguel Torres` [ES/CL] → SKIP (same family but distinct legal operations, different labels)

### §11.4.o — Collaboration / JV labels (existing §11.4.l extended)

A collaboration label `"A & B"` or `"A x B"` where A and B are known producers/winemakers → the collaboration is its own producer row. Never auto-merge with single-name `A` or `B`.

**Handling:**
- If ownership clearly resides with one party → PARENT_CHILD with that party as parent
- If JV with shared ownership → SKIP; record both parties in metadata
- Always flag to user review if ambiguous

**Examples:**
- `Anne Sophie Pic & Michel Chapoutier` vs `Chapoutier` → PARENT_CHILD (Chapoutier as parent; collab is sub-label)
- `M. Chapoutier & Giaconda` vs `Giaconda` → PARENT_CHILD (Giaconda as parent; Chapoutier is visiting winemaker)

### §11.4.p — Wine merchant suffix in parens (existing §11.4.d reinforced)

When a producer row appears as `"X (Merchant)"` where Merchant is a known wine merchant (BBR, Kermit Lynch, Skurnik, Corney & Barrow, Sotheby's, etc.) **and** X is also a producer row on its own, treat the `"X (Merchant)"` row as a merchant-labeled variant of X → **MERGE to X**.

**Do NOT apply** when:
- X is also a merchant name (could be ambiguous)
- The Merchant in question is the actual producer (rare but possible)

**Example:** `Churchill's (Berry Bros. & Rudd)` → MERGE to `Churchill's`.

### §11.4.q — Hospices de Beaune (and de Nuits) négociant variants

The auction bottles from Hospices de Beaune / Hospices de Nuits are sold under the format `"Hospices de Beaune (<Négociant>)"` where the négociant is the entity that purchased and bottled that year's lot.

**Rules:**
1. `Hospices de Beaune (X)` vs `Hospices de Beaune (Y)` where X ≠ Y → **SKIP** (distinct négociant bottlings)
2. `Hospices de Beaune (X)` vs `X` (the négociant itself) → **PARENT_CHILD** with `X` as parent, HdB row as child (the HdB row is a sub-catalog of X's own bottlings)
3. `Hospice de Beaune` vs `Hospices de Beaune` (punctuation variants) → MERGE

Same rules apply to Hospices de Nuits.

### §11.4.r — "& Fils" / "et Fils" / "Père et Fils" suffix variants

A producer name with `"& Fils"`, `"et Fils"`, `"Père et Fils"`, `"Pere et Fils"`, `"S&J"`, or similar family-suffix markers should be treated as a **likely MERGE candidate** with the base name, unless web evidence shows generational or estate split.

**Signals for MERGE:**
- Same website/KG entry
- Same location
- Shared wine labels between the two rows

**Signals for SKIP:**
- Different websites confirmed
- Public record of generational split (son created separate estate)

**Examples:**
- `Clavelier` vs `Clavelier et Fils` → typically MERGE (same domain)
- `Alex Moreau` vs `Alex et Benoit Moreau` → MERGE if same estate, SKIP if sons split
- `Fery-Meunier` vs `Jean Fery & Fils` → SKIP if different estates confirmed

### §11.4.s — Producer + sub-brand / sub-vineyard rows

When a producer row `"X"` and another row `"X <Sub-brand>"` or `"X <Vineyard>"` both exist:

**MERGE** if:
- The sub-brand is a product line name sold under X's main label (e.g., `Carlei` + `Carlei Green Vineyards` = same producer)
- LWIN or TTB shows single entity

**PARENT_CHILD** if:
- The sub-brand has its own commercial identity with separate website (e.g., `Contino` is a CVNE-owned estate with distinct identity → CVNE parent, Contino child)

**SKIP** if:
- The sub-brand is actually a different producer on the same estate site (rare)

**Examples:**
- `Knappstein` vs `Knappstein Lenswood Vineyards` → MERGE (line name)
- `CVNE (Contino)` vs `Contino` → PARENT_CHILD (CVNE = parent, Contino = child)
- `Obsidian Ridge` vs `Obsidian` → MERGE (both are Obsidian Wine Co.)

---

## User review curation — 50 representative pairs

Organized by pattern cluster. User thumbs-up/down per cluster approves the rule for all downstream decisions in that cluster.

### Cluster 1 — Hospices de Beaune (5 pairs)
1. `Hospices de Beaune (Joseph Drouhin)` vs `Joseph Drouhin` → rule §11.4.q.2 → PARENT_CHILD
2. `Hospices de Beaune (Pierre Andre)` vs `Pierre Andre` → rule §11.4.q.2 → PARENT_CHILD
3. `Hospices de Beaune (Camille Giroud)` vs `Hospices de Beaune (Pierre Morey)` → §11.4.q.1 → SKIP
4. `Hospices de Beaune (Faiveley)` vs `Hospice de Beaune` → §11.4.q.3 (punctuation) → MERGE
5. `Hospices de Beaune (Domaine Mortet)` vs `Hospices de Beaune (Maison Leroy)` → §11.4.q.1 → SKIP

### Cluster 2 — Global commercial brands (5 pairs)
6. `Layer Cake` [AR] vs `Layer Cake` [US] → §11.4.n.1 → MERGE
7. `Tussock Jumper` [AU] vs `Tussock Jumper` [US] → §11.4.n.1 → MERGE
8. `Baron Philippe de Rothschild` [CL] vs `Baron Philippe de Rothschild` [AR] → §11.4.n.1 → MERGE
9. `Niepoort` [ES] vs `Niepoort` [PT] → §11.4.n.1 → MERGE
10. `Bousquet` [AR] vs `Bousquet` [FR] → §11.4.n cross-check → SKIP (distinct Bousquet families)

### Cluster 3 — "& Fils" / "et Fils" variants (5 pairs)
11. `Clavelier` vs `Clavelier et Fils` → §11.4.r → MERGE
12. `Dampt` vs `Daniel Dampt & Fils` → §11.4.r → MERGE (or PC)
13. `Alex Moreau` vs `Alex et Benoit Moreau` → §11.4.r → MERGE (same estate generation)
14. `Fery-Meunier` vs `Jean Fery & Fils` → §11.4.r → SKIP (different estates likely)
15. `Protheau & Fils` vs `Jean-Francois Protheau` → §11.4.r → MERGE (generation continuity)

### Cluster 4 — Collaboration / JV labels (5 pairs)
16. `Chapoutier` vs `Anne Sophie Pic & Michel Chapoutier` → §11.4.o → PARENT_CHILD (Chapoutier parent)
17. `Giaconda` vs `M. Chapoutier & Giaconda` → §11.4.o → PARENT_CHILD (Giaconda parent)
18. `Rodrigo Mendez` vs `Vinos Atlantico & Rodrigo Mendez` → §11.4.o → PARENT_CHILD
19. `Tournon M.Chapoutier` vs `M. Chapoutier & Giaconda` → §11.4.o → SKIP (distinct JVs)
20. `No Control (Vincent Marie)` vs `Marie et Vincent Tricot` → §11.4.o → SKIP (different entities)

### Cluster 5 — Merchant bundled names (3 pairs)
21. `Churchill's (Berry Bros. & Rudd)` vs `Churchill's` → §11.4.p → MERGE
22. `Corney & Barrow (Sichel)` vs `Sichel` → §11.4.p → MERGE
23. `Berry Bros. & Rudd (Rosa)` vs `Berry Bros. & Rudd (Roche Bellene)` → §11.4.p → SKIP (distinct underlying producers behind same merchant)

### Cluster 6 — Shared-surname family splits (5 pairs)
24. `Brundlmayer` vs `Josef & Philip Brundlmayer` → §11.4.m → SKIP
25. `Willi Brundlmayer` vs `Josef & Philip Brundlmayer` → §11.4.m → SKIP
26. `Valentin Bianchi` vs `Elsa Bianchi` → §11.4.m → SKIP (different family members, different estates)
27. `Jacques Germanier` vs `Jean Rene Germanier` → §11.4.m → SKIP
28. `Benedikt Baltes` vs `Bertram-Baltes` → §11.4.m → SKIP (different people)

### Cluster 7 — Sub-brand / sub-vineyard rows (5 pairs)
29. `Obsidian Ridge` vs `Obsidian` → §11.4.s → MERGE (Obsidian Wine Co.)
30. `Knappstein` vs `Knappstein Lenswood Vineyards` → §11.4.s → MERGE (line label)
31. `Carlei` vs `Carlei Green Vineyards` → §11.4.s → MERGE
32. `Carlei` vs `Carlei Estate` → §11.4.s → MERGE
33. `CVNE (Contino)` vs `Contino` → §11.4.s → PARENT_CHILD (CVNE parent, Contino child)

### Cluster 8 — Compound-name / accent variants (3 pairs)
34. `Pico della Mirandola` vs `Pico Mirandola` → §11.4.h → MERGE
35. `Hospice de Nuits` vs `Hospices de Nuits` → §11.4.q.3 → MERGE
36. `Vermillon` vs `Vermillion` → §11.4.h → MERGE

### Cluster 9 — Borderline auto-apply merges at 0.92 conf (5 pairs)
37. `Maison Champy` vs `Champy Pere` → conf 0.92, §11.4.h → MERGE (confirmed by website)
38. `Barons de Rothschild` vs `Barons Rothschild Lafite Legen` → MERGE (DBR identity)
39. `Baron Philippe de Rothschild` vs `Baron de Rothschild` → MERGE (BPDR)
40. `Sainte Marie Vv Blanc` vs `Sainte Marie Vv` → MERGE (cuvée variants of Château Sainte-Marie)
41. `Gaffeliere Naudes` vs `La Gaffeliere` → MERGE (St-Émilion classified growth)

### Cluster 10 — Same-first-word siblings to spot-check (4 pairs)
42. `Château Haut-Brion` vs `La Mission Haut-Brion` → §11.4.k → SKIP (distinct estates)
43. `de Chevalier` sample sibling → SKIP (distinct)
44. `Pierre Gaillard` sibling → SKIP (multiple Gaillards in Rhône)
45. `Il Poggione` sibling → SKIP (distinct estates)

### Cluster 11 — Scorecard yellow flags to sanity-check (5 pairs)
46. `Château Pavie` (1 wine in DB — verify this is the primary Pavie row, not a duplicate we missed)
47. `Pingus` (2 wines — is `Dominio de Pingus` vs `Pingus` handled correctly?)
48. `Almaviva` (2 wines — should have ~25 vintages, investigate LWIN link)
49. `Penfolds` (211 wines but 0% flagship match — Grange exists but DB name pattern?)
50. `Trimbach` (75 wines but 0% flagship — Clos Sainte Hune stored under what name?)

---

## Required sign-offs from user

1. **Approve §11.4.m-s as drafted** (or flag edits)
2. **Thumbs-up pattern clusters 1-11** — each cluster approval validates all ~50-700 downstream decisions in that cluster
3. **Spot-check 5 individual pairs from each cluster** — if any look wrong, flag them

After sign-off → I apply §11 amendments to `docs/IDENTITY_RULES.md`, regenerate `producer_dedup_routing_stage3` if any rules changed verdicts, then move to Step 10 execution.
