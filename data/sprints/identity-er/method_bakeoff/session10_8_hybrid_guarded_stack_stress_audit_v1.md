# Session 10.8 - hybrid guarded stack stress audit

- Date: 2026-04-22
- Candidate: `hybrid_guarded_fils_person_alias_v1`
- Benchmark-clear reference: `data/sprints/identity-er/method_bakeoff/session10_8_hybrid_guarded_stack_confirmation_v1.md`
- Incremental spend: `$0.00`

## Goal

Stress the late zero-cost overlay families against the strongest remaining frozen-file analogues before calling the recommendation frozen.

## `place_alias`

Exact trigger hits:

- `pair_id 96369`: `MERGE` | `Stadt Krems` vs `Krems` | cluster `11.4.h` | inside benchmark `True`

Summary:

- Exact hits total: `1`
- Exact hits outside benchmark: `0`
- Note: No broader frozen ledger rows with `Stadt`/`Weingut`/`Winzer` name prefixes were found outside the single benchmark case.

Interpretation:

- The institutional-prefix place-alias family remains benchmark-unique in the frozen corpora.
- That is supportive, but it still does not create an independent outside-benchmark execution claim.

## `maison_alias`

Exact rich-context hits:

- `pair_id 67132`: `MERGE` | `Ardhuy Cabotte` vs `de la Cabotte` | cluster `11.4.h` | inside benchmark `True`

Broader name-only analogues:

- `pair_id 67132`: `MERGE` | `Ardhuy Cabotte` vs `de la Cabotte` | cluster `11.4.h` | inside benchmark `True`
- `pair_id 90721`: `SKIP` | `de la Gaffeliere` vs `Canon la Gaffeliere` | cluster `11.4.m` | inside benchmark `False`

Interpretation:

- Exact hits outside benchmark: `0`
- Broader name-only outside-benchmark hits: `1`
- The exact `Maison de la <token>` wine-list shape is still benchmark-unique.
- But the broader lexical shell does have an outside-benchmark SKIP (`de la Gaffeliere` / `Canon la Gaffeliere`), which proves the wine-list phrase is necessary and the family must stay narrow.

## `fils_person_alias`

Exact hits:

- `pair_id 19129`: `SKIP` | `Jean Boillot & Fils` vs `Jean-Marc Boillot` | cluster `11.4.m` | inside benchmark `True`
- `pair_id 4784`: `MERGE` | `Protheau & Fils` vs `Jean-Francois Protheau` | cluster `11.4.h` | inside benchmark `True`

Broader name-only analogues:

- `pair_id 4067`: `MERGE` | `Vocoret` vs `Vocoret et Fils` | cluster `11.4.h` | inside benchmark `True` | shared `vocoret` | hyphen-first `False` | person-has-fils `False`
- `pair_id 19129`: `SKIP` | `Jean Boillot & Fils` vs `Jean-Marc Boillot` | cluster `11.4.m` | inside benchmark `True` | shared `boillot, jean` | hyphen-first `True` | person-has-fils `False`
- `pair_id 52229`: `MERGE` | `Amiot Bonfils` vs `Guy Amiot et Fils` | cluster `11.4.f` | inside benchmark `True` | shared `amiot` | hyphen-first `False` | person-has-fils `False`
- `pair_id 65678`: `SKIP` | `Decelle Villa` vs `Decelle & Fils` | cluster `11.4.o` | inside benchmark `True` | shared `decelle` | hyphen-first `False` | person-has-fils `False`
- `pair_id 103518`: `DEFERRED_SPRINT_7` | `Clavelier` vs `Clavelier et Fils` | cluster `11.4.h` | inside benchmark `False` | shared `clavelier` | hyphen-first `False` | person-has-fils `False`
- `pair_id 123950`: `SKIP` | `Fery-Meunier` vs `Jean Fery & Fils` | cluster `11.4.m` | inside benchmark `False` | shared `fery` | hyphen-first `True` | person-has-fils `False`
- `pair_id 4784`: `MERGE` | `Protheau & Fils` vs `Jean-Francois Protheau` | cluster `11.4.h` | inside benchmark `True` | shared `protheau` | hyphen-first `True` | person-has-fils `False`
- `pair_id 32548`: `MERGE` | `Metrat et Fils ` vs `Metrat B` | cluster `11.4.h` | inside benchmark `True` | shared `metrat` | hyphen-first `False` | person-has-fils `False`
- `pair_id 87456`: `SKIP` | `Grivelet Pere & Fils` vs `Grivelet-Cusset` | cluster `11.4.m` | inside benchmark `False` | shared `grivelet` | hyphen-first `True` | person-has-fils `False`
- `pair_id 107035`: `SKIP` | `Daniel Dampt & Fils` vs `Dampt` | cluster `11.4.m` | inside benchmark `False` | shared `dampt` | hyphen-first `False` | person-has-fils `False`
- `pair_id 136777`: `SKIP` | `Roy Fils` vs `Roy Pere Fils` | cluster `11.4.m` | inside benchmark `False` | shared `fils, roy` | hyphen-first `False` | person-has-fils `True`
- `pair_id 3695`: `SKIP` | `Jean Paul Gauffroy et Fils` vs `Gauffroy Marc & Fils` | cluster `11.4.m` | inside benchmark `False` | shared `fils, gauffroy` | hyphen-first `False` | person-has-fils `True`
- `pair_id 12249`: `SKIP` | `Gauffroy Marc & Fils` vs `Gauffroy-Jacob` | cluster `11.4.h` | inside benchmark `True` | shared `gauffroy` | hyphen-first `True` | person-has-fils `False`
- `pair_id 17949`: `PARENT_CHILD` | `Quancard Pere et Fils` vs `de Paillet-Quancard` | cluster `11.4.g` | inside benchmark `False` | shared `quancard` | hyphen-first `False` | person-has-fils `False`
- `pair_id 75683`: `SKIP` | `Marc Parce` vs `Parce Fils` | cluster `11.4.m` | inside benchmark `False` | shared `parce` | hyphen-first `False` | person-has-fils `False`
- `pair_id 97583`: `SKIP` | `Michaut` vs `Michaut Pere & Fils` | cluster `11.4.m` | inside benchmark `False` | shared `michaut` | hyphen-first `False` | person-has-fils `False`

Interpretation:

- Exact hits outside benchmark: `0`
- Broader name-only outside-benchmark hits: `9`
- The exact two-word hyphenated personal-name shape appears only on two benchmark cases: one MERGE (`Protheau & Fils` / `Jean-Francois Protheau`) and one SKIP (`Jean Boillot & Fils` / `Jean-Marc Boillot`).
- Outside the benchmark, the broader `Fils` naming family has many nearby non-MERGE cases, which means this rule must stay pinned to the full packet-conditioned trigger and should not be generalized from names alone.

## Holdout Coverage

- `place_exact` hits in the existing 24-case holdout: `0`
- `maison_name_broad` hits in the existing 24-case holdout: `0`
- `fils_exact` hits in the existing 24-case holdout: `0`

The current holdout still does not independently test any of the three late overlay families.

## Frozen Stop Point

- Frozen-file confirmation exhausted: `true`
- Why: The final stack now has repeated benchmark clearance plus the strongest honest frozen-file stress audit available. The existing holdout does not cover the late overlay families, and the remaining outside-benchmark corpora only show adjacent analogues rather than faithful full-packet rerun coverage.
- Next non-frozen requirement: Restore a working packet-build/runtime path or preserve richer outside-benchmark structured packets before making a fresh independent confirmation claim.

## Recommendation

> The benchmark-clearing hybrid guarded stack has now been stress-audited as far as frozen local artifacts honestly allow. Two late overlay families are benchmark-unique in the frozen corpora, while the `Fils` family is only safe when kept extremely narrow and packet-conditioned. No existing frozen holdout cases independently cover those late overlays, so the recommendation is now frozen at: benchmark-clear and stress-audited, but not freshly independently validated.
