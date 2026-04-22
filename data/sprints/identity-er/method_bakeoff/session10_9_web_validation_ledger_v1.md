# Session 10.9 - web validation ledger

- Date: 2026-04-22
- Candidate under test: `hybrid_guarded_fils_person_alias_v1`
- Local snapshot artifact: `data/sprints/identity-er/method_bakeoff/session10_9_case_snapshots_v1.json`
- Incremental model spend: `$0.00`

## Goal

Pressure-test the current benchmark-clearing stack against independent web evidence plus live read-only canonical snapshots, with special focus on the three late overlay families that were not independently covered by the frozen holdout.

## Headline

The current recommended stack does **not** clear independent web validation strongly enough to keep its prior recommendation.

Why:

- Exact late-overlay benchmark wins checked: `3`
- Clearly supported by outside evidence: `1`
- Clearly contradicted by outside evidence: `1`
- Still unresolved / not independently confirmed: `1`
- Negative / guard controls checked: `4`
- Clear support for staying distinct: `3`
- Ambiguous control with strong continuity signals: `1`

The single biggest problem is the `maison_alias` benchmark win:

- `Ardhuy Cabotte` / `de la Cabotte` no longer looks like a trustworthy `MERGE`
- outside evidence instead points toward a Rhône `Domaine la Cabotte` under the d'Ardhuy family and a separate Burgundy `Maison la Cabotte` line tied to Nicolas Potel

That means the exact late overlay family that pushed the stack from guarded to benchmark-perfect is not independently surviving contact with real-world evidence.

## Case Ledger

| Case | Family | Expected behavior from current stack | Web-grounded finding | Validation result |
|---|---|---|---|---|
| `Stadt Krems` / `Krems` | `place_alias` | merge | Official Weingut Stadt Krems material sells a wine literally titled `KREMS` while identifying the winery as Weingut Stadt Krems. | `supports_merge` |
| `Tenuta Brunelli` / `Brunelli` | `place_alias` guard control | no merge | Official Brunelli site is Valpolicella; official Tenuta Brunelli / Martoccia material is Montalcino. | `supports_distinct` |
| `de la Gaffeliere` / `Canon la Gaffeliere` | `maison_alias` guard control | no merge | Official Saint-Émilion pages show different estates, owners, addresses, and site links. | `supports_distinct` |
| `Ardhuy Cabotte` / `de la Cabotte` | `maison_alias` | merge | Official d'Ardhuy and Cabotte sources describe the Rhône `Domaine la Cabotte`; Burgundy `Maison la Cabotte` evidence points to Nicolas Potel rather than the same producer identity. | `contradicts_merge` |
| `Jean Boillot & Fils` / `Jean-Marc Boillot` | `fils_person_alias` negative exact | no merge | Current outside evidence still supports Jean-Marc as his own domaine and keeps the split plausible despite one overlapping cuvée trap. | `supports_distinct` |
| `Fery-Meunier` / `Jean Fery & Fils` | `fils_person_alias` analogue control | no merge | Official Jean Fery plus RVF/Pappers data show shared Echevronne address / phone / Jean-Louis Fery continuity; this looks historically entangled, not cleanly distinct. | `ambiguous_continuity` |
| `Protheau & Fils` / `Jean-Francois Protheau` | `fils_person_alias` | merge | Legal / trade evidence suggests a Meursault-centered continuity path, but the continuity is still not cleanly documented from primary producer material. | `unresolved` |

## Notes By Family

### `place_alias`

This family held up best.

- [Weingut Stadt Krems story](https://www.weingutstadtkrems.at/weingut) identifies the winery as municipally owned Weingut Stadt Krems in Krems.
- The winery's own `KREMS` tech sheet ([PDF](https://www.weingutstadtkrems.at/_files/ugd/c17f2a_e250f2b48e4a403abdf3497809b77247.pdf)) presents `KREMS` as a wine from Weingut Stadt Krems.
- The negative analogue also held: [Brunelli Wine](https://www.brunelliwine.com/en/) is a Valpolicella Amarone producer, while [Tenuta Brunelli / Martoccia](https://www.poderemartoccia.it/) is a Montalcino estate.

Interpretation:

- `place_alias` has real external support.
- The same-region guard still looks important and honest.

### `maison_alias`

This family failed independent validation.

- The official [d'Ardhuy history page](https://www.ardhuy.com/en/histoire-domaine/gabriel-liogier-d-ardhuy) says Gabriel d'Ardhuy acquired Rhône `Domaine La Cabotte` in 1980 and that it is now independently run by a daughter and nephew.
- The official [Domaine la Cabotte site](https://www.cabotte.com/historique/) says Marie-Pierre Plumet d'Ardhuy took over the Rhône estate and bought the family vineyard in `2018`.
- The Burgundy `Maison la Cabotte` evidence now points elsewhere: [SoilAir Selection](https://www.soilairselection.com/maison-la-cabotte) describes `Maison la Cabotte` as a Burgundy line produced by Nicolas Potel.
- The negative analogue stayed clean: official Saint-Émilion pages for [Château La Gaffelière](https://vins-saint-emilion.com/en/castle/chateau-la-gaffeliere-2/) and [Château Canon la Gaffelière](https://vins-saint-emilion.com/en/castle/chateau-canon-la-gaffeliere-4/) show distinct estates.

Interpretation:

- The exact late `maison_alias` benchmark win does not survive web scrutiny.
- This is not a small caveat; it directly challenges one of the zero-cost overlays that made the stack benchmark-perfect.

### `fils_person_alias`

This family did not independently clear.

- The negative exact `Jean Boillot & Fils` / `Jean-Marc Boillot` still looks plausibly distinct from current outside evidence, including the official [Jean-Marc Boillot site](https://jeanmarc-boillot.com/).
- But the analogue controls are messy:
  - [Domaine Jean Fery](https://www.fery-vin.fr/) and [Maison Fery Meunier](https://www.larvf.com/maison-fery-meunier%2C10572%2C405554.asp) share Echevronne coordinates and wine footprint.
  - [Pappers on Maison Fery Meunier](https://www.pappers.fr/entreprise/maison-fery-meunier-399867001) shows Jean-Louis Fery and a cited relation to [S2V / Domaine Jean Fery et Fils](https://www.pappers.fr/entreprise/s2v-340727072).
  - [Pappers on Domaine Jean François Protheau](https://www.pappers.fr/entreprise/domaine-jean-francois-protheau-385363791) shows a Mercurey-to-Meursault continuity path, but the exact `Protheau & Fils` relationship is still not cleanly proven from producer-owned material.

Interpretation:

- `fils_person_alias` remains unresolved, not independently confirmed.
- The old frozen-file negatives around this naming family are less clean than they looked during the benchmark-only bakeoff.

## Recommendation

Demote `hybrid_guarded_fils_person_alias_v1` from "recommended best current method" to:

> benchmark-clear but web-challenged provisional artifact

What should happen next:

1. Do **not** call this method independently production-validated.
2. Do **not** treat the late overlay stack as redo-quality-ready.
3. Re-open the truth status of the late benchmark wins, especially `Ardhuy Cabotte` / `de la Cabotte`.
4. If we continue, the next session should be a benchmark-truth and entity-history repair pass, not another method bakeoff.

## Bottom Line

The broad guarded base still looks directionally promising, but the final benchmark-clearing overlay stack does not survive fresh web validation cleanly enough to remain the trusted recommendation. One late overlay is contradicted, and another still lacks strong independent confirmation.
