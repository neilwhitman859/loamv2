# Evidence Packet v1

**Date:** 2026-04-20  
**Deliverable:** standard per-pair evidence packet for merge-only producer dedup adjudication  
**Not doing:** candidate-generation rebuild, bakeoff scoring, execution SQL, or parent-child modeling  
**Stop when:** one stable packet schema and one worked example exist

## Purpose

`evidence_packet_v1` is the standard input object for adjudicating one producer pair.

It is designed to work for two adjacent phases:

1. **Bakeoff evaluation** - all contenders see the same grounded packet.
2. **Queue production** - the winning adjudicator sees the same packet shape at scale.

The packet is intentionally **merge-only**. `PARENT_CHILD` stays out of scope. The adjudicator may return `MERGE`, `SKIP`, or `FLAGGED`.

## Design Rules

- Ground the decision in trusted evidence, not free-form intuition.
- Keep the packet compact enough that a human can audit it quickly from the packet alone.
- Separate **model-visible evidence** from **evaluation-only metadata** so we do not leak the benchmark answer into the packet.
- Make every field traceable to one of four origins:
  - `db` - directly from Loam tables
  - `derived` - deterministic transform over DB fields
  - `retrieval` - fetched page evidence
  - `benchmark_overlay` - evaluation metadata, hidden from the adjudicator
- Prefer structured lists over prose blobs.
- Missing evidence should be explicit as `missing` or `[]`, never hidden in narration.

## What The Adjudicator Should And Should Not See

### Visible to the adjudicator

- pair header and candidate-family context
- row-level producer identity
- local wine-list coherence
- contradiction flags
- external evidence with citations
- deterministic survivor recommendation if a merge is chosen
- the allowed output contract

### Hidden from the adjudicator

- `expected_verdict`
- historical ladder verdicts
- previous model outputs
- benchmark stratum labels that would hint at the answer
- historical failure-mode labels like `double_skip_web_merge`

Those fields can exist in the outer evaluation harness, but not in the model-visible packet.

## Canonical Format

Store packets as **one JSON object per pair** in a JSONL file. A Markdown renderer can be built later from the same JSON object, but JSON is the source of truth.

The row shape is:

```json
{
  "packet_version": "v1",
  "packet_id": "producer_pair_4067_v1",
  "envelope": {},
  "evidence": {}
}
```

## Packet Schema

### 1. Envelope

The envelope is orchestration metadata. Some fields are safe to show to the adjudicator; some are evaluation-only.

```json
{
  "packet_version": "v1",
  "packet_id": "producer_pair_<pair_id>_v1",
  "pair_id": 4067,
  "producer_id_a": "uuid",
  "producer_id_b": "uuid",
  "pair_tier": "core | tail | unknown",
  "candidate_family": "same_country_lexical_alias | cross_country_same_brand | rare_wine_anchor | catalog_coherence | mixed",
  "source_methods": ["blocking:s2_trigram", "blocking:s9_substring"],
  "generated_at": "2026-04-20T21:30:00-04:00",
  "data_cutoff_at": "2026-04-20T21:25:00-04:00",
  "completeness": {
    "local_catalog": "complete | partial | missing",
    "retrieval": "complete | partial | missing",
    "survivor_calc": "complete | partial | missing"
  },
  "benchmark_overlay": {
    "benchmark_case_id": "blind_core_audit_002",
    "expected_verdict": "MERGE",
    "source_of_truth": "blind_audit_reconstructed"
  }
}
```

### Envelope field notes

| Field | Req | Origin | Notes |
|---|---|---|---|
| `packet_version` | yes | derived | Hard-coded schema tag. |
| `packet_id` | yes | derived | Stable unique row id for joins and scoring. |
| `pair_id` | yes | db | `producer_dedup_pairs.id` when a pair already exists. |
| `producer_id_a`, `producer_id_b` | yes | db | Stable row ids. |
| `pair_tier` | yes | derived | `core`, `tail`, or `unknown` if not yet classified. |
| `candidate_family` | yes | derived | Family-level label for the pair builder / bakeoff. |
| `source_methods` | yes | db + derived | Usually taken from `producer_dedup_pairs.signals` and normalized to names. |
| `generated_at`, `data_cutoff_at` | yes | derived | Reproducibility. |
| `completeness` | yes | derived | Explicitly marks missing packet sections. |
| `benchmark_overlay` | optional | benchmark_overlay | Hidden from adjudicators; used by the scorer only. |

## 2. Evidence

The evidence object is the model-visible packet.

```json
{
  "pair": {},
  "side_a": {},
  "side_b": {},
  "comparison": {},
  "external_evidence": {},
  "survivor_if_merge": {}
}
```

### 2.1 Pair

This is the compact header for the adjudicator.

```json
{
  "display_name": "Vocoret <-> Vocoret et Fils",
  "names": {
    "a": "Vocoret",
    "b": "Vocoret et Fils"
  },
  "normalized_names": {
    "a": "vocoret",
    "b": "vocoret et fils"
  },
  "country_pair": ["FR", "FR"],
  "rule_paths_to_check": ["11.4.h", "11.4.m"],
  "why_this_pair_exists": [
    "same-country lexical alias candidate",
    "substring containment",
    "trigram similarity 0.533"
  ]
}
```

### Pair field notes

- `rule_paths_to_check` is a deterministic shortlist of potentially relevant rules, not a verdict.
- It is acceptable to surface two or three competing rule paths if the packet is ambiguous.
- Do not include any field that says or implies "the old ladder thought this was MERGE."

### 2.2 Side A / Side B

Each side uses the same structure.

```json
{
  "producer_id": "uuid",
  "name": "Vocoret",
  "name_normalized": "vocoret",
  "producer_snapshot": {
    "country": "France",
    "region": "Burgundy",
    "appellation": null,
    "website_url": null,
    "year_established": null,
    "producer_type": null,
    "parent_producer_id": null,
    "deleted_at": null
  },
  "catalog_summary": {
    "wine_count": 15,
    "wines_with_lwin": 15,
    "wines_with_prices": 0,
    "wines_with_scores": 0,
    "representative_wines": [
      "Vocoret, Chablis",
      "Vocoret, Chablis Grand Cru, Les Clos",
      "Vocoret, Chablis Premier Cru, Vaillons"
    ],
    "dominant_places": ["Chablis"],
    "sparsity_flags": []
  }
}
```

### Side field notes

- `producer_snapshot` should stay narrow. Include only fields that help identity.
- `representative_wines` should favor recognizable anchor wines, not random rows.
- `dominant_places` is derived from `wines.appellation_id`, `wines.region_id`, and related names.
- `sparsity_flags` examples:
  - `thin_catalog`
  - `no_external_ids`
  - `no_prices_or_scores`
  - `empty_row`

### 2.3 Comparison

This is where the packet earns its keep.

```json
{
  "lexical": {
    "trigram_similarity": 0.533,
    "containment": "a_in_b",
    "shared_core_tokens": ["vocoret"],
    "wrapper_tokens_only_on_b": ["et", "fils"]
  },
  "geography": {
    "same_country": true,
    "same_region": true,
    "same_appellation": false,
    "conflict_notes": []
  },
  "catalog": {
    "exact_overlap_count": 0,
    "anchor_overlap_examples": [
      "Both sides bottle Chablis",
      "Both sides bottle Chablis Premier Cru Vaillons"
    ],
    "rare_anchor_wines": [],
    "portfolio_shape_comment": "Side B is a thinner but coherent subset of Side A's Chablis portfolio."
  },
  "support_signals": [
    {
      "code": "lexical_short_full_form",
      "strength": "high",
      "summary": "Short form vs full form only; distinguishing tokens are generic family/estate suffix."
    },
    {
      "code": "catalog_subset_match",
      "strength": "medium",
      "summary": "Side B's catalog fits inside Side A's place/style footprint."
    }
  ],
  "contradiction_flags": [
    {
      "code": "catalog_asymmetry",
      "severity": "low",
      "summary": "Side B is much smaller, so overlap is suggestive but not exhaustive."
    }
  ]
}
```

### Comparison field notes

`support_signals` and `contradiction_flags` should be the main decision surface.

Each item should be:

- short
- typed
- evidence-backed
- reusable across packets

Recommended `strength` values:

- `high`
- `medium`
- `low`

Recommended `severity` values:

- `high`
- `medium`
- `low`

Recommended contradiction codes for v1:

- `country_conflict`
- `region_conflict`
- `facility_only_signal`
- `collaboration_label_pattern`
- `shared_surname_split_risk`
- `merchant_prefix_pattern`
- `global_brand_country_split`
- `catalog_asymmetry`
- `generic_name_collision`
- `sparse_web`

### 2.4 External Evidence

This section carries citations, not prose essays.

```json
{
  "official_domain_hits": [
    {
      "ref_id": "official_1",
      "subject": "pair",
      "domain": "vocoret-vins.com",
      "url": "https://www.vocoret-vins.com/_catalogue.php",
      "page_title": "Domaine Vocoret - Chablis",
      "claim_summary": "Official site identifies SCEA Vocoret et Fils in Chablis and presents the Chablis portfolio under that producer identity.",
      "supports": "MERGE",
      "retrieved_at": "2026-04-20"
    }
  ],
  "secondary_hits": [
    {
      "ref_id": "secondary_1",
      "subject": "pair",
      "domain": "bourgogne-wines.com",
      "url": "https://www.bourgogne-wines.com/our-expertise/passionate-men-and-women/the-producers-behind-the-reputation-of-the-wines-of-bourgogne%2C2507%2C9363.html?args=Y29tcF9pZD0xNTAyJmFjdGlvbj12aWV3RmljaGUmaWQ9VklOQk9VMDAwMDIwMTEzMSZ8",
      "page_title": "Domaine Vocoret producer profile",
      "claim_summary": "Regional producer directory lists Domaine Vocoret in Chablis and supports the same estate identity.",
      "supports": "MERGE",
      "retrieved_at": "2026-04-20"
    }
  ],
  "retrieval_gaps": []
}
```

### External evidence rules

- Prefer **official-domain evidence** first.
- If no official site exists or it is too thin, use importer, regulatory, or regional-body pages next.
- Use secondary sources only as support, not as the main truth source, unless the pair is genuinely sparse-web.
- Every hit needs:
  - a domain
  - a URL
  - a short claim summary
  - a direction: `MERGE`, `SKIP`, or `NEUTRAL`
- Do not paste long page text into the packet.

### 2.5 Survivor If Merge

This section is only a helper. It is not the verdict.

```json
{
  "candidate_order": [
    {
      "rank": 1,
      "producer_id": "uuid_for_vocoret_et_fils",
      "name": "Vocoret et Fils",
      "why": [
        "full on-label form beats shorthand",
        "fits survivor rule 11.6 label-form priority"
      ]
    },
    {
      "rank": 2,
      "producer_id": "uuid_for_vocoret",
      "name": "Vocoret",
      "why": [
        "short alias form"
      ]
    }
  ],
  "recommended_survivor_producer_id": "uuid_for_vocoret_et_fils",
  "alias_to_preserve": "Vocoret",
  "survivor_confidence": "medium",
  "only_apply_if_verdict": "MERGE"
}
```

### Survivor rules

- Compute this deterministically from the survivor policy in `docs/IDENTITY_RULES.md`.
- Always separate survivor ranking from the merge verdict.
- If the packet cannot rank survivors cleanly, return:

```json
{
  "candidate_order": [],
  "recommended_survivor_producer_id": null,
  "alias_to_preserve": null,
  "survivor_confidence": "low",
  "only_apply_if_verdict": "MERGE"
}
```

## 3. Adjudication Output Contract

The adjudicator returns a separate result object. This keeps packet generation and decisioning cleanly separated.

```json
{
  "packet_id": "producer_pair_4067_v1",
  "verdict": "MERGE | SKIP | FLAGGED",
  "confidence": 0.94,
  "rule_ids": ["11.4.h"],
  "reason": "Short/full-form alias for the same Chablis estate; local catalog and external evidence align.",
  "key_support_refs": ["official_1", "lexical_short_full_form", "catalog_subset_match"],
  "key_contradiction_refs": ["catalog_asymmetry"],
  "survivor_producer_id": "uuid_or_null",
  "follow_up": null
}
```

### Output rules

- `MERGE` requires a survivor if `survivor_if_merge.recommended_survivor_producer_id` is non-null.
- `SKIP` leaves `survivor_producer_id = null`.
- `FLAGGED` means "do not auto-apply; needs human or better evidence."
- `PARENT_CHILD` is not allowed in v1.

## Rendering Limits

To keep packets human-auditable:

- max `5` representative wines per side
- max `3` anchor overlap examples
- max `4` official hits
- max `2` secondary hits
- max `8` combined support/contradiction items
- rendered Markdown target: about `80-120` lines per packet

If a pair needs more than this to be understood, the right answer is usually `FLAGGED`, not a bloated packet.

## Generation Notes

### Pull directly from current tables

- `producer_dedup_pairs`
- `producers`
- `wines`
- `wine_vintages`
- `wine_vintage_prices`
- `wine_vintage_scores`
- `wine_grapes`
- `external_ids`

### Derive, do not hallucinate

- normalized names
- candidate family
- rule paths to check
- dominant places
- overlap anchors
- support signals
- contradiction flags
- survivor ranking

### Retrieval order

1. Official producer domain
2. Regional or regulatory body
3. Importer/distributor page
4. Secondary source only if the pair is sparse-web

## Worked Example

This is a condensed example for benchmark case `blind_core_audit_002` / pair `4067`.

```json
{
  "packet_version": "v1",
  "packet_id": "producer_pair_4067_v1",
  "envelope": {
    "pair_id": 4067,
    "producer_id_a": "2da66547-7598-42ab-b98c-624f496be252",
    "producer_id_b": "e02536aa-192a-479b-97ee-7a512a519067",
    "pair_tier": "core",
    "candidate_family": "same_country_lexical_alias",
    "source_methods": ["blocking:s2_trigram", "blocking:s9_substring"],
    "generated_at": "2026-04-20T21:30:00-04:00",
    "data_cutoff_at": "2026-04-20T21:25:00-04:00",
    "completeness": {
      "local_catalog": "complete",
      "retrieval": "partial",
      "survivor_calc": "complete"
    },
    "benchmark_overlay": {
      "benchmark_case_id": "blind_core_audit_002",
      "expected_verdict": "MERGE",
      "source_of_truth": "blind_audit_reconstructed"
    }
  },
  "evidence": {
    "pair": {
      "display_name": "Vocoret <-> Vocoret et Fils",
      "names": {
        "a": "Vocoret",
        "b": "Vocoret et Fils"
      },
      "normalized_names": {
        "a": "vocoret",
        "b": "vocoret et fils"
      },
      "country_pair": ["FR", "FR"],
      "rule_paths_to_check": ["11.4.h", "11.4.m"],
      "why_this_pair_exists": [
        "same-country lexical alias candidate",
        "substring containment",
        "trigram similarity 0.533"
      ]
    },
    "side_a": {
      "producer_id": "2da66547-7598-42ab-b98c-624f496be252",
      "name": "Vocoret",
      "name_normalized": "vocoret",
      "producer_snapshot": {
        "country": "France",
        "region": "Burgundy",
        "appellation": null,
        "website_url": null,
        "year_established": null,
        "producer_type": null,
        "parent_producer_id": null,
        "deleted_at": null
      },
      "catalog_summary": {
        "wine_count": 15,
        "wines_with_lwin": 15,
        "wines_with_prices": 0,
        "wines_with_scores": 0,
        "representative_wines": [
          "Vocoret, Chablis",
          "Vocoret, Chablis Grand Cru, Les Clos",
          "Vocoret, Chablis Grand Cru, Valmur",
          "Vocoret, Chablis Premier Cru, Montee de Tonnerre",
          "Vocoret, Chablis Premier Cru, Vaillons"
        ],
        "dominant_places": ["Chablis"],
        "sparsity_flags": []
      }
    },
    "side_b": {
      "producer_id": "e02536aa-192a-479b-97ee-7a512a519067",
      "name": "Vocoret et Fils",
      "name_normalized": "vocoret et fils",
      "producer_snapshot": {
        "country": "France",
        "region": "Burgundy",
        "appellation": null,
        "website_url": null,
        "year_established": null,
        "producer_type": null,
        "parent_producer_id": null,
        "deleted_at": null
      },
      "catalog_summary": {
        "wine_count": 2,
        "wines_with_lwin": 2,
        "wines_with_prices": 0,
        "wines_with_scores": 0,
        "representative_wines": [
          "Domaine Vocoret et Fils, Chablis",
          "Vocoret et Fils, Chablis Premier Cru, Vaillons"
        ],
        "dominant_places": ["Chablis"],
        "sparsity_flags": ["thin_catalog"]
      }
    },
    "comparison": {
      "lexical": {
        "trigram_similarity": 0.533,
        "containment": "a_in_b",
        "shared_core_tokens": ["vocoret"],
        "wrapper_tokens_only_on_b": ["et", "fils"]
      },
      "geography": {
        "same_country": true,
        "same_region": true,
        "same_appellation": false,
        "conflict_notes": []
      },
      "catalog": {
        "exact_overlap_count": 0,
        "anchor_overlap_examples": [
          "Both sides bottle Chablis",
          "Both sides bottle Chablis Premier Cru Vaillons"
        ],
        "rare_anchor_wines": [],
        "portfolio_shape_comment": "Side B looks like a thinner subset of the same Chablis estate footprint."
      },
      "support_signals": [
        {
          "code": "lexical_short_full_form",
          "strength": "high",
          "summary": "Short form vs full estate form; extra tokens are generic family suffix."
        },
        {
          "code": "catalog_subset_match",
          "strength": "medium",
          "summary": "Both sides center on Chablis and share a Vaillons bottling anchor."
        }
      ],
      "contradiction_flags": [
        {
          "code": "catalog_asymmetry",
          "severity": "low",
          "summary": "Side B is thin, so the local overlap is supportive but not exhaustive."
        }
      ]
    },
    "external_evidence": {
      "official_domain_hits": [
        {
          "ref_id": "official_1",
          "subject": "pair",
          "domain": "vocoret-vins.com",
          "url": "https://www.vocoret-vins.com/_catalogue.php",
          "page_title": "Domaine Vocoret - Chablis",
          "claim_summary": "Official site presents the Chablis estate as Vocoret et Fils in Chablis, consistent with the fuller label form.",
          "supports": "MERGE",
          "retrieved_at": "2026-04-20"
        }
      ],
      "secondary_hits": [
        {
          "ref_id": "secondary_1",
          "subject": "pair",
          "domain": "bourgogne-wines.com",
          "url": "https://www.bourgogne-wines.com/our-expertise/passionate-men-and-women/the-producers-behind-the-reputation-of-the-wines-of-bourgogne%2C2507%2C9363.html?args=Y29tcF9pZD0xNTAyJmFjdGlvbj12aWV3RmljaGUmaWQ9VklOQk9VMDAwMDIwMTEzMSZ8",
          "page_title": "Domaine Vocoret producer profile",
          "claim_summary": "Regional directory reinforces a single Chablis producer identity built around the Vocoret estate.",
          "supports": "MERGE",
          "retrieved_at": "2026-04-20"
        }
      ],
      "retrieval_gaps": []
    },
    "survivor_if_merge": {
      "candidate_order": [
        {
          "rank": 1,
          "producer_id": "e02536aa-192a-479b-97ee-7a512a519067",
          "name": "Vocoret et Fils",
          "why": [
            "full on-label form beats shorthand",
            "better fits survivor rule 11.6 label-form priority"
          ]
        },
        {
          "rank": 2,
          "producer_id": "2da66547-7598-42ab-b98c-624f496be252",
          "name": "Vocoret",
          "why": [
            "short alias form"
          ]
        }
      ],
      "recommended_survivor_producer_id": "e02536aa-192a-479b-97ee-7a512a519067",
      "alias_to_preserve": "Vocoret",
      "survivor_confidence": "medium",
      "only_apply_if_verdict": "MERGE"
    }
  }
}
```

## Decision

`evidence_packet_v1` should be the standard packet format for the next two artifacts:

1. the bakeoff harness
2. the adjudication queue builder

If we later need a richer packet, create `v2` rather than quietly changing this shape mid-bakeoff.
