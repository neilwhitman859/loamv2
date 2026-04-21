# session6_first_real_bakeoff_v1 - first real adjudication bakeoff

- Generated: 2026-04-20T22:28:05-04:00
- Benchmark: `producer_dedup_benchmark_v1`
- Cases scored: 152 / 152
- Full benchmark run: yes

## Overall summary

| Contender | Method class | Model(s) | Exact acc | False merge | Hard missed | Soft missed | Safe flag | Survivor acc | Auditability | Cost/pair | Total cost | Production gate | Fallback gate |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| deterministic_control_v1 | deterministic control | none | 0.3618 | 49 | 31 | 5 | 12 | 1.0000 | 0.9605 | 0.000000 | 0.0000 | fail | fail |
| haiku_single_v1 | single-model adjudicator | Claude Haiku 4.5 (`claude-haiku-4-5-20251001`) | 0.2237 | 0 | 5 | 44 | 69 | 1.0000 | 0.2632 | 0.002203 | 0.3349 | fail | fail |
| gemini_single_v1 | single-model adjudicator | Gemini 3 Flash Preview (`google/gemini-3-flash-preview`) | 0.5921 | 10 | 4 | 13 | 35 | 0.9804 | 0.7105 | 0.001479 | 0.2248 | fail | fail |
| gpt5mini_single_v1 | single-model adjudicator | GPT-5.4-mini (`openai/gpt-5.4-mini`) | 0.5789 | 14 | 16 | 7 | 27 | 1.0000 | 0.8026 | 0.003518 | 0.5347 | fail | fail |
| haiku_gemini_consensus_v1 | consensus adjudicator | Claude Haiku 4.5 + Gemini 3 Flash Preview | 0.2039 | 0 | 0 | 49 | 72 | 1.0000 | 0.2697 | 0.003682 | 0.5597 | fail | fail |
| sonnet_single_v1 | single-model adjudicator | Claude Sonnet 4.5 (`claude-sonnet-4-5-20250929`) | 0.7368 | 11 | 9 | 10 | 10 | 1.0000 | 0.8882 | 0.008125 | 1.2350 | fail | fail |

## Core/tail breakdown

| Contender | Tier | Cases | False merge | Hard missed | Soft missed | Safe flag | Exact acc | Flag rate |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| deterministic_control_v1 | core | 101 | 39 | 19 | 1 | 4 | 0.3762 | 0.0495 |
| deterministic_control_v1 | tail | 28 | 6 | 7 | 2 | 6 | 0.2500 | 0.2857 |
| haiku_single_v1 | core | 101 | 0 | 2 | 27 | 45 | 0.2673 | 0.7129 |
| haiku_single_v1 | tail | 28 | 0 | 1 | 7 | 13 | 0.2500 | 0.7143 |
| gemini_single_v1 | core | 101 | 8 | 2 | 5 | 22 | 0.6337 | 0.2673 |
| gemini_single_v1 | tail | 28 | 2 | 0 | 4 | 6 | 0.5714 | 0.3571 |
| gpt5mini_single_v1 | core | 101 | 12 | 7 | 3 | 16 | 0.6238 | 0.1881 |
| gpt5mini_single_v1 | tail | 28 | 2 | 3 | 0 | 7 | 0.5714 | 0.2500 |
| haiku_gemini_consensus_v1 | core | 101 | 0 | 0 | 29 | 47 | 0.2475 | 0.7525 |
| haiku_gemini_consensus_v1 | tail | 28 | 0 | 0 | 8 | 14 | 0.2143 | 0.7857 |
| sonnet_single_v1 | core | 101 | 9 | 8 | 4 | 6 | 0.7327 | 0.0990 |
| sonnet_single_v1 | tail | 28 | 2 | 1 | 1 | 2 | 0.7857 | 0.1071 |

## Stratum breakdown

| Contender | Stratum | Cases | False merge | Hard missed | Soft missed | Safe flag | Exact acc | Flag rate |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| deterministic_control_v1 | blind_core_audit | 100 | 39 | 19 | 1 | 4 | 0.3700 | 0.0500 |
| deterministic_control_v1 | known_false_merge_patterns | 16 | 5 | 0 | 0 | 3 | 0.5000 | 0.1875 |
| deterministic_control_v1 | known_missed_merge_patterns | 16 | 0 | 9 | 2 | 0 | 0.3125 | 0.1250 |
| deterministic_control_v1 | tail_random_sample | 20 | 5 | 3 | 2 | 5 | 0.2500 | 0.3500 |
| haiku_single_v1 | blind_core_audit | 100 | 0 | 2 | 27 | 44 | 0.2700 | 0.7100 |
| haiku_single_v1 | known_false_merge_patterns | 16 | 0 | 0 | 0 | 15 | 0.0625 | 0.9375 |
| haiku_single_v1 | known_missed_merge_patterns | 16 | 0 | 3 | 12 | 0 | 0.0625 | 0.7500 |
| haiku_single_v1 | tail_random_sample | 20 | 0 | 0 | 5 | 10 | 0.2500 | 0.7500 |
| gemini_single_v1 | blind_core_audit | 100 | 8 | 2 | 5 | 21 | 0.6400 | 0.2600 |
| gemini_single_v1 | known_false_merge_patterns | 16 | 0 | 0 | 0 | 10 | 0.3750 | 0.6250 |
| gemini_single_v1 | known_missed_merge_patterns | 16 | 0 | 2 | 5 | 0 | 0.5625 | 0.3125 |
| gemini_single_v1 | tail_random_sample | 20 | 2 | 0 | 3 | 4 | 0.5500 | 0.3500 |
| gpt5mini_single_v1 | blind_core_audit | 100 | 12 | 7 | 3 | 16 | 0.6200 | 0.1900 |
| gpt5mini_single_v1 | known_false_merge_patterns | 16 | 0 | 0 | 0 | 7 | 0.5625 | 0.4375 |
| gpt5mini_single_v1 | known_missed_merge_patterns | 16 | 0 | 7 | 4 | 0 | 0.3125 | 0.2500 |
| gpt5mini_single_v1 | tail_random_sample | 20 | 2 | 2 | 0 | 4 | 0.6000 | 0.2000 |
| haiku_gemini_consensus_v1 | blind_core_audit | 100 | 0 | 0 | 29 | 46 | 0.2500 | 0.7500 |
| haiku_gemini_consensus_v1 | known_false_merge_patterns | 16 | 0 | 0 | 0 | 15 | 0.0625 | 0.9375 |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns | 16 | 0 | 0 | 15 | 0 | 0.0625 | 0.9375 |
| haiku_gemini_consensus_v1 | tail_random_sample | 20 | 0 | 0 | 5 | 11 | 0.2000 | 0.8000 |
| sonnet_single_v1 | blind_core_audit | 100 | 9 | 8 | 4 | 6 | 0.7300 | 0.1000 |
| sonnet_single_v1 | known_false_merge_patterns | 16 | 0 | 0 | 0 | 2 | 0.8750 | 0.1250 |
| sonnet_single_v1 | known_missed_merge_patterns | 16 | 0 | 0 | 5 | 0 | 0.6875 | 0.3125 |
| sonnet_single_v1 | tail_random_sample | 20 | 2 | 1 | 1 | 2 | 0.7000 | 0.1500 |

## Winner-selection table

| Contender | Eligibility | Production gate | Fallback gate | Production gate failures | Fallback gate failures |
|---|---|---|---|---|---|
| deterministic_control_v1 | ineligible | fail | fail | false_merge_zero, blind_core_false_merge_zero, blind_core_hard_missed_zero, known_false_merge_false_merge_zero, known_missed_hard_lte_2, tail_false_merge_zero, tail_hard_missed_lte_2, tail_flag_rate_lte_0_25, schema_valid_rate_1_00 | false_merge_zero, blind_core_false_merge_zero |
| haiku_single_v1 | ineligible | fail | fail | blind_core_hard_missed_zero, blind_core_soft_missed_lte_1, known_false_merge_safe_flag_lte_4, known_missed_hard_lte_2, known_missed_soft_lte_4, tail_flag_rate_lte_0_25, auditability_score_gte_0_95, schema_valid_rate_1_00 | auditability_score_gte_0_95, flag_rate_total_lte_0_35 |
| gemini_single_v1 | ineligible | fail | fail | false_merge_zero, blind_core_false_merge_zero, blind_core_hard_missed_zero, blind_core_soft_missed_lte_1, known_false_merge_safe_flag_lte_4, known_missed_soft_lte_4, tail_false_merge_zero, tail_flag_rate_lte_0_25, auditability_score_gte_0_95, schema_valid_rate_1_00 | false_merge_zero, blind_core_false_merge_zero, auditability_score_gte_0_95 |
| gpt5mini_single_v1 | ineligible | fail | fail | false_merge_zero, blind_core_false_merge_zero, blind_core_hard_missed_zero, blind_core_soft_missed_lte_1, known_false_merge_safe_flag_lte_4, known_missed_hard_lte_2, tail_false_merge_zero, auditability_score_gte_0_95, schema_valid_rate_1_00 | false_merge_zero, blind_core_false_merge_zero, auditability_score_gte_0_95 |
| haiku_gemini_consensus_v1 | ineligible | fail | fail | blind_core_soft_missed_lte_1, known_false_merge_safe_flag_lte_4, known_missed_soft_lte_4, tail_flag_rate_lte_0_25, auditability_score_gte_0_95, schema_valid_rate_1_00 | auditability_score_gte_0_95, flag_rate_total_lte_0_35 |
| sonnet_single_v1 | ineligible | fail | fail | false_merge_zero, blind_core_false_merge_zero, blind_core_hard_missed_zero, blind_core_soft_missed_lte_1, known_missed_soft_lte_4, tail_false_merge_zero, auditability_score_gte_0_95, schema_valid_rate_1_00 | false_merge_zero, blind_core_false_merge_zero, auditability_score_gte_0_95 |

## Error ledger

| Contender | Case | Pair | Expected | Predicted | Packet refs used |
|---|---|---:|---|---|---|
| deterministic_control_v1 | blind_core_audit_001 | 645 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | blind_core_audit_003 | 13568 | MERGE | SKIP | catalog_subset_match, lexical_near_exact, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_005 | 25412 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | blind_core_audit_006 | 37630 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_007 | 47775 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_009 | 54064 | MERGE | SKIP | catalog_subset_match, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_011 | 68320 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_012 | 71588 | MERGE | SKIP | shared_core_token, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_013 | 71929 | MERGE | SKIP | lexical_short_full_form, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_016 | 96369 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | blind_core_audit_017 | 96741 | MERGE | SKIP | shared_core_token, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | blind_core_audit_018 | 100006 | MERGE | SKIP | catalog_subset_match, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_019 | 100909 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_020 | 104522 | MERGE | SKIP | shared_core_token, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_023 | 136068 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_024 | 137389 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | blind_core_audit_025 | 138090 | MERGE | FLAGGED | - |
| deterministic_control_v1 | blind_core_audit_026 | 139102 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_027 | 141172 | MERGE | SKIP | lexical_short_full_form, catalog_asymmetry, country_conflict |
| deterministic_control_v1 | blind_core_audit_028 | 142095 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | blind_core_audit_031 | 1517 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_034 | 7234 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | blind_core_audit_035 | 7324 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_038 | 18367 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_039 | 18610 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_040 | 19129 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | blind_core_audit_042 | 22194 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_045 | 27965 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_046 | 30499 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | blind_core_audit_047 | 54025 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_049 | 55160 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_055 | 62908 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | blind_core_audit_057 | 65678 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | blind_core_audit_059 | 77183 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_060 | 78749 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_064 | 93489 | SKIP | MERGE | catalog_subset_match, lexical_short_full_form, sparse_web |
| deterministic_control_v1 | blind_core_audit_067 | 103859 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_070 | 115288 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_071 | 115471 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_072 | 116827 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, sparse_web |
| deterministic_control_v1 | blind_core_audit_073 | 116871 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_074 | 117740 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_075 | 117979 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_076 | 119470 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, sparse_web |
| deterministic_control_v1 | blind_core_audit_077 | 119498 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, sparse_web |
| deterministic_control_v1 | blind_core_audit_078 | 119507 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_079 | 119640 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_080 | 121141 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_083 | 122328 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_085 | 122496 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_086 | 123050 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_087 | 123192 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_088 | 123480 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_095 | 146118 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_096 | 147103 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_097 | 147297 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, sparse_web |
| deterministic_control_v1 | blind_core_audit_098 | 154358 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | blind_core_audit_099 | 154381 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, sparse_web |
| deterministic_control_v1 | blind_core_audit_100 | 157991 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| deterministic_control_v1 | known_false_merge_patterns_003 | 355 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | known_false_merge_patterns_004 | 17186 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | known_false_merge_patterns_011 | 78125 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | known_false_merge_patterns_012 | 107981 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | known_false_merge_patterns_013 | 12249 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_001 | 4784 | MERGE | FLAGGED | shared_core_token, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_002 | 10596 | MERGE | SKIP | catalog_subset_match, lexical_short_full_form, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_003 | 11105 | MERGE | SKIP | catalog_subset_match, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_008 | 67132 | MERGE | SKIP | shared_core_token, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_010 | 83519 | MERGE | SKIP | shared_core_token, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_011 | 88568 | MERGE | FLAGGED | shared_core_token, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_012 | 123759 | MERGE | SKIP | catalog_subset_match, catalog_asymmetry, shared_surname_split_risk |
| deterministic_control_v1 | known_missed_merge_patterns_013 | 4054 | MERGE | SKIP | catalog_subset_match, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_014 | 136270 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_015 | 137796 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | known_missed_merge_patterns_016 | 143564 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | tail_random_sample_001 | 36774 | MERGE | SKIP | catalog_subset_match, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | tail_random_sample_002 | 40974 | MERGE | FLAGGED | shared_core_token, sparse_web |
| deterministic_control_v1 | tail_random_sample_003 | 136710 | MERGE | FLAGGED | - |
| deterministic_control_v1 | tail_random_sample_004 | 138971 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | tail_random_sample_005 | 139566 | MERGE | SKIP | cross_country_name_match, lexical_near_exact, shared_surname_split_risk, sparse_web |
| deterministic_control_v1 | tail_random_sample_008 | 21177 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | tail_random_sample_011 | 37923 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | tail_random_sample_013 | 58847 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | tail_random_sample_015 | 95007 | SKIP | MERGE | catalog_subset_match, sparse_web |
| deterministic_control_v1 | tail_random_sample_017 | 116232 | SKIP | MERGE | catalog_subset_match, lexical_near_exact, sparse_web |
| gemini_single_v1 | blind_core_audit_007 | 47775 | MERGE | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| gemini_single_v1 | blind_core_audit_010 | 55533 | MERGE | FLAGGED | - |
| gemini_single_v1 | blind_core_audit_012 | 71588 | MERGE | FLAGGED | - |
| gemini_single_v1 | blind_core_audit_017 | 96741 | MERGE | FLAGGED | - |
| gemini_single_v1 | blind_core_audit_018 | 100006 | MERGE | FLAGGED | - |
| gemini_single_v1 | blind_core_audit_019 | 100909 | MERGE | SKIP | shared_surname_split_risk, lexical_short_full_form |
| gemini_single_v1 | blind_core_audit_025 | 138090 | MERGE | SKIP | country_conflict, catalog_asymmetry |
| gemini_single_v1 | blind_core_audit_029 | 151521 | MERGE | FLAGGED | - |
| gemini_single_v1 | blind_core_audit_035 | 7324 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry |
| gemini_single_v1 | blind_core_audit_056 | 63829 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| gemini_single_v1 | blind_core_audit_057 | 65678 | SKIP | MERGE | catalog_subset_match, sparse_web |
| gemini_single_v1 | blind_core_audit_060 | 78749 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry |
| gemini_single_v1 | blind_core_audit_062 | 93107 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| gemini_single_v1 | blind_core_audit_064 | 93489 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, sparse_web |
| gemini_single_v1 | blind_core_audit_067 | 103859 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry |
| gemini_single_v1 | blind_core_audit_093 | 140166 | SKIP | MERGE | cross_country_name_match, lexical_near_exact, shared_surname_split_risk |
| gemini_single_v1 | known_missed_merge_patterns_001 | 4784 | MERGE | FLAGGED | - |
| gemini_single_v1 | known_missed_merge_patterns_004 | 22770 | MERGE | SKIP | sparse_web, catalog_subset_match |
| gemini_single_v1 | known_missed_merge_patterns_008 | 67132 | MERGE | FLAGGED | - |
| gemini_single_v1 | known_missed_merge_patterns_009 | 68215 | MERGE | SKIP | sparse_web, catalog_subset_match |
| gemini_single_v1 | known_missed_merge_patterns_010 | 83519 | MERGE | FLAGGED | shared_surname_split_risk, shared_core_token |
| gemini_single_v1 | known_missed_merge_patterns_011 | 88568 | MERGE | FLAGGED | - |
| gemini_single_v1 | known_missed_merge_patterns_016 | 143564 | MERGE | FLAGGED | - |
| gemini_single_v1 | tail_random_sample_002 | 40974 | MERGE | FLAGGED | sparse_web, shared_core_token |
| gemini_single_v1 | tail_random_sample_003 | 136710 | MERGE | FLAGGED | - |
| gemini_single_v1 | tail_random_sample_005 | 139566 | MERGE | FLAGGED | - |
| gemini_single_v1 | tail_random_sample_010 | 33897 | SKIP | MERGE | shared_core_token, sparse_web |
| gemini_single_v1 | tail_random_sample_016 | 107502 | SKIP | MERGE | lexical_short_full_form, shared_surname_split_risk |
| gpt5mini_single_v1 | blind_core_audit_007 | 47775 | MERGE | SKIP | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_012 | 71588 | MERGE | SKIP | shared_core_token, catalog_asymmetry |
| gpt5mini_single_v1 | blind_core_audit_015 | 77833 | MERGE | SKIP | catalog_subset_match, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_017 | 96741 | MERGE | SKIP | shared_surname_split_risk, shared_core_token, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_018 | 100006 | MERGE | FLAGGED | - |
| gpt5mini_single_v1 | blind_core_audit_022 | 123240 | MERGE | SKIP | lexical_near_exact, catalog_subset_match, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_025 | 138090 | MERGE | FLAGGED | - |
| gpt5mini_single_v1 | blind_core_audit_027 | 141172 | MERGE | FLAGGED | lexical_short_full_form, country_conflict, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_029 | 151521 | MERGE | SKIP | lexical_near_exact, catalog_subset_match, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_030 | 156806 | MERGE | SKIP | catalog_subset_match, lexical_near_exact, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_041 | 20479 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_042 | 22194 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_052 | 59385 | SKIP | MERGE | shared_core_token, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_056 | 63829 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_057 | 65678 | SKIP | MERGE | catalog_subset_match, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_060 | 78749 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_061 | 84071 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_062 | 93107 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_064 | 93489 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_066 | 102676 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| gpt5mini_single_v1 | blind_core_audit_067 | 103859 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| gpt5mini_single_v1 | blind_core_audit_069 | 113145 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_001 | 4784 | MERGE | FLAGGED | - |
| gpt5mini_single_v1 | known_missed_merge_patterns_003 | 11105 | MERGE | SKIP | shared_surname_split_risk, catalog_subset_match, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_004 | 22770 | MERGE | FLAGGED | - |
| gpt5mini_single_v1 | known_missed_merge_patterns_005 | 29684 | MERGE | FLAGGED | - |
| gpt5mini_single_v1 | known_missed_merge_patterns_007 | 45402 | MERGE | SKIP | catalog_subset_match, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_008 | 67132 | MERGE | SKIP | shared_surname_split_risk, shared_core_token, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_009 | 68215 | MERGE | SKIP | catalog_subset_match, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_010 | 83519 | MERGE | SKIP | shared_surname_split_risk, shared_core_token, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_011 | 88568 | MERGE | SKIP | shared_core_token, sparse_web |
| gpt5mini_single_v1 | known_missed_merge_patterns_012 | 123759 | MERGE | FLAGGED | - |
| gpt5mini_single_v1 | known_missed_merge_patterns_013 | 4054 | MERGE | SKIP | catalog_subset_match, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | tail_random_sample_002 | 40974 | MERGE | SKIP | shared_core_token, sparse_web |
| gpt5mini_single_v1 | tail_random_sample_003 | 136710 | MERGE | SKIP | country_conflict, shared_surname_split_risk, sparse_web |
| gpt5mini_single_v1 | tail_random_sample_006 | 11935 | SKIP | MERGE | shared_core_token, sparse_web |
| gpt5mini_single_v1 | tail_random_sample_016 | 107502 | SKIP | MERGE | lexical_short_full_form, shared_surname_split_risk, sparse_web |
| haiku_gemini_consensus_v1 | blind_core_audit_001 | 645 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_002 | 4067 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_003 | 13568 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_004 | 13580 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_005 | 25412 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_006 | 37630 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_007 | 47775 | MERGE | FLAGGED | shared_surname_split_risk, catalog_asymmetry, sparse_web, lexical_short_full_form, catalog_subset_match |
| haiku_gemini_consensus_v1 | blind_core_audit_008 | 52229 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_009 | 54064 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_010 | 55533 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_011 | 68320 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_012 | 71588 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_013 | 71929 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_014 | 77572 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_015 | 77833 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_016 | 96369 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_017 | 96741 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_018 | 100006 | MERGE | FLAGGED | shared_surname_split_risk, catalog_asymmetry, sparse_web, catalog_subset_match, shared_surname_split_risk |
| haiku_gemini_consensus_v1 | blind_core_audit_019 | 100909 | MERGE | FLAGGED | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, shared_surname_split_risk, catalog_asymmetry |
| haiku_gemini_consensus_v1 | blind_core_audit_020 | 104522 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_021 | 104541 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_022 | 123240 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_023 | 136068 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_024 | 137389 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_025 | 138090 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_026 | 139102 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_027 | 141172 | MERGE | FLAGGED | lexical_short_full_form, country_conflict, catalog_asymmetry, catalog_asymmetry, sparse_web |
| haiku_gemini_consensus_v1 | blind_core_audit_029 | 151521 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | blind_core_audit_030 | 156806 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_001 | 4784 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_002 | 10596 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_003 | 11105 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_004 | 22770 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_005 | 29684 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_006 | 32548 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_007 | 45402 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_008 | 67132 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_009 | 68215 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_010 | 83519 | MERGE | FLAGGED | shared_surname_split_risk, shared_core_token, shared_surname_split_risk, sparse_web |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_011 | 88568 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_012 | 123759 | MERGE | FLAGGED | shared_surname_split_risk, catalog_subset_match, catalog_asymmetry, sparse_web, catalog_asymmetry |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_013 | 4054 | MERGE | FLAGGED | shared_surname_split_risk, catalog_subset_match, catalog_subset_match, shared_surname_split_risk |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_014 | 136270 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | known_missed_merge_patterns_016 | 143564 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | tail_random_sample_001 | 36774 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | tail_random_sample_002 | 40974 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | tail_random_sample_003 | 136710 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | tail_random_sample_004 | 138971 | MERGE | FLAGGED | - |
| haiku_gemini_consensus_v1 | tail_random_sample_005 | 139566 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_001 | 645 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_002 | 4067 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_003 | 13568 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_004 | 13580 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_005 | 25412 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_006 | 37630 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_007 | 47775 | MERGE | SKIP | shared_surname_split_risk, catalog_asymmetry, sparse_web, lexical_short_full_form, catalog_subset_match |
| haiku_single_v1 | blind_core_audit_008 | 52229 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_009 | 54064 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_010 | 55533 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_011 | 68320 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_012 | 71588 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_013 | 71929 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_014 | 77572 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_015 | 77833 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_016 | 96369 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_017 | 96741 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_018 | 100006 | MERGE | SKIP | shared_surname_split_risk, catalog_asymmetry, sparse_web, catalog_subset_match |
| haiku_single_v1 | blind_core_audit_019 | 100909 | MERGE | FLAGGED | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk, catalog_asymmetry, sparse_web |
| haiku_single_v1 | blind_core_audit_020 | 104522 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_021 | 104541 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_022 | 123240 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_023 | 136068 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_024 | 137389 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_025 | 138090 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_026 | 139102 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_027 | 141172 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_029 | 151521 | MERGE | FLAGGED | - |
| haiku_single_v1 | blind_core_audit_030 | 156806 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_001 | 4784 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_002 | 10596 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_003 | 11105 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_004 | 22770 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_005 | 29684 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_006 | 32548 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_007 | 45402 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_008 | 67132 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_009 | 68215 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_010 | 83519 | MERGE | SKIP | shared_surname_split_risk, shared_core_token, shared_surname_split_risk, sparse_web |
| haiku_single_v1 | known_missed_merge_patterns_011 | 88568 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_012 | 123759 | MERGE | SKIP | shared_surname_split_risk, sparse_web, catalog_asymmetry |
| haiku_single_v1 | known_missed_merge_patterns_013 | 4054 | MERGE | SKIP | shared_surname_split_risk, catalog_subset_match |
| haiku_single_v1 | known_missed_merge_patterns_014 | 136270 | MERGE | FLAGGED | - |
| haiku_single_v1 | known_missed_merge_patterns_016 | 143564 | MERGE | FLAGGED | - |
| haiku_single_v1 | tail_random_sample_001 | 36774 | MERGE | FLAGGED | - |
| haiku_single_v1 | tail_random_sample_002 | 40974 | MERGE | FLAGGED | - |
| haiku_single_v1 | tail_random_sample_003 | 136710 | MERGE | FLAGGED | - |
| haiku_single_v1 | tail_random_sample_004 | 138971 | MERGE | FLAGGED | - |
| haiku_single_v1 | tail_random_sample_005 | 139566 | MERGE | FLAGGED | - |
| sonnet_single_v1 | blind_core_audit_003 | 13568 | MERGE | SKIP | shared_surname_split_risk, catalog_asymmetry, lexical_near_exact, catalog_subset_match |
| sonnet_single_v1 | blind_core_audit_008 | 52229 | MERGE | FLAGGED | - |
| sonnet_single_v1 | blind_core_audit_009 | 54064 | MERGE | FLAGGED | - |
| sonnet_single_v1 | blind_core_audit_011 | 68320 | MERGE | SKIP | shared_surname_split_risk, lexical_short_full_form, catalog_subset_match |
| sonnet_single_v1 | blind_core_audit_013 | 71929 | MERGE | SKIP | shared_surname_split_risk, catalog_asymmetry, lexical_short_full_form |
| sonnet_single_v1 | blind_core_audit_015 | 77833 | MERGE | FLAGGED | - |
| sonnet_single_v1 | blind_core_audit_017 | 96741 | MERGE | FLAGGED | - |
| sonnet_single_v1 | blind_core_audit_019 | 100909 | MERGE | SKIP | shared_surname_split_risk, catalog_subset_match, lexical_short_full_form |
| sonnet_single_v1 | blind_core_audit_021 | 104541 | MERGE | SKIP | catalog_subset_match, catalog_asymmetry |
| sonnet_single_v1 | blind_core_audit_022 | 123240 | MERGE | SKIP | catalog_asymmetry, sparse_web, lexical_near_exact, catalog_subset_match |
| sonnet_single_v1 | blind_core_audit_025 | 138090 | MERGE | SKIP | country_conflict, catalog_asymmetry |
| sonnet_single_v1 | blind_core_audit_030 | 156806 | MERGE | SKIP | lexical_near_exact, catalog_subset_match, catalog_asymmetry, sparse_web |
| sonnet_single_v1 | blind_core_audit_042 | 22194 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, sparse_web |
| sonnet_single_v1 | blind_core_audit_047 | 54025 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry |
| sonnet_single_v1 | blind_core_audit_051 | 59253 | SKIP | MERGE | catalog_subset_match, catalog_asymmetry, shared_surname_split_risk |
| sonnet_single_v1 | blind_core_audit_056 | 63829 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| sonnet_single_v1 | blind_core_audit_062 | 93107 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| sonnet_single_v1 | blind_core_audit_063 | 93353 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, shared_surname_split_risk |
| sonnet_single_v1 | blind_core_audit_064 | 93489 | SKIP | MERGE | lexical_short_full_form, catalog_subset_match, sparse_web |
| sonnet_single_v1 | blind_core_audit_067 | 103859 | SKIP | MERGE | catalog_subset_match |
| sonnet_single_v1 | blind_core_audit_089 | 124166 | SKIP | MERGE | catalog_subset_match, shared_surname_split_risk |
| sonnet_single_v1 | known_missed_merge_patterns_001 | 4784 | MERGE | FLAGGED | - |
| sonnet_single_v1 | known_missed_merge_patterns_005 | 29684 | MERGE | FLAGGED | - |
| sonnet_single_v1 | known_missed_merge_patterns_008 | 67132 | MERGE | FLAGGED | - |
| sonnet_single_v1 | known_missed_merge_patterns_011 | 88568 | MERGE | FLAGGED | - |
| sonnet_single_v1 | known_missed_merge_patterns_012 | 123759 | MERGE | FLAGGED | - |
| sonnet_single_v1 | tail_random_sample_002 | 40974 | MERGE | SKIP | sparse_web |
| sonnet_single_v1 | tail_random_sample_003 | 136710 | MERGE | FLAGGED | - |
| sonnet_single_v1 | tail_random_sample_006 | 11935 | SKIP | MERGE | shared_core_token, sparse_web |
| sonnet_single_v1 | tail_random_sample_016 | 107502 | SKIP | MERGE | lexical_short_full_form, shared_surname_split_risk |
