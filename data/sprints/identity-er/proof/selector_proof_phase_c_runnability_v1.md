# Phase C shortlist smoke status

- status: `blocked`

No reusable `shortlist_generation_v1` runner exists yet outside the proof-bundle scaffolding. The repo has the frozen manifest plus build-time helper internals in `pipeline/identity/selector_proof_v1.py`, but no standalone shortlist builder that can be run honestly on the 48 proof anchors without inventing new code mid-proof.

## Evidence

- Only `pipeline/identity/selector_proof_v1.py` references `shortlist_generation_v1` in executable code.
- `data/sprints/identity-er/proof/phase_c_shortlist_manifest.json` is a frozen expectation object, not a generated shortlist run.
- No other `pipeline/identity/*.py` file provides a reusable shortlist-builder entrypoint for Session 10.7.
