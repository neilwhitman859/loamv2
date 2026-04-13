"""Centralized Anthropic model ID constants.

All pipeline scripts that call the Anthropic API should import from here
instead of hardcoding model strings. This makes model upgrades a one-line
change and prevents stale references (Sprint 3 Track 5, S2.5 F5).

Edge function model IDs are maintained separately in
supabase/functions/enrich-wine/index.ts (TypeScript).
"""

# Current model IDs — update these when Anthropic releases new versions
HAIKU_MODEL = "claude-haiku-4-5-20251001"
SONNET_MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-6"
