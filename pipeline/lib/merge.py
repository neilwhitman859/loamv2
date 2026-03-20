"""
Core merge library for the Loam wine database.

Provides multi-source matching, additive field merging, and data grade
calculation. Designed to be imported by promotion and import scripts.

Usage:
    from pipeline.lib.merge import MergeEngine

    engine = MergeEngine()
    engine.init()
    producer = engine.match_producer("Domaine de la Romanee-Conti", country_id)
    wine = engine.match_wine(producer["id"], "La Tache")
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.lib.db import get_supabase
from pipeline.lib.normalize import normalize, slugify
from pipeline.lib.resolve import ReferenceResolver, CONFIDENCE_RANK


class MergeEngine:
    """
    3-tier matching engine for producer and wine deduplication.

    Producer matching:
      Tier 1: Exact normalized name match on producers.name_normalized
      Tier 2: Alias match via producer_aliases table
      Tier 3: Fuzzy match via pg_trgm similarity (RPC function)

    Wine matching:
      Tier 1: Key match (LWIN, barcode, or external ID)
      Tier 2: Exact normalized name match within the same producer
      Tier 3: Fuzzy name match within the same producer (RPC function)
    """

    def __init__(self, verbose: bool = True):
        self.sb = get_supabase()
        self.verbose = verbose
        self.resolver = ReferenceResolver(verbose=verbose)
        self._initialized = False

    def init(self):
        """Load all reference data. Must be called before any matching."""
        if self._initialized:
            return
        self.resolver.init_sync()
        self._initialized = True

    # ── Delegation to resolver ───────────────────────────────────

    def resolve_country(self, name):
        return self.resolver.resolve_country(name)

    def resolve_region(self, name, country_id=None):
        return self.resolver.resolve_region(name, country_id)

    def resolve_appellation(self, name, country_id=None):
        return self.resolver.resolve_appellation(name, country_id)

    def resolve_grape(self, name):
        return self.resolver.resolve_grape(name)

    def resolve_publication(self, name):
        return self.resolver.resolve_publication(name)

    def resolve_classification(self, system, level):
        return self.resolver.resolve_classification(system, level)

    def resolve_source_type(self, slug):
        if not slug:
            return None
        return self.resolver.source_types.get(slug)

    # ── Producer Matching ────────────────────────────────────────

    def match_producer(self, name: str, country_id: str | None = None,
                       fuzzy: bool = True, fuzzy_threshold: float = 0.4) -> dict | None:
        """
        Match a producer by name with three-tier matching.

        Returns: { id, name, confidence, match_tier } or None
        """
        if not name:
            return None
        norm = normalize(name)

        # Tier 1: Exact normalized match
        query = (self.sb.table("producers")
                 .select("id,name,name_normalized,country_id")
                 .eq("name_normalized", norm)
                 .is_("deleted_at", "null")
                 .limit(5))
        result = query.execute()
        exact_matches = result.data or []

        if exact_matches:
            same_country = None
            if country_id:
                same_country = next((p for p in exact_matches if p.get("country_id") == country_id), None)
            best = same_country or exact_matches[0]
            return {"id": best["id"], "name": best["name"], "confidence": 1.0, "match_tier": 1}

        # Tier 2: Alias match
        alias_result = (self.sb.table("producer_aliases")
                        .select("producer_id,name")
                        .eq("name_normalized", norm)
                        .limit(5)
                        .execute())
        alias_matches = alias_result.data or []

        if alias_matches:
            producer_id = alias_matches[0]["producer_id"]
            producer_result = (self.sb.table("producers")
                               .select("id,name,country_id")
                               .eq("id", producer_id)
                               .is_("deleted_at", "null")
                               .limit(1)
                               .execute())
            if producer_result.data:
                p = producer_result.data[0]
                return {"id": p["id"], "name": p["name"], "confidence": 0.9, "match_tier": 2}

        # Tier 3: Fuzzy match via pg_trgm RPC
        if fuzzy:
            try:
                rpc_result = self.sb.rpc("match_producer_fuzzy", {
                    "p_name_normalized": norm,
                    "p_country_id": country_id,
                    "p_threshold": fuzzy_threshold,
                    "p_limit": 1,
                }).execute()
                if rpc_result.data and len(rpc_result.data) > 0:
                    best = rpc_result.data[0]
                    return {
                        "id": best["id"],
                        "name": best["name"],
                        "confidence": float(best.get("sim", 0.5)),
                        "match_tier": 3,
                    }
            except Exception:
                pass  # RPC may not exist

        return None

    # ── Wine Matching ────────────────────────────────────────────

    def match_wine(self, producer_id: str, wine_name: str | None = None,
                   lwin: str | None = None, barcode: str | None = None,
                   external_id: str | None = None, external_system: str | None = None,
                   fuzzy: bool = True, fuzzy_threshold: float = 0.4) -> dict | None:
        """
        Match a wine with three-tier matching.

        Returns: { id, name, lwin, confidence, match_tier } or None
        """
        if not producer_id:
            return None

        # Tier 1: Key match — LWIN
        if lwin:
            result = (self.sb.table("wines")
                      .select("id,name,lwin")
                      .eq("lwin", lwin)
                      .is_("deleted_at", "null")
                      .limit(1)
                      .execute())
            if result.data:
                w = result.data[0]
                return {"id": w["id"], "name": w["name"], "lwin": w.get("lwin"),
                        "confidence": 1.0, "match_tier": 1}

        # Tier 1: Key match — barcode
        if barcode:
            result = (self.sb.table("wines")
                      .select("id,name,lwin")
                      .eq("barcode", barcode)
                      .is_("deleted_at", "null")
                      .limit(1)
                      .execute())
            if result.data:
                w = result.data[0]
                return {"id": w["id"], "name": w["name"], "lwin": w.get("lwin"),
                        "confidence": 1.0, "match_tier": 1}

        # Tier 1: Key match — external ID
        if external_id and external_system:
            result = (self.sb.table("external_ids")
                      .select("entity_id")
                      .eq("entity_type", "wine")
                      .eq("system", external_system)
                      .eq("external_id", external_id)
                      .limit(1)
                      .execute())
            if result.data:
                wine_id = result.data[0]["entity_id"]
                wine_result = (self.sb.table("wines")
                               .select("id,name,lwin")
                               .eq("id", wine_id)
                               .is_("deleted_at", "null")
                               .limit(1)
                               .execute())
                if wine_result.data:
                    w = wine_result.data[0]
                    return {"id": w["id"], "name": w["name"], "lwin": w.get("lwin"),
                            "confidence": 1.0, "match_tier": 1}

        # Tier 2: Exact normalized name match within producer
        if wine_name:
            norm = normalize(wine_name)
            result = (self.sb.table("wines")
                      .select("id,name,lwin,name_normalized")
                      .eq("producer_id", producer_id)
                      .eq("name_normalized", norm)
                      .is_("deleted_at", "null")
                      .limit(5)
                      .execute())
            if result.data:
                w = result.data[0]
                return {"id": w["id"], "name": w["name"], "lwin": w.get("lwin"),
                        "confidence": 0.95, "match_tier": 2}

        # Tier 3: Fuzzy name match within producer
        if fuzzy and wine_name:
            try:
                rpc_result = self.sb.rpc("match_wine_fuzzy", {
                    "p_producer_id": producer_id,
                    "p_name_normalized": normalize(wine_name),
                    "p_threshold": fuzzy_threshold,
                    "p_limit": 1,
                }).execute()
                if rpc_result.data and len(rpc_result.data) > 0:
                    best = rpc_result.data[0]
                    return {
                        "id": best["id"],
                        "name": best["name"],
                        "lwin": best.get("lwin"),
                        "confidence": float(best.get("sim", 0.5)),
                        "match_tier": 3,
                    }
            except Exception:
                pass  # RPC may not exist

        return None
