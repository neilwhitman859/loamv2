"""Serper.dev (Google SERP API) wrapper for wine producer context fetching.

Single-purpose: given a producer name (and optional country), return
snippet/knowledge-graph context useful for identity disambiguation by LLM.

Cost: $0.001/query at Starter tier. ~2 queries per dedup pair.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from typing import Optional

import requests

from pipeline.lib.db import get_env


SERPER_URL = "https://google.serper.dev/search"
CACHE_DIR = Path(__file__).resolve().parents[2] / "data" / "stats" / "serper_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

_credit_lock = Lock()
_credits_used = {"n": 0}


@dataclass
class SerperResult:
    query: str
    knowledge_graph: Optional[dict] = None
    organic: list[dict] = field(default_factory=list)
    error: Optional[str] = None
    cached: bool = False
    credits: int = 0

    def as_snippet_block(self, max_organic: int = 3, max_snippet_chars: int = 200) -> str:
        """Compact text block for injection into LLM prompt."""
        lines = [f"Search: {self.query!r}"]
        if self.knowledge_graph:
            kg = self.knowledge_graph
            lines.append(
                f"  KG: {kg.get('title', '')!r} ({kg.get('type', '')}) — "
                f"{kg.get('website', '(no website)')}"
            )
            desc = (kg.get("description") or "")[:max_snippet_chars]
            if desc:
                lines.append(f"      {desc}")
        for i, o in enumerate(self.organic[:max_organic], start=1):
            title = (o.get("title") or "")[:120]
            link = o.get("link", "")
            # Compact link — just domain and path
            lines.append(f"  [{i}] {title!r}")
            lines.append(f"      {link[:100]}")
            snippet = (o.get("snippet") or "")[:max_snippet_chars]
            if snippet:
                lines.append(f"      {snippet}")
        return "\n".join(lines)


def _cache_key(query: str, num: int) -> str:
    import hashlib
    h = hashlib.sha1(f"{query}|{num}".encode("utf-8")).hexdigest()[:16]
    return h


def _load_cache(key: str) -> Optional[dict]:
    p = CACHE_DIR / f"{key}.json"
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cache(key: str, data: dict) -> None:
    p = CACHE_DIR / f"{key}.json"
    p.write_text(json.dumps(data, default=str), encoding="utf-8")


def search(query: str, num: int = 5, country: Optional[str] = None,
           use_cache: bool = True, timeout: int = 20,
           max_retries: int = 3) -> SerperResult:
    """Execute a Serper search.

    - Caches responses to disk (same query + num = cached)
    - Retries on transient errors (429, 5xx) with exponential backoff
    - Returns SerperResult with knowledge_graph, organic, etc.
    """
    ck = _cache_key(query, num)
    if use_cache:
        cached = _load_cache(ck)
        if cached:
            return SerperResult(
                query=query,
                knowledge_graph=cached.get("knowledgeGraph"),
                organic=cached.get("organic", []),
                cached=True,
                credits=0,
            )

    key = get_env("SERPER_API_KEY")
    if not key:
        return SerperResult(query=query, error="no SERPER_API_KEY")

    payload = {"q": query, "num": num}
    if country:
        payload["gl"] = country.lower()  # e.g. "us", "fr", "it"

    for attempt in range(max_retries):
        try:
            r = requests.post(
                SERPER_URL,
                headers={"X-API-KEY": key, "Content-Type": "application/json"},
                json=payload,
                timeout=timeout,
            )
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            j = r.json()
            credits = int(j.get("credits", 1))
            with _credit_lock:
                _credits_used["n"] += credits
            # Cache
            _save_cache(ck, j)
            return SerperResult(
                query=query,
                knowledge_graph=j.get("knowledgeGraph"),
                organic=j.get("organic", []),
                credits=credits,
            )
        except requests.RequestException as e:
            if attempt == max_retries - 1:
                return SerperResult(query=query, error=str(e)[:200])
            time.sleep(2 ** attempt)
    return SerperResult(query=query, error="max retries exceeded")


def producer_context(name: str, country: Optional[str] = None,
                      num: int = 5) -> SerperResult:
    """Fetch Serper context for a producer name.

    Uses a "wine producer" disambiguation query — biases Google toward
    returning wine-related results even when the name is ambiguous.
    """
    base = name.strip()
    # Add "wine producer" or "winery" to bias toward wine results
    if country and country.upper() in ("US",):
        query = f"{base} winery"
    else:
        query = f"{base} wine producer"
    return search(query, num=num, country=country)


def credits_used() -> int:
    """Total credits used this process."""
    with _credit_lock:
        return _credits_used["n"]


if __name__ == "__main__":
    import sys
    q = sys.argv[1] if len(sys.argv) > 1 else "Berry Bros & Rudd"
    result = producer_context(q)
    print(result.as_snippet_block())
    print(f"\ncredits so far: {credits_used()}, cached: {result.cached}")
