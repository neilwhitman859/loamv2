"""
Supabase client and environment loading for the Loam pipeline.

Usage:
    from pipeline.lib.db import get_supabase, get_env

    sb = get_supabase()
    result = sb.table('wines').select('*').limit(5).execute()
"""

import os
from pathlib import Path
from functools import lru_cache

from dotenv import load_dotenv
from supabase import create_client, Client


def get_env(key: str, required: bool = True) -> str | None:
    """Get an environment variable, loading .env if needed."""
    _ensure_env()
    val = os.environ.get(key)
    if required and not val:
        raise RuntimeError(f"Missing required env var: {key}")
    return val


@lru_cache(maxsize=1)
def _ensure_env():
    """Load .env file from project root."""
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    return True


@lru_cache(maxsize=1)
def get_supabase() -> Client:
    """Get a singleton Supabase client using service role key."""
    _ensure_env()
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_ROLE") or os.environ.get("SUPABASE_ANON_KEY")
    if not url or not key:
        raise RuntimeError("Missing SUPABASE_URL and/or SUPABASE_SERVICE_ROLE in .env")
    return create_client(url, key)


async def fetch_all(table: str, columns: str = "*", filters: dict | None = None,
                    batch_size: int = 1000) -> list[dict]:
    """
    Fetch all rows from a Supabase table, paginating past the 1000-row limit.

    Args:
        table: Table name
        columns: Column selection string (e.g. "id,name,country_id")
        filters: Optional dict of eq filters
        batch_size: Rows per page (max 1000)

    Returns:
        List of row dicts
    """
    sb = get_supabase()
    all_rows = []
    offset = 0
    while True:
        query = sb.table(table).select(columns).range(offset, offset + batch_size - 1)
        if filters:
            for k, v in filters.items():
                query = query.eq(k, v)
        result = query.execute()
        all_rows.extend(result.data)
        if len(result.data) < batch_size:
            break
        offset += batch_size
    return all_rows


def batch_upsert(table: str, rows: list[dict], on_conflict: str | None = None,
                 batch_size: int = 200) -> int:
    """
    Insert/upsert rows in batches. Returns count of rows inserted.

    Args:
        table: Table name
        rows: List of row dicts
        on_conflict: Conflict column(s) for upsert (comma-separated)
        batch_size: Rows per batch
    """
    sb = get_supabase()
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        if on_conflict:
            result = sb.table(table).upsert(batch, on_conflict=on_conflict).execute()
        else:
            result = sb.table(table).insert(batch).execute()
        inserted += len(result.data) if result.data else 0
    return inserted


def batch_insert(table: str, rows: list[dict], batch_size: int = 200) -> int:
    """Insert rows in batches with per-row fallback on error. Returns count inserted."""
    sb = get_supabase()
    inserted = 0
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        try:
            result = sb.table(table).insert(batch).execute()
            inserted += len(result.data) if result.data else len(batch)
        except Exception as e:
            print(f"  Batch {i}-{i+len(batch)} error: {e}")
            # Fall back to one-by-one
            for row in batch:
                try:
                    sb.table(table).insert(row).execute()
                    inserted += 1
                except Exception as row_err:
                    title = row.get("title") or row.get("name") or row.get("wine_name") or "unknown"
                    print(f"  Row error ({title}): {row_err}")
    return inserted
