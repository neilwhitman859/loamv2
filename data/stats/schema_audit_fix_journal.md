# Schema Audit Fix Journal

Append-only log of what the nightly auto-fix has done, what worked, and what
should be improved. Read this before changing the fix logic — it prevents
repeating mistakes and records useful threshold calibrations.

---

### 2026-04-07 (manual run — first --fix execution)
- **VACUUMed:** source_enofile, source_lcbo (both were >10% dead)
- **Indexes created:** 0 (all 36 had been added earlier this session)
- **What worked:** autocommit fix (conn.commit() before setting autocommit=True) resolved psycopg2 ProgrammingError
- **What to improve:** The VACUUM threshold is 10% — seems right. Tables like source_kansas_brands (19%) were already vacuumed manually earlier and dropped below threshold cleanly.
- **Notes:** FK index creation was idempotent — IF NOT EXISTS means re-running on a clean DB is a no-op, which is the right behavior.

---
