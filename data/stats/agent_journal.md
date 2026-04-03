# Data Accuracy Agent — Journal

This file is read and appended by the nightly data-accuracy-agent. It tracks learnings, patterns, and focus decisions across runs. The agent should read this at the start of every session and append a new entry at the end.

## How to use this journal

At the START of each run:
- Read the last 5 entries to understand recent patterns
- Check if previous recommendations were acted on (compare to stats)
- Adjust phase time allocation based on what's been productive vs diminishing returns

At the END of each run, append an entry:
```
### {date} — Run #{n}
**Duration:** X min
**Key numbers:** wines X, grapes X, readiness X
**What worked:** (which phases produced the most value)
**What didn't:** (which phases hit diminishing returns or found nothing)
**Patterns:** (recurring issues, root causes identified)
**Unresolved backlog:** (grape names, producer names, data conflicts that need human attention)
**Focus shift for next run:** (what to prioritize or deprioritize tomorrow)
```

---

(No entries yet — first run pending)
