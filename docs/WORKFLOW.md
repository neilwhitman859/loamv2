# Loam — Workflow Guide

How to work with Claude Code on this project. Keep this open or bookmarked.

---

## Starting a Session

1. Open Claude Code (any machine — Flipper, browser, Chrome Remote Desktop)
2. Claude reads `CLAUDE.md` automatically and gives you a briefing:
   - What happened last session
   - Current DB state (queried live)
   - Open items
   - Suggested next step
3. Confirm the plan or redirect to something else

If the briefing is wrong or missing context, tell Claude. It means `CLAUDE.md` needs updating.

---

## During a Session

- **Work normally.** Claude handles the bookkeeping:
  - Updates `CLAUDE.md` at natural breakpoints (pipeline completions, schema changes, direction shifts)
  - Logs significant decisions to `docs/DECISIONS.md` automatically
  - Updates `docs/SCHEMA.md` when the database schema changes
  - Commits after meaningful milestones
- **If Claude isn't logging something it should:** say **"log that"** — forces a `DECISIONS.md` entry
- **If you want a status check mid-session:** say **"briefing"** — get current state summary
- **If Claude nudges you about updating docs or committing:** listen to it. That's by design.

---

## Ending a Session

1. Say **"wrap up"**
2. Claude will:
   - Update `CLAUDE.md` with final state and next steps
   - Update `DECISIONS.md` if any unlogged decisions
   - Commit and push to GitHub
3. You're done. Next session picks up from the files.

---

## Switching Machines

Nothing special. Just start a new session on the other machine. `CLAUDE.md` has everything Claude needs. The whole point of this system is that context lives in files, not in conversation history.

---

## Key Phrases

| Say this | What happens |
|---|---|
| **"wrap up"** | End-of-session routine: update files, commit, push |
| **"log that"** | Force a `DECISIONS.md` entry for something Claude didn't catch |
| **"briefing"** | Get current state summary anytime |

---

## If Something Feels Off

- Claude making decisions that contradict earlier ones → check `docs/DECISIONS.md`, the answer should be there. If not, the decision wasn't logged. Log it now.
- Claude's briefing has wrong numbers → it should be querying the DB live. If it's using stale data from `CLAUDE.md`, tell it to query.
- A doc feels outdated → tell Claude to update it. The files are meant to stay current.
