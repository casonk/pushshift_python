# LESSONSLEARNED.md

Tracked durable lessons for `pushshift_python`.
Unlike `CHATHISTORY.md`, this file should keep only reusable lessons that should change how future sessions work in this repo.

## How To Use

- Read this file after `AGENTS.md` and before `CHATHISTORY.md` when resuming work.
- Add lessons that generalize beyond a single session.
- Keep entries concise and action-oriented.
- Do not use this file for transient status updates or full session logs.

## Lessons

- Treat `pushshift_python.py` as a stable import facade over the `_pushshift_*` modules. When documenting or changing architecture, describe the underlying query, analytics, and utility modules instead of calling the repo monolithic.
