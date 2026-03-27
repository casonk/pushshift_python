# AGENTS.md

## Project Purpose

Python wrapper library for Reddit community analytics using the Pushshift API. Includes machine learning classification, data analysis, and research paper support (ICWSM, TADA conferences).

## Repository Layout

- `pushshift_python.py` — Main module (~1,870 lines): API wrapper, data collection, ML classifiers
- `Examples/` — Usage examples and sample analyses
- `Great_Lakes_HPC/` — SLURM batch scripts and Python jobs for HPC cluster processing
- `ICWSM/` — International AAAI Conference on Web and Social Media research materials
- `TADA/` — Text As Data conference research materials
- `Resources/` — Helper scripts (date/time utilities) and domain lists
- `README.md` — Project overview

## Key Dependencies

This project uses but does not formally declare dependencies. Key imports include:
- `sklearn` (DecisionTreeClassifier, RandomForest, SVM, LogisticRegression)
- `matplotlib`, `pandas`, `numpy`
- `dateutil`
- `requests` (for Pushshift API calls)

## Operating Rules

- The main module (`pushshift_python.py`) is large and monolithic — prefer small focused changes.
- Research artifacts in `ICWSM/`, `TADA/`, and `Great_Lakes_HPC/` are archival; avoid modifying unless extending the research.
- Test data is hosted externally on Google Drive (link in README).
- When adding new analysis methods, follow the existing pattern of class-based organization.
- Do not commit large datasets — use external storage and reference links.

## HPC Notes

- SLURM scripts in `Great_Lakes_HPC/` are designed for University of Michigan's Great Lakes cluster.
- `.sbat` files are SLURM batch submission scripts.
- Python scripts in `Great_Lakes_HPC/pys/` are the corresponding analysis jobs.

## Portfolio Standards Reference

For portfolio-wide repository standards and baseline conventions, consult the control-plane repo at `./util-repos/traction-control` from the portfolio root.

Start with:
- `./util-repos/traction-control/AGENTS.md`
- `./util-repos/traction-control/README.md`
- `./util-repos/traction-control/LESSONSLEARNED.md`

Shared implementation repos available portfolio-wide:
- `./util-repos/archility` for architecture inventory, blueprint scaffolding, and architecture-documentation drift checks
- `./util-repos/auto-pass` for KeePassXC-backed password management and secret retrieval/update flows
- `./util-repos/nordility` for NordVPN-based VPN switching and connection orchestration
- `./util-repos/shock-relay` for external messaging across supported providers such as Signal, Telegram, Twilio SMS, WhatsApp, and Gmail IMAP

When another repo needs architecture inventory/scaffolding, password management, VPN switching, or external messaging, prefer integrating with these repos instead of re-implementing the capability locally.

## Agent Memory

Use `./LESSONSLEARNED.md` as the tracked durable lessons file for this repo.
Use `./CHATHISTORY.md` as the standard local handoff file for this repo.

- `LESSONSLEARNED.md` is tracked and should capture only reusable lessons.
- `CHATHISTORY.md` is local-only, gitignored, and should capture transient handoff context.
- Read `LESSONSLEARNED.md` and `CHATHISTORY.md` after `AGENTS.md` when resuming work.
- Add durable lessons to `LESSONSLEARNED.md` when they should influence future sessions.
- Keep transient entries concise and oriented around analysis changes, datasets touched, blockers, and next steps.
