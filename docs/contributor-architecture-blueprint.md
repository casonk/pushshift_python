# Contributor Architecture Blueprint

This document is a concise map of how `pushshift_python` exposes a stable import surface while delegating collection, analytics, and research workflows to the internal module stack.

## High-Level Layers

1. Public library surface (`pushshift_python.py`)
   - The top-level module is a facade that re-exports the working symbols from the `_pushshift_*` modules.
   - Preserve that import surface when refactoring internals so downstream notebooks, scripts, and research artifacts do not break.
2. Collection layer (`_pushshift_queries.py`, `_pushshift_api.py`, `_pushshift_subreddits.py`)
   - `query` normalizes shared query metadata and post fields.
   - `pushshift_file_query` parses local monthly `.zst` archives into DataFrames or CSVs.
   - `pushshift_web_query` pages through the Pushshift HTTP API for comments and submissions.
   - `api_agent` and `subreddits` handle Reddit OAuth-backed subreddit inventory refreshes and filtering.
3. Analytics layer (`_pushshift_community.py`, `_pushshift_modeling.py`, `_pushshift_scalers.py`, `_pushshift_file_handler.py`)
   - `community` converts collected post data into author, reference, and feature views.
   - `modeling` trains sklearn classifiers and stores prediction, confusion-matrix, PR, and ROC artifacts.
   - Scalers and file utilities support feature normalization and bulk CSV assembly.
4. Consumer layer (`Examples/`, `Great_Lakes_HPC/`, `ICWSM/`, `TADA/`, `Resources/`)
   - Examples and notebooks exercise the public facade interactively.
   - Great Lakes scripts operationalize collection and metric generation for cluster workloads.
   - `ICWSM/` and `TADA/` are archival research outputs that depend on stable collected datasets and analysis flows.
   - `Resources/Scripts/` provides date-partition helpers used by batch workflows.

## Key Flows

- Pushshift archive path: local `.zst` files -> `pushshift_file_query` -> collected DataFrame/CSV -> `community` feature engineering -> `modeling` or research outputs
- Pushshift API path: `pushshift_web_query` -> paginated Pushshift HTTP responses -> collected DataFrame/CSV -> `community` feature engineering -> `modeling` or research outputs
- Subreddit inventory path: `api_agent` -> Reddit OAuth API -> `subreddits.master` -> downstream subreddit filtering and selection
- Batch execution path: `Great_Lakes_HPC/pys/` + `Great_Lakes_HPC/sbats/` -> public facade imports + `Resources/Scripts/` helpers -> generated metrics and research-ready artifacts

## Key Entry Points

- `pushshift_python.py`: public import surface used by notebooks, tests, and batch jobs
- `_pushshift_queries.py`: core collection logic for file-backed and web-backed acquisition
- `_pushshift_community.py`: community statistics, labels, and feature matrix creation
- `_pushshift_modeling.py`: classifier training and evaluation
- `Great_Lakes_HPC/pys/`: operational batch jobs for cluster runs
- `Examples/documentation.ipynb`: interactive usage reference
- `.github/workflows/ci.yml`: lint and test validation

## Validation

```bash
python -m pip install --upgrade pip
if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
MPLCONFIGDIR=/tmp/matplotlib pytest -q
```

If test data is external, document the assumption rather than committing large artifacts.
