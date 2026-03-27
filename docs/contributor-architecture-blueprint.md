# Contributor Architecture Blueprint

This document is a concise map of how `pushshift_python` combines API access, analysis helpers, and archival research material.

## High-Level Layers

1. Core library layer (`pushshift_python.py`)
   - The main module contains the API wrapper, data collection helpers, and analysis utilities.
   - Because the module is monolithic, focused changes are safer than wide refactors.
2. Research artifact layer (`ICWSM/`, `TADA/`)
   - Conference-specific notebooks, scripts, and derived materials are archival.
   - Changes here should preserve reproducibility for the original research outputs.
3. Example and helper layer (`Examples/`, `Resources/`)
   - Examples demonstrate library usage and small analyses.
   - Helper files capture reusable utilities and supporting data.
4. HPC execution layer (`Great_Lakes_HPC/`)
   - SLURM submission scripts and cluster-oriented analysis jobs live here.
   - Cluster assumptions should remain explicit and isolated from the core module.

## Key Entry Points

- `pushshift_python.py`: primary library surface
- `Examples/`: small usage examples
- `Great_Lakes_HPC/`: cluster execution artifacts
- `.github/workflows/ci.yml`: formatting, lint, and test validation

## Validation

```bash
python -m pip install --upgrade pip
if [ -f requirements.txt ]; then pip install -r requirements.txt; fi
pytest -q
```

If test data is external, document the assumption rather than committing large artifacts.
