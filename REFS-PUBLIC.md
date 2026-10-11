# REFS-PUBLIC.md - Public References

> Record external public repositories, datasets, documentation, APIs, or other
> public resources that this repository utilizes or depends on.
> This file is tracked and intentionally kept free of private or local-only details.

## Public Repositories

- No fixed external code repository is the main upstream; the repo centers on public Reddit data surfaces.

## Public Datasets and APIs

- https://pushshift.io/ - Pushshift API used for Reddit data collection
- https://api.pushshift.io/reddit/search/ - Pushshift Reddit search endpoint used by query construction
- https://files.pushshift.io/reddit/ - downloadable Pushshift Reddit data archives referenced by the file-ingestion workflow
- https://www.reddit.com/dev/api/ - Reddit OAuth and API reference for subreddit refresh helpers

## Documentation and Specifications

- No single public specification beyond the API surfaces above is required at the repo level; research workflows vary by experiment.
- https://requests.readthedocs.io/ - Requests HTTP client reference used by Reddit and Pushshift fetch helpers
- https://pandas.pydata.org/docs/ - pandas tabular-data processing reference used by research notebooks and scripts
- https://numpy.org/doc/ - NumPy array-processing reference used by analysis workflows
- https://scikit-learn.org/stable/ - scikit-learn modeling reference used by research analysis workflows
- https://matplotlib.org/stable/contents.html - Matplotlib plotting reference used by visualization workflows
- https://networkx.org/documentation/stable/ - NetworkX graph-analysis reference used by community and network analysis workflows
- https://seaborn.pydata.org/ - seaborn statistical visualization reference used by research analysis workflows
- https://dateutil.readthedocs.io/ - python-dateutil parsing reference used by timestamp handling
- https://python-zstandard.readthedocs.io/ - zstandard decompression reference used by Pushshift archive ingestion

## Notes

- Local research exports, models, and cluster jobs are repo-specific and stay out of this tracked public reference file.
