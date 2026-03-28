# pushshift_python

A Python wrapper for Reddit community analytics via the [Pushshift API](https://pushshift.io/). Includes data collection, ML classification, and research support.

## Features

- Reddit data collection via Pushshift API
- ML classifiers (Decision Tree, Random Forest, SVM, Logistic Regression)
- Statistical analysis with sklearn, pandas, numpy
- Network analysis with NetworkX
- HPC support (SLURM scripts for Great Lakes cluster)

## Structure

```
pushshift_python.py      # Public facade that re-exports the internal library surface
_pushshift_queries.py    # Shared query base plus file/web collectors
_pushshift_community.py  # Community analytics and feature engineering
_pushshift_modeling.py   # sklearn-based model training and evaluation
_pushshift_api.py        # Reddit OAuth helper for subreddit refreshes
_pushshift_subreddits.py # Subreddit inventory filtering utilities
Great_Lakes_HPC/       # SLURM batch scripts for HPC
Resources/             # Date partition helpers and supporting data
Examples/              # Usage examples
ICWSM/                 # AAAI Web & Social Media research
TADA/                  # Text As Data research
tests/                 # Public API and HPC smoke coverage
```

## Installation

```bash
pip install -e .
```

The project currently targets Python 3.10+.

For a non-editable local environment, `pip install .` also works.

## Test Data

Available on [Google Drive](https://drive.google.com/drive/folders/13gL5B8e0Onml_j30iT6OVM2kIkOlJvO1).

## Documentation

[Examples/documentation.ipynb](Examples/documentation.ipynb)

## Development

Run the test suite with:

```bash
MPLCONFIGDIR=/tmp/matplotlib pytest -q
```

## License

[MIT](LICENSE)
