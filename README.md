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
pushshift_python.py    # Main module — API wrapper, ML, analysis
Examples/              # Usage examples
Great_Lakes_HPC/       # SLURM batch scripts for HPC
ICWSM/                 # AAAI Web & Social Media research
TADA/                  # Text As Data research
Resources/             # Helper scripts, domain lists
```

## Installation

```bash
pip install -r requirements.txt
```

## Test Data

Available on [Google Drive](https://drive.google.com/drive/folders/13gL5B8e0Onml_j30iT6OVM2kIkOlJvO1).

## Documentation

[documentation.ipynb](https://github.com/casonk/pushshift_python/blob/master/documentation.ipynb)

## License

[MIT](LICENSE)
