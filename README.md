PyCPS
=====

A python package for working with the [Current Population Survey](http://www.census.gov/cps/).

Documentation is available at [readthedocs](http://pycps.readthedocs.org/en/latest/).

Neither the Census Bureau nor the NBER provide a clean, RESTful API for getting CPS data.
This makes working with the CPS a pain, and reproducibility nearly impossible.

## What does it do?

There's a few related functions PyCPS provides:

1. Downloading data dictionaries and monthly data files
2. Standardizing variables across months
3. Merging to create time series

## Installation

- From pip: `pip install pycps`
- From source:

    ```
    git clone https://github.com/TomAugspurger/pycps
    cd pycps
    pip install .
    pip install -r requirements.txt
    pip install git+https://github.com/PyTables/PyTables
    python setup.py install
    ```

## Dependencies

See `requirements.txt`

## Python

Developed with python 3, aims to be compatible with python 2.
