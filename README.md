PyCPS
=====

A python package for working with the [Current Population Survey](http://www.census.gov/cps/).

**Warning** this is alpha quality. The API will change. Docs are coming.

## Why?

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
    ```

## Dependencies

See `requirements.txt`

Installing via `pip` will take care of most everything, however I've had
some troubles automating the installation of pytables. I recommend installing
that separately with `pip instsall tables`.


## Python

Developed with python 3, aims to be compatible with python 2.
