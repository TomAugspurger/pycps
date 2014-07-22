Introduction to PyCPS
=====================

PyCPS is a package for working with the [Current Population Survey](http://www.census.gov/cps/).


PyCPS package tries to make up for neither the Census Bureau nor the NBER provide a clean, RESTful API for getting CPS data.
This makes working with the CPS a pain, and reproducibility nearly impossible.

The basic goal is to construct a somewhat consistent timeseries from the monthly
CPS files.
This goal is complicated by the fact that the CPS wasn't really designed to be a
longitudinal dataset.

There's a few related functions PyCPS provides:

1. Downloading data dictionaries and monthly data files
2. Parse those files
3. Standardizing variables across months
4. Merging to create time series

