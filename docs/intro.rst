Introduction to PyCPS
=====================

PyCPS is a package for working with the `Current Population Survey`_

.. _Current Population Survey: http://www.census.gov/cps/


I wrote PyCPS to as part of a project using CPS data.
I wanted my results to be reproducable, and a big part of that involves
getting the data. The CPS doesn't have an API to use, so this is the result.

The CPS
=======

This is a very brief overview of the CPS, you can get more detailed explanations
on the `Census Bureau's`_ or `BLS's`_ websites.
A selected household is interviewed for four consecutive months,
exits the survey for the next eight months, and then returns to be surveyed
for a final four months. In total, a household is interviewed for eight
months, spread over a sixteen month period.

.. _Census Bureau's: http://www.census.gov/cps
.. _BLS's: http://www.bls.gov/cps/

The basic goal of this package is to construct a somewhat consistent
timeseries from the monthly CPS files.
This goal is complicated by the fact that the CPS wasn't really designed to be a
longitudinal dataset.

There's a few related functions PyCPS provides:

1. Download data dictionaries and monthly data files
2. Standardize features across months as much as possible
3. Merge households across months to create a time series

