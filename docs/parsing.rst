Parsing
=======

The api for parsing is again pretty simple::

    from pycps import api

    api.parse()

Again, this takes settings from ``settings.json`` and applies the
functions in the actual library to the data downloaded.

Standardization
===============

A key part to constructing the timeseries is making variables consistent
across the different months and, potentially, different survey designs.

There are two set of data (data dictionaries and montly files), and so two sets
of transformations to apply.

Data Dictionaries
-----------------

There are two basic types of transformations on the data dictionaries:

1. name changes
2. data changes

The first is a simple renaming from one data dictionary to the next.
In ``pycps``, the canonical names are from the January 2013 data dictionary,
and all others are mapped to that.
The renamings can be specified in ``data.json``, in the ``col_rename_by_dd`` field.
There's a key for each data dictionry.
The values are mappings from the old name to the new name.
For example, January 2010 renames the age variable from ``PEAGE`` to ``PRTAGE``:

    "cpsm2010-01": {"PEAGE": "PRTAGE"}

The second kind of transformation, data changes, involve a bit more work.
This is where you fix mistakes or inconsistencies in the actual data dictionaries themselves.

For example, in the August 2004 data dictionary, the CPS incorrectly defines the Filler located
in columns 410 and 411.
To fix these, you need to define a python function that takes a DataFrame of formatted lines as
input, and returns the corrected DataFrame.
A formatted line is a 4-tuple of ``(field name, width, start, end)``, like
``('FILLER', 2, 410, 411)``.
Each data dictionary will have a list of functions (possibly empty) to apply.

The functions should be defined as a closure in DDParser.make_consistent and added to the
dictionary in ``dispatch``.
If you find a necessary fix, please submit it as a pull request on Github.

Monthly Files
-------------

Monthly files also require some adjustment to make them consistent acros data dictoinaries.
``pycpys.parsing.py`` contains a function, ``fixup_by_dd`` that achieves this.
Again, you define a closure inside ``fixup_by_dd`` and add that to the list of functions for that dictionary.
Each function should take a parsed monthly file, contained in a DataFrame, as the input and return a fixed DataFrame.