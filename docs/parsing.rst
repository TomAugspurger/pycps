Parsing
=======

The api for parsing is again pretty simple::

    from pycps import api

    api.parse()

Or more likely you'll use it from the command line.
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

The fixups to be applied are just python functions that take and return a DataFrame.
By default, the functions come from the file ``pycps.data_dictionary_fixups``. Look
in there to see example fixups.

The fixup functions must follow this naming scheme:  ``<dd_name>_<function_id>``
``dd_name`` is the usual data dictionary name used throughout the project, with
underscores substituded for hyphens. For example, ``cpsm1994_0`.
The data dictionary name is followed by an underscroe and then an identifier
for that function. I recommend including the starting column number for the error,
and a breif description. So for the above example with the August 2004
data dictionary having the wrong location for the filler, the function would be
``cpsm2004_08_filler_411``.

Functions from the default ``data_dictionary_fixups.py`` and any user supplied functions
are applied. Which function to apply to which data dictionary is determined automatically
from the function name. They are applied in order.


Monthly Files
-------------

Monthly files may also require some adjustment to make them consistent acros data dictoinaries.
The default fixups are in ``pycps/monthly_data_fixups.py``. There's a couple rules for
defining additional fixups:

1. You must export the fixups in a dictionary ``FIXUP_BY_DD``.
2. The dictionary's keys should be data dictionary names (e.g. ``cpsm1994-04``)
3. The dictionary's values should be tuples of (functions, kwargs) where kwargs
is the usual dictionary mapping keywords to values.
4. Each function defined should have a signature expecting ``(DataFrame, *args, **kwargs)``
5. Each funciton should return a DataFrame

Sorry about all the restrictions. But this is the best I've come up with for now.

If you see some fixups that would be useful for anyone doing analysis on CPS data,
please submit a Pull Request on GitHub adding the fixup to ``monthly_data_fixups``
or ``data_dictionary_fixups``.
