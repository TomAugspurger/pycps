Data Dictionary Modifications
=============================

We have to fix the CPS's mistakes and inconsistencies in the data dictionaries.
The first class of fixes is just plain mistakes: wrong length, etc.

For example in `cpsm2005-08.ddf`, line 2421 is

    FILLER          2                                          (411 - 412)

but should be

    FILLER          2                                          (410 - 411)

## Fixup Functions

In DDParser there's a method, `special` that's contains a bunch of closures (defined by you or the library) and a dictionary dispatching those by name.
There's a key per data dictionary, and value is a list of fixup-functoins to be applied in that order.

api.................

    ('FILLER', 2, 411, 412)

The closures should assert that they actually get what is expected.
They should define the correct formatted value and return than.
Please submit any additional fixes as Pull Requests, and document them below.

## List of Fixups

Data Dictionary | Line | Note
----------------|------|-----
cpsm2005-08.ddf | 2421 | Wrong stated start and end. Changed to 410 - 411
