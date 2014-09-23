# -*- coding: utf-8 -*-
"""
Module for fixing up data dictionary mistakes.
Each function should start with the pattern:

    <dd_name>_<function_id>

For <function_id> I recommend <line_number_description>.
For example: ``m1998_01_535_unknown``, where <dd_name>
is ``m1998_01``, or ``mYYYY_mm``.

Functions from this file are applied to the data dictionary
in ``DDParser.make_consistent``.

Each function should expect a DataFrame and return a fixed DataFrame
whose columns are ("id", "length", "start", "end")
"""
import pandas as pd


def _insert_unknown(df, start, end):
    """
    For DRYness.
    start : where the unknown field starts (last known end + 1)
    end : where the unknown field ends (next known start - 1)
    """
    length = end - start + 1
    good_low = df.loc[:df[df.end == (start - 1)].index[0]]
    good_high = df.loc[df[df.start == (end + 1)].index[0]:]
    fixed = pd.concat([good_low,
                       pd.DataFrame([['unknown', length, start, end]],
                                    columns=['id', 'length', 'start',
                                             'end']),
                       good_high],
                      ignore_index=True)
    return fixed


def cpsm1998_01_149_unknown(formatted):
    return _insert_unknown(formatted, 149, 150)


def cpsm1998_01_535_unknown(formatted):
    return _insert_unknown(formatted, 536, 539)


def cpsm1998_01_556_unknown(formatted):
    return _insert_unknown(formatted, 557, 558)


def cpsm1998_01_632_unknown(formatted):
    return _insert_unknown(formatted, 633, 638)


def cpsm1998_01_680_unknown(formatted):
    return _insert_unknown(formatted, 681, 682)


def cpsm1998_01_786_unknown(formatted):
    return _insert_unknown(formatted, 787, 790)


def cpsm2004_05_filler_411(formatted):
    """
    See below
    """
    fixed = formatted.copy()
    fixed.loc[185] = ('FILLER', 2, 410, 411)
    return fixed


def cpsm2004_08_filler_411(formatted):
    """
    See below
    """
    fixed = formatted.copy()
    fixed.loc[185] = ('FILLER', 2, 410, 411)
    return fixed


def cpsm2005_08_filler_411(formatted):
    """
    Mistake in Data Dictionary:

    FILLER          2                                      (411 - 412)

    should be:

    FILLER          2                                      (410 - 411)

    Everything else looks ok.
    """
    fixed = formatted.copy()
    fixed.loc[185] = ('FILLER', 2, 410, 411)
    return fixed


def cpsm2009_01_filler_399(formatted):
    assert formatted.loc[399].values.tolist() == ['FILLER', 45, 932,
                                                  950]
    fixed = formatted.copy()
    fixed.loc[399] = ('FILLER', 19, 932, 950)
    return fixed


def cpsm2012_05_remove_filler_114(formatted):
    """
    Says

    FILLER  2 Starting February 2004    114 - 115

    which is wrong
    """
    fixed = pd.concat([formatted.loc[:42], formatted.loc[44:]],
                      ignore_index=True)
    return fixed


def cpsm2012_05_insert_filler_637(formatted):
    """
    The filler is in the file (line 4253) but it's indented for reasons
    """
    return _insert_unknown(formatted, start=637, end=638)
