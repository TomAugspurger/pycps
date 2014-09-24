# -*- coding: utf-8 -*-
"""
A collection of functions for modifying the actual data files.

The global variable ``FIXUP_BY_DD` is a dictionary for dispatching
the fixups. The keys are data dictionary names and the values
are list of tuples of (functions, kwargs) to be applied.
kwargs should be a dict.

Each function should take a DataFrame as the first argument,
and *args and **kwargs for future flexability, though in
practice only kwargs should be used.
They should all return a DataFrame.

The functions will be applied in order.
"""
import logging

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


def compute_hrhhid2(df, *args, **kwargs):
    """
    pre may2004 need to fill out the ids by creating HRHHID2 manually:
    (ignore the position values, this is from jan2013)

    HRHHID2        5         HOUSEHOLD IDENTIFIER (part 2) 71 - 75

         EDITED UNIVERSE:    ALL HHLD's IN SAMPLE

         Part 1 of this number is found in columns 1-15 of the record.
         Concatenate this item with Part 1 for matching forward in time.

         The component parts of this number are as follows:
         71-72     Numeric component of the sample number (HRSAMPLE)
         73-74     Serial suffix-converted to numerics (HRSERSUF)
         75        Household Number (HUHHNUM)

    NOTE: not documented but sersuf of -1 seems to map to '00'
    """
    import string

    hrsample = df['HRSAMPLE'].str.extract(r'(\d+)')
    hrsersuf = df['HRSERSUF'].astype(str)

    huhhnum = df['HUHHNUM'].replace(-1, np.nan)

    to_drop = df[pd.isnull(huhhnum)]
    if to_drop.shape[1] > 0:
        logger.info("Dropping {}".format(to_drop.index))
    huhhnum = huhhnum.dropna().astype(str)

    sersuf_d = {a: str(ord(a.lower()) - 96).zfill(2) for a in hrsersuf.unique()
                if a in list(string.ascii_letters)}
    sersuf_d['-1.0'] = '00'
    sersuf_d['-1'] = '00'
    hrsersuf = hrsersuf.map(sersuf_d)  # 10x faster than replace

    hrhhid2 = hrsample + hrsersuf + huhhnum
    to_drop = df[pd.isnull(hrhhid2)]
    if to_drop.shape[1] > 0:
        logger.info("Dropping {}".format(to_drop.index))

    hrhhid2 = hrhhid2.dropna()
    df = df.copy()
    df['HRHHID2'] = hrhhid2.str.strip('.0').astype(np.int64)
    return df


def year2_to_year4(df, *args, **kwargs):
    """
    Some years are encoded as two digits.

    Parameters
    ----------
    df : DataFrame
    prefix: First two digits of the year; default '19'

    Returns
    -------
    fixed : DataFrame
    """
    prefix = str(kwargs.get('prefix', '19'))
    df['HRYEAR4'] = (prefix + df.HRYEAR4.astype(str)).astype(np.int64)
    return df


FIXUP_BY_DD = {"cpsm1989-01": [],
               "cpsm1992-01": [],
               'cpsm1994-01': [(compute_hrhhid2, {}), (year2_to_year4, {})],
               'cpsm1994-04': [(compute_hrhhid2, {}), (year2_to_year4, {})],
               'cpsm1995-06': [(compute_hrhhid2, {}), (year2_to_year4, {})],
               'cpsm1995-09': [(compute_hrhhid2, {}), (year2_to_year4, {})],
               'cpsm1998-01': [(compute_hrhhid2, {})],
               'cpsm2003-01': [(compute_hrhhid2, {})],
               "cpsm2004-05": [],
               "cpsm2005-08": [],
               "cpsm2005-11": [],
               "cpsm2007-01": [],
               "cpsm2009-01": [],
               "cpsm2010-01": [],
               "cpsm2012-05": [],
               "cpsm2013-01": [],
               "cpsm2014-01": []}
