# -*- coding: utf-8 -*-
"""
Merge the different months by person.
"""
import datetime
from operator import and_  # same as &
from functools import reduce

import arrow
import pandas as pd


def make_months(base):
    """
    Given a first, base month, return the list
    of months a person is interviewed.

    Parameters
    ----------
    base: str or Arrow
        if str it should be iso-8601 style for the month: YYYY-mm

    Returns
    -------
    months: [Arrow]

    Notes
    -----
    If you start in month t, months contain
    [t, t+1, t+2, t+3, t+12, t+13, t+14, t+15]
    """
    if not isinstance(base, arrow.Arrow):
        base = arrow.get(base, format='%Y-%m')

    months = [base.replace(months=d) for d in
              [0, 1, 2, 3, 12, 13, 14, 15]]
    return months


def match(left, right, match_funcs):
    """
    Find the common people in left and right
    where match_func is true

    Parameters
    ----------
    left: DataFrame
    right: DataFrame
    match_funcs: [function]
        each should be a binary function. See Notes

    Returns
    -------
    sub_right: DataFrame
        a subset of right

    Notes
    -----
    Each match_func in match_funcs should be a binary
    funciton (takes 2 parameters). Each should return an Index.

    Both left and right should have their indexes set to
        HRHHID, HRHHID2, PULINENO

    Examples
    --------
    >>>def age(left, right):
           '''
           Age must match within -1 to +3 years.
           '''
           age_diff = left['age'] - right['age']
           age_idx = age_diff[(age_diff > -1) & (age_diff < 3)].index
           return age_idx


    >>>common = match(left, right, age)
    """
    idxs = (match_func(left, right) for match_func in match_funcs)
    common = reduce(and_, idxs)
    return right.loc[common]


def merge(dfs):
    """
    Adds the month in sample to the index. Concats along the 0 axis.

    Parameters
    ----------
    dfs: list of DataFrames
    """
    dfs = [df.set_index('HRMIS', append=True) for df in dfs]
    return pd.concat(dfs)

#-----------------------------------------------------------------------------
# Example match functions

def match_age(left, right):
    age_diff = left['PRTAGE'] - right['PRTAGE']
    age_idx = (age_diff[(age_diff > -1) & (age_diff < 3)]).index
    return age_idx


def match_exact(left, right, kind):
    cidx = left.index.intersection(right.index)
    same = left.loc[cidx, kind] == right.loc[cidx, kind]
    same_idx = same[same].index
    return same_idx


def match_sex(left, right):
    sex_idx = match_exact(left, right, kind='PESEX')
    return sex_idx


def match_race(left, right):
    race_idx = match_exact(left, right, kind='PTDTRACE')
    return race_idx


#-----------------------------------------------------------------------------
#

def make_wave_id(df):
    """
    Assign a wave a unique ID (datetime64 for first month in sample). Adds
    that id as a column in df. Allows for easy querying later.

    Parameters
    ----------
    df: DataFrame
        a frame containing observations for specific wave.
    """
    A = slice(None)
    year, month = df.loc[(A, A, A, 1), :].iloc[0][['HRYEAR4', 'HRMONTH']]
    df['wave_id'] = pd.Timestamp(datetime.datetime(year, month, 1))
    return df
