# -*- coding: utf-8 -*-
"""
Merge the different months by person.
"""
from operator import and_  # same as &
from functools import reduce

import arrow

def make_months(base):
    """
    Given a first, base month, return the list
    of months a person is interviewed.

    Parameters
    ----------
    base: str or Arrow
        if str it should be iso-8601 style for the month
        YYYY-mm

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

#-----------------------------------------------------------------------------
# Example merge funcs
# TODO: should these be locing on left or right?
def match_age(left, right):
    age_diff = left['PRTAGE'] - right['PRTAGE']
    age_idx = age_diff[(age_diff > -1) & (age_diff < 3)].index
    return age_idx


def match_sex(left, right):
    return left['PESEX'][(left['PESEX'] == right['PESEX'])].index


def match_race(left, right):
    return left['PTRACE'][(left['PTRACE'] == right['PTRACE'])].index