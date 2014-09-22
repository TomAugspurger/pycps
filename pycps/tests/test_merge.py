# -*- coding: utf-8 -*-
import datetime
import unittest

import arrow
import pandas as pd
import pandas.util.testing as tm

from pycps import merge as m


class TestMerge(unittest.TestCase):

    def setUp(self):
        """
        Defines age, sex, race.

        The rows should match *only* on:
        match age
        match sex
        match race
        match age, sex
        match age, race
        match sex, race
        match age, sex, race
        """
        self.left = pd.DataFrame({"PRTAGE": [1, 0, 0, 1, 1, 0, 1],
                                  "PESEX": [0, 1, 0, 1, 0, 1, 1],
                                  "PTDTRACE": [0, 0, 1, 0, 1, 1, 1],
                                  "HRMIS": [1, 1, 1, 1, 1, 1, 1]})
        self.right = pd.DataFrame({"PRTAGE": [1, 9, 9, 1, 1, 9, 1],
                                   "PESEX": [9, 1, 9, 1, 9, 1, 1],
                                   "PTDTRACE": [9, 9, 1, 9, 1, 1, 1],
                                   "HRMIS": [2, 2, 2, 2, 2, 2, 2]})

    def test_make_months(self):
        base = arrow.get('2014-01', format='%Y-%m')
        result = m.make_months(base)
        expected = [arrow.get('2014-01', format='%Y-%m'),
                    arrow.get('2014-02', format='%Y-%m'),
                    arrow.get('2014-03', format='%Y-%m'),
                    arrow.get('2014-04', format='%Y-%m'),
                    arrow.get('2015-01', format='%Y-%m'),
                    arrow.get('2015-02', format='%Y-%m'),
                    arrow.get('2015-03', format='%Y-%m'),
                    arrow.get('2015-04', format='%Y-%m')]
        self.assertEqual(result, expected)

        base = '2014-01'
        result = m.make_months(base)
        self.assertEqual(result, expected)

    def test_match(self):
        left = pd.DataFrame({'A': [1, 3, 5, 7],
                             'B': [1, 3, 5, 7]},
                             index=['a', 'b', 'c', 'd'])
        right = pd.DataFrame({'A': [0, 1, 5, 8],
                              'B': [1, 4, 8, 5]},
                              index=['a', 'b', 'c', 'd'])
        f = lambda x, y: x['A'] > y['A']
        result = m.match(left, right, [f])
        expected = pd.DataFrame({'A': [0, 1], 'B': [1, 4]}, index=['a', 'b'])
        tm.assert_frame_equal(result, expected)

    def test_match_multi(self):
        left = pd.DataFrame({'A': [1, 3, 5, 7],
                             'B': [1, 4, 5, 7]},
                             index=['a', 'b', 'c', 'd'])
        right = pd.DataFrame({'A': [0, 1, 5, 8],
                              'B': [-1, 4, 8, 5]},
                              index=['a', 'b', 'c', 'd'])
        f = lambda x, y: x['A'] >= y['A']
        g = lambda x, y: x['B'] <= y['B']

        result = m.match(left, right, [f, g])
        expected = pd.DataFrame({'A': [1, 5], 'B': [4, 8]},
                                index=['b', 'c'])
        tm.assert_frame_equal(result, expected)

    def test_make_wave_id(self):
        # unsorted, just in case
        idx = pd.MultiIndex.from_tuples([(17156360780, 65001, -1, 4),
                                         (45110260160, 65001, -1, 1),
                                         (92129160240, 65001, 1, 3)])
        df = pd.DataFrame({'HRYEAR4': [1999, 1999, 1999],
                           'HRMONTH': [1, 1, 3]}, index=idx)
        expected = df.copy()
        expected['wave_id'] = pd.Timestamp(datetime.datetime(1999, 1, 1))
        result = m.make_wave_id(df)
        tm.assert_frame_equal(result, expected)

        df = df.sort_index()
        tm.assert_frame_equal(result, expected)

    def test_match_age(self):
        result = m.match_age(self.left, self.right)
        expected = pd.Index([0, 3, 4, 6])
        tm.assert_index_equal(result, expected)

    def test_match_sex(self):
        result = m.match_sex(self.left, self.right)
        expected = pd.Index([1, 3, 5, 6])
        tm.assert_index_equal(result, expected)

    def test_match_race(self):
        result = m.match_race(self.left, self.right)
        expected = pd.Index([2, 4, 5, 6])
        tm.assert_index_equal(result, expected)

    def test_merge(self):
        df0 = self.left.loc[[6], :]
        df1 = self.right.loc[[6], :]
        result = m.merge([df0, df1])
        idx = pd.MultiIndex.from_tuples([(6, 1), (6, 2)],
                                        names=[None, "HRMIS"])
        expected = pd.DataFrame({"PRTAGE": [1, 1],
                                 "PESEX": [1, 1],
                                 "PTDTRACE": [1, 1]},
                                index=idx)
        tm.assert_frame_equal(result, expected)
