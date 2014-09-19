# -*- coding: utf-8 -*-
import os
from os.path import sep
import json
import logging
import unittest
from pathlib import Path

import pandas as pd
import pandas.util.testing as tm

import pycps.parsers as p
from pycps.compat import StringIO

mdir = os.path.dirname(__file__)
mdir = os.path.sep.join(os.path.abspath(mdir).split(os.path.sep)[:-2])
# set the working directory to tests
os.chdir(sep.join(os.path.abspath(__file__).replace('.', sep).split(sep)[:-2]))
logging.disable(logging.CRITICAL)

def _skip_if_no_tables():
    try:
        import tables
    except ImportError:
        import nose
        raise(nose.SkipTest("tables not installed"))

class TestReaderSettings(unittest.TestCase):

    def setUp(self):
        self.settings_file = StringIO('''"""
Specify paths, etc.

TODO: document

NOTE: you can't have \'{\' or \'}\' in your filepaths.
These are reserved to substitue in other paths.
"""
{
    "data_path": "data/",
    "dd_path": "{data_path}/data_dictionaries/",
    "dd_store": "{dd_path}/dds.hdf",
    "monthly_path": "{data_path}/monthly/"
}''')

    def test_maybe_open_str(self):
        with p._open_file_or_stringio('files/maybe_open.txt') as f:
            result = f.readline()

        expected = "foobarbaz\n"
        self.assertEqual(result, expected)

    def test_maybe_open_sio(self):
        f = StringIO("foobarbaz\n")
        with p._open_file_or_stringio(f) as g:
            result = g.readline()

        expected = "foobarbaz\n"
        self.assertEqual(result, expected)

    def test_skip_module_docstring(self):
        f = p._skip_module_docstring(self.settings_file)
        self.assertEqual(next(f), "{\n")

    def test_read_setting(self):
        result = p.read_settings(self.settings_file)['data_path']
        expected = 'data/'
        self.assertEqual(result, expected)

    def test_substitue(self):
        result = p.read_settings(self.settings_file)['dd_path']
        expected = 'data/data_dictionaries/'
        self.assertEqual(result, expected)

#     def test_open_file_or_stringio(self):
#         import ipdb; ipdb.set_trace()
#         with tmpfile('test_settings.json', 'foo\nbar') as f:
#             with p._open_file_or_stringio(f) as g:
#                 pass
#             with p._open_file_or_stringio(StringIO("foo")):
#                 pass

# @contextmanager
# def tmpfile(filepath, contents):
#     if os.path.exists(filepath):
#         raise ValueError("PathExists")
#     try:
#         with open(filepath, 'w') as f:
#             f.write(contents)
#             yield f
#     finally:
#         os.unlink(f)


class TestDDParser(unittest.TestCase):

    def setUp(self):
        self.testfile = Path('files/cpsm2007-01.ddf')
        settings = {'outpath': 'dds/',
                    'dd_path': 'tmp/',
                    'dd_store': 'baz.h5'}
        with open(mdir + '/pycps/info.json') as f:
            info = json.load(f)
        self.parser = p.DDParser(self.testfile, settings, info)

    def test_formatter(self):
        s = 'H-MONTH     CHARACTER*002 .     (0038:0039)'.rstrip()
        regex = self.parser.make_regex(style=0)
        m = regex.match(s)
        expected = ('HhMONTH', 2, 38, 39)
        self.assertEqual(expected, self.parser.formatter(m))

    def test_regex_paddding_trailing_space(self):
        s = 'PADDING  CHARACTER*039          (0472:0600) '.rstrip()
        expected = ('PADDING', '039', '0472', '0600')
        regex = self.parser.make_regex(style=0)
        self.assertEqual(expected, regex.match(s).groups())

    def test_trailing_space(self):
        s = 'HUTYPC       2     TYPE C NON-INTERVIEW REASON                (45 - 46) '
        expected = ('HUTYPC', '2', 'TYPE C NON-INTERVIEW REASON', '45', '46')
        regex = self.parser.make_regex(style=2)
        self.assertEqual(regex.match(s).groups(), expected)

    def test_regex_paddding(self):
        s = 'PADDING  CHARACTER*039          (0472:0600)'.rstrip()
        regex = self.parser.make_regex(style=0)
        expected = ('PADDING', '039', '0472', '0600')
        self.assertEqual(expected, regex.match(s).groups())

    def test_store_name_basic(self):
        expected = 'cpsm2007-01'
        self.assertEqual(expected, self.parser.store_name)

    def test_aug05_regex_basic(self):
        ring = 'HRHHID          15     HOUSEHOLD IDENTIFIER   (Part 1)             (1 - 15)'.rstrip()
        self.parser.regex = self.parser.make_regex(2)
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER   (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_with_parens_id(self):
        ring = 'HRHHID       15    HOUSEHOLD IDENTIFIER   (Part 1)            1 - 15'.rstrip()
        self.parser.regex = self.parser.make_regex(2)
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER   (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_dont_pickup_first_number(self):
        ring = 'HRMONTH      2     MONTH OF INTERVIEW                      16-17'.rstrip()
        self.parser.regex = self.parser.make_regex(2)
        expected = ('HRMONTH', '2', 'MONTH OF INTERVIEW', '16', '17')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_with_nums_in_description(self):
        ring = 'PRPTHRS      2     AT WORK 1-34 BY HOURS AT WORK           403 - 404'.rstrip()
        self.parser.regex = self.parser.make_regex(2)
        expected = ('PRPTHRS', '2', 'AT WORK 1-34 BY HOURS AT WORK', '403', '404')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def tets_jan98_style_miss_first(self):
        ring = 'DATA        SIZE  BEGIN'.rstrip()
        self.parser.regex = self.parser.make_regex(style=1)
        expected = None
        self.assertEqual(expected, self.parser.regex.match(ring))

    def test_jan98_basic(self):
        ring = 'D HRHHID     15      1'.rstrip()
        self.parser.regex = self.parser.make_regex(style=1)
        expected = ('HRHHID', '15', '1')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_tabs(self):
        ring = 'HRHHID\t\t15\t\tHOUSEHOLD IDENTIFIER (Part 1)\t\t\t\t\t 1- 15'.rstrip()
        self.parser.regex = self.parser.make_regex(2)
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_may12_style_tabs(self):
        ring = 'HRHHID\t\t15\t\tHOUSEHOLD IDENTIFIER (Part 1)\t\t\t\t\t 1- 15\r\n'.rstrip()
        self.parser.regex = self.parser.make_regex(2)
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_formfeed(self):
        ring = 'PRDTIND2     2     DETAILED INDUSTRY RECODE - JOB 2        (474 - 475)'
        self.parser.regex = self.parser.make_regex(2)
        expected = ('PRDTIND2', '2', 'DETAILED INDUSTRY RECODE - JOB 2', '474', '475')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_replacer(self):
        st = "H$LIVQRT    CHARACTER*002 .     (0006:0007)"
        regex = self.parser.make_regex(style=0)
        expected = ('HdLIVQRT', 2, 6, 7)
        actual = self.parser.formatter(regex.match(st))
        self.assertEqual(expected, actual)

    def test_is_consistent(self):
        formatted = pd.DataFrame([('HhMONTH', 2, 1, 2),
                                  ('foo', 3, 3, 5)],
                                 columns=['id', 'length', 'start', 'end'])
        self.assertTrue(self.parser.is_consistent(formatted))

    def test_inconsistent_width(self):
        formatted = pd.DataFrame([('HhMONTH', 2, 1, 2),
                                  ('foo', 3, 3, 10)],
                                 columns=['id', 'length', 'start', 'end'])
        self.assertFalse(self.parser.is_consistent(formatted))

    def test_is_consistent_continuity_raises(self):
        formatted = pd.DataFrame([('HhMONTH', 2, 1, 2),
                                  ('foo', 3, 4, 6)],
                                 columns=['id', 'length', 'start', 'end'])
        self.assertFalse(self.parser.is_consistent(formatted))

    def test_regex_04_08_katrina(self):
        line = 'HURHHSCRN*   2     Is there anyone living or staying here who had to         (886-887)'
        expected = ('HURHHSCRN', 2, 886, 887)
        regex = self.parser.make_regex(style=2)
        result = self.parser.formatter(regex.match(line))
        self.assertEqual(result, expected)

    def test_regularize_ids(self):
        dd = pd.DataFrame({'end': {0: 15, 1: 17, 2: 21, 3: 23, 4: 26},
                           'start': {0: 1, 1: 16, 2: 18, 3: 22, 4: 24},
                           'length': {0: 15, 1: 2, 2: 4, 3: 2, 4: 3},
                           'id': {0: 'HRHHID', 1: 'HRMONTH',
                                  2: 'HRYEAR4', 3: 'HURESPLI', 4: 'HUFINAL'}})
        reg = {'HRHHID': 'foo'}
        ex = pd.DataFrame({'end': {0: 15, 1: 17, 2: 21, 3: 23, 4: 26},
                           'start': {0: 1, 1: 16, 2: 18, 3: 22, 4: 24},
                           'length': {0: 15, 1: 2, 2: 4, 3: 2, 4: 3},
                           'id': {0: 'foo', 1: 'HRMONTH',
                                  2: 'HRYEAR4', 3: 'HURESPLI', 4: 'HUFINAL'}})
        result = self.parser.regularize_ids(dd, reg)
        tm.assert_frame_equal(result, ex)

    def test_month_to_dd(self):
        months = ['1989-01', '1989-03', '1989-12',
                  '1992-01', '1992-02', '1993-12',
                  '1994-01', '1994-02', '1994-03',
                  '1994-04', '1994-05', '1995-05',
                  '1995-06', '1995-07', '1995-08',
                  '1995-09', '1996-01', '1997-12',
                  '1998-01', '2000-01', '2002-12',
                  '2003-01', '2004-02', '2004-04',
                  '2004-05', '2004-06', '2005-07',
                  '2005-08', '2005-09', '2005-10',
                  '2005-11', '2006-01', '2006-12',
                  '2007-01', '2008-09', '2008-12',
                  '2009-01', '2009-06', '2009-12',
                  '2010-01', '2010-11', '2012-02',
                  '2012-05', '2012-07', '2012-12',
                  '2013-01', '2013-02', '2013-03'
                ]
        dds = ["cpsm1989-01", "cpsm1992-01", "cpsm1994-01", "cpsm1994-04",
               "cpsm1995-06", "cpsm1995-09", "cpsm1998-01", "cpsm2003-01",
               "cpsm2004-05", "cpsm2005-08", "cpsm2005-11", "cpsm2007-01",
               "cpsm2009-01", "cpsm2010-01", "cpsm2012-05", "cpsm2013-01"] * 3
        dds = sorted(dds)
        for month, dd in zip(months, dds):
            result = p._month_to_dd(month)
            self.assertEqual(result, dd)

    def test_full_run(self):
        result = self.parser.run()
        expected = pd.read_csv('files/jan08expected.csv')
        tm.assert_frame_equal(result, expected)

    def test_standardize_ids(self):
        # test '63' '63B', '65B', '63A'] for hrsample
        pass

class testHDFStore(unittest.TestCase):

    def setUp(self):
        _skip_if_no_tables()
        self.hdfpath = '_hdfstore_.h5'
        self.hdf = pd.HDFStore(self.hdfpath)
        self.dd =  pd.DataFrame([['HRHHID',15, 1, 15],
                                 ['HRMONTH', 2, 16, 17],
                                 ['HRYEAR4', 4, 18, 21]],
                                columns=['id', 'length', 'start', 'end'])
        self.infile = StringIO("""000000000000000111999
000000000000001121999
000000000000002012000
000000000000003022000
""")

    def tearDown(self):
        self.hdf.close()
        os.unlink(self.hdfpath)

    def test_read_monthly(self):
        result = p.read_monthly(self.infile, self.dd)
        # expected = pd.DataFrame([['000000000000000', 11, 1999],
        #                          ['000000000000001', 12, 1999],
        #                          ['000000000000002', 1, 2000],
        #                          ['000000000000003', 2, 2000]],
        #                         columns=['HRHHID', 'HRMONTH', 'HRYEAR4'])
        expected = pd.DataFrame([[0, 11, 1999],
                                 [1, 12, 1999],
                                 [2, 1, 2000],
                                 [3, 2, 2000]],
                                columns=['HRHHID', 'HRMONTH', 'HRYEAR4'])

        tm.assert_frame_equal(result, expected)
