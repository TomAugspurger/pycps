import os
import unittest
from pathlib import Path
from contextlib import contextmanager

import pycps.parsers as p
from pycps.compat import StringIO


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
    "monthly_path": "{data_path}/monthly/"
}''')

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
        self.testfile = Path('files/jan2007.ddf')  # TODO: filepath
        settings = {'outpath': 'dds/'}
        self.parser = p.DDParser(self.testfile, settings)

    def test_formatter(self):
        s = 'H-MONTH     CHARACTER*002 .     (0038:0039)'.rstrip()
        m = self.parser.regex.match(s)
        expected = ('HhMONTH', 2, 38, 39)
        self.assertEqual(expected, self.parser.formatter(m))

    def test_regex_paddding_trailing_space(self):
        s = 'PADDING  CHARACTER*039          (0472:0600) '.rstrip()
        expected = ('PADDING', '039', '0472', '0600')
        self.assertEqual(expected, self.parser.regex.match(s).groups())

    def test_regex_paddding(self):
        s = 'PADDING  CHARACTER*039          (0472:0600)'.rstrip()
        expected = ('PADDING', '039', '0472', '0600')
        self.assertEqual(expected, self.parser.regex.match(s).groups())

    def test_store_name_basic(self):
        expected = 'jan2003'
        self.assertEqual(expected, self.parser.get_store_name())

    def test_aug05_regex_basic(self):
        ring = 'HRHHID          15     HOUSEHOLD IDENTIFIER   (Part 1)             (1 - 15)'.rstrip()
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER   (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_with_parens_id(self):
        ring = 'HRHHID       15    HOUSEHOLD IDENTIFIER   (Part 1)            1 - 15'.rstrip()
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER   (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_dont_pickup_first_number(self):
        ring = 'HRMONTH      2     MONTH OF INTERVIEW                      16-17'.rstrip()
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('HRMONTH', '2', 'MONTH OF INTERVIEW', '16', '17')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_with_nums_in_description(self):
        ring = 'PRPTHRS      2     AT WORK 1-34 BY HOURS AT WORK           403 - 404'.rstrip()
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('PRPTHRS', '2', 'AT WORK 1-34 BY HOURS AT WORK', '403', '404')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def tets_jan98_style_miss_first(self):
        ring = 'DATA        SIZE  BEGIN'.rstrip()
        self.parser.regex = self.parser.make_regex(style='jan1998')
        expected = None
        self.assertEqual(expected, self.parser.regex.match(ring))

    def test_jan98_basic(self):
        ring = 'D HRHHID     15      1'.rstrip()
        self.parser.regex = self.parser.make_regex(style='jan1998')
        expected = ('HRHHID', '15', '1')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_aug05_style_tabs(self):
        ring = 'HRHHID\t\t15\t\tHOUSEHOLD IDENTIFIER (Part 1)\t\t\t\t\t 1- 15'.rstrip()
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_may12_style_tabs(self):
        ring = 'HRHHID\t\t15\t\tHOUSEHOLD IDENTIFIER (Part 1)\t\t\t\t\t 1- 15\r\n'.rstrip()
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('HRHHID', '15', 'HOUSEHOLD IDENTIFIER (Part 1)', '1', '15')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_formfeed(self):
        ring = 'PRDTIND2     2     DETAILED INDUSTRY RECODE - JOB 2        (474 - 475)'
        self.parser.regex = self.parser.make_regex(style='aug2005')
        expected = ('PRDTIND2', '2', 'DETAILED INDUSTRY RECODE - JOB 2', '474', '475')
        self.assertEqual(expected, self.parser.regex.match(ring).groups())

    def test_replacer(self):
        st = "H$LIVQRT    CHARACTER*002 .     (0006:0007)"
        self.parser.regex = self.parser.make_regex()
        expected = ('HdLIVQRT', 2, 6, 7)
        actual = self.parser.formatter(self.parser.regex.match(st))
        self.assertEqual(expected, actual)
