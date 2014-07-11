import os
import unittest
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
    "dd_path": "{data}/data_dictionaries/",
    "monthly_path": "{data}/monthly/"
}''')

    def test_skip_module_docstring(self):
        f = p._skip_module_docstring(self.settings_file)
        self.assertEqual(next(f), "{\n")

    def test_read_setting(self):
        result = p.read_settings(self.settings_file)['data_path']
        expected = 'data/'
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