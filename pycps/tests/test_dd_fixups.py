# -*- coding: utf-8 -*-

import os
from os.path import sep
import logging
import unittest

import pycps.parsers as par
import pycps.data_dictionary_fixups as ddf

mdir = os.path.dirname(__file__)
mdir = os.path.sep.join(os.path.abspath(mdir).split(os.path.sep)[:-2])
# set the working directory to tests
os.chdir(sep.join(os.path.abspath(__file__).replace('.', sep).split(sep)[:-2]))
logging.disable(logging.CRITICAL)


class TestFixup(unittest.TestCase):

    def setUp(self):

        self.settings = par.read_settings(mdir + "/pycps/settings.json")

    def test_sub_path_import(self):
        p = "pycps/data_dictionary_fixups.py"
        result = par._sub_path_import(p)
        expected = "pycps.data_dictionary_fixups"
        self.assertEqual(result, expected)

    def test_parser(self):
        # This will have to be updated whenever a new fu func is added
        result = dict(self.settings['data_dictionary_fixups'])
        expected = {'cpsm1998_01': [ddf.cpsm1998_01_149_unknown,
                                    ddf.cpsm1998_01_535_unknown,
                                    ddf.cpsm1998_01_556_unknown,
                                    ddf.cpsm1998_01_632_unknown,
                                    ddf.cpsm1998_01_680_unknown,
                                    ddf.cpsm1998_01_786_unknown],
                    'cpsm2004_05': [ddf.cpsm2004_05_filler_411],
                    'cpsm2004_08': [ddf.cpsm2004_08_filler_411],
                    'cpsm2005_08': [ddf.cpsm2005_08_filler_411],
                    'cpsm2009_01': [ddf.cpsm2009_01_filler_399],
                    'cpsm2012_05': [ddf.cpsm2012_05_insert_filler_637,
                                    ddf.cpsm2012_05_remove_filler_114]}
        self.assertEqual(result, expected)
