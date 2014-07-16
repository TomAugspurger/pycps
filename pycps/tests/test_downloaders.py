# -*- coding: utf-8 -*-
import unittest

from pycps import downloaders as d


class TestDownloaders(unittest.TestCase):

    def test_all_monthly_files(self):
        result = list(d.all_monthly_files('files/trimmed_nber.html'))
        expected = ['cpsb7601.Z', 'jan94pub.zip']
        self.assertEqual(result, expected)

    def test_all_monthly_dd(self):
        result = list(d.all_monthly_files('files/trimmed_nber.html',
                                          kind='dictionary'))
        expected = ['cpsbjan03.ddf']
        self.assertEqual(result, expected)

    def test_all_monthly_other(self):
        with self.assertRaises(ValueError):
            list(d.all_monthly_files(kind='foo'))  # need to thunk

    def test_rename_monthly_Z(self):
        result = d.rename_cps_monthly('cpsb9102.Z')
        expected = 'cpsm1991-02.Z'
        self.assertEqual(result, expected)

    def test_rename_monthly_zip(self):
        result = d.rename_cps_monthly('jan98pub.zip')
        expected = 'cpsm1998-01.zip'
        self.assertEqual(result, expected)

    def test_rename_dd_year_only(self):
        result = d.rename_cps_monthly('cps89.ddf')
        expected = 'cpsm1989-01.ddf'
        self.assertEqual(result, expected)

    def test_rename_dd_year_month(self):
        result = d.rename_cps_monthly('cpsbmay12.ddf')
        expected = 'cpsm2012-05.ddf'
        self.assertEqual(result, expected)

    def test_rename_dd_asc(self):
        result = d.rename_cps_monthly('jan98dd.asc')
        expected = 'cpsm1998-01.asc'
        self.assertEqual(result, expected)

    def test_filter_monthly_files_basic(self):
        files = iter(['cpsm1994-01.zip', 'cpsm1994-03.zip',
                      'cpsm1994-04.zip', 'cpsm1994-05.zip'])
        months = ['1994-01', '1994-03', '1994-04']
        result = list(d.filter_monthly_files(files, months=months))
        expected = ['cpsm1994-01.zip', 'cpsm1994-03.zip',
                    'cpsm1994-04.zip']
        self.assertEqual(result, expected)

    def test_filter_monthly_files_nested(self):
        files = iter(['cpsm1994-01.zip', 'cpsm1994-03.zip',
                      'cpsm1994-04.zip', 'cpsm1994-05.zip',
                      'cpsm1994-11.zip', 'cpsm1994-12.zip',
                      'cpsm1995-01.zip'])
        months = [['1994-01', '1994-04'], ['1994-11', '1995-01']]
        result = list(d.filter_monthly_files(files, months=months))
        expected = ['cpsm1994-01.zip', 'cpsm1994-03.zip',
                    'cpsm1994-04.zip', 'cpsm1994-11.zip',
                    'cpsm1994-12.zip', 'cpsm1995-01.zip']
        self.assertEqual(result, expected)

    def test_filter_monthly_files_mix(self):
        months = [['1994-01', '1994-04'], '1995-01']
        files = iter(['cpsm1994-01.zip', 'cpsm1994-03.zip'])
        with self.assertRaises(ValueError):
            list(d.filter_monthly_files(files, months=months))

    def test_filter_monthly_files_None(self):
        files = ['cpsm1994-01.zip', 'cpsm1994-03.zip']
        result = list(d.filter_monthly_files(iter(files)))
        self.assertEqual(files, list(result))

    def test_rename_cps_monthly_valueerror_ext(self):
        files = 'cps89.foo'
        with self.assertRaises(ValueError):
            d.rename_cps_monthly(files)

    def test_rename_cps_monthly_valueerror_ddf(self):
        files = 'foobarbaz.ddf'
        with self.assertRaises(ValueError):
            d.rename_cps_monthly(files)

    def test_dl_month_strpath(self):
        # TODO: mock
        self.skipTest("TODO: mock")
        result = d.download_month('cpsb7601.Z', 'files/')

    def test_dl_month_pathpath(self):
        self.skipTest("TODO: mock")
        result = d.download_month('cpsb7601.Z', Path('files/'))

    def test_dl_month_noparents(self):
        self.skipTest("TODO: mock")
        result = d.download_month('cpsb7601.Z', Path('files/foo/bar/baz/'))
