# -*- coding: utf-8 -*-
import os
import unittest

from pycps import downloaders as d


class TestDownloaders(unittest.TestCase):

    def test_all_monthly_files(self):
        result = list(d.all_monthly_files('files/trimmed_nber.html'))
        expected = [('cpsb7601.Z', 'cpsm1976-01.Z'),
                    ('jan94pub.zip', 'cpsm1994-01.zip')]
        self.assertEqual(result, expected)

    def test_all_monthly_dd(self):
        result = list(d.all_monthly_files('files/trimmed_nber.html',
                                          kind='dictionary'))
        expected = [('cpsbjan03.ddf', 'cpsm2003-01.ddf')]
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
        files = [('jan94.zip', 'cpsm1994-01.zip'),
                 ('mar94.zip', 'cpsm1994-03.zip'),
                 ('apr94.zip', 'cpsm1994-04.zip'),
                 ('may94.zip', 'cpsm1994-05.zip')]
        months = ['1994-01', '1994-03', '1994-04']
        result = list(d.filter_monthly_files(files, months=months))
        expected = files[0:3]
        self.assertEqual(result, expected)

    def test_filter_monthly_files_nested(self):
        files = [('jan94.zip', 'cpsm1994-01.zip'),
                 ('mar94.zip', 'cpsm1994-03.zip'),
                 ('apr94.zip', 'cpsm1994-04.zip'),
                 ('nov94.zip', 'cpsm1994-11.zip'),
                 ('dec94.zip', 'cpsm1994-12.zip'),
                 ('jan95.zip', 'cpsm1995-01.zip')]
        months = [['1994-01', '1994-04'], ['1994-11', '1995-01']]
        result = list(d.filter_monthly_files(files, months=months))
        expected = files
        self.assertEqual(result, expected)

    def test_filter_monthly_files_mix(self):
        months = [['1994-01', '1994-04'], '1995-01']
        files = iter([('jan94.zip', 'cpsm1994-01.zip'),
                      ('mar94.zip', 'cpsm1994-03.zip')])
        with self.assertRaises(ValueError):
            list(d.filter_monthly_files(files, months=months))

    def test_filter_monthly_files_None(self):
        files = [('jan94.zip', 'cpsm1994-01.zip'),
                 ('mar94.zip', 'cpsm1994-03.zip')]
        result = list(d.filter_monthly_files(iter(files)))
        expected = files
        self.assertEqual(expected, list(result))

    def test_rename_cps_monthly_valueerror_ext(self):
        files = 'cps89.foo'
        with self.assertRaises(ValueError):
            d.rename_cps_monthly(files)

    def test_rename_cps_monthly_valueerror_ddf(self):
        files = 'foobarbaz.ddf'
        with self.assertRaises(ValueError):
            d.rename_cps_monthly(files)

    def test_rename_already_formatted(self):
        dd = expected = 'cpsm1994-01.ddf'
        result = d.rename_cps_monthly(dd)
        self.assertEqual(result, expected)

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


class TestCached(unittest.TestCase):

    def setUp(self):
        self.tmpdir = '_testcached_'
        os.mkdir(self.tmpdir)
        self.tmpfiles = [os.path.join(self.tmpdir, x) for x in
                         ['c1.ddf', 'c2.asc', 'c3.zip', 'z4.Z', 'c5.foo']]
        for f in self.tmpfiles:
            open(f, 'w')

    def tearDown(self):
        [os.remove(x) for x in self.tmpfiles]
        os.rmdir(self.tmpdir)

    def test_check_cached_data(self):
        result = d.check_cached(self.tmpdir, kind='data')
        expected = ['c3.zip', 'c4.Z']

    def test_check_cached_ddf(self):
        result = d.check_cached(self.tmpdir, kind='dictionary')
        expected = ['c1.ddf', 'c2.asc']

    def test_check_cached_other(self):
        with self.assertRaises(ValueError):
            result = d.check_cached(self.tmpdir, kind='foo')
