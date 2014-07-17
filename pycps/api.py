# -*- coding: utf-8 -*-
"""
Functions to step through

1. Downloading Data Dictionaries and Monthly files
2. Parsing downloaded files
3. Storing parsed files in HDF stores.
4. Merging stored files.

Define your preferences in settings.json in this folder.
"""
from pathlib import Path
from functools import partial
from operator import itemgetter

import pandas as pd

import pycps.downloaders as dl
import pycps.parsers as par

# TODO argparse CLI

_HERE_ = Path(__file__).parent
#-----------------------------------------------------------------------------
# Downloading
#-----------------------------------------------------------------------------

def download(overwrite_cached=False):
    settings = par.read_settings(str(_HERE_ / 'settings.json'))
    cached_dd = dl.check_cached(settings['dd_path'], kind='dictionary')
    cached_month = dl.check_cached(settings['data_path'], kind='data')

    # TODO: only needed dds
    dd_range = [par._month_to_dd(settings['date_start']),
                par._month_to_dd(settings['date_end'])]
    dds = dl.all_monthly_files(kind='dictionary')
    dds = filter(itemgetter(1), dds)  # make sure not None cpsdec!
    dds = dl.filter_dds(dds, months=[par._month_to_dd(settings['date_start']),
                                     par._month_to_dd(settings['date_end'])])

    data = dl.all_monthly_files()
    data = dl.filter_monthly_files(data, months=[[settings['date_start'],
                                                  settings['date_end']]])
    if not overwrite_cached:
        def is_new(x, cache=None):
            return dl.rename_cps_monthly(x[1]) not in cache

        dds = filter(partial(is_new, cache=cached_dd), dds)
        data = filter(partial(is_new, cache=cached_month), data)

    for month, renamed in dds:
        dl.download_month(month, Path(settings['dd_path']))
        # TODO: logging
        print(renamed)

    for month, renamed in data:
        dl.download_month(month, Path(settings['data_path']))
        # TODO: logging
        print(renamed)

#-----------------------------------------------------------------------------
# Parsing
#-----------------------------------------------------------------------------

def parse():
    settings = par.read_settings(str(_HERE_ / 'settings.json'))
    dd_path = Path(settings['dd_path'])
    dds = [x for x in dd_path.iterdir() if x.suffix == '.ddf']
    data_path = Path(settings['data_path'])
    months = [x for x in data_path.iterdir() if x.suffix in ('.Z', '.zip')]

    for dd in dds:
        parser = par.DDParser(dd, settings)
        parser.run()
        parser.write()
        # TODO: logging
        print("Added ", dd)

    for month in months:
        dd = pd.read_hdf(settings['dd_path'], key=par._month_to_dd(str(month)))
        df = par.read_monthly(str(month), dd)
        par.write_monthly(df, settings['data_path'])

#-----------------------------------------------------------------------------
# Merge
#-----------------------------------------------------------------------------