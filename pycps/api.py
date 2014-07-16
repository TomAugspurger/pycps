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
    cached_dd = [x.name for x in Path(settings['dd_path']).iterdir()]
    cached_month = [x.name for x in Path(settings['data_path']).iterdir()]

    # DL all dds; only some months
    # TODO: caching
    dds = dl.all_monthly_files(kind='dictionary')
    data = dl.all_monthly_files()
    data = dl.filter_monthly_files(data, months=[[settings['date_start'],
                                                  settings['date_end']]])
    if not overwrite_cached():
        def is_new(x, cache=None):
            return dl.rename_cps_monthly(x) not in cache

        dds = filter(partial(is_new, cache=cached_dd), dds)
        data = filter(partial(is_new, cache=cached_month), data)

    for month in dds:
        dl.download_month(month, Path(settings['dd_path']))
        # TODO: logging
        print(month)

    for month in data:
        dl.download_month(month, Path(settings['data_path']))
        # TODO: logging
        print(month)

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