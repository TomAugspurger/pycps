# -*- coding: utf-8 -*-
"""
Functions to step through

1. Downloading Data Dictionaries and Monthly files
2. Parsing downloaded files
3. Storing parsed files in HDF stores.
4. Merging stored files.

Define your preferences in settings.json in this folder.
"""
import json
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
    cached_month = dl.check_cached(settings['monthly_path'], kind='data')

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
        dl.download_month(month, Path(settings['monthly_path']))
        # TODO: logging
        print(renamed)

#-----------------------------------------------------------------------------
# Parsing
#-----------------------------------------------------------------------------

def parse():
    settings = par.read_settings(str(_HERE_ / 'settings.json'))
    dd_path = Path(settings['dd_path'])
    dds = [x for x in dd_path.iterdir() if x.suffix == '.ddf']
    monthly_path = Path(settings['monthly_path'])
    months = [x for x in monthly_path.iterdir() if x.suffix in ('.Z', '.zip')]

    settings['raise_warnings'] = False

    with (_HERE_ / 'data.json').open() as f:
        data = json.load(f)

    for dd in dds:
        parser = par.DDParser(dd, settings)
        try:
            df = parser.run()
            df = parser.regularize_ids(df, data['col_rename_by_dd'][parser.store_name])
        except Exception as e:
            if not settings['raise_warnings']:
                print('skipping {}'.format(parser.store_name))
                continue
            else:
                raise e
        parser.write(df)
        # TODO: logging
        print("Added ", dd)

    for month in months:
        dd_name = par._month_to_dd(str(month))
        dd = pd.read_hdf(settings['dd_store'], key=dd_name)
        cols = data['columns_by_dd'][dd_name]
        dd = dd[dd.id.isin(cols)]
        df = par.read_monthly(str(month), dd)
        df = par.fixup_by_dd(df, dd_name)
        # do special stuff like compute HRHHID2, bin things, etc.
        # TODO: special stuff

        df = df.set_index(['HRHHID', 'HRHHID2', 'PULINENO'])
        par.write_monthly(df, settings['monthly_store'], month.stem)
        print("Added ", month)

#-----------------------------------------------------------------------------
# Merge
#-----------------------------------------------------------------------------