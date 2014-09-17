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
import logging
import argparse
from pathlib import Path
from functools import partial
from operator import itemgetter

import pandas as pd

import pycps.downloaders as dl
import pycps.parsers as par
from pycps.setup_logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)
# TODO argparse CLI

_HERE_ = Path(__file__).parent
#-----------------------------------------------------------------------------
# Downloading
#-----------------------------------------------------------------------------

def download(kind, settings, overwrite=False):
    """
    Download files from NBER.

    kind : {'dictionary', 'data'}
    settings : dict
    overwrite : bool; default True
        Whether to overwrite existing files
    """
    s_path = {'dictionary': 'dd_path', 'data': 'monthly_path'}[kind]
    cached = dl.check_cached(settings[s_path], kind=kind)

    files = dl.all_monthly_files(kind=kind)
    if kind == 'dictionary':
        files = filter(itemgetter(1), files)   # make sure not None cpsdec!
        months = [par._month_to_dd(settings['date_start']),
                  par._month_to_dd(settings['date_end'])]
    else:
        months = [[settings['date_start'], settings['date_end']]]

    files = dl.filter_monthly(files, months=months, kind=kind)

    def is_new(x, cache=None):
        return dl.rename_cps_monthly(x[1]) not in cache

    for month, renamed in files:
        if is_new((month, renamed), cache=cached) or overwrite:
            dl.download_month(month, Path(settings[s_path]))
            logger.info("Downloaded {}".format(renamed))
        else:
            logger.info("Using cached {}".format(renamed))

#-----------------------------------------------------------------------------
# Parsing
#-----------------------------------------------------------------------------

def parse(kind, settings, overwrite=False):
    """
    Parse downloaded files, store in HDFStore.

    Parameters
    ----------

    kind : {'dictionary', 'data'}
    settings : dict
    overwrite : bool
    """
    s_path = {'dictionary': 'dd_path', 'data': 'monthly_path'}[kind]
    path_ = Path(settings[s_path])

    suffix_d = {'data': ('.Z', '.zip'), 'dictionary': ('.ddf', '.asc', '.txt')}
    suffixes = suffix_d[kind]

    files = [x for x in path_.iterdir() if x.suffix in suffixes]
    id_cols = ['HRHHID', 'HRHHID2', 'PULINENO']

    with open(settings['info']) as f:
        data = json.load(f)

    if kind == 'dictionary':
        files.append(_HERE_ / Path('cpsm2014-01.ddf'))
        for f in files:
            parser = par.DDParser(f, settings)
            df = parser.run()
            parser.write(df)
            logging.info("Added {} to {}".format(f, parser.store_path))
    else:
        for f in files:
            dd_name = par._month_to_dd(str(f))
            store_path = settings['monthly_store']

            dd = pd.read_hdf(settings['dd_store'], key=dd_name)
            cols = data['columns_by_dd'][dd_name]
            sub_dd = dd[dd.id.isin(cols)]

            if len(cols) != len(sub_dd):
                missing = set(cols) - set(sub_dd.id.values)
                raise ValueError("IDs {} are not in the Data "
                                 "Dictionary".format(missing))

            with pd.get_store(store_path) as store:
                try:
                    cached_cols = store.select(f.stem).columns
                    newcols = set(cols) - set(cached_cols) - set(id_cols)
                    if len(newcols) == 0:
                        logger.info("Using cached {}".format(f.stem))
                        continue

                except KeyError:
                    pass

            # Assuming no new rows
            df = par.read_monthly(str(f), sub_dd)
            df = par.fixup_by_dd(df, dd_name)
            # do special stuff like compute HRHHID2, bin things, etc.
            # TODO: special stuff

            df = df.set_index(id_cols)
            par.write_monthly(df, store_path, f.stem)
            logging.info("Added {} to {}".format(f, settings['monthly_store']))


# -----------------------------------------------------------------------------
# Merge
# -----------------------------------------------------------------------------


def merge():
    # settings = par.read_settings(str(_HERE_ / 'settings.json'))
    store = pd.HDFStore('data/monthly/monthly.hdf')  # from settings
    months = (x.strftime('cpsm%Y-%m') for x in m.make_months('1995-09-01'))
    months = enumerate(months, 1)

    mis, month = next(months)
    df0 = store.select(month).query('HRMIS == @mis')

    match_funcs = [m.match_age, m.match_sex, m.match_race]
    dfs = [df0]
    for mis, month in months:
        dfn = store.select(month).query('HRMIS == @mis')
        dfs.append(m.match(df0, dfn, match_funcs))

    df = m.merge(dfs)
    df = df.sort_index()
    df = m.make_wave_id(df)


def main(config):
    settings = par.read_settings(config.settings)
    overwrite = config.overwrite

    if config.json_path is not None:
        settings['info'] = config.json_path

    if config.download_dictionaries:
        download('dictionary', settings, overwrite=overwrite)
    if config.download_monthly:
        download('data', settings, overwrite=overwrite)

    if config.parse_monthly:
        parse('data', settings, overwrite=overwrite)
    if config.parse_dictionaries:
        parse('dictionary', settings, overwrite=overwrite)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Invoke pycps")
    parser.add_argument("-s", "--settings", default="pycps/settings.json",
                        help="path to JSON settings file")
    parser.add_argument("-i", "--info", default="pycps/info.json",
                        help="Path to info.json")
    parser.add_argument("-d", "--download-dictionaries", default=False,
                        action="store_true",
                        help="Download data dictionaries")
    parser.add_argument("-m", "--download-monthly", default=False,
                        action="store_true",
                        help="Download monthly data files")
    parser.add_argument("-p", "--parse-dictionaries", default=False,
                        action="store_true",
                        help="Parse data dictionaries")
    parser.add_argument("-x", "--parse-monthly", default=False,
                        action="store_true",
                        help="Parse monthly data files")
    parser.add_argument("-o", "--overwrite", default=False,
                        action="store_true",
                        help="Overwrite existing cache")
    config = parser.parse_args()
    main(config)
