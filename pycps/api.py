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
from operator import itemgetter

import pandas as pd

import pycps.merge as m
import pycps.parsers as par
import pycps.downloaders as dl
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
    with open(settings['info_path']) as f:
        info = json.load(f)

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
            parser = par.DDParser(f, settings, info)
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

            fixups = settings['FIXUP_BY_DD'].get(dd_name)
            logger.info("Applying {} to {}".format(fixups, f.stem))
            df = par.fixup_by_dd(df, fixups)
            # TODO: special stuff

            df = df.set_index(id_cols)
            par.write_monthly(df, store_path, f.stem)
            logging.info("Added {} to {}".format(f, settings['monthly_store']))


# -----------------------------------------------------------------------------
# Merge
# -----------------------------------------------------------------------------


def merge(settings, overwrite=False):
    """
    Merges interviews over time by household.

    Parameters
    ----------
    settings : JSON settings file
    overwrite : bool
        whether to overwrite existing files

    Returns
    -------
    None (IO)
    """
    STORE_FMT = 'm%Y_%m'
    store_path = settings['monthly_store']
    start = settings['date_start']
    end = settings['date_end']
    all_months = pd.date_range(start=start, end=end, freq='m')

    if overwrite:
        logger.info("Merging for {}".format(all_months))
    else:
        with pd.get_store(settings['merged_store']) as store:
            cached = set(store.keys())
            all_m = set([x.strftime('/' + STORE_FMT) for x in all_months])
            logger.info("Using cached for {}".format(cached & all_m))
            new = all_m - cached
            all_months = filter(lambda x: x.strftime('/' + STORE_FMT) in new,
                                all_months)

    for m0 in all_months:
        months = (x.strftime('cpsm%Y-%m')
                  for x in m.make_months(m0.strftime('%Y-%m-%d')))
        months = enumerate(months, 1)

        mis, month = next(months)
        df0 = pd.read_hdf(store_path, key=month).query('HRMIS == @mis')
        match_funcs = [m.match_age, m.match_sex, m.match_race]
        dfs = [df0]
        for mis, month in months:
            try:
                dfn = pd.read_hdf(store_path, key=month).query('HRMIS == @mis')
                dfs.append(m.match(df0, dfn, match_funcs))
            except KeyError:
                msg = "The panel for {} has no monthly data file for {}"
                logger.warn(msg.format(m0, month))
                continue

        df = m.merge(dfs)
        df = df.sort_index()
        df = m.make_wave_id(df)

        store_key = df['wave_id'].iloc[0].strftime(STORE_FMT)
        df.to_hdf(settings["merged_store"], store_key)
        logger.info("Added merged {} to {}".format(store_key,
                                                   settings['merged_store']))


def main(config):
    settings = par.read_settings(config.settings)
    overwrite = config.overwrite

    # Overwrite default info file?
    if config.info is not None:
        settings['info'] = config.info

    if config.monthly_data_fixups:
        import importlib
        fixup_file = config.monthly_data_fixups.strip('.py')
        user_fixups = importlib.import_module(fixup_file).FIXUP_BY_DD

        if config.append_fixups:
            # merge the user supplied with the defaults.
            from pycps.monthly_data_fixups import FIXUP_BY_DD

            for dd in FIXUP_BY_DD:
                new = user_fixups.get(dd)

                if new is not None:
                    for x in new:
                        FIXUP_BY_DD[dd].append(x)

        else:
            FIXUP_BY_DD = user_fixups.FIXUP_BY_DD
    else:
        from pycps.monthly_data_fixups import FIXUP_BY_DD

    # Fixups will be passed and accessed via settings
    settings['FIXUP_BY_DD'] = FIXUP_BY_DD

    if config.download_dictionaries:
        download('dictionary', settings, overwrite=overwrite)
    if config.download_monthly:
        download('data', settings, overwrite=overwrite)

    if config.parse_dictionaries:
        parse('dictionary', settings, overwrite=overwrite)
    if config.parse_monthly:
        parse('data', settings, overwrite=overwrite)

    if config.merge:
        merge(settings, overwrite=overwrite)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Invoke pycps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--settings", default="pycps/settings.json",
                        metavar='',
                        help="path to JSON settings file")
    parser.add_argument("-i", "--info", default="pycps/info.json",
                        metavar='',
                        help="Path to info.json")
    parser.add_argument("--monthly-data-fixups",
                        default=None, metavar='',
                        help="path to file containing data fixup functions. "
                             "This file must be in the current directory")
    parser.add_argument("--append-fixups", default=True, metavar='',
                        help="Whether to add or replace with user supplied "
                             "fixups")
    parser.add_argument("-d", "--download-dictionaries", default=False,
                        action="store_true",
                        help="Download data dictionaries")
    parser.add_argument("-y", "--download-monthly", default=False,
                        action="store_true",
                        help="Download monthly data files")
    parser.add_argument("-p", "--parse-dictionaries", default=False,
                        action="store_true",
                        help="Parse data dictionaries")
    parser.add_argument("-x", "--parse-monthly", default=False,
                        action="store_true",
                        help="Parse monthly data files")
    parser.add_argument("-m", "--merge", default=False,
                        action="store_true",
                        help="Merge monthly files by household")
    parser.add_argument("-o", "--overwrite", default=False,
                        action="store_true",
                        help="Overwrite existing cache")
    config = parser.parse_args()
    main(config)
