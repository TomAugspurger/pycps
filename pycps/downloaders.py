# -*- coding: utf-8 -*-
"""
Downloaders for

- monthly data files
- data dictionaries

from http://www.nber.org/data/cps_basic.html

TODO: March Supplements
"""
import re
import logging
import datetime
from pathlib import Path
from itertools import chain
from functools import partial
from operator import itemgetter

import arrow
import requests
from lxml import html
from pandas.core.common import is_list_like

logger = logging.getLogger(__name__)

# Special pattern for dds from Jan 1994 - Jun 1995
DATAWEB_MATCHER = re.compile(r"\w{3}\d{2}_\w{3}\d{2}_dd.txt")


def all_monthly_files(site='http://www.nber.org/data/cps_basic.html',
                      kind='data'):
    """
    Find all matching monthly data files and data dictionaries
    from the NBER's CPS site.

    Parameters
    ----------
    site: str
    kind: {'data', 'dictionary'}
        whether to get the actual data file or the data-dictionary
    """
    logger.info("Fetching monthly files from {}".format(site))
    if kind == 'data':
        regex = re.compile(r'cpsb\d{4}.Z|\w{3}\d{2}pub.zip')
    elif kind == 'dictionary':
        regex = re.compile(r'[\w\d]*\.(ddf|asc)|January_2013_Record_Layout.(txt)')
    else:
        raise ValueError("Kind must be one of `data`, or `dictionary`. "
                         "Got {} instead.".format(kind))
    root = html.parse(site).getroot()
    partial_matcher = partial(_matcher, regex=regex)

    for _, _, fname_, _ in filter(partial_matcher, root.iterlinks()):
        fname = fname_.split('/')[-1]
        yield fname, rename_cps_monthly(fname)

    # nber.org/data/cps_basic.html doesn't have the Jan94 - aug95
    # data dictionaries
    # use the ones at http://thedataweb.rm.census.gov/pub/cps/basic/

    if kind == 'dictionary':
        for fname in ['jan94_mar94_dd.txt', 'apr94_may95_dd.txt',
                      'jun95_aug95_dd.txt']:
            yield fname, rename_cps_monthly(fname)


def rename_cps_monthly(cpsname):
    """
    hardcoded. cpsb9102.Z   -> cpsm1991-02.Z
               jan98pub.zip -> cpsm1998-01.zip

    Parameters
    ----------
    cpsname: str

    Results
    -------
    myname: str
        formatted like cpsmYYYY-MM.ext
    """
    fname, ext = cpsname.split('.')

    # if already formatted then skip to end
    if re.match(r'cpsm\d{4}-\d{2}', cpsname):
        return cpsname

    if ext == 'Z':  # could be DRYer
        dt = datetime.datetime.strptime(fname, 'cpsb%y%m')
    elif ext == 'zip':
        dt = datetime.datetime.strptime(fname, '%b%ypub')
    elif ext == 'asc':
        dt = datetime.datetime.strptime(fname, '%b%ydd')
    elif ext == 'ddf':
        if fname.startswith('cpsb'):
            dt = datetime.datetime.strptime(fname, 'cpsb%b%y')
        elif fname == 'cpsrwdec07':
            # TODO: special case this one?
            print('skipping cpsrwdec07')
            return None
        elif fname.startswith('cps'):
            dt = datetime.datetime.strptime(fname, 'cps%y')
        else:
            raise ValueError
    elif ext == 'txt':
        if DATAWEB_MATCHER.match(cpsname):
            # expecting jan94_mar94_dd
            dt = datetime.datetime.strptime(fname, "%b%y" + fname[-9:])
        else:
            # expecting January_2013_Record_Layout
            dt = datetime.datetime.strptime(fname.split('_')[1], '%Y')
    else:
        raise ValueError
    return dt.strftime('cpsm%Y-%m') + '.' + ext


def filter_monthly(files, months=None, kind='data'):
    """
    Filter files according to whether dates fall in months.

    Parameters
    ----------
    files: (cpsname, rename_cps_monthly)
    months: list of, or list of list of, str or Arrow
        Months to yield.
        If list of list of str or Arrow, the lists will
        be expanded to ranges and and values falling in
        a range will be yielded (inclusive).
        Should be 'YYYY-MM'.
    kind: {'data', 'dictionary'}

    Returns
    -------
    filtered: (cpsname, rename_cps_monthly)

    """
    if kind == 'dictionary':
        filtered = filter_dds(files, months=months)
    elif kind == 'data':
        filtered = filter_monthly_files(files, months=months)
    else:
        raise ValueError("kind must be 'data' or 'dictionary'.")
    return filtered


def filter_dds(files, months=None):
    #TODO: REFACTOR, generalize, test
    rng = [arrow.get(x[-7:]) for x in months]

    f = lambda x: arrow.get(x[1].split('.')[0][-7:])  # expecting 1989-01.ddf
    return filter(lambda x: rng[0] <= f(x) <= rng[1], files)


def filter_monthly_files(files, months=None):
    """
    Filter the generator from all_monthly_files down to
    what you want, probably from the settings file.

    Parameters
    ----------
    filenames: (str, str)
        in (cpsname, rename_cps_monthly) style
    months: list of, or list of list of, str or Arrow
        Months to yield.
        If list of list of str or Arrow, the lists will
        be expanded to ranges and and values falling in
        a range will be yielded (inclusive).
        Should be 'YYYY-MM'.

    kind: {'both', 'dd', 'data'}

    Returns
    -------
    filtered: generator

    Examples
    --------

    """
    files = list(files)  # have to thunk
    file_dates = [arrow.get(x.split('.')[0], format='cpsm%Y-%m')
                  for _, x in files]

    if months is None:
        months = [['1936-01', arrow.now().strftime('%Y-%m')]]

    is_nested = [is_list_like(x) for x in months]
    a = lambda x: arrow.get(x)

    if any(is_nested) and not all(is_nested):
        raise ValueError("Can't mix yet")
    elif all(is_nested):
        ranges = (arrow.Arrow.range('month', start=a(x), end=a(y))
                  for (x, y) in months)
        months = list(chain.from_iterable(ranges))
    else:
        months = [a(x) for x in months]

    filtered = filter(lambda x: x[1] in months, zip(files, file_dates))
    filtered = map(itemgetter(0), filtered)
    return filtered


def download_month(month, datapath):
    """
    Fetch and write a single month's data
    from http://www.nber.org/cps-basic/ or
    http://thedataweb.rm.census.gov/pub/cps/basic/

    Parameters
    ----------
    month: str
    datapath: Path

    Returns
    -------
    None: IO ()

    """
    # special case January 1994 thru June 1995
    if DATAWEB_MATCHER.match(month):
        myname = rename_cps_monthly(month)
        dt_start = datetime.datetime.strptime(month[:5], '%b%y')
        dt_end = datetime.datetime.strptime(month[6:11], '%b%y')
        base = "http://thedataweb.rm.census.gov/pub/cps/basic/{}-{}/".format(
            dt_start.strftime("%Y%m"), dt_end.strftime("%Y%m"))
    else:
        base = "http://www.nber.org/cps-basic/"
        myname = rename_cps_monthly(month)

    if myname is None:
        return None

    if not isinstance(datapath, Path):
        datapath = Path(datapath)

    if not datapath.exists():
        datapath.mkdir(parents=True)

    logger.info("Fetching monthly file from {}".format(base + month))
    r = requests.get(base + month, stream=True)
    outpath = datapath / myname
    logger.info("Writing {}".format(outpath))
    with outpath.open('wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            f.write(chunk)
            f.flush()


#-----------------------------------------------------------------------------

def _matcher(link, regex):
    try:
        _, fldr, file_ = link[2].split('/')
        if regex.match(file_):
            return file_
    except ValueError:
        pass


def check_cached(directory, kind='data'):
    """
    Check for existing files on disk in 'directory'.

    Parameters
    ----------
    directory: str or Path
    kind: {'data', 'dictionary'}

    Returns
    -------
    cached: [str]

    """
    if kind == 'data':
        suffixes = ('.Z', '.zip')
    elif kind == 'dictionary':
        suffixes = ('.ddf', '.asc', '.txt')
    else:
        raise ValueError("kind must be 'data' or 'dictionary'.")
    if not isinstance(directory, Path):
        directory = Path(directory)
    try:
        cached = [x.name for x in directory.iterdir()
                  if x.suffix in suffixes]
    except OSError:
        # no cache files
        cached = []

    return cached
