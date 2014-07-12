"""
Downloaders for

- monthly data files
- data dictionaries

from http://www.nber.org/data/cps_basic.html

TODO: March Supplements
"""
import re
import datetime
from itertools import chain
from functools import partial

import arrow
from lxml import html
from pandas.core.common import is_list_like

from pycps.parsers import read_settings


def all_monthly_files(site='http://www.nber.org/data/cps_basic.html'):
    """
    Find all matching monthly data files and data dictionaries
    from the NBER's CPS site.
    """
    regex = re.compile(r'cpsb\d{4}.Z|\w{3}\d{2}pub.zip|\.[ddf,asc]$')
    root = html.parse(site).getroot()
    partial_matcher = partial(_matcher, regex=regex)

    for _, _, fname_, _ in filter(partial_matcher, root.iterlinks()):
        fname = fname_.split('/')[-1]
        yield fname


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
        formatted like cpsmYYYY-MM
    """
    fname, ext = cpsname.split('.')
    if ext == 'Z':
        dt = datetime.datetime.strptime(fname, 'cpsb%y%m')
    else:
        dt = datetime.datetime.strptime(fname, '%b%ypub')
    return dt.strftime('cpsm%Y-%m') + '.' + ext


def filter_monthly_files(files, months=None, kind='both'):
    """
    Filter the generator from all_monthly_files down to
    what you want, probably from the settings file.

    Parameters
    ----------
    filenames: str
        in rename_cps_monthly style
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

    >>>filter_monthly_files(months=['1997-01', '1998-01', '1999-01'])

    >>>filter_monthly_files(months=[['1994-01', '1994-06'],
                                    ['1995-01', '1995-06']])
    """
    files = list(files)  # have to thunk
    file_dates = [arrow.get(x.split('.')[0], format='cpsm%Y-%m')
                  for x in files]

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

    matched = filter(lambda x: x[1] in months, zip(files, file_dates))
    for f, date in matched:
        yield f


def monthly_data(month,
                 site='http://www.nber.org/data/cps_basic.html',
                 **kwargs):
    root = html.parse(site).getroot()


def data_dictionary():
    pass


def march_supplement():
    pass


#-----------------------------------------------------------------------------

def _matcher(link, regex):
    try:
        _, fldr, file_ = link[2].split('/')
        if regex.match(file_):
            return file_
    except ValueError:
        pass

def _exists(path_name):
    no_ext = path_name.split('.')[0]
    if exists(path_name) or exists(no_ext + '.gz') or exists(no_ext + '.gz'):
        return True



if __name__ == '__main__':
    settings = read_settings('./settings.json')
