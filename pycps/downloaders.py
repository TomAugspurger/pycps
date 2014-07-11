"""
Downloaders for

- monthly data files
- data dictionaries

from http://www.nber.org/data/cps_basic.html

TODO: March Supplements
"""
from lxml.html import parse
from functools import partial

from pycps.parsers import read_settings


def all_monthly_data():
    regex = re.compile(r'cpsb\d{4}.Z|\w{3}\d{2}pub.zip|\.[ddf,asc]$')
    site = 'http://www.nber.org/data/cps_basic.html'
    root = html.parse(site).getroot()
    partial_matcher = partial(_matcher, regex=regex)

    for link in ifilter(partial_matcher, root.iterlinks()):
        _, _, _fname, _ = link
        fname = _fname.split('/')[-1]
        existing = _exists(os.path.join(out_dir, fname))
        if not existing:
            downloader(fname, out_dir)
            print('Added {}'.format(fname))

    renamer(out_dir)


def monthly_data(month, **kwargs):
    site = 'http://www.nber.org/data/cps_basic.html'
    root = html.parse(site).getroot()


def data_dictionary():
    pass


def march_supplement():
    pass


#-

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
