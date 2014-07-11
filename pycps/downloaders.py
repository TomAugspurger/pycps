"""
Downloaders for

- monthly data files
- data dictionaries

from http://www.nber.org/data/cps_basic.html

TODO: March Supplements
"""
from pycps.readers import read_settings


if __name__ == '__main__':
    settings = read_settings('./settings.json')
