# -*- coding: utf-8 -*-
import os
import json
import logging
import logging.config

__version__ = '0.2.0'

import pycps
from pycps import api
from pycps import compat
from pycps import downloaders
from pycps import parsers


logger = logging.getLogger(__name__)

def setup_logging(path_=None):
    if path_ is None:
        path_ = os.path.dirname(pycps.__file__) + '/logging.json'
    with open (path_) as f:
        config = json.load(f)
    logging.config.dictConfig(config)

setup_logging()

