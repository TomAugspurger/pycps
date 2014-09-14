# -*- coding: utf-8 -*-
import logging
import logging.config

__version__ = '0.2.0'

from pycps import api
from pycps import compat
from pycps import downloaders
from pycps import parsers

logger = logging.getLogger(__name__)
logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'standard': {
                    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                },
            },
        'handlers': {
            'default': {
                'level': 'INFO',
                'class': 'logging.StreamHandler'
                },
            },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': 'INFO',
                'propagate': True
                }
            },
    })
