Downloading
===========

Let's get some data::

    from pycps import api

    api.download()

This starts by reading in your settings.
Based on the ``date_start`` and ``date_end`` values,
it figures out the necessary data dictionaries.
Those and the actual monthly data files are saved to their
paths defined in ``settings.json``.
The files are downloaded from http://www.nber.org/data/cps_basic.html.

At this point you have all your data dictionaries and monthly files
downloaded in their raw form.
The next step is parsing.