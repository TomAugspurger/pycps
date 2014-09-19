API
===

This module is best used via the command line.
You will mostly work through the ``pycps/api.py`` entry point,
which takes a number of command line arguments.

.. code-block:: rst

    $ python pycps/api.py -h
    usage: api.py [-h] [-s] [-i] [-d] [-y] [-p] [-x] [-m] [-o]

    Invoke pycps

    optional arguments:
      -h, --help            show this help message and exit
      -s , --settings       path to JSON settings file (default:
                            pycps/settings.json)
      -i , --info           Path to info.json (default: pycps/info.json)
      -d, --download-dictionaries
                            Download data dictionaries (default: False)
      -y, --download-monthly
                            Download monthly data files (default: False)
      -p, --parse-dictionaries
                            Parse data dictionaries (default: False)
      -x, --parse-monthly   Parse monthly data files (default: False)
      -m, --merge           Merge monthly files by household (default: False)
      -o, --overwrite       Overwrite existing cache (default: False)


In standard fashion, these flags can be combined to do multiple things.
If you just want to get going, you'll probably want to download all
the data dictionaries and monthly files, and merge them together by houehold.
To do this, use

.. code-block:: rst

    python pycps/api.py -dypxm

Some settings (such as paths where you want the data to be stored) can
be specified in a JSON settings file. ``PyCPS`` comes with one, which you can
edit directly, or copy it, make your adjustments, and point to it when running:

.. code-block:: rst

    python pycps/api.py --settings='pycps/mysettings.json'

The next section describes the settings file.
