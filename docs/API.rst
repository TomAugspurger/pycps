API
===

This module is best used via the command line.
You will mostly work through the ``pycps/api.py`` entry point,
which takes a number of command line arguments.

.. code-block:: rst

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

``api.py`` takes a number of arguments.


- settings (-s): The path to a JSON settings file

- json-path: The path the the JSON schema

- download-dictionaries (-d)


In standard fashion, these flags can be combined to do multiple things.
For example to download and parse data dictionaries only, overwritting
any you may have downloaded previously, you would use

.. code-block:: rst

    python pycps.py -dpo
