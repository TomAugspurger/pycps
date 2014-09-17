API
===

This module is best used via the command line.
You will mostly work through the ``pycps/api.py`` entry point,
which takes a number of command line arguments.

.. ipython::

    In [1]: !python ../pycps/api.py -h

``api.py`` takes a number of arguments.


- settings (-s): The path to a JSON settings file

- json-path: The path the the JSON schema

- download-dictionaries (-d)


In standard fashion, these flags can be combined to do multiple things.
For example to download and parse data dictionaries only, overwritting
any you may have downloaded previously, you would use

.. code-block:: rst

    python pycps.py -dpo
