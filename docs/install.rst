Setup
=====

To install the latest version of pycps, you can clone the `Github repository`_.

.. _Github repository: https://github.com/TomAugspurger/pycps

.. code-block:: rst

    git clone https://github.com/TomAugspurger/pycps
    cd pycps

Install the dependencies with pip

.. code-block:: rst

    pip install .
    pip install -r requirements.txt
    pip install tables
    pytohn setup.py install

Note: pycps uses HDF5_ for file storage,
so you'll need to have that installed before install the python bindings
from pytables.

.. _HDF5: http://www.hdfgroup.org

At the time of this writing, a bug in the current version
of pytables, prevents it from being installed with Cython 0.21. You can
install the development version with

.. code-block:: rst

    pip install git+https://github.com/PyTables/PyTables


Then setup your configuration. You'll want to look at ``pycps/settings.json``
to configure the paths for data storage and a few other things.
