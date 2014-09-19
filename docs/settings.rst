Settings
========

In `pycps/settings.json` you can specify values specific to your project.
The file is basically a JSON file, with some comments.

Paths
-----

You should define several paths:

    * data_path: The root directory for the downloaded data
    * dd_path: the directory that will hold the downloaded data dictionaries
    * dd_store: path to an HDFStore for the data dictionary
    * monthly_path: subdirectory for monthly files
    * monthly_store: path to an HDFStore for the monthly files
    * merged_store: path to the final HDFStore, containing the merged files.

Paths can extend other paths by refering to the parent in curly braces.
In this example, ``dd_path`` extends ``data_path``:

    {
        "data_path": "data/",
        "dd_path": "{data_path}/data_dictionaries/",
    }

I haven't implemented escaping yet, which means you can't use curly braces
in your path names; they can only refer to parents. File an issue if this
is a problem for you.

Dates
-----

You should also define the months of data you need for your project.

    * date_start: YYYY-MM string with the first month to download
    * date_end: YYYY-MM string with the last month to download

All months between ``date_start`` and ``date_end``, inclusive,
will be downloaded and parsed.

Example
-------

Here's an example settings file:

.. code-block:: rst

    {
        "data_path": "data",
        "dd_path": "{data_path}/data_dictionaries/",
        "dd_store": "{dd_path}/dds.hdf",
        "monthly_path": "{data_path}/monthly/",
        "monthly_store": "{monthly_path}/monthly.hdf",
        "merged_store": "{monthly_path}/merged.hdf",
        "date_start": "1995-09",
        "date_end": "2014-05",
        "info_path": "pycps/info.json"
    }

