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

Paths can extend other paths by refering to the parent in curly braces.
In this example, ``dd_path`` extends ``data_path``:

    {
        "data_path": "data/",
        "dd_path": "{data_path}/data_dictionaries/",
    }

Dates
-----

You should also define the months of data you need for your project.

    * date_start: YYYY-MM string with the first month to download
    * date_end: YYYY-MM string with the last month to download

All months between ``date_start`` and ``date_end``, inclusive, will be downloaded.

Example
-------

Here's an example settings file:

    {
        "data_path": "data/",
        "dd_path": "{data_path}/data_dictionaries/",
        "dd_store": "{dd_path}/dds.hdf",
        "monthly_path": "{data_path}/monthly/",
        "monthly_store": "{monthly_path}/monthly.hdf",
        "date_start": "1995-09",
        "date_end": "2014-05",
        "raise_warnings": true
    }

