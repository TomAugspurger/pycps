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

And some other values

    * date_start: YYYY-MM string with the first month to download
    * date_end: YYYY-MM string with the last month to download
