"""
Aim for 2.7+
"""
import sys

ISPY2 = sys.version_info[0] == 2

if ISPY2:
    from StringIO import StringIO
    from functools import ifilter, map
    filter = ifilter
    map = map
else:
    from io import StringIO
