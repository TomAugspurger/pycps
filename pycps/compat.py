"""
Aim for 2.7+
"""
import sys

ISPY2 = sys.version_info[0] == 2

if ISPY2:
    from StringIO import StringIO
    from itertools import ifilter, imap
    filter = ifilter
    map = imap
else:
    from io import StringIO

