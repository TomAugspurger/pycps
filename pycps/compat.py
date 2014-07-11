"""
Aim for 2.7+
"""
import sys

ISPY2 = sys.version_info[0] == 2

if ISPY2:
    from StringIO import StringIO
else:
    from io import StringIO
