"""
Read all the things.
"""
from contextlib import contextmanager, wraps
from itertools import dropwhile
import json

from pycps.compat import StringIO


# @wraps
@contextmanager
def _open_file_or_stringio(maybe_file):
    """
    Align API
    """
    if isinstance(maybe_file, StringIO):
        yield maybe_file
    else:
        yield open(maybe_file)


def _skip_module_docstring(f):
    next(f)  # first """
    f = dropwhile(lambda x: not x.startswith('"""'), f)
    next(f)  # second """
    return f


def read_settings(filepath):
    with _open_file_or_stringio(filepath) as f:
        f = _skip_module_docstring(f)
        f = ''.join(list(f))  # TODO: could be lazier
        f = json.loads(f)
    return f
