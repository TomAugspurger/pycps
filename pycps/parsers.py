"""
Read all the things.
"""
import re
import json
from itertools import dropwhile
from contextlib import contextmanager

import pandas as pd

from pycps.compat import StringIO


#-----------------------------------------------------------------------------
# Settings

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
    """
    Mostly internal method to read a (technically invalid) JSON
    file that has comments mixed in.

    Parameters
    ----------
    filepath: str or StringIO
        should be JSON like file

    Retruns
    -------
    settings: dict

    """
    with _open_file_or_stringio(filepath) as f:
        f = _skip_module_docstring(f)
        f = ''.join(list(f))  # TODO: could be lazier
        # TODO: skiplines starting with comment char
        f = json.loads(f)
        f = {k: _sub_path(v, f) for k, v in f.items()}  # TODO: py2
    return f

def _sub_path(v, f):
    pat = r'\{(\w*)\}'
    m = re.match(pat, v)
    if m:
        to_sub = m.groups()[0]
        v = re.sub(pat, f[to_sub].rstrip('/\\'), v)
    return v

#-----------------------------------------------------------------------------
# Data Dictionaries

class DDParser:
    """
    Data Dictionary Parser

    Parameters
    ----------

    infile: pathlib.Path
    settings: dict

    Notes
    -----

    *file should be a Path object
    *path should be a str.
    """
    def __init__(self, infile, settings):
        self.infile = infile
        self.outpath = settings['dd_path']

        styles = {"jan1989": 0, "jan1992": 0, "jan1994": 2,
                  "apr1994": 2, "jun1995": 2, "sep1995": 2,
                  "jan1998": 1, "jan2003": 2, "may2004": 2,
                  "aug2005": 2, "jan2007": 2, "jan2009": 2,
                  "jan2010": 2, "may2012": 2, "jan2013": 2
              }

        self.store_name = infile.stem
        # default to most recent

        self.style = styles.get(self.store_name, max(styles.values()))
        self.regex = self.make_regex(style=self.style)

    @staticmethod
    def _is_consistent(formatted):
        """
        Given a list of tuples, make sure the column numbering is
        internally consistent.

        Checks that

        1. width == (end - start) + 1
        2. start_1 == end_0 + 1
        """
        g = iter(formatted)
        current = next(g)
        i = 0

        while True:  # till stopIteration
            assert current[1] == current[3] - current[2] + 1
            try:
                old, current, i = current, next(g), i + 1
                assert current[2] == old[3] + 1
            except AssertionError:
                # TODO: logging
                print("Round {}\n0: {}\n1: {}\n".format(i, old, current))
            except StopIteration:
                # last one should still check first criteria
                assert old[1] == old[3] - old[2] + 1
                raise StopIteration

    def run(self):
        # make this as streamlike as possible.
        with self.infile.open() as f:
            # get all header lines
            lines = (self.regex.match(line) for line in f)
            lines = filter(None, lines)

            # regularize format; intentional thunk
            formatted = [self.formatter(x) for x in lines]

            # ensure consistency across lines
            try:
                self._is_consistent(formatted)
                import ipdb; ipdb.set_trace()
            except StopIteration:  # good till thru the end
                df = pd.DataFrame(formatted,
                                  columns=['id', 'length', 'start', 'end'])
            except WidthError:
                raise ValueError
                # recover
            except ContinuityError:
                raise ValueError
                # recover

            return df

    @staticmethod
    def make_regex(style=None):
        """
        Regex factory to match. Each dd has a style (just an id for that regex).
        Some dds share styles.
        The default style is the most recent.
        """
        # As new styles are added the current default should be moved into the dict.
        # TODO: this smells terrible
        default = re.compile(r'[\x0c]{0,1}(\w+)[\s\t]*(\d{1,2})[\s\t]*(.*?)[\s\t]*\(*(\d+)\s*-\s*(\d+)\)*\s*$')
        d = {0: re.compile(r'(\w{1,2}[\$\-%]\w*|PADDING)\s*CHARACTER\*(\d{3})\s*\.{0,1}\s*\((\d*):(\d*)\).*'),
             1: re.compile(r'D (\w+) \s* (\d{1,2}) \s* (\d*)'),
             2: default}
        return d.get(style, default)

    def formatter(self, match):
        """
        Conditional on a match, format them into a nice tuple of
            id, length, start, end

        match is a regex object.
        """
        # TODO: namedtuple return values
        if self.style == 1:
            id_, length, start = match.groups()
            length = int(length)
            start = int(start)
            end = start + length - 1
        else:
            try:
                id_, length, start, end = match.groups()
                id_ = self.handle_replacers(id_)
            except ValueError:
                id_, length, description, start, end = match.groups()
            length = int(length)
            start = int(start)
            end = int(end)
        return (id_, length, start, end)

    def writer(self):
        """
        Once you have all the dataframes, write them to that outfile,
        an HDFStore.
        """
        store = pd.HDFStore(self.outpath)
        df = self.dataframes[0]  # Only should happen in the old ones.
        store.append(self.store_name, df, append=False)
        store.close()

    def handle_replacers(self, id_):
        """
        Prefer ids to be valid python names.
        """
        replacers = {'$': 'd', '%': 'p', '-': 'h'}
        for bad_char, good_char in replacers.items():
            id_ = id_.replace(bad_char, good_char)
        return id_


class WidthError(ValueError):
    """
    The stated width does not equal the computed width
    """
    def __init__(self):
        """
        """
        pass


class ContinuityError(ValueError):
    """
    Two subsequent lines don't align cleanly.
    """
    def __init__(self):
        """
        """
        pass

#-----------------------------------------------------------------------------
# Monthly Data Files