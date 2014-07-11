"""
Read all the things.
"""
from contextlib import contextmanager
from itertools import dropwhile
import json
import re

from pycps.compat import StringIO


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

        if regex is None:
            self.regex = self.make_regex(style=style)

        styles = {"jan1989": 0,
                  "jan1992": 0,
                  "jan1994": 2,
                  "apr1994": 2,
                  "jun1995": 2,
                  "sep1995": 2,
                  "jan1998": 1,
                  "jan2003": 2,
                  "may2004": 2,
                  "aug2005": 2,
                  "jan2007": 2,
                  "jan2009": 2,
                  "jan2010": 2,
                  "may2012": 2,
                  "jan2013": 2
              }

        self.store_name = infile.stem
        self.style = styles[self.store_name]

        self.dataframes = []
        self.next_line = None
        self.holder = []
        self.pos_id = 0
        self.pos_len = 1
        self.pos_start = 2
        self.pos_end = 3

    def run(self):
        with open(self.infile, 'r') as f:
            for line in f:
                # if self.previous_line is None:
                    # self.ids = []
                    # self.lenghts = []
                    # self.starts = []
                    # self.ends = []
                self.analyze(line.rstrip(), f)

            # Finally
            to_be_df = self.holder
            df = pd.DataFrame(to_be_df, columns=['id', 'length', 'start',
                                                 'end'])
            # Some years need to grab the very last one
            # If there's only one, it's been picked up.
            if len(self.dataframes) > 0:
                df = pd.concat([self.common, df])
            self.dataframes.append(df)

    def analyze(self, line, f):
        maybe_groups = self.regex.match(line)

        if maybe_groups:
            formatted = self.formatter(maybe_groups)
            # Return to main for loop under run
            if len(self.holder) == 0:
                self.holder.append(formatted)
            # Fake break
            elif formatted[self.pos_start] > self.holder[-1][self.pos_end] + 1:
                bad = ('/Volumes/HDD/Users/tom/DataStorage/CPS/dds/cpsbmay04.ddf',
                       '/Volumes/HDD/Users/tom/DataStorage/CPS/dds/cpsbaug05.ddf')
                if formatted == ('FILLER', 2, 411, 412) and self.infile in bad:
                    # cpsbmay04 dd is wrong. Should be 410-411 not 411-412
                    formatted = (formatted[0], formatted[1], 410, 411)
                    self.holder.append(formatted)
                    print("Fixed {}".format(line))
                else:
                    self.handle_padding(formatted, f)
            # Real break
            elif formatted[self.pos_start] < self.holder[-1][self.pos_end]:
                self.handle_real_break(formatted)
            else:
                self.holder.append(formatted)


    def handle_padding(self, formatted, f):
        """
        CPS left out some padding characters.

        Unpure.  Need to know next line to determine pad len.
        """
        # Can't use f.readline() cause final line would be infinite loop.

        # dr = it.dropwhile(lambda x: not self.regex.match(x), f)
        # next_line = next(dr)
        # maybe_groups = self.regex.match(next_line)

        # next_formatted = self.formatter(maybe_groups)
        last_formatted = self.holder[-1]
        pad_len = formatted[self.pos_start] - last_formatted[self.pos_end] - 1
        pad_str = last_formatted[self.pos_end] + 1
        pad_end = pad_len + pad_str - 1
        pad = ('PADDING', pad_len, pad_str, pad_end)

        self.holder.append(pad)
        self.holder.append(formatted)  # goto next line

    def handle_real_break(self, formatted):
        """
        CPS reuses some codes and then starts over.
        """
        to_be_df = self.holder
        df = pd.DataFrame(to_be_df, columns=['id', 'length', 'start',
                                             'end'])

        if len(self.dataframes) == 0:
            self.dataframes.append(df)
            common_index_pt = df[df['start'] == formatted[self.pos_end]].index[0] - 1
            self.common = df.ix[:common_index_pt]
        else:
            df = pd.concat([self.common, df], ignore_index=True)
            self.dataframes.append(df)

        self.holder = [formatted]  # next line

    def make_regex(self, style=None):
        default = re.compile(r'[\x0c]{0,1}(\w+)[\s\t]*(\d{1,2})[\s\t]*(.*?)[\s\t]*\(*(\d+)\s*-\s*(\d+)\)*$')
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
        if self.style == 'jan1998':
            id_, length, start = match.groups()
            length = int(length)
            start = int(start)
            end = start + length - 1
        else:
            try:
                id_, length, start, end = match.groups()
                if self.style is None:
                   id_ = self.handle_replacers(id_)
            except ValueError:
                id_, length, description, start, end = match.groups()
            length = int(length)
            start = int(start)
            end = int(end)
        return (id_, length, start, end)

    def len_checker(self):
        # Will fail cause CPS screwed up w/ padding.
        for df in self.dataframes:
            assert (df.end - df.start == df.length - 1).all()

    def con_checker(self):
        for df in self.dataframes:
            if not (df.end.shift() - df.start + 1 == 0)[1:].all():
                badlines = df[~(df.end.shift() - df.start + 1 == 0)][1:].index
                for line in badlines:
                    print(df.ix[line - 1:line + 1])
                raise ValueError('Continuity is Broken Around Here')

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
        replacers = {'$': 'd', '%': 'p', '-': 'h'}
        for bad_char, good_char in replacers.iteritems():
            id_ = id_.replace(bad_char, good_char)
        return id_
