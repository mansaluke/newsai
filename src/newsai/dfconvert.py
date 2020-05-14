__all__ = ["run_from_ipython", "Dstore", "mkdir_p", "create_path"]

import sys
from os import getcwd, makedirs
from os.path import dirname, join, exists, isdir
import json
import pickle
from datetime import datetime
import pandas as pd
try:
    import dask.dataframe as dd
except ImportError:
    pass
from newsai.utils.ipython import run_from_ipython
from newsai.utils.nlogger import Log

log = Log(__name__)

if run_from_ipython:
    log.info('Ipython active')
    standardpath = join(dirname(getcwd()), 'data')
else:
    standardpath = join(getcwd(), r'data')


storagetypes = ["pickle", "json", "csv", "parquet", "feather", "h5"]
storagetypes_dict = {"pickle": "", "json": "json", "csv": "csv",
                     "parquet": "parquet", "feather": "feather", "HDF5": "h5"}


class Dstore(object):
    """
    loads/stores specified files to and from pickle, json, csv or parquet formats 
    e.g.1 Dstore('test.json').load_df()
    e.g.2 Dstore('test.json').store_df(df)
    *args = path parts. if no path entered the default path is used
    parquet format: "test.parquet.gzip" 
    to store and load in current directory simply: Dstore('test.csv', 'fanalysis').load_df()
    """

    def __init__(self, filename, *args):
        if args:
            self.filename = join(*args, filename)
        else:
            self.filename = join(standardpath, filename)
        try:
            self.filetype = self.filename.split(".", -1)[1]
        except IndexError:
            self.filetype = "pickle"

    def load_df(self, **kwargs):
        filename = self.filename
        filetype = self.filetype
        if filetype in storagetypes:
            fn = "self." + filetype + "_load(filename, **kwargs)"
            log.info("Loading " + filetype + ": " + filename)

            if not exists(filename):
                raise FileNotFoundError

            dataframe = eval(fn)

            if dataframe is not None:
                log.info("dataframe loaded successfully")

            return dataframe
        else:
            raise ValueError(
                "Cannot load that file type. Please check file name and try again.")

    def store_df(self, dataframe, **kwargs):
        """
        when replace_existing set to true any existing files with the same name will be automatically replaced
        """
        filename = self.filename
        filetype = self.filetype

        if filetype in storagetypes:
            fn = "self.dfto" + filetype + "(dataframe, filename, **kwargs)"
            log.info("Storing " + filetype + ": " + filename)

            if exists(filename):
                raise FileExistsError

            create_path(dirname(filename))
            exec(fn)

            log.info("DataFrame stored successfully")
            return filename

        else:
            raise ValueError(
                "Cannot store that file type. Please check file name and try again.")

    def append_df(self, dataframe, replace_existing=True, **kwargs):
        """
        when replace_existing set to true any existing files with the same name will be automatically replaced
        """

        filename = self.filename
        filetype = self.filetype

        if filetype in storagetypes:

            if not exists(filename):
                raise FileNotFoundError

            fn = "self." + filetype + \
                "_dfappend(dataframe, filename, **kwargs)"
            log.info("Appending " + filetype + ": " + filename)

            exec(fn)
            log.info("DataFrame appended successfully")
            return filename

        else:
            raise ValueError(
                "Cannot store that file type. Please check file name and try again.")

    ###########################################################
    # LOAD FUNCTIONS
    ###########################################################

    def pickle_load(self, pfile):
        with open(pfile, 'rb') as pObject:
            df = pickle.load(pObject)
        df = pd.DataFrame(df)
        return df

    def json_load(self, jfile, orient='index'):
        df = pd.read_json(jfile, orient=orient)
        return df

    def csv_load(self, csvfile, encoding=None):
        encoding_types = ["utf-8", "ISO-8859-1", "cp1252"]

        def _load(csvfile):
            for e in encoding_types:
                try:
                    df = pd.read_csv(csvfile, encoding=e)
                    break
                except UnicodeError:
                    pass
            return df
        if encoding is not None:
            return pd.read_csv(csvfile, encoding=encoding)
        else:
            return _load(csvfile)

    def h5_load(self, hdffile):
        try:
            df = pd.read_hdf(hdffile, 'df')
        except Exception as e:
            log.error(f'Could not load data using pandas read_hdf. Error {e}')
            raise
        return df

    ###########################################################
    # STORAGE FUNCTIONS
    ###########################################################

    def dftopickle(self, dataframe, filename):
        with open(filename, 'wb') as fObject:
            pickle.dump(dataframe, fObject, pickle.HIGHEST_PROTOCOL)

    def dftojson(self, dataframe, filename):
        dataframe.to_json(filename, orient='records')

    def dftocsv(self, dataframe, filename, encoding='utf-8'):
        dataframe.to_csv(filename, sep=',', encoding=encoding, index=False)

    def dftoh5(self, dataframe, filename):
        # e.g. data.h5
        dataframe.to_hdf(filename, key='df', mode='w',
                         format='t',  data_columns=True)

    ###########################################################
    # APPEND FUNCTIONS
    ###########################################################

    def pickle_dfappend(self, dataframe, filename):
        with open(filename, 'ab') as fObject:
            pickle.dump(dataframe, fObject, pickle.HIGHEST_PROTOCOL)

    def json_dfappend(self, dataframe, filename):
        raise TypeError(
            "Cannot append to json. Please use a different file type")

    def csv_dfappend(self, dataframe, filename, encoding="utf-8"):
        with open(filename, 'a') as f:
            dataframe.to_csv(f, header=False, sep=',',
                             encoding=encoding, index=False)

    def h5_dfappend(self, dataframe, filename):

        # store = pd.HDFStore(filename)
        # store.append('dataframe', dataframe, format='t',  data_columns=True)
        # store.close()
        # TODO update key
        dataframe.to_hdf(filename, key='df1', append=True,
                         mode='w', format='t',  data_columns=True)


def mkdir_p(path):
    import errno
    try:
        path = path
        makedirs(path)
        return path
    except OSError as exc:
        if exc.errno == errno.EEXIST and isdir(path):
            return path
        else:
            raise


def create_path(path):
    if not exists(path):
        a = True
        while a is True:
            user_input = input(
                path +
                ' does not exist. Create folder? (y/n): '
            ).casefold()
            if user_input == 'y':
                path = mkdir_p(path)
            elif user_input == 'n':
                pass
            a = False
    else:
        pass
    return path
